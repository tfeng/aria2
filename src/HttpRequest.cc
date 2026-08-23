/* <!-- copyright */
/*
 * aria2 - The high speed download utility
 *
 * Copyright (C) 2006 Tatsuhiro Tsujikawa
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
 *
 * In addition, as a special exception, the copyright holders give
 * permission to link the code of portions of this program with the
 * OpenSSL library under certain conditions as described in each
 * individual source file, and distribute linked combinations
 * including the two.
 * You must obey the GNU General Public License in all respects
 * for all of the code used other than OpenSSL.  If you modify
 * file(s) with this exception, you may extend this exception to your
 * version of the file(s), but you are not obligated to do so.  If you
 * do not wish to do so, delete this exception statement from your
 * version.  If you delete this exception statement from all source
 * files in the program, then also delete it here.
 */
/* copyright --> */
#include "HttpRequest.h"

#include <cassert>
#include <numeric>
#include <vector>

#include "Segment.h"
#include "Range.h"
#include "CookieStorage.h"
#include "Option.h"
#include "util.h"
#include "base64.h"
#include "prefs.h"
#include "AuthConfigFactory.h"
#include "AuthConfig.h"
#include "a2functional.h"
#include "TimeA2.h"
#include "array_fun.h"
#include "Request.h"
#include "DownloadHandlerConstants.h"
#include "MessageDigest.h"
#include "LogFactory.h"
#include "Logger.h"
#include "fmt.h"

namespace aria2 {

namespace {
// FXPlayer extension (2026-09-10): the user asked for WARN-level instrumentation of exactly how
// XNXX/its CDN is accessed, so a future account block can be diagnosed from ~/.aria2/aria2.log alone —
// the existing per-request header dump (HttpConnection.cc's MSG_RECEIVE_RESPONSE) only logs at INFO,
// which most real deployments (including this one) don't keep enabled, and it isn't scoped to any one
// site anyway. A plain substring match, not an exact host allowlist: XNXX serves from several
// same-family hostnames (www.xnxx.com, www.xnxx.gold, and per-video CDN edges like
// hls-gold-cdn77.xnxx-cdn.com, thumb-cdn77.xnxx-cdn.com, mp4-gcore.xnxx-cdn.com) and new CDN edge names
// can appear without a code change — matching "xnxx" anywhere in the host covers the whole family
// without needing to enumerate it, at the cost of (harmlessly) also matching if some unrelated host
// ever contained that substring.
bool isXnxxDiagHost(const std::string& host)
{
  return util::toLower(host).find("xnxx") != std::string::npos;
}
} // namespace

const std::string HttpRequest::USER_AGENT("aria2");

HttpRequest::HttpRequest()
    : cookieStorage_(nullptr),
      fxCookiesProvided_(false),
      authConfigFactory_(nullptr),
      option_(nullptr),
      endOffsetOverride_(0),
      userAgent_(USER_AGENT),
      contentEncodingEnabled_(true),
      acceptMetalink_(false),
      noCache_(true),
      acceptGzip_(false),
      noWantDigest_(false)
{
}

HttpRequest::~HttpRequest() = default;

void HttpRequest::setSegment(std::shared_ptr<Segment> segment)
{
  segment_ = std::move(segment);
}

void HttpRequest::setRequest(std::shared_ptr<Request> request)
{
  request_ = std::move(request);
}

int64_t HttpRequest::getStartByte() const
{
  if (!segment_) {
    return 0;
  }

  return fileEntry_->gtoloff(segment_->getPositionToWrite());
}

int64_t HttpRequest::getEndByte() const
{
  if (!segment_ || !request_) {
    return 0;
  }
  if (request_->isPipeliningEnabled()) {
    auto endByte = fileEntry_->gtoloff(segment_->getPosition() +
                                       segment_->getLength() - 1);
    return std::min(endByte, fileEntry_->getLength() - 1);
  }
  if (endOffsetOverride_ > 0) {
    return endOffsetOverride_ - 1;
  }
  return 0;
}

Range HttpRequest::getRange() const
{
  // content-length is always 0
  if (!segment_) {
    return Range();
  }
  return Range(getStartByte(), getEndByte(), fileEntry_->getLength());
}

bool HttpRequest::isRangeSatisfied(const Range& range) const
{
  if (!segment_) {
    return true;
  }
  return getStartByte() == range.startByte &&
         (getEndByte() == 0 || getEndByte() == range.endByte) &&
         (fileEntry_->getLength() == 0 ||
          fileEntry_->getLength() == range.entityLength);
}

namespace {
std::string getHostText(const std::string& host, uint16_t port)
{
  auto hosttext = host;
  if (!(port == 80 || port == 443)) {
    hosttext += ':';
    hosttext += util::uitos(port);
  }
  return hosttext;
}
} // namespace

std::string HttpRequest::createRequest()
{
  authConfig_ = authConfigFactory_->createAuthConfig(request_, option_);
  auto requestLine = request_->getMethod();
  requestLine += ' ';
  if (proxyRequest_) {
    if (getProtocol() == "ftp" && request_->getUsername().empty() &&
        authConfig_) {
      // Insert user into URI, like ftp://USER@host/
      auto uri = getCurrentURI();
      assert(uri.size() >= 6);
      uri.insert(6, util::percentEncode(authConfig_->getUser()) + '@');
      requestLine += uri;
    }
    else {
      requestLine += getCurrentURI();
    }
  }
  else {
    requestLine += getDir();
    requestLine += getFile();
    requestLine += getQuery();
  }
  requestLine += " HTTP/1.1\r\n";

  std::vector<std::pair<std::string, std::string>> builtinHds;
  builtinHds.reserve(20);
  builtinHds.emplace_back("User-Agent:", userAgent_);
  std::string acceptTypes = "*/*";
  if (acceptMetalink_) {
    // The mime types of Metalink are used for "transparent metalink".
    const auto metalinkTypes = getMetalinkContentTypes();
    for (size_t i = 0; metalinkTypes[i]; ++i) {
      acceptTypes += ',';
      acceptTypes += metalinkTypes[i];
    }
  }
  builtinHds.emplace_back("Accept:", acceptTypes);
  if (contentEncodingEnabled_) {
    std::string acceptableEncodings;
#ifdef HAVE_ZLIB
    if (acceptGzip_) {
      acceptableEncodings += "deflate, gzip";
    }
#endif // HAVE_ZLIB
    if (!acceptableEncodings.empty()) {
      builtinHds.emplace_back("Accept-Encoding:", acceptableEncodings);
    }
  }
  builtinHds.emplace_back("Host:", getHostText(getURIHost(), getPort()));
  if (noCache_) {
    builtinHds.emplace_back("Pragma:", "no-cache");
    builtinHds.emplace_back("Cache-Control:", "no-cache");
  }
  if (!request_->isKeepAliveEnabled() && !request_->isPipeliningEnabled()) {
    builtinHds.emplace_back("Connection:", "close");
  }
  if (segment_ && segment_->getLength() > 0 &&
      (request_->isPipeliningEnabled() || getStartByte() > 0 ||
       getEndByte() > 0)) {
    std::string rangeHeader = "bytes=";
    rangeHeader += util::uitos(getStartByte());
    rangeHeader += '-';

    if (request_->isPipeliningEnabled() || getEndByte() > 0) {
      // FTP via http proxy does not support endbytes, but in that
      // case, request_->isPipeliningEnabled() is false and
      // getEndByte() is 0.
      rangeHeader += util::itos(getEndByte());
    }
    builtinHds.emplace_back("Range:", rangeHeader);
  }
  if (proxyRequest_) {
    if (request_->isKeepAliveEnabled() || request_->isPipeliningEnabled()) {
      builtinHds.emplace_back("Connection:", "Keep-Alive");
    }
  }
  if (proxyRequest_ && !proxyRequest_->getUsername().empty()) {
    builtinHds.push_back(getProxyAuthString());
  }
  if (authConfig_) {
    auto authText = authConfig_->getAuthText();
    std::string val = "Basic ";
    val += base64::encode(std::begin(authText), std::end(authText));
    builtinHds.emplace_back("Authorization:", val);
  }
  if (!request_->getReferer().empty()) {
    builtinHds.emplace_back("Referer:", request_->getReferer());
  }
  // FXPlayer extension: when the RPC caller supplied a per-domain cookie map at all (even an
  // empty one — see fxCookiesProvided_'s own doc comment), it is the SOLE authority for the
  // Cookie header on every request this HttpRequest represents, keyed by THIS request's own
  // current host (getHost() — already redirect-correct, same accessor cookieStorage_'s lookup
  // below uses). A host with no entry in the map gets no Cookie header at all; cookieStorage_
  // (this daemon's own long-lived cookie jar, shared and accumulated across every download it has
  // ever run) is never consulted as a fallback in that case. Only when the caller never used this
  // feature (fxCookiesProvided_ == false) does the pre-existing cookieStorage_ behavior apply,
  // unchanged, below.
  if (fxCookiesProvided_) {
    auto it = fxCookiesByDomain_.find(getHost());
    if (it != fxCookiesByDomain_.end() && !it->second.empty()) {
      builtinHds.emplace_back("Cookie:", it->second);
    }
  }
  else if (cookieStorage_) {
    std::string cookiesValue;
    auto path = getDir();
    path += getFile();
    auto cookies = cookieStorage_->criteriaFind(
        getHost(), path, Time().getTimeFromEpoch(), getProtocol() == "https");
    for (auto c : cookies) {
      cookiesValue += c->toString();
      cookiesValue += ';';
    }
    if (!cookiesValue.empty()) {
      builtinHds.emplace_back("Cookie:", cookiesValue);
    }
  }
  if (!ifModSinceHeader_.empty()) {
    builtinHds.emplace_back("If-Modified-Since:", ifModSinceHeader_);
  }
  if (!noWantDigest_) {
    // Send Want-Digest header field with only limited hash algorithms:
    // SHA-512, SHA-256, and SHA-1.
    std::string wantDigest;
    if (MessageDigest::supports("sha-512")) {
      wantDigest += "SHA-512;q=1, ";
    }
    if (MessageDigest::supports("sha-256")) {
      wantDigest += "SHA-256;q=1, ";
    }
    if (MessageDigest::supports("sha-1")) {
      wantDigest += "SHA;q=0.1, ";
    }
    if (!wantDigest.empty()) {
      wantDigest.erase(wantDigest.size() - 2);
      builtinHds.emplace_back("Want-Digest:", wantDigest);
    }
  }
  for (const auto& builtinHd : builtinHds) {
    auto it = std::find_if(std::begin(headers_), std::end(headers_),
                           [&builtinHd](const std::string& hd) {
                             return util::istartsWith(hd, builtinHd.first);
                           });
    if (it == std::end(headers_)) {
      requestLine += builtinHd.first;
      requestLine += ' ';
      requestLine += builtinHd.second;
      requestLine += "\r\n";
    }
  }
  // append additional headers given by user.
  for (const auto& hd : headers_) {
    requestLine += hd;
    requestLine += "\r\n";
  }
  requestLine += "\r\n";
  // FXPlayer extension (2026-09-10): full outgoing-request diagnostic for XNXX/its CDN, at WARN so it
  // shows up in ~/.aria2/aria2.log under normal (non-debug) log-level settings. Dumps requestLine
  // VERBATIM (not eraseConfidentialInfo'd, unlike the generic INFO-level dump in HttpConnection.cc) —
  // this IS the user's own private local log, and the whole point of this diagnostic is to have every
  // real header/cookie value available if an account gets blocked again, not a redacted approximation.
  if (isXnxxDiagHost(getHost())) {
    std::string cookieSource;
    if (fxCookiesProvided_) {
      auto it = fxCookiesByDomain_.find(getHost());
      cookieSource = (it != fxCookiesByDomain_.end() && !it->second.empty())
                         ? "app-per-domain-map(hit)"
                         : "app-per-domain-map(no-cookie-for-this-host)";
    }
    else if (cookieStorage_) {
      // Shouldn't normally happen for an FXPlayer-submitted job (fxCookiesProvided_ is always set once
      // the app's "cookies" mergeOption is present at all, even empty) — surfaced anyway so a future
      // regression in that wiring is immediately visible here instead of silently falling back to the
      // daemon's own long-lived, cross-download cookie jar.
      cookieSource = "UNEXPECTED: legacy daemon cookieStorage_ (app per-domain map not supplied)";
    }
    else {
      cookieSource = "none";
    }
    A2_LOG_WARN(fmt(
        "[xnxx-diag] OUTGOING request host=%s port=%u proto=%s cookieSource=%s tlsImpersonation=%s "
        "keepAlive=%s pipelining=%s\n%s",
        getHost().c_str(), static_cast<unsigned>(getPort()), getProtocol().c_str(),
        cookieSource.c_str(),
#ifdef HAVE_BORINGSSL_IMPERSONATE
        "chrome116-boringssl",
#else
        "NONE-plain-tls-stack",
#endif
        request_->isKeepAliveEnabled() ? "yes" : "no",
        request_->isPipeliningEnabled() ? "yes" : "no", requestLine.c_str()));
  }
  return requestLine;
}

std::string HttpRequest::createProxyRequest() const
{
  assert(proxyRequest_);

  std::string requestLine = "CONNECT ";
  requestLine += getURIHost();
  requestLine += ':';
  requestLine += util::uitos(getPort());
  requestLine += " HTTP/1.1\r\nUser-Agent: ";
  requestLine += userAgent_;
  requestLine += "\r\nHost: ";
  requestLine += getURIHost();
  requestLine += ':';
  requestLine += util::uitos(getPort());
  requestLine += "\r\n";

  if (!proxyRequest_->getUsername().empty()) {
    auto auth = getProxyAuthString();
    requestLine += auth.first;
    requestLine += ' ';
    requestLine += auth.second;
    requestLine += "\r\n";
  }

  requestLine += "\r\n";

  return requestLine;
}

std::pair<std::string, std::string> HttpRequest::getProxyAuthString() const
{
  auto authText = proxyRequest_->getUsername();
  authText += ':';
  authText += proxyRequest_->getPassword();
  std::string val = "Basic ";
  val += base64::encode(std::begin(authText), std::end(authText));
  return std::make_pair("Proxy-Authorization:", val);
}

void HttpRequest::enableContentEncoding() { contentEncodingEnabled_ = true; }

void HttpRequest::disableContentEncoding() { contentEncodingEnabled_ = false; }

void HttpRequest::addHeader(const std::string& headersString)
{
  util::split(std::begin(headersString), std::end(headersString),
              std::back_inserter(headers_), '\n', true);
}

void HttpRequest::clearHeader() { headers_.clear(); }

void HttpRequest::setCookieStorage(CookieStorage* cookieStorage)
{
  cookieStorage_ = cookieStorage;
}

CookieStorage* HttpRequest::getCookieStorage() const { return cookieStorage_; }

void HttpRequest::setFxCookiesByDomain(std::map<std::string, std::string> cookiesByDomain)
{
  fxCookiesByDomain_ = std::move(cookiesByDomain);
  fxCookiesProvided_ = true;
}

void HttpRequest::setAuthConfigFactory(AuthConfigFactory* factory)
{
  authConfigFactory_ = factory;
}

void HttpRequest::setOption(const Option* option) { option_ = option; }

void HttpRequest::setProxyRequest(std::shared_ptr<Request> proxyRequest)
{
  proxyRequest_ = std::move(proxyRequest);
}

bool HttpRequest::isProxyRequestSet() const { return proxyRequest_.get(); }

bool HttpRequest::authenticationUsed() const { return authConfig_.get(); }

const std::unique_ptr<AuthConfig>& HttpRequest::getAuthConfig() const
{
  return authConfig_;
}

int64_t HttpRequest::getEntityLength() const
{
  assert(fileEntry_);
  return fileEntry_->getLength();
}

const std::string& HttpRequest::getHost() const { return request_->getHost(); }

uint16_t HttpRequest::getPort() const { return request_->getPort(); }

const std::string& HttpRequest::getMethod() const
{
  return request_->getMethod();
}

const std::string& HttpRequest::getProtocol() const
{
  return request_->getProtocol();
}

const std::string& HttpRequest::getCurrentURI() const
{
  return request_->getCurrentUri();
}

const std::string& HttpRequest::getDir() const { return request_->getDir(); }

const std::string& HttpRequest::getFile() const { return request_->getFile(); }

const std::string& HttpRequest::getQuery() const
{
  return request_->getQuery();
}

std::string HttpRequest::getURIHost() const { return request_->getURIHost(); }

void HttpRequest::setUserAgent(std::string userAgent)
{
  userAgent_ = std::move(userAgent);
}

void HttpRequest::setFileEntry(std::shared_ptr<FileEntry> fileEntry)
{
  fileEntry_ = std::move(fileEntry);
}

void HttpRequest::setIfModifiedSinceHeader(std::string value)
{
  ifModSinceHeader_ = std::move(value);
}

bool HttpRequest::conditionalRequest() const
{
  if (!ifModSinceHeader_.empty()) {
    return true;
  }
  for (auto& h : headers_) {
    if (util::istartsWith(h, "if-modified-since") ||
        util::istartsWith(h, "if-none-match")) {
      return true;
    }
  }
  return false;
}

} // namespace aria2
