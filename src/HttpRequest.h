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
#ifndef D_HTTP_REQUEST_H
#define D_HTTP_REQUEST_H

#include "common.h"

#include <cassert>
#include <map>
#include <string>
#include <vector>
#include <memory>

#include "FileEntry.h"

namespace aria2 {

class Request;
class Segment;
struct Range;
class Option;
class CookieStorage;
class AuthConfigFactory;
class AuthConfig;

class HttpRequest {
private:
  static const std::string USER_AGENT;

  std::shared_ptr<Request> request_;

  std::shared_ptr<FileEntry> fileEntry_;

  std::shared_ptr<Segment> segment_;

  std::shared_ptr<Request> proxyRequest_;

  std::unique_ptr<AuthConfig> authConfig_;

  CookieStorage* cookieStorage_;

  // FXPlayer extension: app-supplied domain -> cookie map (fxplayer.addMerge's "cookies" option),
  // consulted in createRequest() by this request's OWN CURRENT host (getHost() — correctly
  // reflects a redirect target, not just the originally-submitted URI) in preference to
  // cookieStorage_. fxCookiesProvided_ tracks whether the option was set AT ALL (vs. left
  // undefined) — see its own comment at the fxCookiesProvided_ declaration for why that
  // distinction is load-bearing, not cosmetic.
  std::map<std::string, std::string> fxCookiesByDomain_;

  // True iff the RPC caller supplied a "cookies" option for this job (even an empty one). When
  // true, createRequest() treats fxCookiesByDomain_ as authoritative for the Cookie header on
  // EVERY request this HttpRequest represents: a host with no entry gets no Cookie header sent,
  // and cookieStorage_ (this daemon's own long-lived, cross-download cookie jar) is never
  // consulted as a fallback. Without this flag, "the map has no entry for this host" would be
  // indistinguishable from "the caller never used this feature," and the daemon's own
  // accumulated jar would silently fill the gap — exactly the two-cookie-jar inconsistency this
  // whole mechanism exists to close. False (the default) preserves the pre-existing
  // cookieStorage_-only behavior untouched for every caller that doesn't set "cookies".
  bool fxCookiesProvided_;

  AuthConfigFactory* authConfigFactory_;

  const Option* option_;

  // Historically, aria2 did not specify end byte marker unless http
  // pipelining is enabled. Sometimes end byte is known because the
  // segment/piece ahead of this request was already acquired. In this
  // case, specifying end byte enables to reuse connection. To achieve
  // this, if endOffsetOverride_ is more than 0, its value - 1 is used
  // as an end byte. Please note that FTP protocol cannot specify end
  // bytes and it is also true if it is used via HTTP proxy.
  int64_t endOffsetOverride_;

  std::vector<std::string> headers_;

  std::string userAgent_;

  std::string ifModSinceHeader_;

  bool contentEncodingEnabled_;

  // If true, metalink content types are sent in Accept header field.
  bool acceptMetalink_;

  bool noCache_;

  bool acceptGzip_;

  // Don't send Want-Digest header field
  bool noWantDigest_;

  std::pair<std::string, std::string> getProxyAuthString() const;

public:
  HttpRequest();
  ~HttpRequest();

  const std::shared_ptr<Segment>& getSegment() const { return segment_; }

  void setSegment(std::shared_ptr<Segment> segment);

  void setRequest(std::shared_ptr<Request> request);

  int64_t getEntityLength() const;

  const std::string& getHost() const;

  uint16_t getPort() const;

  const std::string& getMethod() const;

  const std::string& getProtocol() const;

  const std::string& getCurrentURI() const;

  const std::string& getDir() const;

  const std::string& getFile() const;

  const std::string& getQuery() const;

  std::string getURIHost() const;

  Range getRange() const;

  /**
   * Inspects whether the specified response range is satisfiable
   * with request range.
   */
  bool isRangeSatisfied(const Range& range) const;

  const std::shared_ptr<Request>& getRequest() const { return request_; }

  int64_t getStartByte() const;

  int64_t getEndByte() const;

  /**
   * Returns string representation of http request.  It usually starts
   * with "GET ..." and ends with "\r\n".  The AuthConfig for this
   * request is resolved using authConfigFactory_ and stored in
   * authConfig_.  getAuthConfig() returns AuthConfig used in the last
   * invocation of createRequest().
   */
  std::string createRequest();

  /**
   * Returns string representation of http tunnel request.
   * It usually starts with "CONNECT ..." and ends with "\r\n".
   */
  std::string createProxyRequest() const;

  void enableContentEncoding();

  void disableContentEncoding();

  void setUserAgent(std::string userAgent);

  // accepts multiline headers, delimited by LF
  void addHeader(const std::string& headers);

  void clearHeader();

  void addAcceptType(const std::string& type);

  void setAcceptMetalink(bool f) { acceptMetalink_ = f; }

  void setCookieStorage(CookieStorage* cookieStorage);

  CookieStorage* getCookieStorage() const;

  // FXPlayer extension: see fxCookiesByDomain_'s own doc comment. `cookiesByDomain` becoming the
  // authority (fxCookiesProvided_ = true) happens unconditionally here, including when it's
  // empty — this setter IS the "the caller used this feature" signal, distinct from never
  // calling it at all.
  void setFxCookiesByDomain(std::map<std::string, std::string> cookiesByDomain);

  void setAuthConfigFactory(AuthConfigFactory* factory);
  void setOption(const Option* option);

  /*
   * To use proxy, pass proxy string to Request::setUri() and set it this
   * object.
   */
  void setProxyRequest(std::shared_ptr<Request> proxyRequest);

  /*
   * Returns true if non-Null proxy request is set by setProxyRequest().
   * Otherwise, returns false.
   */
  bool isProxyRequestSet() const;

  // Returns true if authentication was used in the last
  // createRequest().
  bool authenticationUsed() const;

  // Returns AuthConfig used in the last invocation of
  // createRequest().
  const std::unique_ptr<AuthConfig>& getAuthConfig() const;

  void setFileEntry(std::shared_ptr<FileEntry> fileEntry);

  const std::shared_ptr<FileEntry>& getFileEntry() const { return fileEntry_; }

  void enableNoCache() { noCache_ = true; }

  void disableNoCache() { noCache_ = false; }

  void enableAcceptGZip() { acceptGzip_ = true; }

  void disableAcceptGZip() { acceptGzip_ = false; }

  bool acceptGZip() const { return acceptGzip_; }

  void setEndOffsetOverride(int64_t offset) { endOffsetOverride_ = offset; }

  void setIfModifiedSinceHeader(std::string value);

  const std::string& getIfModifiedSinceHeader() const
  {
    return ifModSinceHeader_;
  }

  // Returns true if request is conditional:more specifically, the
  // request is considered to be conditional if the client sent
  // "If-Modified-Since" or "If-None-Match" request-header field.
  bool conditionalRequest() const;

  void setNoWantDigest(bool b) { noWantDigest_ = b; }
};

} // namespace aria2

#endif // D_HTTP_REQUEST_H
