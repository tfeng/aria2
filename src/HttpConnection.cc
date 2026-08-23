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
#include "HttpConnection.h"

#include <sstream>

#include "util.h"
#include "message.h"
#include "prefs.h"
#include "LogFactory.h"
#include "DlRetryEx.h"
#include "DlAbortEx.h"
#include "Request.h"
#include "Segment.h"
#include "HttpRequest.h"
#include "HttpResponse.h"
#include "HttpHeaderProcessor.h"
#include "HttpHeader.h"
#include "Logger.h"
#include "SocketCore.h"
#include "Option.h"
#include "CookieStorage.h"
#include "AuthConfigFactory.h"
#include "AuthConfig.h"
#include "a2functional.h"
#include "fmt.h"
#include "SocketRecvBuffer.h"
#include "array_fun.h"

namespace aria2 {

namespace {
// FXPlayer extension (2026-09-10): same host match as HttpRequest.cc's identically-named helper (kept
// as a separate file-local copy rather than a shared header change, since it's a one-line check used in
// exactly these two translation units) — see that copy's own doc comment for why a substring match
// rather than an exact-host allowlist.
bool isXnxxDiagHost(const std::string& host)
{
  return util::toLower(host).find("xnxx") != std::string::npos;
}
} // namespace

HttpRequestEntry::HttpRequestEntry(std::unique_ptr<HttpRequest> httpRequest)
    : httpRequest_{std::move(httpRequest)},
      proc_{
          make_unique<HttpHeaderProcessor>(HttpHeaderProcessor::CLIENT_PARSER)}
{
}

void HttpRequestEntry::resetHttpHeaderProcessor()
{
  proc_ = make_unique<HttpHeaderProcessor>(HttpHeaderProcessor::CLIENT_PARSER);
}

std::unique_ptr<HttpRequest> HttpRequestEntry::popHttpRequest()
{
  return std::move(httpRequest_);
}

const std::unique_ptr<HttpHeaderProcessor>&
HttpRequestEntry::getHttpHeaderProcessor() const
{
  return proc_;
}

HttpConnection::HttpConnection(
    cuid_t cuid, const std::shared_ptr<SocketCore>& socket,
    const std::shared_ptr<SocketRecvBuffer>& socketRecvBuffer)
    : cuid_(cuid),
      socket_(socket),
      socketRecvBuffer_(socketRecvBuffer),
      socketBuffer_(socket)
{
}

HttpConnection::~HttpConnection() = default;

std::string HttpConnection::eraseConfidentialInfo(const std::string& request)
{
  std::istringstream istr(request);
  std::string result;
  std::string line;
  while (getline(istr, line)) {
    if (util::istartsWith(line, "Authorization: ")) {
      result += "Authorization: <snip>\n";
    }
    else if (util::istartsWith(line, "Proxy-Authorization: ")) {
      result += "Proxy-Authorization: <snip>\n";
    }
    else if (util::istartsWith(line, "Cookie: ")) {
      result += "Cookie: <snip>\n";
    }
    else if (util::istartsWith(line, "Set-Cookie: ")) {
      result += "Set-Cookie: <snip>\n";
    }
    else {
      result += line;
      result += "\n";
    }
  }
  return result;
}

void HttpConnection::sendRequest(std::unique_ptr<HttpRequest> httpRequest,
                                 std::string request)
{
  A2_LOG_INFO(
      fmt(MSG_SENDING_REQUEST, cuid_, eraseConfidentialInfo(request).c_str()));
  socketBuffer_.pushStr(std::move(request));
  socketBuffer_.send();
  outstandingHttpRequests_.push_back(
      make_unique<HttpRequestEntry>(std::move(httpRequest)));
}

void HttpConnection::sendRequest(std::unique_ptr<HttpRequest> httpRequest)
{
  auto req = httpRequest->createRequest();
  sendRequest(std::move(httpRequest), std::move(req));
}

void HttpConnection::sendProxyRequest(std::unique_ptr<HttpRequest> httpRequest)
{
  auto req = httpRequest->createProxyRequest();
  sendRequest(std::move(httpRequest), std::move(req));
}

std::unique_ptr<HttpResponse> HttpConnection::receiveResponse()
{
  if (outstandingHttpRequests_.empty()) {
    throw DL_ABORT_EX(EX_NO_HTTP_REQUEST_ENTRY_FOUND);
  }
  if (socketRecvBuffer_->bufferEmpty()) {
    if (socketRecvBuffer_->recv() == 0 && !socket_->wantRead() &&
        !socket_->wantWrite()) {
      throw DL_RETRY_EX(EX_GOT_EOF);
    }
  }

  const auto& proc = outstandingHttpRequests_.front()->getHttpHeaderProcessor();
  if (proc->parse(socketRecvBuffer_->getBuffer(),
                  socketRecvBuffer_->getBufferLength())) {
    A2_LOG_INFO(fmt(MSG_RECEIVE_RESPONSE, cuid_,
                    eraseConfidentialInfo(proc->getHeaderString()).c_str()));
    // FXPlayer extension (2026-09-10): mirrors the outgoing-request diagnostic in HttpRequest.cc's
    // createRequest() — same reasoning (WARN so it survives normal log-level settings, un-redacted
    // since this is the user's own private local log). This is the RAW response header block
    // (status line + every header exactly as the server sent it, including any Set-Cookie, Retry-After,
    // or CDN/WAF-identifying headers a block/challenge response would carry) — getHeaderString() here,
    // unlike HttpHeader's own table_, isn't limited to the handful of header names aria2 tokenizes for
    // its own use.
    const auto& originatingRequest = outstandingHttpRequests_.front()->getHttpRequest();
    if (originatingRequest && isXnxxDiagHost(originatingRequest->getHost())) {
      A2_LOG_WARN(fmt("[xnxx-diag] INCOMING response host=%s\n%s",
                      originatingRequest->getHost().c_str(), proc->getHeaderString().c_str()));
    }
    auto result = proc->getResult();
    if (result->getStatusCode() / 100 == 1) {
      socketRecvBuffer_->drain(proc->getLastBytesProcessed());
      outstandingHttpRequests_.front()->resetHttpHeaderProcessor();
      return nullptr;
    }

    auto httpResponse = make_unique<HttpResponse>();
    httpResponse->setCuid(cuid_);
    httpResponse->setHttpHeader(std::move(result));
    httpResponse->setHttpRequest(
        outstandingHttpRequests_.front()->popHttpRequest());
    socketRecvBuffer_->drain(proc->getLastBytesProcessed());
    outstandingHttpRequests_.pop_front();
    return httpResponse;
  }

  socketRecvBuffer_->drain(proc->getLastBytesProcessed());
  return nullptr;
}

bool HttpConnection::isIssued(const std::shared_ptr<Segment>& segment) const
{
  for (const auto& entry : outstandingHttpRequests_) {
    if (*entry->getHttpRequest()->getSegment() == *segment) {
      return true;
    }
  }
  return false;
}

bool HttpConnection::sendBufferIsEmpty() const
{
  return socketBuffer_.sendBufferIsEmpty();
}

void HttpConnection::sendPendingData() { socketBuffer_.send(); }

} // namespace aria2
