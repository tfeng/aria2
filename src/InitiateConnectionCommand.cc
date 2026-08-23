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
#include "InitiateConnectionCommand.h"
#include "Request.h"
#include "DownloadEngine.h"
#include "Option.h"
#include "Logger.h"
#include "LogFactory.h"
#include "message.h"
#include "prefs.h"
#include "NameResolver.h"
#include "SocketCore.h"
#include "FileEntry.h"
#include "RequestGroup.h"
#include "Segment.h"
#include "a2functional.h"
#include "InitiateConnectionCommandFactory.h"
#include "util.h"
#include "RecoverableException.h"
#include "fmt.h"
#include "SocketRecvBuffer.h"
#include "BackupIPv4ConnectCommand.h"
#include "ConnectCommand.h"
#include "DlAbortEx.h"
#include "error_code.h"

namespace aria2 {

InitiateConnectionCommand::InitiateConnectionCommand(
    cuid_t cuid, const std::shared_ptr<Request>& req,
    const std::shared_ptr<FileEntry>& fileEntry, RequestGroup* requestGroup,
    DownloadEngine* e)
    : AbstractCommand(cuid, req, fileEntry, requestGroup, e)
{
  setTimeout(std::chrono::seconds(getOption()->getAsInt(PREF_DNS_TIMEOUT)));
  // give a chance to be executed in the next loop in DownloadEngine
  setStatus(Command::STATUS_ONESHOT_REALTIME);
  disableReadCheckSocket();
  disableWriteCheckSocket();
}

InitiateConnectionCommand::~InitiateConnectionCommand() = default;

namespace {
// FXPlayer extension (2026-09-21): true if `hostname` is exactly `suffix`, or ends with it on a
// label (dot) boundary — so a configured suffix "xnxx-cdn.com" matches "hls-gold-cdn77.xnxx-cdn.com"
// but NOT "evilxnxx-cdn.com" (no dot boundary — a naive endsWith would wrongly allow this). Both
// sides are compared case-insensitively (hostnames are case-insensitive per spec, and PREF_FX_
// ALLOWED_HOST_SUFFIXES's own doc comment makes no promise the app lower-cases before sending).
bool fxHostMatchesAllowedSuffix(const std::string& hostname, const std::string& suffix)
{
  if (suffix.empty()) {
    return false;
  }
  if (util::strieq(hostname, suffix)) {
    return true;
  }
  return hostname.size() > suffix.size() &&
         hostname[hostname.size() - suffix.size() - 1] == '.' &&
         util::iendsWith(hostname, suffix);
}

// Throws DL_ABORT_EX2 (FX_HOST_NOT_ALLOWED) if `req`'s current target host isn't covered by this
// request's PREF_FX_ALLOWED_HOST_SUFFIXES, when that option is defined for it. See that pref's own
// doc comment (prefs.h) for why "defined but empty" must still reject, and
// InitiateConnectionCommand::executeInternal's call site for why THIS is the chokepoint: every
// connection attempt — including every hop of an HTTP redirect, which mutates the same Request's
// host in place and routes back through this exact command (HttpResponse::processRedirect →
// Request::redirectUri → AbstractCommand::prepareForRetry → CreateRequestCommand →
// InitiateConnectionCommandFactory) — passes through here strictly before any DNS/socket work.
void fxEnforceAllowedHost(const Request& req, const std::shared_ptr<Option>& option)
{
  if (!option->defined(PREF_FX_ALLOWED_HOST_SUFFIXES)) {
    return; // job didn't ask for this restriction — pre-existing unrestricted behavior
  }
  const std::string& hostname = req.getHost();
  // Manual line-walk (matches HttpRequestCommand.cc's PREF_FX_COOKIES decode — same "\n"-joined
  // wire encoding, same style) rather than a generic split helper, so both FXPlayer per-job
  // extensions decode their shared line-based format identically.
  const auto& encoded = option->get(PREF_FX_ALLOWED_HOST_SUFFIXES);
  size_t lineStart = 0;
  while (lineStart <= encoded.size()) {
    auto lineEnd = encoded.find('\n', lineStart);
    if (lineEnd == std::string::npos) {
      lineEnd = encoded.size();
    }
    if (lineEnd > lineStart &&
        fxHostMatchesAllowedSuffix(hostname, encoded.substr(lineStart, lineEnd - lineStart))) {
      return; // allowed
    }
    if (lineEnd == encoded.size()) {
      break;
    }
    lineStart = lineEnd + 1;
  }
  throw DL_ABORT_EX2(
      fmt("FXPlayer host allowlist: refusing to connect to '%s' - not in this job's "
          "allowed host suffixes",
          hostname.c_str()),
      error_code::FX_HOST_NOT_ALLOWED);
}
} // namespace

bool InitiateConnectionCommand::executeInternal()
{
  std::string hostname;
  uint16_t port;
  std::shared_ptr<Request> proxyRequest = createProxyRequest();
  if (!proxyRequest) {
    hostname = getRequest()->getHost();
    port = getRequest()->getPort();
  }
  else {
    hostname = proxyRequest->getHost();
    port = proxyRequest->getPort();
  }
  // Checked against the REQUEST's real target (not `hostname`, which is the PROXY's address when
  // a proxy is configured) — the allowlist is about which resource this job is ultimately
  // fetching, not which literal TCP peer it connects through. Before any DNS/socket work, and
  // ahead of the try/catch below so a violation propagates as a clean job/segment failure (like
  // any other DL_ABORT_EX2, e.g. HttpResponse::validateResponse's Content-Range check) rather than
  // being treated as a transient connection failure worth retrying via a different cached IP.
  fxEnforceAllowedHost(*getRequest(), getOption());
  std::vector<std::string> addrs;
  std::string ipaddr = resolveHostname(addrs, hostname, port);
  if (ipaddr.empty()) {
    addCommandSelf();
    return false;
  }
  try {
    auto c = createNextCommand(hostname, ipaddr, port, addrs, proxyRequest);
    c->setStatus(Command::STATUS_ONESHOT_REALTIME);
    getDownloadEngine()->setNoWait(true);
    getDownloadEngine()->addCommand(std::move(c));
    return true;
  }
  catch (RecoverableException& ex) {
    // Catch exception and retry another address.
    // See also AbstractCommand::checkIfConnectionEstablished

    // TODO ipaddr might not be used if pooled socket was found.
    getDownloadEngine()->markBadIPAddress(hostname, ipaddr, port);
    if (!getDownloadEngine()->findCachedIPAddress(hostname, port).empty()) {
      A2_LOG_INFO_EX(EX_EXCEPTION_CAUGHT, ex);
      A2_LOG_INFO(
          fmt(MSG_CONNECT_FAILED_AND_RETRY, getCuid(), ipaddr.c_str(), port));
      auto command =
          InitiateConnectionCommandFactory::createInitiateConnectionCommand(
              getCuid(), getRequest(), getFileEntry(), getRequestGroup(),
              getDownloadEngine());
      getDownloadEngine()->setNoWait(true);
      getDownloadEngine()->addCommand(std::move(command));
      return true;
    }
    getDownloadEngine()->removeCachedIPAddress(hostname, port);
    throw;
  }
}

void InitiateConnectionCommand::setConnectedAddrInfo(
    const std::shared_ptr<Request>& req, const std::string& hostname,
    const std::shared_ptr<SocketCore>& socket)
{
  auto endpoint = socket->getPeerInfo();
  req->setConnectedAddrInfo(hostname, endpoint.addr, endpoint.port);
}

std::shared_ptr<BackupConnectInfo>
InitiateConnectionCommand::createBackupIPv4ConnectCommand(
    const std::string& hostname, const std::string& ipaddr, uint16_t port,
    Command* mainCommand)
{
  // Prepare IPv4 backup connection attempt in "Happy Eyeballs"
  // fashion.
  std::shared_ptr<BackupConnectInfo> info;
  char buf[sizeof(in6_addr)];
  if (inetPton(AF_INET6, ipaddr.c_str(), &buf) == -1) {
    return info;
  }
  A2_LOG_INFO("Searching IPv4 address for backup connection attempt");
  std::vector<std::string> addrs;
  getDownloadEngine()->findAllCachedIPAddresses(std::back_inserter(addrs),
                                                hostname, port);
  for (std::vector<std::string>::const_iterator i = addrs.begin(),
                                                eoi = addrs.end();
       i != eoi; ++i) {
    if (inetPton(AF_INET, (*i).c_str(), &buf) == 0) {
      info = std::make_shared<BackupConnectInfo>();
      auto command = make_unique<BackupIPv4ConnectCommand>(
          getDownloadEngine()->newCUID(), *i, port, info, mainCommand,
          getRequestGroup(), getDownloadEngine());
      A2_LOG_INFO(fmt("Issue backup connection command CUID#%" PRId64
                      ", addr=%s",
                      command->getCuid(), (*i).c_str()));
      getDownloadEngine()->addCommand(std::move(command));
      return info;
    }
  }
  return info;
}

void InitiateConnectionCommand::setupBackupConnection(
    const std::string& hostname, const std::string& addr, uint16_t port,
    ConnectCommand* c)
{
  std::shared_ptr<BackupConnectInfo> backupConnectInfo =
      createBackupIPv4ConnectCommand(hostname, addr, port, c);
  if (backupConnectInfo) {
    c->setBackupConnectInfo(backupConnectInfo);
  }
}

} // namespace aria2
