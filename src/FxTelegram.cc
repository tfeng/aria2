/* <!-- copyright */
/*
 * aria2 - The high speed download utility (FXPlayer fork)
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 */
/* copyright --> */
#include "FxTelegram.h"

#include <fcntl.h>
#include <signal.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <deque>
#include <fstream>
#include <map>
#include <mutex>
#include <set>
#include <sstream>
#include <thread>

#include "LogFactory.h"
#include "Logger.h"
#include "ValueBase.h"
#include "ValueBaseJsonParser.h"
#include "fmt.h"
#include "json.h"
#include "util.h"

namespace aria2 {

namespace fxtg {

namespace {

using Clock = std::chrono::steady_clock;

// At most this many downloads talk to Telegram at once (the phone's own cap, TelegramSaveManager).
const size_t MAX_ACTIVE = 3;
// Helper restart backoff: a helper that dies within FAST_EXIT of starting counts as a fast failure. After
// FAST_EXITS_BEFORE_SLOWDOWN of them in a row, restarts slow down and waiting downloads fail (2013) instead of
// waiting for a helper that may never come up (a missing binary used to hold its job forever).
const auto RESPAWN_DELAY = std::chrono::seconds(5);
const auto RESPAWN_DELAY_AFTER_FAST_EXITS = std::chrono::seconds(60);
const auto FAST_EXIT = std::chrono::seconds(30);
const int FAST_EXITS_BEFORE_SLOWDOWN = 3;
// A helper whose TDLib hasn't reported its first authorization state by then is ended and started again.
const auto STARTING_TIMEOUT = std::chrono::seconds(120);
// shutdown() waits this long for the helper to close TDLib.
const auto SHUTDOWN_WAIT = std::chrono::seconds(5);
// helper.log (the helper's stderr: TDLib's own log) is moved to helper.log.1 at a start once it is larger than this.
const off_t HELPER_LOG_MAX = 4 * 1024 * 1024;
// TDLib's download priority for these files (1..32; the phone's saves use 16 too).
const int DOWNLOAD_PRIORITY = 16;
// Resolving a message (getChat/loadChats/getMessage) that takes longer than this fails the download (2013).
const auto RESOLVE_TIMEOUT = std::chrono::seconds(300);
// TDLib ends a download that fails (network, file reference, DC trouble) by reporting the file inactive and not
// complete -- no error. A download that stays inactive for INACTIVE_GRACE is asked for again, at most MAX_RESTARTS
// times without progress in between; then it fails (2013).
const auto INACTIVE_GRACE = std::chrono::seconds(5);
const int MAX_RESTARTS = 3;
// A download without a new byte for STALL_WARN is logged; for STALL_FAIL it fails (2013, partial file kept).
const auto STALL_WARN = std::chrono::seconds(120);
const auto STALL_FAIL = std::chrono::seconds(600);
// Telegram's flood waits (TDLib error 429 "retry after N"): a download waits up to this long, queued; a longer wait
// fails it (2013) instead.
const int64_t MAX_FLOOD_WAIT_SECONDS = 600;
// Discards (deleteFile) sent per tick.
const size_t DISCARDS_PER_TICK = 5;

enum class Phase { QUEUED, CHAT, MESSAGE, DOWNLOADING };

struct Download {
  a2_gid_t gid = 0;
  int64_t chatId = 0;
  int64_t msgId = 0;
  Phase phase = Phase::QUEUED;
  Clock::time_point phaseAt;  // when the current phase began
  Clock::time_point retryAt;  // QUEUED: not admitted before this (a flood wait)
  bool chatRetried = false;   // getChat already retried once after loading the chat lists
  bool waitingForChats = false;
  int32_t fileId = 0;
  // DOWNLOADING: whether TDLib has answered our downloadFile; from then on an inactive, incomplete file means TDLib
  // stopped the download (see INACTIVE_GRACE).
  bool answered = false;
  bool tdlibActive = false; // TDLib's is_downloading_active, as last reported
  Clock::time_point inactiveSince;
  int restarts = 0;
  bool logged = false; // the "download start" line was written
  int64_t downloaded = 0;
  int64_t total = 0;
  int64_t speed = 0;
  int64_t lastSampleBytes = 0;
  Clock::time_point lastSampleAt;
  Clock::time_point progressAt; // when `downloaded` last grew (or the download was (re)sent)
  bool stallLogged = false;
  int64_t startBytes = 0; // TDLib's prefix when this attempt's downloadFile was first sent
  Clock::time_point startedAt;
};

std::mutex mu;
std::condition_variable helperExited; // notified (under mu) when helperAlive becomes false
std::string helperPath;
std::string dataDir;
bool enabled = false;
bool shuttingDown = false;

pid_t helperPid = -1;
int toHelper = -1;
bool helperAlive = false;
Clock::time_point spawnedAt;
Clock::time_point exitedAt;
int fastExits = 0;
uint64_t helperGeneration = 0; // bumped per spawn, so a dead reader thread can't touch a newer helper's state
// Why the current helper is expected to exit (we ended it, or it said why it is leaving); "" = it isn't.
std::string exitReason;

AuthState auth;
int64_t apiId = 0;
std::string apiHash;

std::map<a2_gid_t, Download> downloads;
std::deque<a2_gid_t> queueOrder;
std::deque<Event> events;
// (chat, message) -> TDLib file id, for discard(). File ids only hold for one TDLib instance: cleared when the helper
// exits (discard() then looks the message up again).
std::map<std::pair<int64_t, int64_t>, int32_t> knownFiles;
// Messages whose TDLib copy is to be deleted once the account is ready (see discard()).
std::set<std::pair<int64_t, int64_t>> pendingDiscards;

enum class ChatLoad { NOT_LOADED, LOADING_MAIN, LOADING_ARCHIVE, LOADED };
ChatLoad chatLoad = ChatLoad::NOT_LOADED;
Clock::time_point chatLoadStartedAt;
Clock::time_point chatLoadRetryAt; // a flood wait paused the load until then (epoch = not paused)

std::string credentialsPath() { return dataDir + "/fx-telegram.json"; }
std::string helperLogPath() { return dataDir + "/helper.log"; }

const char* resultName(Result result)
{
  switch (result) {
  case Result::OK:
    return "ok";
  case Result::CANCELED:
    return "stopped";
  case Result::UNAVAILABLE:
    return "unavailable";
  case Result::NOT_AUTHORIZED:
    return "not signed in";
  case Result::FAILED:
  default:
    return "error";
  }
}

double secondsSince(Clock::time_point t, Clock::time_point now)
{
  return std::chrono::duration<double>(now - t).count();
}

// ---- small JSON helpers -------------------------------------------------------------------------------------

const Dict* asDict(const ValueBase* v) { return downcast<Dict>(v); }

const Dict* dictField(const Dict* d, const char* key) { return d ? downcast<Dict>(d->get(key)) : nullptr; }

std::string stringField(const Dict* d, const char* key)
{
  const auto* s = d ? downcast<String>(d->get(key)) : nullptr;
  return s ? s->s() : std::string();
}

int64_t intField(const Dict* d, const char* key)
{
  const auto* i = d ? downcast<Integer>(d->get(key)) : nullptr;
  return i ? i->i() : 0;
}

bool boolField(const Dict* d, const char* key)
{
  const auto* b = d ? downcast<Bool>(d->get(key)) : nullptr;
  return b && b->val();
}

std::unique_ptr<Dict> request(const char* type, const std::string& extra)
{
  auto d = Dict::g();
  d->put("@type", type);
  if (!extra.empty()) {
    d->put("@extra", extra);
  }
  return d;
}

// The wait Telegram asked for when `code`/`text` is a flood-wait error (TDLib: 429 "Too Many Requests: retry after
// N"; 420 "FLOOD_WAIT_N" from the raw API), in seconds; -1 for any other error.
int64_t floodWaitSeconds(int64_t code, const std::string& text)
{
  if (code != 429 && code != 420 && text.find("FLOOD_WAIT") == std::string::npos) {
    return -1;
  }
  for (const char* key : {"retry after ", "FLOOD_WAIT_"}) {
    const auto pos = text.find(key);
    if (pos != std::string::npos) {
      const auto n = strtoll(text.c_str() + pos + strlen(key), nullptr, 10);
      if (n > 0) {
        return n;
      }
    }
  }
  return 30; // no number: a cautious default
}

// ---- helper process (caller holds mu) --------------------------------------------------------------------------

void sendLocked(const Dict& req)
{
  if (!helperAlive || toHelper < 0) {
    return;
  }
  auto line = json::encode(&req);
  line += '\n';
  const char* p = line.data();
  size_t left = line.size();
  while (left > 0) {
    const ssize_t n = ::write(toHelper, p, left);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      A2_LOG_WARN(fmt("[fxtg] write to helper failed errno=%d", errno));
      return; // the reader thread sees the helper exit and cleans up
    }
    p += n;
    left -= static_cast<size_t>(n);
  }
}

// Ends the current helper on purpose: closing its stdin makes it close TDLib and exit; the reader thread then logs
// the exit with `why` (not as an error) and the next tick starts a new one.
void endHelperLocked(const std::string& why)
{
  if (exitReason.empty()) {
    exitReason = why;
  }
  if (toHelper >= 0) {
    ::close(toHelper);
    toHelper = -1;
  }
}

void finishLocked(a2_gid_t gid, Result result, const std::string& message, const std::string& path = "",
                  int64_t size = 0)
{
  auto it = downloads.find(gid);
  Event ev;
  ev.gid = gid;
  ev.result = result;
  ev.message = message;
  ev.path = path;
  ev.size = size;
  const auto gidHex = GroupId::toHex(gid);
  if (it != downloads.end()) {
    const auto& d = it->second;
    ev.downloaded = result == Result::OK ? size : d.downloaded;
    const auto now = Clock::now();
    if (result == Result::OK) {
      const int64_t fetched = size - d.startBytes;
      if (!d.logged || fetched <= 0) {
        A2_LOG_WARN(fmt("[fxtg] gid=%s already downloaded (%sB)", gidHex.c_str(), util::abbrevSize(size).c_str()));
      }
      else {
        const double took = std::max(0.001, secondsSince(d.startedAt, now));
        A2_LOG_WARN(fmt("[fxtg] gid=%s downloaded %sB in %.1fs (%sB/s)%s", gidHex.c_str(),
                        util::abbrevSize(fetched).c_str(), took,
                        util::abbrevSize(static_cast<int64_t>(fetched / took)).c_str(),
                        d.startBytes > 0 ? fmt(", resumed at %sB", util::abbrevSize(d.startBytes).c_str()).c_str()
                                         : ""));
      }
    }
    else if (result == Result::CANCELED) {
      A2_LOG_WARN(d.total > 0 ? fmt("[fxtg] gid=%s stopped at %sB of %sB", gidHex.c_str(),
                                    util::abbrevSize(d.downloaded).c_str(), util::abbrevSize(d.total).c_str())
                              : fmt("[fxtg] gid=%s stopped before its download started", gidHex.c_str()));
    }
    else if (d.total > 0) {
      A2_LOG_WARN(fmt("[fxtg] gid=%s failed (%s) at %sB of %sB: %s", gidHex.c_str(), resultName(result),
                      util::abbrevSize(d.downloaded).c_str(), util::abbrevSize(d.total).c_str(), message.c_str()));
    }
    else {
      A2_LOG_WARN(fmt("[fxtg] gid=%s failed (%s): %s", gidHex.c_str(), resultName(result), message.c_str()));
    }
    downloads.erase(it);
  }
  else {
    A2_LOG_WARN(fmt("[fxtg] gid=%s failed (%s): %s", gidHex.c_str(), resultName(result), message.c_str()));
  }
  auto q = std::find(queueOrder.begin(), queueOrder.end(), gid);
  if (q != queueOrder.end()) {
    queueOrder.erase(q);
  }
  events.push_back(std::move(ev));
}

void failAllLocked(Result result, const std::string& message)
{
  std::vector<a2_gid_t> gids;
  for (const auto& kv : downloads) {
    gids.push_back(kv.first);
  }
  for (auto gid : gids) {
    finishLocked(gid, result, message);
  }
}

bool messageInUseLocked(int64_t chatId, int64_t msgId)
{
  for (const auto& kv : downloads) {
    if (kv.second.chatId == chatId && kv.second.msgId == msgId) {
      return true;
    }
  }
  return false;
}

void loadCredentialsLocked()
{
  std::ifstream in(credentialsPath());
  if (!in) {
    return;
  }
  std::stringstream ss;
  ss << in.rdbuf();
  const auto text = ss.str();
  ssize_t error = 0;
  auto v = json::ValueBaseJsonParser().parseFinal(text.c_str(), text.size(), error);
  if (error < 0) {
    A2_LOG_WARN(fmt("[fxtg] could not read %s", credentialsPath().c_str()));
    return;
  }
  const auto* d = asDict(v.get());
  apiId = intField(d, "apiId");
  apiHash = stringField(d, "apiHash");
}

void saveCredentialsLocked()
{
  auto d = Dict::g();
  d->put("apiId", Integer::g(apiId));
  d->put("apiHash", apiHash);
  const auto tmp = credentialsPath() + ".tmp";
  {
    const int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
    if (fd < 0) {
      A2_LOG_ERROR(fmt("[fxtg] could not write %s errno=%d", tmp.c_str(), errno));
      return;
    }
    const auto text = json::encode(d.get());
    const bool ok = ::write(fd, text.data(), text.size()) == static_cast<ssize_t>(text.size());
    ::fsync(fd);
    ::close(fd);
    if (!ok) {
      ::unlink(tmp.c_str());
      return;
    }
  }
  if (::rename(tmp.c_str(), credentialsPath().c_str()) != 0) {
    A2_LOG_ERROR(fmt("[fxtg] could not replace %s errno=%d", credentialsPath().c_str(), errno));
  }
}

void sendTdlibParametersLocked()
{
  auto r = request("setTdlibParameters", "auth");
  r->put("use_test_dc", Bool::gFalse());
  r->put("database_directory", dataDir + "/db");
  r->put("files_directory", dataDir + "/files");
  r->put("database_encryption_key", "");
  r->put("use_file_database", Bool::gTrue());
  r->put("use_chat_info_database", Bool::gTrue());
  r->put("use_message_database", Bool::gFalse());
  r->put("use_secret_chats", Bool::gFalse());
  r->put("api_id", Integer::g(apiId));
  r->put("api_hash", apiHash);
  r->put("system_language_code", "en");
  r->put("device_model", "aria2 (FXPlayer server)");
  r->put("system_version", "");
  r->put("application_version", "1.0");
  sendLocked(*r);
}

void queueLocked(Download& d, Clock::time_point retryAt, bool front)
{
  d.phase = Phase::QUEUED;
  d.phaseAt = Clock::now();
  d.retryAt = retryAt;
  d.waitingForChats = false;
  d.answered = false;
  d.tdlibActive = false;
  if (std::find(queueOrder.begin(), queueOrder.end(), d.gid) == queueOrder.end()) {
    if (front) {
      queueOrder.push_front(d.gid);
    }
    else {
      queueOrder.push_back(d.gid);
    }
  }
}

void requeueActiveLocked()
{
  size_t requeued = 0;
  for (auto& kv : downloads) {
    if (kv.second.phase != Phase::QUEUED) {
      queueLocked(kv.second, Clock::time_point(), true);
      ++requeued;
    }
  }
  if (requeued > 0) {
    A2_LOG_WARN(fmt("[fxtg] %lu download(s) requeued until the helper is back",
                    static_cast<unsigned long>(requeued)));
  }
}

// A flood wait for `d` (from the request named `what`): wait queued, or fail when Telegram asks for too long.
void floodWaitLocked(Download& d, int64_t seconds, const char* what)
{
  if (seconds > MAX_FLOOD_WAIT_SECONDS) {
    finishLocked(d.gid, Result::FAILED, fmt("Telegram flood wait of %" PRId64 "s (%s)", seconds, what));
    return;
  }
  A2_LOG_WARN(fmt("[fxtg] gid=%s flood wait %" PRId64 "s (%s); download queued until then",
                  GroupId::toHex(d.gid).c_str(), seconds, what));
  queueLocked(d, Clock::now() + std::chrono::seconds(seconds), false);
}

// ---- TDLib responses (reader thread, caller holds mu) ----------------------------------------------------------

void handleFileLocked(const Dict* file)
{
  const auto fileId = static_cast<int32_t>(intField(file, "id"));
  if (fileId == 0) {
    return;
  }
  const auto* local = dictField(file, "local");
  const int64_t size = intField(file, "size") > 0 ? intField(file, "size") : intField(file, "expected_size");
  const bool complete = boolField(local, "is_downloading_completed");
  const bool active = boolField(local, "is_downloading_active");
  const bool canDownload = boolField(local, "can_be_downloaded");
  const int64_t prefix = intField(local, "downloaded_prefix_size");
  const auto path = stringField(local, "path");
  const auto now = Clock::now();
  // Decided first, applied after: finishLocked changes `downloads`.
  std::vector<std::pair<a2_gid_t, Result>> ended;
  for (auto& kv : downloads) {
    auto& d = kv.second;
    if (d.phase != Phase::DOWNLOADING || d.fileId != fileId) {
      continue;
    }
    if (size > 0) {
      d.total = size;
    }
    if (prefix > d.downloaded) {
      d.progressAt = now;
      d.stallLogged = false;
      d.restarts = 0;
    }
    d.downloaded = prefix;
    if (!active && (d.tdlibActive || d.inactiveSince == Clock::time_point())) {
      d.inactiveSince = now;
    }
    d.tdlibActive = active;
    if (complete) {
      ended.emplace_back(d.gid, path.empty() ? Result::FAILED : Result::OK);
    }
    else if (!active && !canDownload) {
      ended.emplace_back(d.gid, Result::UNAVAILABLE);
    }
  }
  for (const auto& e : ended) {
    if (e.second == Result::OK) {
      finishLocked(e.first, Result::OK, "downloaded", path, size);
    }
    else if (e.second == Result::UNAVAILABLE) {
      finishLocked(e.first, Result::UNAVAILABLE, "Telegram says the file can't be downloaded");
    }
    else {
      finishLocked(e.first, Result::FAILED, "TDLib reported the download complete without a local path");
    }
  }
}

// The file of a message's video, document, animation or video note (nullptr for anything else).
const Dict* messageFile(const Dict* message)
{
  const auto* content = dictField(message, "content");
  const auto type = stringField(content, "@type");
  if (type == "messageVideo") {
    return dictField(dictField(content, "video"), "video");
  }
  if (type == "messageDocument") {
    return dictField(dictField(content, "document"), "document");
  }
  if (type == "messageAnimation") {
    return dictField(dictField(content, "animation"), "animation");
  }
  if (type == "messageVideoNote") {
    return dictField(dictField(content, "video_note"), "video");
  }
  return nullptr;
}

void requestChatLocked(Download& d)
{
  d.phase = Phase::CHAT;
  d.phaseAt = Clock::now();
  auto r = request("getChat", "chat:" + GroupId::toHex(d.gid));
  r->put("chat_id", Integer::g(d.chatId));
  sendLocked(*r);
}

void sendDownloadFileLocked(Download& d)
{
  d.answered = false;
  d.inactiveSince = Clock::time_point();
  d.progressAt = Clock::now();
  auto r = request("downloadFile", "dl:" + GroupId::toHex(d.gid));
  r->put("file_id", Integer::g(d.fileId));
  r->put("priority", Integer::g(DOWNLOAD_PRIORITY));
  r->put("offset", Integer::g(0));
  r->put("limit", Integer::g(0));
  r->put("synchronous", Bool::gFalse());
  sendLocked(*r);
}

// Cancels TDLib's download of `d`'s file (keeping the partial file), unless another download uses the same file.
void cancelTdlibDownloadLocked(const Download& d)
{
  if (d.phase != Phase::DOWNLOADING || d.fileId == 0) {
    return;
  }
  for (const auto& kv : downloads) {
    if (kv.first != d.gid && kv.second.phase == Phase::DOWNLOADING && kv.second.fileId == d.fileId) {
      return;
    }
  }
  auto r = request("cancelDownloadFile", "");
  r->put("file_id", Integer::g(d.fileId));
  r->put("only_if_pending", Bool::gFalse());
  sendLocked(*r);
}

void deleteFileLocked(int32_t fileId)
{
  auto r = request("deleteFile", "");
  r->put("file_id", Integer::g(fileId));
  sendLocked(*r);
}

void handleChatListsLoadedLocked()
{
  chatLoad = ChatLoad::LOADED;
  chatLoadRetryAt = Clock::time_point();
  A2_LOG_WARN(fmt("[fxtg] chat lists loaded in %.1fs", secondsSince(chatLoadStartedAt, Clock::now())));
  for (auto& kv : downloads) {
    auto& d = kv.second;
    if (d.waitingForChats) {
      d.waitingForChats = false;
      d.chatRetried = true;
      requestChatLocked(d);
    }
  }
}

void continueChatLoadLocked()
{
  auto r = request("loadChats", chatLoad == ChatLoad::LOADING_MAIN ? "load:main" : "load:archive");
  auto list = Dict::g();
  list->put("@type", chatLoad == ChatLoad::LOADING_MAIN ? "chatListMain" : "chatListArchive");
  r->put("chat_list", std::move(list));
  r->put("limit", Integer::g(500));
  sendLocked(*r);
}

void startChatLoadLocked(int64_t chatId)
{
  if (chatLoad == ChatLoad::LOADING_MAIN || chatLoad == ChatLoad::LOADING_ARCHIVE) {
    return;
  }
  A2_LOG_WARN(fmt("[fxtg] chat %" PRId64 " not known to this sign-in yet: loading the chat lists", chatId));
  chatLoad = ChatLoad::LOADING_MAIN;
  chatLoadStartedAt = Clock::now();
  chatLoadRetryAt = Clock::time_point();
  continueChatLoadLocked();
}

bool parseGidExtra(const std::string& extra, const char* prefix, a2_gid_t& gid)
{
  const size_t n = strlen(prefix);
  return extra.compare(0, n, prefix) == 0 && GroupId::toNumericId(gid, extra.c_str() + n) == 0;
}

std::string discardExtra(int64_t chatId, int64_t msgId)
{
  return fmt("discard:%" PRId64 ":%" PRId64, chatId, msgId);
}

bool parseDiscardExtra(const std::string& extra, int64_t& chatId, int64_t& msgId)
{
  long long c = 0, m = 0;
  if (extra.compare(0, 8, "discard:") != 0 || sscanf(extra.c_str() + 8, "%lld:%lld", &c, &m) != 2) {
    return false;
  }
  chatId = c;
  msgId = m;
  return true;
}

void handleAuthStateLocked(const Dict* state)
{
  const auto type = stringField(state, "@type");
  const auto previous = auth.state;
  auth.detail.clear();
  if (type == "authorizationStateWaitTdlibParameters") {
    if (apiId != 0 && !apiHash.empty()) {
      auth.state = "starting";
      sendTdlibParametersLocked();
    }
    else {
      auth.state = "wait-credentials";
    }
  }
  else if (type == "authorizationStateWaitPhoneNumber") {
    auth.state = "wait-phone";
  }
  else if (type == "authorizationStateWaitCode") {
    auth.state = "wait-code";
    auth.detail = stringField(dictField(dictField(state, "code_info"), "type"), "@type");
  }
  else if (type == "authorizationStateWaitOtherDeviceConfirmation") {
    // QR-code login: the link an already signed-in client confirms (the FXPlayer app's own TDLib,
    // confirmQrCodeAuthentication), so the server signs in without a login code. TDLib renews it before it expires.
    auth.state = "wait-qr";
    auth.detail = stringField(state, "link");
  }
  else if (type == "authorizationStateWaitPassword") {
    auth.state = "wait-password";
    auth.detail = stringField(state, "password_hint");
  }
  else if (type == "authorizationStateReady") {
    auth.state = "ready";
    auth.error.clear();
    chatLoad = ChatLoad::NOT_LOADED;
  }
  else if (type == "authorizationStateLoggingOut" || type == "authorizationStateClosing") {
    auth.state = "closing";
  }
  else if (type == "authorizationStateClosed") {
    // The client is gone for good (after a log out -- here or from another device -- or TDLib closing itself): end
    // the helper so the next tick starts a fresh one with a new client.
    auth.state = "closing";
    endHelperLocked("Telegram client closed");
  }
  else {
    auth.state = "unsupported";
    auth.detail = type;
  }
  // State only: the QR link is a login token and the password hint is the user's.
  if (auth.state != previous) {
    A2_LOG_WARN(fmt("[fxtg] sign-in state %s -> %s%s%s", previous.c_str(), auth.state.c_str(),
                    auth.state == "unsupported" ? " " : "", auth.state == "unsupported" ? type.c_str() : ""));
  }
}

void handleErrorLocked(const std::string& extra, int64_t code, const std::string& text)
{
  a2_gid_t gid = 0;
  const auto flood = floodWaitSeconds(code, text);
  if (extra == "auth") {
    if (auth.state == "starting" &&
        (text.find("lock") != std::string::npos || text.find("already in use") != std::string::npos)) {
      // TDLib's database is still held, e.g. by the previous helper of a quick restart: try again with a new one
      // instead of asking for credentials.
      A2_LOG_WARN(fmt("[fxtg] TDLib could not open its database (%s); restarting the helper", text.c_str()));
      endHelperLocked("TDLib database in use");
      return;
    }
    auth.error = text;
    if (auth.state == "starting") {
      auth.state = "wait-credentials"; // setTdlibParameters refused (e.g. a wrong API ID/hash)
    }
    A2_LOG_WARN(fmt("[fxtg] sign-in step failed code=%" PRId64 " %s", code, text.c_str()));
    return;
  }
  if (extra == "load:main" || extra == "load:archive") {
    if (flood >= 0) {
      A2_LOG_WARN(fmt("[fxtg] flood wait %" PRId64 "s while loading the chat lists", flood));
      chatLoadRetryAt = Clock::now() + std::chrono::seconds(flood);
      return;
    }
    // 404 = the list is fully loaded; anything else ends that list's load too (a chat still missing then counts as
    // unavailable).
    if (code != 404) {
      A2_LOG_WARN(fmt("[fxtg] loading the chat lists: code=%" PRId64 " %s", code, text.c_str()));
    }
    if (extra == "load:main") {
      chatLoad = ChatLoad::LOADING_ARCHIVE;
      continueChatLoadLocked();
    }
    else {
      handleChatListsLoadedLocked();
    }
    return;
  }
  if (parseGidExtra(extra, "chat:", gid)) {
    auto it = downloads.find(gid);
    if (it == downloads.end() || it->second.phase != Phase::CHAT) {
      return;
    }
    if (flood >= 0) {
      floodWaitLocked(it->second, flood, "getChat");
      return;
    }
    if (code == 400 && !it->second.chatRetried && chatLoad != ChatLoad::LOADED) {
      // A chat this database hasn't seen yet (a fresh sign-in knows none): load the chat lists, then retry once.
      it->second.waitingForChats = true;
      startChatLoadLocked(it->second.chatId);
      return;
    }
    finishLocked(gid,
                 code == 401 ? Result::NOT_AUTHORIZED : code == 400 ? Result::UNAVAILABLE : Result::FAILED,
                 fmt("chat not available: %s", text.c_str()));
    return;
  }
  if (parseGidExtra(extra, "msg:", gid)) {
    auto it = downloads.find(gid);
    if (it == downloads.end() || it->second.phase != Phase::MESSAGE) {
      return;
    }
    if (flood >= 0) {
      floodWaitLocked(it->second, flood, "getMessage");
      return;
    }
    finishLocked(gid,
                 code == 401 ? Result::NOT_AUTHORIZED
                             : (code == 400 || code == 404) ? Result::UNAVAILABLE : Result::FAILED,
                 fmt("message not available: %s", text.c_str()));
    return;
  }
  if (parseGidExtra(extra, "dl:", gid)) {
    auto it = downloads.find(gid);
    if (it == downloads.end() || it->second.phase != Phase::DOWNLOADING) {
      return;
    }
    if (flood >= 0) {
      floodWaitLocked(it->second, flood, "downloadFile");
      return;
    }
    finishLocked(gid, code == 401 ? Result::NOT_AUTHORIZED : Result::FAILED,
                 fmt("download failed: code=%" PRId64 " %s", code, text.c_str()));
    return;
  }
  int64_t chatId = 0, msgId = 0;
  if (parseDiscardExtra(extra, chatId, msgId)) {
    A2_LOG_INFO(fmt("[fxtg] discard of chat=%" PRId64 " msg=%" PRId64 ": %s", chatId, msgId, text.c_str()));
    return;
  }
  A2_LOG_INFO(fmt("[fxtg] TDLib error extra=%s code=%" PRId64 " %s", extra.c_str(), code, text.c_str()));
}

void handleLineLocked(const Dict* msg)
{
  const auto type = stringField(msg, "@type");
  const auto extra = stringField(msg, "@extra");
  a2_gid_t gid = 0;

  if (type == "updateAuthorizationState") {
    handleAuthStateLocked(dictField(msg, "authorization_state"));
    return;
  }
  if (type == "updateFile") {
    handleFileLocked(dictField(msg, "file"));
    return;
  }
  if (type == "fxHelperExiting") {
    // The helper is leaving on its own (a signal, e.g. systemd stopping the service): not a crash.
    exitReason = "helper got " + stringField(msg, "reason");
    return;
  }
  if (type == "error") {
    handleErrorLocked(extra, intField(msg, "code"), stringField(msg, "message"));
    return;
  }

  if (extra == "load:main" || extra == "load:archive") {
    continueChatLoadLocked(); // "ok": more chats may follow
    return;
  }
  if (parseGidExtra(extra, "chat:", gid)) {
    auto it = downloads.find(gid);
    if (it != downloads.end() && it->second.phase == Phase::CHAT) {
      it->second.phase = Phase::MESSAGE;
      it->second.phaseAt = Clock::now();
      auto r = request("getMessage", "msg:" + GroupId::toHex(gid));
      r->put("chat_id", Integer::g(it->second.chatId));
      r->put("message_id", Integer::g(it->second.msgId));
      sendLocked(*r);
    }
    return;
  }
  if (parseGidExtra(extra, "msg:", gid)) {
    auto it = downloads.find(gid);
    if (it == downloads.end() || it->second.phase != Phase::MESSAGE) {
      return;
    }
    const auto* file = messageFile(msg);
    if (!file) {
      finishLocked(gid, Result::UNAVAILABLE,
                   fmt("message has no video or document (%s)",
                       stringField(dictField(msg, "content"), "@type").c_str()));
      return;
    }
    auto& d = it->second;
    d.fileId = static_cast<int32_t>(intField(file, "id"));
    d.phase = Phase::DOWNLOADING;
    d.phaseAt = Clock::now();
    knownFiles[{d.chatId, d.msgId}] = d.fileId;
    const auto* local = dictField(file, "local");
    const int64_t size = intField(file, "size") > 0 ? intField(file, "size") : intField(file, "expected_size");
    const int64_t prefix = intField(local, "downloaded_prefix_size");
    if (!d.logged && !boolField(local, "is_downloading_completed")) {
      d.logged = true;
      d.startBytes = prefix;
      d.startedAt = Clock::now();
      A2_LOG_WARN(fmt("[fxtg] gid=%s download start chat=%" PRId64 " msg=%" PRId64 " size=%sB%s",
                      GroupId::toHex(gid).c_str(), d.chatId, d.msgId, util::abbrevSize(size).c_str(),
                      prefix > 0 ? fmt(" (resuming at %sB)", util::abbrevSize(prefix).c_str()).c_str() : ""));
    }
    sendDownloadFileLocked(d);
    handleFileLocked(file); // already complete (an earlier attempt finished it)
    return;
  }
  if (parseGidExtra(extra, "dl:", gid)) {
    auto it = downloads.find(gid);
    if (it == downloads.end() || it->second.phase != Phase::DOWNLOADING) {
      return;
    }
    it->second.answered = true;
    handleFileLocked(msg); // the "file" answer to downloadFile
    return;
  }
  int64_t chatId = 0, msgId = 0;
  if (parseDiscardExtra(extra, chatId, msgId)) {
    // The answer to discard()'s lookup: delete the file unless a download started using it meanwhile.
    const auto* file = messageFile(msg);
    const auto fileId = file ? static_cast<int32_t>(intField(file, "id")) : 0;
    if (fileId != 0 && !messageInUseLocked(chatId, msgId)) {
      deleteFileLocked(fileId);
      A2_LOG_INFO(fmt("[fxtg] discarded TDLib's copy of chat=%" PRId64 " msg=%" PRId64, chatId, msgId));
    }
    return;
  }
}

// The last line of helper.log (TDLib's own log, or "exec failed"), for the exit log line; "" when there is none.
std::string lastHelperLogLine()
{
  const int fd = ::open(helperLogPath().c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    return "";
  }
  char buf[1024];
  const off_t end = ::lseek(fd, 0, SEEK_END);
  const off_t from = end > static_cast<off_t>(sizeof(buf)) ? end - static_cast<off_t>(sizeof(buf)) : 0;
  const ssize_t n = ::pread(fd, buf, sizeof(buf), from);
  ::close(fd);
  if (n <= 0) {
    return "";
  }
  std::string text(buf, static_cast<size_t>(n));
  while (!text.empty() && (text.back() == '\n' || text.back() == '\r')) {
    text.pop_back();
  }
  const auto nl = text.rfind('\n');
  if (nl != std::string::npos) {
    text.erase(0, nl + 1);
  }
  // TDLib colors its lines: drop the escape sequences and anything else unprintable.
  std::string out;
  for (size_t i = 0; i < text.size(); ++i) {
    if (text[i] == '\x1b') {
      while (i < text.size() && text[i] != 'm') {
        ++i;
      }
      continue;
    }
    out += (static_cast<unsigned char>(text[i]) < 0x20) ? ' ' : text[i];
  }
  return out.size() > 200 ? out.substr(0, 200) + "..." : out;
}

void readerLoop(int fromHelper, uint64_t generation)
{
  // Plain read(), not stdio (2026-11-15): a FILE* blocked in getline holds that stream's lock, and exit() locks every
  // stream to flush it -- aria2 then hung at shutdown waiting for a helper line that never came.
  std::string pending;
  char buf[65536];
  bool stale = false;
  while (!stale) {
    const ssize_t n = ::read(fromHelper, buf, sizeof(buf));
    if (n < 0 && errno == EINTR) {
      continue;
    }
    if (n <= 0) {
      break;
    }
    pending.append(buf, static_cast<size_t>(n));
    size_t lineStart = 0;
    for (size_t nl; (nl = pending.find('\n', lineStart)) != std::string::npos; lineStart = nl + 1) {
      ssize_t error = 0;
      auto v = json::ValueBaseJsonParser().parseFinal(pending.data() + lineStart, nl - lineStart, error);
      if (error < 0) {
        continue; // nothing this module needs is lost: see handleLineLocked for the lines that matter
      }
      std::lock_guard<std::mutex> lock(mu);
      if (generation != helperGeneration) {
        stale = true;
        break;
      }
      handleLineLocked(asDict(v.get()));
    }
    pending.erase(0, lineStart);
  }
  ::close(fromHelper);
  const auto lastLine = lastHelperLogLine();

  std::lock_guard<std::mutex> lock(mu);
  if (generation != helperGeneration) {
    return;
  }
  helperAlive = false;
  if (toHelper >= 0) {
    ::close(toHelper);
    toHelper = -1;
  }
  helperPid = -1;
  exitedAt = Clock::now();
  const double uptime = secondsSince(spawnedAt, exitedAt);
  const bool expected = shuttingDown || !exitReason.empty() || auth.state == "closing";
  fastExits = !expected && exitedAt - spawnedAt < FAST_EXIT ? fastExits + 1 : 0;
  if (shuttingDown) {
    A2_LOG_WARN(fmt("[fxtg] helper exited after %.1fs (aria2 shutting down)", uptime));
  }
  else if (expected) {
    // (A signal usually means the whole service is stopping; aria2 then never gets to the restart.)
    A2_LOG_WARN(fmt("[fxtg] helper exited after %.1fs (%s); restarted in %llds unless aria2 is stopping", uptime,
                    exitReason.empty() ? "Telegram client closed" : exitReason.c_str(),
                    static_cast<long long>(RESPAWN_DELAY.count())));
  }
  else if (fastExits <= FAST_EXITS_BEFORE_SLOWDOWN || fastExits % 10 == 0) {
    // In a crash loop only every 10th exit is logged.
    const auto delay = fastExits >= FAST_EXITS_BEFORE_SLOWDOWN ? RESPAWN_DELAY_AFTER_FAST_EXITS : RESPAWN_DELAY;
    A2_LOG_ERROR(fmt("[fxtg] helper exited unexpectedly after %.1fs in state %s%s%s; restarting in %llds%s "
                     "(see %s)",
                     uptime, auth.state.c_str(), lastLine.empty() ? "" : ": ", lastLine.c_str(),
                     static_cast<long long>(delay.count()),
                     fastExits >= FAST_EXITS_BEFORE_SLOWDOWN
                         ? fmt(" (%d quick exits in a row; waiting downloads fail)", fastExits).c_str()
                         : "",
                     helperLogPath().c_str()));
  }
  auth.state = expected ? "closing" : "helper-failed";
  exitReason.clear();
  chatLoad = ChatLoad::NOT_LOADED;
  knownFiles.clear(); // file ids are the dead TDLib instance's
  requeueActiveLocked();
  helperExited.notify_all();
}

void spawnLocked()
{
  ::mkdir(dataDir.c_str(), 0700);
  // TDLib's database holds the account's session: keep the directory private even if something else created it.
  ::chmod(dataDir.c_str(), 0700);
  loadCredentialsLocked();

  const auto logPath = helperLogPath();
  struct stat st;
  if (::stat(logPath.c_str(), &st) == 0 && st.st_size > HELPER_LOG_MAX) {
    ::rename(logPath.c_str(), (logPath + ".1").c_str());
  }

  int toChild[2] = {-1, -1};
  int fromChild[2] = {-1, -1};
  if (::pipe2(toChild, O_CLOEXEC) != 0 || ::pipe2(fromChild, O_CLOEXEC) != 0) {
    A2_LOG_ERROR(fmt("[fxtg] pipe failed errno=%d", errno));
    for (int fd : {toChild[0], toChild[1], fromChild[0], fromChild[1]}) {
      if (fd >= 0) {
        ::close(fd);
      }
    }
    exitedAt = Clock::now();
    ++fastExits;
    return;
  }
  const int logFd = ::open(logPath.c_str(), O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0600);
  // Prepared before fork: between fork and exec only async-signal-safe calls (see fxRunChild).
  std::vector<char*> argv = {const_cast<char*>(helperPath.c_str()), nullptr};
  static const char execFailed[] = "aria2c-tg: exec failed\n";

  const pid_t pid = ::fork();
  if (pid == 0) {
    ::dup2(toChild[0], STDIN_FILENO);
    ::dup2(fromChild[1], STDOUT_FILENO);
    if (logFd >= 0) {
      ::dup2(logFd, STDERR_FILENO);
    }
    // aria2 ignores SIGPIPE and SIGCHLD; the helper gets the defaults back (it sets its own SIGPIPE handling).
    ::signal(SIGPIPE, SIG_DFL);
    ::signal(SIGCHLD, SIG_DFL);
    ::execv(argv[0], argv.data());
    if (::write(STDERR_FILENO, execFailed, sizeof(execFailed) - 1) < 0) {
      // nothing more to do
    }
    _exit(127);
  }
  ::close(toChild[0]);
  ::close(fromChild[1]);
  if (logFd >= 0) {
    ::close(logFd);
  }
  if (pid < 0) {
    A2_LOG_ERROR(fmt("[fxtg] fork failed errno=%d", errno));
    ::close(toChild[1]);
    ::close(fromChild[0]);
    exitedAt = Clock::now();
    ++fastExits;
    return;
  }
  helperPid = pid;
  toHelper = toChild[1];
  helperAlive = true;
  spawnedAt = Clock::now();
  exitReason.clear();
  ++helperGeneration;
  auth.state = "starting";
  auth.detail.clear();
  // aria2 runs with SIGCHLD ignored, so the helper is reaped automatically; its exit shows as EOF on its stdout.
  std::thread(readerLoop, fromChild[0], helperGeneration).detach();
  A2_LOG_WARN(fmt("[fxtg] helper started pid=%d %s", static_cast<int>(pid), helperPath.c_str()));
  // The first request starts TDLib; its answer is the authorization state.
  sendLocked(*request("getAuthorizationState", ""));
}

bool needsUserLocked()
{
  return auth.state == "wait-credentials" || auth.state == "wait-phone" || auth.state == "wait-code" ||
         auth.state == "wait-password" || auth.state == "wait-qr" || auth.state == "unsupported";
}

// Ready-state upkeep of the downloads under way: speeds, resolve timeouts, downloads TDLib stopped, stalls.
void watchDownloadsLocked(Clock::time_point now)
{
  std::vector<std::pair<a2_gid_t, std::string>> failed;
  for (auto& kv : downloads) {
    auto& d = kv.second;
    // Speeds: an average over about 5 s of samples.
    const auto elapsed = secondsSince(d.lastSampleAt, now);
    if (elapsed >= 1.0) {
      const double instant = static_cast<double>(d.downloaded - d.lastSampleBytes) / elapsed;
      d.speed = static_cast<int64_t>(d.speed * 0.8 + std::max(0.0, instant) * 0.2);
      d.lastSampleBytes = d.downloaded;
      d.lastSampleAt = now;
    }
    if (d.phase == Phase::CHAT || d.phase == Phase::MESSAGE) {
      if (now - d.phaseAt >= RESOLVE_TIMEOUT) {
        failed.emplace_back(d.gid, fmt("Telegram did not answer within %llds while looking up the message",
                                       static_cast<long long>(RESOLVE_TIMEOUT.count())));
      }
      continue;
    }
    if (d.phase != Phase::DOWNLOADING || !d.answered) {
      continue;
    }
    const auto gidHex = GroupId::toHex(d.gid);
    if (!d.tdlibActive && now - d.inactiveSince >= INACTIVE_GRACE) {
      // TDLib gave up on the download (it reports no error for that): ask again, it resumes from the partial file.
      if (d.restarts >= MAX_RESTARTS) {
        failed.emplace_back(d.gid, fmt("TDLib stopped the download %d times without progress", d.restarts + 1));
        continue;
      }
      ++d.restarts;
      A2_LOG_WARN(fmt("[fxtg] gid=%s TDLib stopped the download at %sB of %sB; asking again (%d/%d)",
                      gidHex.c_str(), util::abbrevSize(d.downloaded).c_str(), util::abbrevSize(d.total).c_str(),
                      d.restarts, MAX_RESTARTS));
      sendDownloadFileLocked(d);
      continue;
    }
    const auto still = now - d.progressAt;
    if (still >= STALL_FAIL) {
      failed.emplace_back(d.gid, fmt("no progress for %llds", static_cast<long long>(STALL_FAIL.count())));
    }
    else if (still >= STALL_WARN && !d.stallLogged) {
      d.stallLogged = true;
      A2_LOG_WARN(fmt("[fxtg] gid=%s stalled: no progress for %llds at %sB of %sB (fails after %llds)",
                      gidHex.c_str(), static_cast<long long>(STALL_WARN.count()),
                      util::abbrevSize(d.downloaded).c_str(), util::abbrevSize(d.total).c_str(),
                      static_cast<long long>(STALL_FAIL.count())));
    }
  }
  for (const auto& f : failed) {
    auto it = downloads.find(f.first);
    if (it != downloads.end()) {
      cancelTdlibDownloadLocked(it->second);
    }
    finishLocked(f.first, Result::FAILED, f.second);
  }
}

void sendPendingDiscardsLocked()
{
  size_t sent = 0;
  for (auto it = pendingDiscards.begin(); it != pendingDiscards.end() && sent < DISCARDS_PER_TICK;) {
    const auto key = *it;
    it = pendingDiscards.erase(it);
    if (messageInUseLocked(key.first, key.second)) {
      continue; // a download uses it again: its own end discards it
    }
    auto known = knownFiles.find(key);
    if (known != knownFiles.end()) {
      deleteFileLocked(known->second);
      knownFiles.erase(known);
    }
    else {
      auto r = request("getMessage", discardExtra(key.first, key.second));
      r->put("chat_id", Integer::g(key.first));
      r->put("message_id", Integer::g(key.second));
      sendLocked(*r);
    }
    ++sent;
  }
}

} // namespace

void configure(const std::string& helper, const std::string& dir)
{
  std::lock_guard<std::mutex> lock(mu);
  helperPath = helper;
  dataDir = dir;
  enabled = !helper.empty() && !dir.empty();
  auth.configured = enabled;
  auth.state = enabled ? "starting" : "disabled";
  if (enabled) {
    A2_LOG_NOTICE(fmt("[fxtg] Telegram downloads enabled helper=%s dir=%s", helper.c_str(), dir.c_str()));
  }
  else if (!helper.empty() || !dir.empty()) {
    A2_LOG_WARN("[fxtg] Telegram downloads need both --fx-telegram-helper and --fx-telegram-dir; disabled");
  }
}

bool configured()
{
  std::lock_guard<std::mutex> lock(mu);
  return enabled;
}

void start(a2_gid_t gid, int64_t chatId, int64_t msgId)
{
  std::lock_guard<std::mutex> lock(mu);
  if (!enabled) {
    finishLocked(gid, Result::FAILED, "Telegram downloads are not configured on this aria2");
    return;
  }
  Download d;
  d.gid = gid;
  d.chatId = chatId;
  d.msgId = msgId;
  d.lastSampleAt = Clock::now();
  downloads[gid] = d;
  queueLocked(downloads[gid], Clock::time_point(), false);
}

void stop(a2_gid_t gid)
{
  std::lock_guard<std::mutex> lock(mu);
  auto it = downloads.find(gid);
  if (it == downloads.end()) {
    return;
  }
  cancelTdlibDownloadLocked(it->second);
  finishLocked(gid, Result::CANCELED, "stopped");
}

void discard(int64_t chatId, int64_t msgId)
{
  std::lock_guard<std::mutex> lock(mu);
  if (!enabled || messageInUseLocked(chatId, msgId)) {
    return; // a newer job for the same message is using the file
  }
  const auto key = std::make_pair(chatId, msgId);
  auto it = knownFiles.find(key);
  if (it != knownFiles.end() && helperAlive && auth.state == "ready") {
    deleteFileLocked(it->second);
    knownFiles.erase(it);
    pendingDiscards.erase(key);
    return;
  }
  pendingDiscards.insert(key); // sent once the account is ready (tick)
}

bool progress(a2_gid_t gid, Progress& out)
{
  std::lock_guard<std::mutex> lock(mu);
  auto it = downloads.find(gid);
  if (it == downloads.end()) {
    return false;
  }
  const auto& d = it->second;
  out.downloaded = d.downloaded;
  out.total = d.total;
  out.speed = d.speed;
  out.queued = d.phase == Phase::QUEUED;
  return true;
}

std::vector<Event> takeEvents()
{
  std::lock_guard<std::mutex> lock(mu);
  std::vector<Event> out(std::make_move_iterator(events.begin()), std::make_move_iterator(events.end()));
  events.clear();
  return out;
}

void tick()
{
  std::lock_guard<std::mutex> lock(mu);
  if (!enabled || shuttingDown) {
    return;
  }
  const auto now = Clock::now();
  if (!helperAlive) {
    if (fastExits >= FAST_EXITS_BEFORE_SLOWDOWN && !downloads.empty()) {
      failAllLocked(Result::FAILED, fmt("the Telegram helper keeps exiting (see %s)", helperLogPath().c_str()));
    }
    const auto delay = fastExits >= FAST_EXITS_BEFORE_SLOWDOWN ? RESPAWN_DELAY_AFTER_FAST_EXITS : RESPAWN_DELAY;
    if (helperPid < 0 && (exitedAt == Clock::time_point() || now - exitedAt >= delay)) {
      spawnLocked();
    }
    return;
  }
  if (auth.state == "starting" && now - spawnedAt >= STARTING_TIMEOUT && toHelper >= 0) {
    A2_LOG_WARN(fmt("[fxtg] TDLib did not start within %llds; restarting the helper",
                    static_cast<long long>(STARTING_TIMEOUT.count())));
    endHelperLocked("TDLib did not start");
    return;
  }
  if (needsUserLocked()) {
    // Nothing can download until someone signs in: fail the waiting jobs (retryable) instead of holding them.
    if (!downloads.empty()) {
      failAllLocked(Result::NOT_AUTHORIZED,
                    fmt("Telegram on the server isn't signed in (state %s)", auth.state.c_str()));
    }
    return;
  }
  if (auth.state != "ready") {
    return;
  }
  watchDownloadsLocked(now);
  if (chatLoadRetryAt != Clock::time_point() && now >= chatLoadRetryAt &&
      (chatLoad == ChatLoad::LOADING_MAIN || chatLoad == ChatLoad::LOADING_ARCHIVE)) {
    chatLoadRetryAt = Clock::time_point();
    continueChatLoadLocked();
  }
  sendPendingDiscardsLocked();

  size_t active = 0;
  for (const auto& kv : downloads) {
    if (kv.second.phase != Phase::QUEUED) {
      ++active;
    }
  }
  for (auto it = queueOrder.begin(); it != queueOrder.end() && active < MAX_ACTIVE;) {
    auto d = downloads.find(*it);
    if (d == downloads.end() || d->second.phase != Phase::QUEUED) {
      it = queueOrder.erase(it);
      continue;
    }
    if (d->second.retryAt > now) {
      ++it; // a flood wait: the ones behind it may go first
      continue;
    }
    it = queueOrder.erase(it);
    requestChatLocked(d->second);
    ++active;
  }
}

void shutdown()
{
  std::unique_lock<std::mutex> lock(mu);
  if (!enabled || shuttingDown) {
    return;
  }
  shuttingDown = true;
  if (!helperAlive) {
    return;
  }
  auth.state = "closing";
  const auto startedAt = Clock::now();
  endHelperLocked("aria2 shutting down");
  if (!helperExited.wait_for(lock, SHUTDOWN_WAIT, [] { return !helperAlive; })) {
    A2_LOG_WARN(fmt("[fxtg] helper still closing after %.1fs; leaving it to finish on its own",
                    secondsSince(startedAt, Clock::now())));
  }
}

AuthState authState()
{
  std::lock_guard<std::mutex> lock(mu);
  return auth;
}

void setCredentials(int64_t id, const std::string& hash)
{
  std::lock_guard<std::mutex> lock(mu);
  if (!enabled) {
    return;
  }
  apiId = id;
  apiHash = hash;
  ::mkdir(dataDir.c_str(), 0700);
  saveCredentialsLocked();
  auth.error.clear();
  if (auth.state == "wait-credentials") {
    auth.state = "starting";
    sendTdlibParametersLocked();
  }
}

void submitPhone(const std::string& phone)
{
  std::lock_guard<std::mutex> lock(mu);
  auth.error.clear();
  auto r = request("setAuthenticationPhoneNumber", "auth");
  r->put("phone_number", phone);
  sendLocked(*r);
}

void submitCode(const std::string& code)
{
  std::lock_guard<std::mutex> lock(mu);
  auth.error.clear();
  auto r = request("checkAuthenticationCode", "auth");
  r->put("code", code);
  sendLocked(*r);
}

void submitPassword(const std::string& password)
{
  std::lock_guard<std::mutex> lock(mu);
  auth.error.clear();
  auto r = request("checkAuthenticationPassword", "auth");
  r->put("password", password);
  sendLocked(*r);
}

void requestQrLogin()
{
  std::lock_guard<std::mutex> lock(mu);
  auth.error.clear();
  auto r = request("requestQrCodeAuthentication", "auth");
  r->put("other_user_ids", List::g());
  sendLocked(*r);
}

void logOut()
{
  std::lock_guard<std::mutex> lock(mu);
  auth.error.clear();
  sendLocked(*request("logOut", "auth"));
}

} // namespace fxtg

} // namespace aria2
