/* <!-- copyright */
/*
 * aria2 - The high speed download utility
 *
 * Copyright (C) 2009 Tatsuhiro Tsujikawa
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
#include "RpcMethodImpl.h"

#include <cassert>
#include <algorithm>
#include <sstream>
#include <fstream>
#include <map>
#include <set>
#include <ctime>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <strings.h>
#include <dirent.h>
#include <sys/wait.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/stat.h>
#include <thread>
#include <mutex>
#include <atomic>
#include <chrono>
#include <memory>
#ifdef __linux__
#include <sys/prctl.h>
#endif

#include "Logger.h"
#include "LogFactory.h"
#include "DlAbortEx.h"
#include "Option.h"
#include "OptionParser.h"
#include "OptionHandler.h"
#include "DownloadEngine.h"
#include "RequestGroup.h"
#include "download_helper.h"
#include "util.h"
#include "fmt.h"
#include "RpcRequest.h"
#include "PieceStorage.h"
#include "DownloadContext.h"
#include "DiskAdaptor.h"
#include "FileEntry.h"
#include "prefs.h"
#include "message.h"
#include "FeatureConfig.h"
#include "array_fun.h"
#include "RpcMethodFactory.h"
#include "RpcResponse.h"
#include "SegmentMan.h"
#include "TimedHaltCommand.h"
#include "PeerStat.h"
#include "base64.h"
#include "BitfieldMan.h"
#include "SessionSerializer.h"
#include "MessageDigest.h"
#include "message_digest_helper.h"
#include "OpenedFileCounter.h"
#ifdef ENABLE_BITTORRENT
#  include "bittorrent_helper.h"
#  include "BtRegistry.h"
#  include "PeerStorage.h"
#  include "Peer.h"
#  include "BtRuntime.h"
#  include "BtAnnounce.h"
#endif // ENABLE_BITTORRENT
#include "CheckIntegrityEntry.h"

namespace aria2 {

namespace rpc {

namespace {
const char VLB_TRUE[] = "true";
const char VLB_FALSE[] = "false";
const char VLB_ACTIVE[] = "active";
const char VLB_WAITING[] = "waiting";
const char VLB_PAUSED[] = "paused";
const char VLB_REMOVED[] = "removed";
const char VLB_ERROR[] = "error";
const char VLB_COMPLETE[] = "complete";
const char VLB_USED[] = "used";
const char VLB_ZERO[] = "0";

const char KEY_GID[] = "gid";
const char KEY_ERROR_CODE[] = "errorCode";
const char KEY_ERROR_MESSAGE[] = "errorMessage";
const char KEY_STATUS[] = "status";
const char KEY_TOTAL_LENGTH[] = "totalLength";
const char KEY_COMPLETED_LENGTH[] = "completedLength";
const char KEY_DOWNLOAD_SPEED[] = "downloadSpeed";
const char KEY_UPLOAD_SPEED[] = "uploadSpeed";
const char KEY_UPLOAD_LENGTH[] = "uploadLength";
const char KEY_CONNECTIONS[] = "connections";
const char KEY_BITFIELD[] = "bitfield";
const char KEY_PIECE_LENGTH[] = "pieceLength";
const char KEY_NUM_PIECES[] = "numPieces";
const char KEY_FOLLOWED_BY[] = "followedBy";
const char KEY_FOLLOWING[] = "following";
const char KEY_BELONGS_TO[] = "belongsTo";
const char KEY_INFO_HASH[] = "infoHash";
const char KEY_NUM_SEEDERS[] = "numSeeders";
const char KEY_PEER_ID[] = "peerId";
const char KEY_IP[] = "ip";
const char KEY_PORT[] = "port";
const char KEY_AM_CHOKING[] = "amChoking";
const char KEY_PEER_CHOKING[] = "peerChoking";
const char KEY_SEEDER[] = "seeder";
const char KEY_INDEX[] = "index";
const char KEY_PATH[] = "path";
const char KEY_SELECTED[] = "selected";
const char KEY_LENGTH[] = "length";
const char KEY_URI[] = "uri";
const char KEY_CURRENT_URI[] = "currentUri";
const char KEY_VERSION[] = "version";
const char KEY_ENABLED_FEATURES[] = "enabledFeatures";
const char KEY_METHOD_NAME[] = "methodName";
const char KEY_PARAMS[] = "params";
const char KEY_SESSION_ID[] = "sessionId";
const char KEY_FILES[] = "files";
const char KEY_DIR[] = "dir";
const char KEY_URIS[] = "uris";
const char KEY_BITTORRENT[] = "bittorrent";
const char KEY_INFO[] = "info";
const char KEY_NAME[] = "name";
const char KEY_ANNOUNCE_LIST[] = "announceList";
const char KEY_COMMENT[] = "comment";
const char KEY_CREATION_DATE[] = "creationDate";
const char KEY_MODE[] = "mode";
const char KEY_SERVERS[] = "servers";
const char KEY_NUM_WAITING[] = "numWaiting";
const char KEY_NUM_STOPPED[] = "numStopped";
const char KEY_NUM_ACTIVE[] = "numActive";
const char KEY_NUM_STOPPED_TOTAL[] = "numStoppedTotal";
const char KEY_VERIFIED_LENGTH[] = "verifiedLength";
const char KEY_VERIFY_PENDING[] = "verifyIntegrityPending";
const char KEY_MERGE[] = "merge";
} // namespace

namespace {
enum FxMergeState {
  FX_MERGE_QUEUED = 0,
  FX_MERGE_DOWNLOADING = 1,
  FX_MERGE_MERGING = 2,
  FX_MERGE_MERGED = 3,
  FX_MERGE_FAILED = 4,
};

struct FxMergeJob {
  a2_gid_t parentGid = 0;
  std::vector<a2_gid_t> childGids;
  std::vector<std::string> segmentPaths;
  std::map<a2_gid_t, std::string> childPath;
  std::map<a2_gid_t, bool> childDone;
  std::map<a2_gid_t, bool> childOK;
  std::string outputPath;
  std::string tmpDir;
  std::string mode = "concat";
  bool remux = false;
  FxMergeState state = FX_MERGE_QUEUED;
  int errorCode = 0;
  std::string errorMessage;
  double mergeProgress = 0.0;
  bool cleanupPending = false;
  bool cleanupDone = false;
  // Backed by a shared atomic (not a plain bool) so a cancel request set on the LIVE map entry (main
  // thread, under fxMergeMutex) is visible to a finalize worker thread that took a VALUE COPY of this
  // job earlier — see fxFinalizeMergeAsync below. A plain bool copy would freeze cancellation at
  // snapshot time; every FxMergeJob copy make from this point on shares the same underlying atomic via
  // the shared_ptr, so the copy and the original always agree.
  std::shared_ptr<std::atomic<bool>> cancelFlag =
      std::make_shared<std::atomic<bool>>(false);
  bool cancelRequested() const
  {
    return cancelFlag->load(std::memory_order_relaxed);
  }
  void requestCancel() { cancelFlag->store(true, std::memory_order_relaxed); }
  std::time_t terminalEpochSec = 0;
  // 2026-11-08: segments found complete on disk from an earlier attempt and reused without a request
  // (fxMergeTrySkipCompletedChild), and the children already checked for that (checked once each).
  size_t skippedCount = 0;
  // 2026-11-14: the video's length from the site (mergeOptions.expectedDurationSeconds; 0 = unknown).
  // fxProbeMergedVideo fails a merged video that is much shorter than this.
  int64_t expectedDurationSec = 0;
  std::set<a2_gid_t> skipChecked;
  // Segment file names an earlier attempt recorded as finished (FX_COMPLETED_LIST), read once.
  bool completedListLoaded = false;
  std::set<std::string> completedOnDisk;
  // 2026-11-14: finished segment names not yet appended to FX_COMPLETED_LIST (see fxFlushCompletedList): the
  // list used to be opened and appended on the main thread once per segment.
  std::vector<std::string> completedUnflushed;
  std::chrono::steady_clock::time_point completedFlushedAt = std::chrono::steady_clock::now();
  // 2026-11-14: an app text fetch (fxplayer.readTextOutput's only client, see fxIsTextFetchOutput). Only such a
  // job may be read back -- and deleted -- by readTextOutput.
  bool textFetch = false;
  // 2026-11-14: a finished segment's size, looked up once (gatherFxMergeStatus used to stat every segment file on
  // every tellStatus poll, on the main thread).
  std::map<a2_gid_t, int64_t> finishedSize;
};

// Guards fxMergeJobs/fxMergeChildToParent/fxMergeOutputOwner. Every access to these three maps used to
// be safe by construction: aria2's RPC handlers and its group-stopped callback all run on the single
// main event-loop thread, so there was never any concurrent access to guard against. That stopped being
// true once fxFinalizeMergeAsync (below) started running the ffmpeg remux/concat/publish sequence on a
// background std::thread instead of blocking the main thread for it — see that function's comment for
// why. Every one of the ~10 functions that touch these maps now takes this lock, including ones that
// call each other while already holding it (fxPruneExpiredMergeJobs -> fxEraseMergeJob, for instance),
// hence recursive_mutex rather than plain mutex — re-auditing every call graph edge for double-locking
// was a needless source of risk when this bought the same safety for free.
std::recursive_mutex fxMergeMutex;

// Guards the single "ffmpeg remux slot": at most one finalize (fxFinalizeMerge, which forks ffmpeg and does
// the concat/remux I/O) runs at a time across the whole process, whether triggered by the automatic
// fxFinalizeMergeAsync path or the synchronous fxplayer.retryMerge RPC. Before this, three HLS downloads
// finishing around the same moment forked three concurrent ffmpeg processes on the daemon; on a resource-
// constrained box (e.g. a Raspberry Pi) that CPU/disk contention was enough to starve the RPC-handling
// thread for tens of seconds at a time, which the FXPlayer client observed as a burst of RPC polling
// timeouts right at each remux's start (root-caused 2026-08-23 by lining up FXPlayer's app.log against this
// daemon's own aria2.log). Every caller of fxFinalizeMerge takes this lock immediately around that call
// (never around anything else) and always releases it before touching fxMergeMutex again, so the two mutexes
// are never held nested in the same order twice -- fxMergeMutex may be held while acquiring this one
// (FxplayerRetryMergeRpcMethod::process does exactly that), but this one must never be held while acquiring
// fxMergeMutex, or the two could deadlock against each other. (2026-11-14: only a finalize that runs ffmpeg or ffprobe
// takes it -- see fxFinalizeNeedsSlot -- and every ffmpeg/ffprobe run has a time limit, see fxRunChild.)
//
// A job waiting for this slot needs no special app-facing status: fxFinalizeMergeAsync already flips
// job.state to FX_MERGE_MERGING (and FxplayerRetryMergeRpcMethod::process's own job.state assignment does
// the same) before the wait begins, so the app's existing "merge phase never counts as a stall" polling
// logic (WebService.swift's pollAria2UntilTerminal, isMergePhase) already treats slot-wait time exactly like
// ffmpeg-running time -- no new RPC field needed, unlike the admission-queue `waiting` counter above, which
// needed one because that wait happened in FX_MERGE_DOWNLOADING state.
std::mutex fxMergeSlotMutex;

std::map<a2_gid_t, FxMergeJob> fxMergeJobs;
std::map<a2_gid_t, a2_gid_t> fxMergeChildToParent;
// Every gid registered as a merge child whose RequestGroup or DownloadResult may still exist (fxMergeChildToParent
// is erased when the job ends, but a failed child's error result can still be serialized after that). See
// fxIsMergeChild. 2026-11-14: no longer grows forever -- an entry goes when its DownloadResult is evicted or removed
// (fxForgetMergeChild, from RequestGroupMan) or when the child is dropped from the queue without ever running.
std::set<a2_gid_t> fxEverMergeChildren;
std::map<std::string, a2_gid_t> fxMergeOutputOwner;

const int FX_MERGE_ERR_SEGMENT = 2000;
const int FX_MERGE_ERR_CONCAT_IO = 2001;
const int FX_MERGE_ERR_REMUX = 2002;
const int FX_MERGE_ERR_RENAME = 2003;
const int FX_MERGE_ERR_PATH = 2004;
const int FX_MERGE_ERR_INIT = 2005;
const int FX_MERGE_ERR_SUPERSEDED = 2006;
const int FX_MERGE_ERR_CANCELED = 2007;
// 2026-11-08: a segment's signed link expired before it could be requested (e.g. held for hours by the
// volume budget) -- nothing was sent; the job's segments are kept so a resubmit with fresh links resumes.
const int FX_MERGE_ERR_LINK_EXPIRED = 2008;
// 2026-11-08: fxplayer.stopMerge -- the app gave up on the job (retry limit, stall); segments kept.
const int FX_MERGE_ERR_STOPPED = 2009;
// 2026-11-14: the merged file is not a playable video (ffprobe finds no video stream, cannot read it, or it is
// far shorter than expected). ffmpeg -c copy can exit 0 and still write a stream-less 262-byte MP4 (seen
// 2026-08-06 on an xHamster AV1 save). Merge-phase failure: segments are not reused on resubmit.
const int FX_MERGE_ERR_INVALID_OUTPUT = 2010;

const std::time_t FX_MERGE_JOB_TTL_SECONDS = 30 * 60;

const char* fxMergeStateName(FxMergeState state)
{
  switch (state) {
  case FX_MERGE_QUEUED:
    return "queued";
  case FX_MERGE_DOWNLOADING:
    return "downloading";
  case FX_MERGE_MERGING:
    return "merging";
  case FX_MERGE_MERGED:
    return "merged";
  case FX_MERGE_FAILED:
  default:
    return "failed";
  }
}

bool fxIsTerminalState(FxMergeState state)
{
  return state == FX_MERGE_MERGED || state == FX_MERGE_FAILED;
}

std::string fxMergeOperationFromOutputPath(const std::string& outputPath)
{
  auto lower = util::toLower(outputPath);
  if (util::endsWith(lower, ".trailer.mp4")) {
    return "download trailer";
  }
  if (util::endsWith(lower, ".jpg") || util::endsWith(lower, ".jpeg") ||
      util::endsWith(lower, ".png") || util::endsWith(lower, ".webp")) {
    return "download thumbnail";
  }
  return "download video";
}

void fxSetTerminalNow(FxMergeJob& job)
{
  if (job.terminalEpochSec == 0) {
    job.terminalEpochSec = std::time(nullptr);
  }
}

void fxEraseMergeJob(a2_gid_t parentGid)
{
  std::lock_guard<std::recursive_mutex> lock(fxMergeMutex);
  auto it = fxMergeJobs.find(parentGid);
  if (it == fxMergeJobs.end()) {
    return;
  }

  const auto& job = it->second;
  for (auto gid : job.childGids) {
    fxMergeChildToParent.erase(gid);
  }

  auto outIt = fxMergeOutputOwner.find(job.outputPath);
  if (outIt != fxMergeOutputOwner.end() && outIt->second == parentGid) {
    fxMergeOutputOwner.erase(outIt);
  }

  fxMergeJobs.erase(it);
}

bool fxKeptSegments(const FxMergeJob& job);
bool fxAllChildrenDone(const FxMergeJob& job);
void fxFlushCompletedList(FxMergeJob& job);
void fxAdoptDefaultTmpDir(FxMergeJob& job);

void fxPruneExpiredMergeJobs()
{
  std::lock_guard<std::recursive_mutex> lock(fxMergeMutex);
  const auto now = std::time(nullptr);
  std::vector<a2_gid_t> expired;
  for (const auto& kv : fxMergeJobs) {
    const auto& job = kv.second;
    if (!fxIsTerminalState(job.state) || job.terminalEpochSec == 0) {
      continue;
    }
    if ((now - job.terminalEpochSec) >= FX_MERGE_JOB_TTL_SECONDS) {
      expired.push_back(kv.first);
    }
  }

  for (auto gid : expired) {
    A2_LOG_INFO(fmt("[fxmerge] evict terminal job parent=%s",
                    GroupId::toHex(gid).c_str()));
    auto& job = fxMergeJobs[gid];
    if (fxKeptSegments(job) && fxAllChildrenDone(job)) {
      fxFlushCompletedList(job);
      fxAdoptDefaultTmpDir(job); // the last chance: without the record only the default name is found
    }
    fxEraseMergeJob(gid);
  }
}

std::string fxShellQuote(const std::string& s)
{
  std::string out = "'";
  for (char c : s) {
    if (c == '\'') {
      out += "'\\''";
    }
    else {
      out += c;
    }
  }
  out += "'";
  return out;
}

std::string fxZeroPadIndex(size_t index)
{
  return fmt("%08lu", static_cast<unsigned long>(index));
}

// A segment's file name: its index plus a fingerprint of the segment's own name in the playlist (the URI's
// last two path components, query and signed tokens dropped). Failed saves keep their segments for the next attempt (2026-11-08),
// and the fingerprint keeps a retry that re-resolved to a different playlist (another quality) from ever
// reusing a file that belongs to the old one -- only a segment with the same name at the same index matches.
std::string fxSegmentFileName(size_t index, const std::string& uri)
{
  // The last two path components: some CDNs name the rendition in the directory, not the file
  // (".../720p.h264.mp4/seg-1-v1-a1.ts"). A component that looks like a signed token (contains ',' or '=')
  // is left out, since it changes on every resolve.
  const auto path = uri.substr(0, uri.find_first_of("?#"));
  std::string material;
  size_t end = path.size();
  for (int taken = 0; taken < 2 && end > 0;) {
    const auto slash = path.rfind('/', end - 1);
    const auto begin = slash == std::string::npos ? 0 : slash + 1;
    const auto component = path.substr(begin, end - begin);
    if (component.find_first_of(",=") == std::string::npos && component.find("://") == std::string::npos &&
        !component.empty()) {
      material = component + "/" + material;
      ++taken;
    }
    if (slash == std::string::npos || slash == 0) {
      break;
    }
    end = slash;
  }
  uint32_t h = 2166136261u; // FNV-1a: stable across builds, unlike std::hash
  for (unsigned char c : material) {
    h ^= c;
    h *= 16777619u;
  }
  return fmt("%s-%08x", fxZeroPadIndex(index).c_str(), static_cast<unsigned int>(h));
}

const String* getStringField(const Dict* dict, const char* key)
{
  if (!dict) {
    return nullptr;
  }
  return downcast<String>(dict->get(key));
}

const Integer* getIntegerField(const Dict* dict, const char* key)
{
  if (!dict) {
    return nullptr;
  }
  return downcast<Integer>(dict->get(key));
}

std::string getHeadersFieldAsOptionValue(const Dict* dict, const char* key)
{
  if (!dict) {
    return "";
  }
  auto value = dict->get(key);
  if (!value) {
    return "";
  }

  if (const auto* s = downcast<String>(value)) {
    return s->s();
  }

  const auto* list = downcast<List>(value);
  if (!list) {
    return "";
  }

  std::string joined;
  for (const auto& elem : *list) {
    const auto* s = downcast<String>(elem);
    if (!s) {
      continue;
    }
    const auto& line = s->s();
    if (line.empty()) {
      continue;
    }
    if (!joined.empty()) {
      joined += "\n";
    }
    joined += line;
  }
  return joined;
}

// FXPlayer extension: encodes mergeOptions["cookies"] — a JSON object mapping domain -> cookie
// string, e.g. {"xnxx.com": "sessid=abc; csrf=xyz", "cdn77.xnxx-cdn.com": "cf_clearance=..."} —
// into PREF_FX_COOKIES's "domain\tcookieValue" (one per line, joined by "\n") on-the-wire format.
// Returns {present=false} when the "cookies" key is absent entirely (old caller / no cookies to
// manage), so FxplayerAddMergeRpcMethod::process can leave PREF_FX_COOKIES undefined and preserve
// the pre-existing cookieStorage_ fallback behavior for anyone who doesn't use this — see
// HttpRequest::createRequest's own comment for why "option not set at all" must stay
// distinguishable from "option set to an empty map" (the latter means the app IS the cookie
// authority for this job, just has nothing for any domain seen so far).
struct FxCookiesField {
  bool present = false;
  std::string encoded;
};

FxCookiesField getFxCookiesFieldAsOptionValue(const Dict* dict, const char* key)
{
  FxCookiesField result;
  if (!dict) {
    return result;
  }
  const auto* cookiesDict = downcast<Dict>(dict->get(key));
  if (!cookiesDict) {
    return result;
  }
  result.present = true;
  std::string encoded;
  for (const auto& elem : *cookiesDict) {
    const auto* value = downcast<String>(elem.second.get());
    if (!value) {
      continue;
    }
    // Domain and cookie value both come from the app's own HTTPCookieStorage-derived data, never
    // user-typed free text, so a bare tab/newline appearing in either is not expected — still,
    // skip (rather than corrupt the line-based encoding) if one ever does.
    if (elem.first.find('\t') != std::string::npos ||
        elem.first.find('\n') != std::string::npos ||
        value->s().find('\t') != std::string::npos ||
        value->s().find('\n') != std::string::npos) {
      continue;
    }
    if (!encoded.empty()) {
      encoded += "\n";
    }
    encoded += elem.first;
    encoded += "\t";
    encoded += value->s();
  }
  result.encoded = std::move(encoded);
  return result;
}

// FXPlayer extension (2026-09-21): encodes mergeOptions["allowedHostSuffixes"] — a JSON array of
// host suffix strings, e.g. ["xnxx-cdn.com"] — into PREF_FX_ALLOWED_HOST_SUFFIXES's one-per-line
// "\n"-joined wire format (see that pref's own doc comment in prefs.h for the enforcement point
// and the "present vs absent" semantics — mirrors FxCookiesField exactly, for the same reason:
// this must stay distinguishable from "not present" so a job that legitimately has zero allowed
// suffixes still fails closed instead of silently becoming unrestricted).
struct FxAllowedHostSuffixesField {
  bool present = false;
  std::string encoded;
};

FxAllowedHostSuffixesField getFxAllowedHostSuffixesFieldAsOptionValue(const Dict* dict,
                                                                       const char* key)
{
  FxAllowedHostSuffixesField result;
  if (!dict) {
    return result;
  }
  const auto* list = downcast<List>(dict->get(key));
  if (!list) {
    return result;
  }
  result.present = true;
  std::string encoded;
  for (const auto& elem : *list) {
    const auto* s = downcast<String>(elem.get());
    if (!s) {
      continue;
    }
    const auto& suffix = s->s();
    // Suffixes are simple hostnames from the app's own fixed source list, never user-typed free
    // text — a bare newline is not expected, but skip (rather than corrupt the line-based
    // encoding, which could silently merge two suffixes into one or truncate the list) if one
    // ever does.
    if (suffix.empty() || suffix.find('\n') != std::string::npos) {
      continue;
    }
    if (!encoded.empty()) {
      encoded += "\n";
    }
    encoded += suffix;
  }
  result.encoded = std::move(encoded);
  return result;
}

// FXPlayer extension: decodes mergeOptions["domainConnectionCaps"] — a JSON object mapping
// domain -> integer connection cap, e.g. {"hls-gold-cdn77.xnxx-cdn.com": 1} — for
// RequestGroupMan::setDomainConnectionCapOverride. Unlike cookies, an absent or empty
// "domainConnectionCaps" is a pure no-op (no distinction needed between "not present" and
// "present but empty" — an override is a positive per-domain assertion, not an authority claim
// over a whole option class, so there's nothing to suppress by its mere presence).
std::vector<std::pair<std::string, int>>
getFxDomainConnectionCaps(const Dict* dict, const char* key)
{
  std::vector<std::pair<std::string, int>> result;
  if (!dict) {
    return result;
  }
  const auto* capsDict = downcast<Dict>(dict->get(key));
  if (!capsDict) {
    return result;
  }
  for (const auto& elem : *capsDict) {
    if (const auto* value = downcast<Integer>(elem.second.get())) {
      // Range-checked BEFORE narrowing: a huge or negative value would otherwise truncate to 0 or
      // less, which removes the override and silently lifts the very clamp the caller asked for.
      const auto v = static_cast<int64_t>(value->i());
      if (v >= 1 && v <= 1000) {
        result.emplace_back(elem.first, static_cast<int>(v));
      }
    }
  }
  return result;
}

// FXPlayer extension: decodes mergeOptions["domainMinAdmissionIntervalMs"] — a JSON object
// mapping domain -> minimum milliseconds between successive connection admissions, e.g.
// {"hls-gold-cdn77.xnxx-cdn.com": 1500} — for
// RequestGroupMan::setDomainMinAdmissionIntervalOverride. Same shape and same no-op-when-absent
// semantics as getFxDomainConnectionCaps above.
std::vector<std::pair<std::string, int>>
getFxDomainMinAdmissionIntervals(const Dict* dict, const char* key)
{
  std::vector<std::pair<std::string, int>> result;
  if (!dict) {
    return result;
  }
  const auto* intervalsDict = downcast<Dict>(dict->get(key));
  if (!intervalsDict) {
    return result;
  }
  for (const auto& elem : *intervalsDict) {
    if (const auto* value = downcast<Integer>(elem.second.get())) {
      // 0 is meaningful (clears the override); cap at one hour.
      const auto v = static_cast<int64_t>(value->i());
      if (v >= 0 && v <= 3600000) {
        result.emplace_back(elem.first, static_cast<int>(v));
      }
    }
  }
  return result;
}

// FXPlayer extension: decodes mergeOptions["domainVolumeBudget"] — a JSON object mapping
// domain -> {"count": N, "windowSeconds": S}, e.g. {"hls*.xnxx-cdn.com": {"count": 900,
// "windowSeconds": 43200}} — for RequestGroupMan::setDomainVolumeBudgetOverride. Range-checked
// before use for the same reason getFxDomainConnectionCaps/getFxDomainMinAdmissionIntervals are:
// an out-of-range value silently truncating to something permissive would defeat the whole point
// of the budget with no visible symptom.
// A volume budget's window is at most a week; app-reported (external) admissions older than that, or more than
// FX_EXTERNAL_VOLUME_MAX_SKEW_SECONDS in the future (clock skew), are ignored, and at most
// FX_EXTERNAL_VOLUME_MAX_ENTRIES are taken per budget key.
const int64_t FX_MAX_BUDGET_WINDOW_SECONDS = 7 * 24 * 3600;
const int64_t FX_EXTERNAL_VOLUME_MAX_AGE_SECONDS = FX_MAX_BUDGET_WINDOW_SECONDS;
const int64_t FX_EXTERNAL_VOLUME_MAX_SKEW_SECONDS = 300;
const size_t FX_EXTERNAL_VOLUME_MAX_ENTRIES = 100000;

struct FxVolumeBudgetEntry {
  std::string domain;
  int count;
  int windowSeconds;
};

std::vector<FxVolumeBudgetEntry>
getFxDomainVolumeBudgets(const Dict* dict, const char* key)
{
  std::vector<FxVolumeBudgetEntry> result;
  if (!dict) {
    return result;
  }
  const auto* budgetsDict = downcast<Dict>(dict->get(key));
  if (!budgetsDict) {
    return result;
  }
  for (const auto& elem : *budgetsDict) {
    const auto* sub = downcast<Dict>(elem.second.get());
    if (!sub) {
      continue;
    }
    const auto* countVal = downcast<Integer>(sub->get("count"));
    const auto* windowVal = downcast<Integer>(sub->get("windowSeconds"));
    if (!countVal || !windowVal) {
      continue;
    }
    const auto count = static_cast<int64_t>(countVal->i());
    const auto window = static_cast<int64_t>(windowVal->i());
    // count<=0 is meaningful (clears the override, via setDomainVolumeBudgetOverride's own
    // contract) so it's allowed through; an upper bound still guards against a nonsensical value.
    if (count > 1000000 || window < 1 || window > FX_MAX_BUDGET_WINDOW_SECONDS) {
      continue;
    }
    result.push_back({elem.first, static_cast<int>(count), static_cast<int>(window)});
  }
  return result;
}

// FXPlayer extension (2026-11-07): decodes mergeOptions["domainVolumeBudgetExternal"] — a JSON object mapping
// domain -> [unix seconds, ...]: the FXPlayer app's own in-app admissions against the same volume budget (see
// RequestGroupMan::domainVolumeBudgetExternal_). Entries outside a sane range are skipped; the list is capped
// so a malformed request can't balloon memory.
std::vector<std::pair<std::string, std::vector<std::chrono::system_clock::time_point>>>
getFxDomainVolumeBudgetExternal(const Dict* dict, const char* key)
{
  std::vector<std::pair<std::string, std::vector<std::chrono::system_clock::time_point>>> result;
  if (!dict) {
    return result;
  }
  const auto* extDict = downcast<Dict>(dict->get(key));
  if (!extDict) {
    return result;
  }
  const int64_t nowSec = std::chrono::duration_cast<std::chrono::seconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count();
  for (const auto& elem : *extDict) {
    const auto* list = downcast<List>(elem.second.get());
    if (!list) {
      continue;
    }
    std::vector<std::chrono::system_clock::time_point> times;
    for (const auto& v : *list) {
      const auto* n = downcast<Integer>(v.get());
      if (!n) {
        continue;
      }
      const int64_t sec = n->i();
      // Within the last 7 days and not in the future (small clock skew tolerated).
      if (sec < nowSec - FX_EXTERNAL_VOLUME_MAX_AGE_SECONDS || sec > nowSec + FX_EXTERNAL_VOLUME_MAX_SKEW_SECONDS) {
        continue;
      }
      times.push_back(std::chrono::system_clock::time_point(std::chrono::seconds(sec)));
      if (times.size() >= FX_EXTERNAL_VOLUME_MAX_ENTRIES) {
        break;
      }
    }
    result.emplace_back(elem.first, std::move(times));
  }
  return result;
}

// FXPlayer extension: decodes mergeOptions["domainVideoGapMs"] — a JSON object mapping
// domain -> {"minMs": N, "maxMs": M} — for RequestGroupMan::setDomainVideoGapOverride. Same
// range-checking rationale as getFxDomainVolumeBudgets above.
struct FxVideoGapEntry {
  std::string domain;
  int minMs;
  int maxMs;
};

std::vector<FxVideoGapEntry> getFxDomainVideoGaps(const Dict* dict, const char* key)
{
  std::vector<FxVideoGapEntry> result;
  if (!dict) {
    return result;
  }
  const auto* gapsDict = downcast<Dict>(dict->get(key));
  if (!gapsDict) {
    return result;
  }
  for (const auto& elem : *gapsDict) {
    const auto* sub = downcast<Dict>(elem.second.get());
    if (!sub) {
      continue;
    }
    const auto* maxVal = downcast<Integer>(sub->get("maxMs"));
    if (!maxVal) {
      continue;
    }
    const auto* minVal = downcast<Integer>(sub->get("minMs"));
    auto minMs = minVal ? static_cast<int64_t>(minVal->i()) : int64_t(0);
    const auto maxMs = static_cast<int64_t>(maxVal->i());
    // maxMs<=0 is meaningful (clears the override); an upper bound of 10 minutes guards against a
    // value large enough to look like a hang.
    if (maxMs > 600000 || minMs < 0) {
      continue;
    }
    result.push_back({elem.first, static_cast<int>(minMs), static_cast<int>(maxMs)});
  }
  return result;
}

bool getBoolField(const Dict* dict, const char* key, bool defval)
{
  if (!dict) {
    return defval;
  }
  if (const auto* b = downcast<Bool>(dict->get(key))) {
    return b->val();
  }
  if (const auto* s = downcast<String>(dict->get(key))) {
    auto t = s->s();
    util::lowercase(t);
    return t == "true" || t == "1";
  }
  return defval;
}

bool fxAllChildrenDone(const FxMergeJob& job)
{
  for (auto gid : job.childGids) {
    auto itr = job.childDone.find(gid);
    if (itr == job.childDone.end() || !itr->second) {
      return false;
    }
  }
  return true;
}

bool fxAnyChildFailed(const FxMergeJob& job)
{
  for (auto gid : job.childGids) {
    auto itr = job.childOK.find(gid);
    if (itr != job.childOK.end() && !itr->second) {
      return true;
    }
  }
  return false;
}

// The app-facing terminal-state fields, shared by aria2.tellStatus (gatherFxMergeStatus) and
// fxplayer.findMergeByOutput so the two can never disagree. retryMerge-eligible: failed, every
// segment downloaded fine, not a cancel, and the segments are still on disk.
bool fxIsRetryable(const FxMergeJob& job)
{
  return job.state == FX_MERGE_FAILED && fxAllChildrenDone(job) && !fxAnyChildFailed(job) &&
         job.errorCode != FX_MERGE_ERR_CANCELED && !job.cleanupDone;
}

// A failed job whose segments are still on disk for the next attempt (2026-11-08): a segment failure,
// expired links or fxplayer.stopMerge keep them; a cancel/delete or supersede cleans them up.
bool fxKeptSegments(const FxMergeJob& job)
{
  // Download-phase failures only: after a concat/remux/rename failure the segments themselves may be the
  // problem, so a full retry downloads them again (as before 2026-11-08) instead of re-merging the same files.
  const bool downloadPhase = job.errorCode == FX_MERGE_ERR_SEGMENT ||
                             job.errorCode == FX_MERGE_ERR_LINK_EXPIRED || job.errorCode == FX_MERGE_ERR_STOPPED;
  return job.state == FX_MERGE_FAILED && downloadPhase && !job.cleanupPending && !job.cleanupDone;
}

// Whether a download-phase failure keeps `job`'s segments for the next attempt: only multi-segment jobs. A
// single-file job (playlist fetch, thumbnail, trailer) has nothing worth resuming and is cleaned up as before.
bool fxKeepsSegmentsOnFailure(const FxMergeJob& job)
{
  return job.childGids.size() > 1;
}

// Name of the per-job list of segments aria2 itself finished (one file name per line), in its tmpDir. Only a
// listed segment is ever reused by a later attempt: "no .aria2 control file" alone isn't proof of a complete
// file (a download of unknown length is resumed without one).
const char FX_COMPLETED_LIST[] = ".fx-completed";

const char* fxMergeOutcome(const FxMergeJob& job, bool retryable)
{
  if (job.state == FX_MERGE_MERGED) {
    return "succeeded";
  }
  if (job.state == FX_MERGE_FAILED) {
    return retryable ? "retryable-failure" : "failed";
  }
  return "running";
}

// Top-level aria2-style `status`: complete/error once terminal, otherwise always active.
const char* fxMergeStatusValue(const FxMergeJob& job)
{
  if (job.state == FX_MERGE_MERGED) {
    return VLB_COMPLETE;
  }
  if (job.state == FX_MERGE_FAILED) {
    return VLB_ERROR;
  }
  return VLB_ACTIVE;
}

bool fxCollectPendingAria2Sidecars(const std::string& dir,
                                   std::vector<std::string>& pending,
                                   std::string& err, bool& dirMissing)
{
  dirMissing = false;
  DIR* d = opendir(dir.c_str());
  if (!d) {
    dirMissing = (errno == ENOENT);
    err = fmt("could not open segments directory: %s errno=%d", dir.c_str(),
              errno);
    return false;
  }

  while (auto* entry = readdir(d)) {
    const char* name = entry->d_name;
    if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) {
      continue;
    }

    std::string base(name);
    if (!util::endsWith(base, ".aria2")) {
      continue;
    }
    pending.push_back(base);
  }

  closedir(d);
  return true;
}

// Sanity check used when the segments directory itself is unreadable (see
// fxSegmentsReadyForFinalize below): verifies every expected segment file is
// actually present and non-empty on disk, independent of the directory
// listing that just failed.
bool fxAllSegmentFilesPresent(const FxMergeJob& job, std::string& err)
{
  for (const auto& path : job.segmentPaths) {
    File f(path);
    if (!f.isFile() || f.size() <= 0) {
      err = fmt("expected segment file missing or empty: %s", path.c_str());
      return false;
    }
  }
  return true;
}

bool fxSegmentsReadyForFinalize(FxMergeJob& job, std::string& err)
{
  std::vector<std::string> pendingSidecars;
  bool dirMissing = false;
  if (!fxCollectPendingAria2Sidecars(job.tmpDir, pendingSidecars, err,
                                     dirMissing)) {
    // Observed intermittently (3 times in production so far, always on a
    // small/fast single-segment job racing alongside other concurrent
    // downloads): opendir() on tmpDir fails with ENOENT even though the
    // child download already completed successfully and its file is on
    // disk. Root cause not confirmed, but since the actual segment file's
    // presence is the thing that matters (not the directory listing used
    // to check for leftover .aria2 sidecars), fall back to verifying the
    // file directly rather than failing a completed download.
    if (dirMissing) {
      std::string filesErr;
      if (fxAllSegmentFilesPresent(job, filesErr)) {
        A2_LOG_WARN(fmt("[fxmerge] parent=%s segments directory listing failed (%s) "
                        "but all %lu segment file(s) verified present on disk; proceeding",
                        GroupId::toHex(job.parentGid).c_str(), err.c_str(),
                        static_cast<unsigned long>(job.segmentPaths.size())));
        return true;
      }
    }
    return false;
  }
  {
    // Only this job's own segments count: the directory can also hold a kept partial from an earlier
    // attempt that resolved to a different playlist (see fxSegmentFileName), which this job never finishes.
    std::set<std::string> own;
    for (const auto& path : job.segmentPaths) {
      own.insert(File(path).getBasename() + ".aria2");
    }
    pendingSidecars.erase(std::remove_if(pendingSidecars.begin(), pendingSidecars.end(),
                                         [&own](const std::string& name) { return own.count(name) == 0; }),
                          pendingSidecars.end());
  }
  if (!pendingSidecars.empty()) {
    std::ostringstream oss;
    const auto sampleCount = std::min<size_t>(pendingSidecars.size(), 3);
    for (size_t i = 0; i < sampleCount; ++i) {
      if (i != 0) {
        oss << ", ";
      }
      oss << pendingSidecars[i];
    }
    err = fmt("segment sidecars still present (%lu), e.g. %s",
              static_cast<unsigned long>(pendingSidecars.size()),
              oss.str().c_str());
    return false;
  }

  return true;
}

// fxWaitForSegmentsReady checks up to FX_SEGMENTS_READY_ATTEMPTS times, FX_SEGMENTS_READY_INTERVAL_US apart (~6 s).
const size_t FX_SEGMENTS_READY_ATTEMPTS = 120;
const useconds_t FX_SEGMENTS_READY_INTERVAL_US = 50000;

bool fxWaitForSegmentsReady(FxMergeJob& job, std::string& err)
{
  // aria2 may still be finalizing segment files after child-stop callbacks fire.
  const size_t maxAttempts = FX_SEGMENTS_READY_ATTEMPTS;
  std::string lastErr;

  for (size_t attempt = 0; attempt < maxAttempts; ++attempt) {
    if (job.cancelRequested()) {
      err = "merge canceled by user";
      return false;
    }

    if (fxSegmentsReadyForFinalize(job, lastErr)) {
      return true;
    }

    if (attempt == 0 || (attempt + 1) == maxAttempts || (attempt % 20) == 19) {
      A2_LOG_WARN(fmt("[fxmerge] parent=%s waiting for segment finalization attempt=%lu/%lu reason=%s",
                      GroupId::toHex(job.parentGid).c_str(),
                      static_cast<unsigned long>(attempt + 1),
                      static_cast<unsigned long>(maxAttempts),
                      lastErr.c_str()));
    }
    usleep(FX_SEGMENTS_READY_INTERVAL_US);
  }

  err = lastErr.empty() ? "segment files not fully finalized" : lastErr;
  return false;
}

// Deletes `path` and, if it is a directory, everything under it. A symlink is removed itself, never followed
// (2026-11-14: File::isDir() stats through links, so a link inside a segments directory deleted its target's tree).
bool fxRemoveTree(const std::string& path)
{
  struct stat st;
  if (lstat(path.c_str(), &st) != 0) {
    return errno == ENOENT;
  }
  if (!S_ISDIR(st.st_mode)) {
    return unlink(path.c_str()) == 0 || errno == ENOENT;
  }

  DIR* dir = opendir(path.c_str());
  if (!dir) {
    return false;
  }

  bool ok = true;
  while (auto* entry = readdir(dir)) {
    const char* name = entry->d_name;
    if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) {
      continue;
    }
    if (!fxRemoveTree(util::applyDir(path, name))) {
      ok = false;
    }
  }
  closedir(dir);

  if (rmdir(path.c_str()) != 0 && errno != ENOENT) {
    ok = false;
  }
  return ok;
}

std::string fxMergePartPath(const FxMergeJob& job)
{
  return util::applyDir(job.tmpDir, ".fxmerge-output.part");
}

void fxMarkFailed(FxMergeJob& job, int code, const std::string& msg,
                  bool cleanupPending = false)
{
  job.state = FX_MERGE_FAILED;
  job.errorCode = code;
  job.errorMessage = msg;
  job.cleanupPending = cleanupPending;
  fxSetTerminalNow(job);
  // Expected app actions (cancel, stop, supersede) and a refused expired link are not errors (2026-11-14 review).
  const bool routine = code == FX_MERGE_ERR_CANCELED || code == FX_MERGE_ERR_STOPPED ||
                       code == FX_MERGE_ERR_SUPERSEDED || code == FX_MERGE_ERR_LINK_EXPIRED;
  A2_LOG(routine ? Logger::A2_WARN : Logger::A2_ERROR,
         fmt("[fxmerge] parent=%s failed code=%d message=%s",
             GroupId::toHex(job.parentGid).c_str(), code, msg.c_str()));
}

// fxNoteSegmentCompleted appends to FX_COMPLETED_LIST once FX_COMPLETED_FLUSH_BATCH names are waiting or
// FX_COMPLETED_FLUSH_INTERVAL has passed since the last write, and always once the job has failed and every
// child has reported (fxSettleFailedJob), i.e. before any later attempt can read the list. A crash in between
// only loses reuse of those few segments (they download again), never correctness: an unlisted segment is
// never reused.
const size_t FX_COMPLETED_FLUSH_BATCH = 32;
const auto FX_COMPLETED_FLUSH_INTERVAL = std::chrono::seconds(10);

void fxFlushCompletedList(FxMergeJob& job)
{
  if (job.completedUnflushed.empty()) {
    return;
  }
  std::ofstream list(util::applyDir(job.tmpDir, FX_COMPLETED_LIST), std::ios::app);
  for (const auto& name : job.completedUnflushed) {
    list << name << '\n';
  }
  list.close();
  if (!list) {
    A2_LOG_INFO(fmt("[fxmerge] parent=%s could not append %lu name(s) to %s in %s",
                    GroupId::toHex(job.parentGid).c_str(),
                    static_cast<unsigned long>(job.completedUnflushed.size()), FX_COMPLETED_LIST,
                    job.tmpDir.c_str()));
  }
  job.completedUnflushed.clear();
  job.completedFlushedAt = std::chrono::steady_clock::now();
}

void fxNoteSegmentCompleted(FxMergeJob& job, const std::string& name)
{
  job.completedUnflushed.push_back(name);
  if (job.completedUnflushed.size() >= FX_COMPLETED_FLUSH_BATCH ||
      std::chrono::steady_clock::now() - job.completedFlushedAt >= FX_COMPLETED_FLUSH_INTERVAL) {
    fxFlushCompletedList(job);
  }
}

// 2026-11-14: a job that superseded a LIVE one downloads into a time-suffixed directory
// ("<output>.segments.<time>.<gid>", see fxSupersedeOwnerForOverwrite). Once it has failed keeping its segments,
// move that directory to the default "<output>.segments" -- the only name a later attempt finds without this
// job's record, which is gone 30 minutes after it ends (FX_MERGE_JOB_TTL_SECONDS) or at a restart. Skipped while
// the default name exists (e.g. the superseded job has not cleaned up yet; tried again when it does, see
// fxCleanupFailureArtifacts) or when the rename fails (tried again when the record expires).
void fxAdoptDefaultTmpDir(FxMergeJob& job)
{
  const auto defaultDir = job.outputPath + ".segments";
  if (job.tmpDir == defaultDir || job.tmpDir.compare(0, defaultDir.size() + 1, defaultDir + ".") != 0 ||
      job.tmpDir.find('/', defaultDir.size()) != std::string::npos) {
    return;
  }
  struct stat st;
  if (lstat(defaultDir.c_str(), &st) == 0 || errno != ENOENT) {
    return;
  }
  if (rename(job.tmpDir.c_str(), defaultDir.c_str()) != 0) {
    A2_LOG_INFO(fmt("[fxmerge] parent=%s could not move kept segments %s to %s errno=%d",
                    GroupId::toHex(job.parentGid).c_str(), job.tmpDir.c_str(), defaultDir.c_str(), errno));
    return;
  }
  const auto oldDir = job.tmpDir;
  auto rebase = [&oldDir, &defaultDir](std::string& path) {
    if (path.compare(0, oldDir.size() + 1, oldDir + "/") == 0) {
      path = defaultDir + path.substr(oldDir.size());
    }
  };
  for (auto& path : job.segmentPaths) {
    rebase(path);
  }
  for (auto& entry : job.childPath) {
    rebase(entry.second);
  }
  job.tmpDir = defaultDir;
  A2_LOG_INFO(fmt("[fxmerge] parent=%s kept segments moved %s -> %s", GroupId::toHex(job.parentGid).c_str(),
                  oldDir.c_str(), defaultDir.c_str()));
}

void fxCleanupFailureArtifacts(FxMergeJob& job)
{
  if (job.cleanupDone) {
    return;
  }
  // The name fxRemuxSegments writes (2026-11-14 review: this said "ffconcat.list", which is never created).
  const auto listPath = util::applyDir(job.tmpDir, "concat.list");
  const auto playlistPath = util::applyDir(job.tmpDir, "remux.m3u8");
  File(fxMergePartPath(job)).remove();
  File(listPath).remove();
  File(playlistPath).remove();
  for (const auto& segmentPath : job.segmentPaths) {
    File(segmentPath).remove();
  }
  fxRemoveTree(job.tmpDir);
  A2_LOG_INFO(fmt("[fxmerge] parent=%s cleanup complete tmpDir=%s",
                  GroupId::toHex(job.parentGid).c_str(), job.tmpDir.c_str()));
  job.cleanupDone = true;
  job.completedUnflushed.clear();
  // A superseded job's directory may have been the default name the job that replaced it is waiting for.
  auto ownerItr = fxMergeOutputOwner.find(job.outputPath);
  if (ownerItr != fxMergeOutputOwner.end() && ownerItr->second != job.parentGid) {
    auto ownerJob = fxMergeJobs.find(ownerItr->second);
    if (ownerJob != fxMergeJobs.end() && fxKeptSegments(ownerJob->second) && fxAllChildrenDone(ownerJob->second)) {
      fxAdoptDefaultTmpDir(ownerJob->second);
    }
  }
}

// A failed job whose children have all reported: delete its files when it doesn't keep them, otherwise write out
// its completed-segment list and give a time-suffixed directory the default name. No-op for anything else, so it
// is safe to call after every child report.
void fxSettleFailedJob(FxMergeJob& job)
{
  if (job.state != FX_MERGE_FAILED || !fxAllChildrenDone(job)) {
    return;
  }
  if (job.cleanupPending) {
    fxCleanupFailureArtifacts(job);
  }
  else if (fxKeptSegments(job)) {
    fxFlushCompletedList(job);
    fxAdoptDefaultTmpDir(job);
  }
}

// A child removed straight from the reserved queue never goes through fxMergeOnGroupStopped, so
// without this its childDone flag would stay false forever and the job could never finish cleanup.
void fxMarkReservedChildRemoved(FxMergeJob& job, a2_gid_t childGid)
{
  job.childDone[childGid] = true;
  job.childOK[childGid] = false;
  fxEverMergeChildren.erase(childGid); // dropped from the queue: no group or result left to serialize
}

// After a user cancel has removed reserved children: if that accounts for every child (i.e. nothing
// is left running that would report back on its own), finish the job now instead of leaving it in
// DOWNLOADING with its segment directory on disk indefinitely. Never touches a job that is
// already merging -- the merge worker owns its files and handles cancel itself.
void fxFinishCancelIfNothingRunning(FxMergeJob& job)
{
  if (job.state != FX_MERGE_DOWNLOADING && job.state != FX_MERGE_QUEUED &&
      job.state != FX_MERGE_FAILED) {
    return;
  }
  if (!fxAllChildrenDone(job)) {
    return;
  }
  if (job.state != FX_MERGE_FAILED) {
    fxMarkFailed(job, FX_MERGE_ERR_CANCELED, "merge canceled by user", true);
  }
  if (job.cleanupPending) {
    fxCleanupFailureArtifacts(job);
  }
}

// Stop every child of `job` that hasn't reported back yet: active ones are force-halted (they
// report through fxMergeOnGroupStopped on a later tick), queued ones are dropped from the reserved
// queue right away. Safe to call from inside RequestGroupMan's stopped-group scan: it only sets
// flags on active groups and edits the separate reserved queue.
void fxHaltRemainingChildren(FxMergeJob& job, DownloadEngine* e)
{
  size_t halted = 0, dropped = 0;
  for (auto childGid : job.childGids) {
    if (job.childDone[childGid]) {
      continue;
    }
    auto child = e->getRequestGroupMan()->findGroup(childGid);
    if (!child) {
      continue;
    }
    if (child->getState() == RequestGroup::STATE_ACTIVE) {
      child->setForceHaltRequested(true, RequestGroup::USER_REQUEST);
      ++halted;
    }
    else if (child->isDependencyResolved()) {
      if (e->getRequestGroupMan()->removeReservedGroup(childGid)) {
        ++dropped;
      }
      fxMarkReservedChildRemoved(job, childGid);
    }
  }
  e->setRefreshInterval(std::chrono::milliseconds(0));
  if (halted + dropped > 0) {
    A2_LOG_WARN(fmt("[fxmerge] parent=%s stopping remaining children: %lu halted, %lu dropped from queue",
                    GroupId::toHex(job.parentGid).c_str(),
                    static_cast<unsigned long>(halted), static_cast<unsigned long>(dropped)));
  }
}

bool fxConcatSegments(FxMergeJob& job, const std::string& partPath,
                      std::string& err)
{
  std::ofstream out(partPath.c_str(), std::ios::binary | std::ios::trunc);
  if (!out) {
    err = "could not open part file for writing";
    return false;
  }

  const auto totalSegments = job.segmentPaths.size();
  size_t idx = 0;
  for (const auto& segmentPath : job.segmentPaths) {
    if (job.cancelRequested()) {
      err = "merge canceled by user";
      return false;
    }

    std::ifstream in(segmentPath.c_str(), std::ios::binary);
    if (!in) {
      err = fmt("could not open segment: %s", segmentPath.c_str());
      return false;
    }
    // An empty segment makes `out << rdbuf()` set failbit even though nothing is wrong.
    if (in.peek() != std::ifstream::traits_type::eof()) {
      out << in.rdbuf();
    }
    if (!out) {
      err = fmt("write failed while concatenating: %s", segmentPath.c_str());
      return false;
    }

    ++idx;
    if (totalSegments > 0) {
      job.mergeProgress = static_cast<double>(idx) /
                          static_cast<double>(totalSegments);
    }
  }
  out.flush();
  if (!out) {
    err = "flush failed for part file";
    return false;
  }
  return true;
}

bool fxSyncFile(const std::string& path, std::string& err)
{
  int fd = open(path.c_str(), O_RDONLY);
  if (fd == -1) {
    err = fmt("failed to open for fsync: %s errno=%d", path.c_str(), errno);
    return false;
  }
  if (fsync(fd) != 0) {
    int e = errno;
    close(fd);
    err = fmt("failed to fsync file: %s errno=%d", path.c_str(), e);
    return false;
  }
  close(fd);
  return true;
}

bool fxSyncDir(const std::string& path, std::string& err)
{
  int fd = open(path.c_str(), O_RDONLY);
  if (fd == -1) {
    err = fmt("failed to open dir for fsync: %s errno=%d", path.c_str(), errno);
    return false;
  }
  if (fsync(fd) != 0) {
    int e = errno;
    close(fd);
    err = fmt("failed to fsync dir: %s errno=%d", path.c_str(), e);
    return false;
  }
  close(fd);
  return true;
}

// How long fxRunChild sleeps between checks of the child, the cancel flag and the time limit.
const int FX_CHILD_POLL_MS = 50;
// After SIGTERM (time limit or cancel), how long the tool gets to exit before SIGKILL.
const auto FX_CHILD_TERM_GRACE = std::chrono::seconds(1);
// The most of the child's captured output kept (ffmpeg's stderr, ffprobe's stdout); the rest is read and dropped.
const size_t FX_CHILD_CAPTURE_CAP = 64 * 1024;
// ffprobe's time limit. A probe that runs out passes (a slow disk is not proof the file is bad).
const auto FX_PROBE_TIMEOUT = std::chrono::seconds(180);
// The remux's time limit (2026-11-14): FX_REMUX_TIMEOUT_BASE + FX_REMUX_TIMEOUT_PER_SEGMENT per segment +
// FX_REMUX_TIMEOUT_PER_GIB per GiB of segment data, at most FX_REMUX_TIMEOUT_MAX. A -c copy remux is I/O-bound:
// a Raspberry Pi on a USB/SMB disk copies roughly 20-50 MB/s, i.e. well under a minute per GiB, so this is ~2-5x
// headroom (1000 segments / 3 GiB -> 600 + 2000 + 360 = 2960 s, ~49 min). Without a limit a hung ffmpeg (stuck mount)
// held fxMergeSlotMutex forever, and fxplayer.retryMerge, which waits for that slot on the main thread while
// holding fxMergeMutex, then hung the whole daemon.
const auto FX_REMUX_TIMEOUT_BASE = std::chrono::seconds(600);
const auto FX_REMUX_TIMEOUT_PER_SEGMENT = std::chrono::seconds(2);
const auto FX_REMUX_TIMEOUT_PER_GIB = std::chrono::seconds(120);
const auto FX_REMUX_TIMEOUT_MAX = std::chrono::hours(6);

struct FxChildRun {
  enum Outcome {
    EXITED,       // exitCode is the tool's exit code
    SIGNALED,     // the tool (or its waiter) died from a signal nobody here sent
    TIMED_OUT,    // stopped by fxRunChild: the time limit ran out
    CANCELED,     // stopped by fxRunChild: the job's cancel flag was set
    START_FAILED, // nothing ran; `error` says why
  };
  Outcome outcome = START_FAILED;
  int exitCode = -1;
  std::string captured;
  std::string error;
};

// The waiter's report to fxRunChild: the tool's wait status, or why it could not be started/waited for.
struct FxChildReport {
  int kind; // 0 = status holds the tool's wait status, 1 = fork failed (value = errno), 2 = waitpid failed
  int value;
};

void fxCloseFd(int& fd)
{
  if (fd != -1) {
    close(fd);
    fd = -1;
  }
}

// 2026-11-14: runs one external tool (ffmpeg, ffprobe) and waits for it, for at most `timeout`, stopping it early
// when `job`'s cancel flag is set; returns how it ended plus the first FX_CHILD_CAPTURE_CAP bytes it wrote to
// `captureFd` (STDOUT_FILENO or STDERR_FILENO; the other one and stdin are /dev/null). `bins` are the paths tried in
// order (the next one only when the previous does not exist); `execErr` is written to the captured stream when none
// can be run (the tool then exits 127). Replaces two hand-copied fork/exec/wait loops (remux and probe) that had
// drifted apart (only one had a time limit).
//
// SIGCHLD: aria2's main loop runs with SIGCHLD set to SIG_IGN (MultiUrlRequestInfo.cc), which is how the hook
// commands it forks (util.cc executeHook, never waited for) are reaped. Under SIG_IGN the kernel discards a child's
// exit status, so the old code switched SIGCHLD to SIG_DFL process-wide, from this worker thread, around each
// tool -- and any hook child that exited inside that window became a zombie. Nothing here touches SIGCHLD in this
// process any more. Instead the forked child is a small "waiter" (no exec) that sets SIGCHLD to SIG_DFL in its OWN
// process, forks the tool, waits for it and writes its wait status to a pipe. The waiter itself may be auto-reaped
// (SIG_IGN) or not (SIG_DFL, e.g. libaria2 use); either way it is reaped below with a non-blocking waitpid on its
// own pid, so no other child's status is ever consumed. The waiter leads its own process group, so a stop signals
// the group: the waiter ignores SIGTERM (it must stay to report) while the tool gets SIGTERM, then SIGKILL after
// FX_CHILD_TERM_GRACE.
//
// Between fork and exec only async-signal-safe calls are made: argv, the messages and /dev/null are all prepared
// before the first fork (allocating in the child of a multithreaded process can deadlock on a malloc lock another
// thread held at fork time), and the waiter's own fork is _Fork() (no atfork handlers) where glibc has it. Both
// processes are SIGKILLed by the kernel if their parent dies (PR_SET_PDEATHSIG), so an aria2 crash leaves no
// ffmpeg behind.
FxChildRun fxRunChild(const char* const* bins, const std::vector<std::string>& args, int captureFd,
                      std::chrono::steady_clock::duration timeout, const FxMergeJob& job, const char* execErr)
{
  FxChildRun run;
  std::vector<char*> argv;
  argv.reserve(args.size() + 2);
  argv.push_back(const_cast<char*>(bins[0]));
  for (const auto& arg : args) {
    argv.push_back(const_cast<char*>(arg.c_str()));
  }
  argv.push_back(nullptr);
  const size_t execErrLen = strlen(execErr);
  const int otherFd = captureFd == STDOUT_FILENO ? STDERR_FILENO : STDOUT_FILENO;

  // Every descriptor close-on-exec from creation: a hook forked by the main thread meanwhile must not inherit
  // them (the final drain below would then wait for that hook). dup2 clears the flag on the tool's own copies.
  int outPipe[2] = {-1, -1};
  int statusPipe[2] = {-1, -1};
  if (pipe2(outPipe, O_CLOEXEC) != 0) {
    run.error = fmt("pipe failed errno=%d", errno);
    return run;
  }
  if (pipe2(statusPipe, O_CLOEXEC) != 0) {
    run.error = fmt("pipe failed errno=%d", errno);
    fxCloseFd(outPipe[0]);
    fxCloseFd(outPipe[1]);
    return run;
  }
  int devnull = open("/dev/null", O_RDWR | O_CLOEXEC);
  const pid_t aria2Pid = getpid();

  const pid_t waiter = fork();
  if (waiter == -1) {
    run.error = fmt("fork failed errno=%d", errno);
    fxCloseFd(outPipe[0]);
    fxCloseFd(outPipe[1]);
    fxCloseFd(statusPipe[0]);
    fxCloseFd(statusPipe[1]);
    fxCloseFd(devnull);
    return run;
  }
  if (waiter == 0) {
    // The waiter. Single-threaded, async-signal-safe calls only.
#ifdef __linux__
    prctl(PR_SET_PDEATHSIG, SIGKILL);
#endif
    if (getppid() != aria2Pid) {
      _exit(126); // aria2 already gone: PDEATHSIG does not fire retroactively
    }
    setpgid(0, 0);
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sigemptyset(&sa.sa_mask);
    sa.sa_handler = SIG_DFL;
    sigaction(SIGCHLD, &sa, nullptr); // only this process: lets waitpid below see the tool's status
    sa.sa_handler = SIG_IGN;
    sigaction(SIGTERM, &sa, nullptr); // a stop's SIGTERM is for the tool; the waiter stays to report
    close(outPipe[0]);
    close(statusPipe[0]);
    const pid_t waiterPid = getpid();
#if defined(__GLIBC__) && (__GLIBC__ > 2 || (__GLIBC__ == 2 && __GLIBC_MINOR__ >= 34))
    const pid_t tool = _Fork();
#else
    const pid_t tool = fork();
#endif
    if (tool == 0) {
      // The tool.
#ifdef __linux__
      prctl(PR_SET_PDEATHSIG, SIGKILL);
#endif
      if (getppid() != waiterPid) {
        _exit(126);
      }
      // Ignored signals stay ignored across exec: give the tool default dispositions and no blocked signals.
      sa.sa_handler = SIG_DFL;
      sigaction(SIGTERM, &sa, nullptr);
      sigaction(SIGPIPE, &sa, nullptr);
      sigset_t none;
      sigemptyset(&none);
      sigprocmask(SIG_SETMASK, &none, nullptr);
      if (devnull != -1) {
        dup2(devnull, STDIN_FILENO);
        dup2(devnull, otherFd);
      }
      dup2(outPipe[1], captureFd);
      for (size_t i = 0; bins[i]; ++i) {
        execv(bins[i], argv.data());
        if (errno != ENOENT) {
          break;
        }
      }
      (void)::write(outPipe[1], execErr, execErrLen);
      _exit(127);
    }
    close(outPipe[1]);
    FxChildReport report = {0, 0};
    if (tool == -1) {
      report.kind = 1;
      report.value = errno;
    }
    else {
      int status = 0;
      pid_t w;
      while ((w = waitpid(tool, &status, 0)) == -1 && errno == EINTR) {
      }
      if (w == tool) {
        report.value = status;
      }
      else {
        report.kind = 2;
        report.value = errno;
      }
    }
    (void)::write(statusPipe[1], &report, sizeof(report));
    _exit(0);
  }

  setpgid(waiter, waiter); // the classic both-sides call; whichever runs first wins, errors are harmless
  fxCloseFd(outPipe[1]);
  fxCloseFd(statusPipe[1]);
  fxCloseFd(devnull);
  fcntl(outPipe[0], F_SETFL, O_NONBLOCK);
  fcntl(statusPipe[0], F_SETFL, O_NONBLOCK);

  char buf[4096];
  auto drainOut = [&]() {
    while (outPipe[0] != -1) {
      const ssize_t n = ::read(outPipe[0], buf, sizeof(buf));
      if (n > 0) {
        if (run.captured.size() < FX_CHILD_CAPTURE_CAP) {
          run.captured.append(buf, std::min(static_cast<size_t>(n), FX_CHILD_CAPTURE_CAP - run.captured.size()));
        }
        continue;
      }
      if (n == -1 && errno == EINTR) {
        continue;
      }
      if (n == 0) {
        fxCloseFd(outPipe[0]); // EOF: the tool (the only writer) is gone
      }
      break;
    }
  };

  FxChildReport report = {0, 0};
  size_t reportBytes = 0;
  bool reportEof = false;
  const auto started = std::chrono::steady_clock::now();
  FxChildRun::Outcome stopReason = FxChildRun::EXITED; // EXITED = not stopping
  std::chrono::steady_clock::time_point killAt;
  bool killed = false;
  while (!reportEof && reportBytes < sizeof(report)) {
    struct pollfd fds[2];
    nfds_t nfds = 0;
    fds[nfds].fd = statusPipe[0];
    fds[nfds].events = POLLIN;
    ++nfds;
    if (outPipe[0] != -1) {
      fds[nfds].fd = outPipe[0];
      fds[nfds].events = POLLIN;
      ++nfds;
    }
    (void)poll(fds, nfds, FX_CHILD_POLL_MS);
    drainOut();
    while (reportBytes < sizeof(report)) {
      const ssize_t n = ::read(statusPipe[0], reinterpret_cast<char*>(&report) + reportBytes,
                               sizeof(report) - reportBytes);
      if (n > 0) {
        reportBytes += static_cast<size_t>(n);
        continue;
      }
      if (n == -1 && errno == EINTR) {
        continue;
      }
      if (n == 0) {
        reportEof = true;
      }
      break;
    }
    if (reportEof || reportBytes == sizeof(report)) {
      break;
    }
    const auto now = std::chrono::steady_clock::now();
    if (stopReason == FxChildRun::EXITED) {
      if (job.cancelRequested()) {
        stopReason = FxChildRun::CANCELED;
      }
      else if (now - started >= timeout) {
        stopReason = FxChildRun::TIMED_OUT;
      }
      if (stopReason != FxChildRun::EXITED) {
        kill(-waiter, SIGTERM);
        killAt = now + FX_CHILD_TERM_GRACE;
      }
    }
    else if (!killed && now >= killAt) {
      kill(-waiter, SIGKILL); // the waiter too: its EOF ends this loop
      killed = true;
    }
  }
  fxCloseFd(statusPipe[0]);

  // The rest of the tool's output. It has exited (or is being killed), so EOF comes at once; bounded anyway.
  const auto drainUntil = std::chrono::steady_clock::now() + std::chrono::seconds(1);
  while (outPipe[0] != -1 && std::chrono::steady_clock::now() < drainUntil) {
    struct pollfd pfd = {outPipe[0], POLLIN, 0};
    (void)poll(&pfd, 1, FX_CHILD_POLL_MS);
    drainOut();
  }
  fxCloseFd(outPipe[0]);

  // Reap the waiter by its own pid (it exits right after reporting): pid = reaped here (SIGCHLD at SIG_DFL),
  // ECHILD = the kernel already did (SIG_IGN). Never a blocking or wildcard wait.
  for (int i = 0; i < 1000; ++i) {
    const pid_t w = waitpid(waiter, nullptr, WNOHANG);
    if (w == waiter || (w == -1 && errno != EINTR)) {
      break;
    }
    if (i == 500) {
      kill(-waiter, SIGKILL);
    }
    usleep(1000);
  }

  if (stopReason != FxChildRun::EXITED) {
    run.outcome = stopReason;
    return run;
  }
  if (reportBytes != sizeof(report)) {
    run.outcome = FxChildRun::SIGNALED; // the waiter died without reporting
    return run;
  }
  if (report.kind == 1) {
    run.outcome = FxChildRun::START_FAILED;
    run.error = fmt("fork failed errno=%d", report.value);
    return run;
  }
  if (report.kind != 0) {
    run.outcome = FxChildRun::SIGNALED;
    run.error = fmt("waitpid failed errno=%d", report.value);
    return run;
  }
  if (WIFEXITED(report.value)) {
    run.outcome = FxChildRun::EXITED;
    run.exitCode = WEXITSTATUS(report.value);
  }
  else {
    run.outcome = FxChildRun::SIGNALED;
  }
  return run;
}

// fxRemuxSegments' time limit for `job` -- see FX_REMUX_TIMEOUT_BASE.
std::chrono::steady_clock::duration fxRemuxTimeout(const FxMergeJob& job)
{
  int64_t bytes = 0;
  for (const auto& path : job.segmentPaths) {
    const int64_t size = File(path).size();
    if (size > 0) {
      bytes += size;
    }
  }
  const int64_t gib = (bytes + (int64_t(1) << 30) - 1) >> 30;
  const auto limit = FX_REMUX_TIMEOUT_BASE +
                     FX_REMUX_TIMEOUT_PER_SEGMENT * static_cast<int64_t>(job.segmentPaths.size()) +
                     FX_REMUX_TIMEOUT_PER_GIB * gib;
  return std::min<std::chrono::steady_clock::duration>(limit, FX_REMUX_TIMEOUT_MAX);
}

bool fxRemuxSegments(FxMergeJob& job, const std::string& partPath,
                     std::string& err)
{
  const auto listPath = util::applyDir(job.tmpDir, "remux.m3u8");
  const auto concatListPath = util::applyDir(job.tmpDir, "concat.list");
  const auto lowerOutput = util::toLower(job.outputPath);
  const bool outputMpegTs = util::endsWith(lowerOutput, ".ts");
  const bool isTsSegmentBundle = !job.segmentPaths.empty() &&
                                util::endsWith(util::toLower(File(job.segmentPaths.front()).getBasename()), ".ts");

  bool hasInitSegment = false;
  size_t mediaStartIndex = 0;

  if (!job.segmentPaths.empty()) {
    const auto& firstPath = job.segmentPaths.front();
    const auto lowerFirst = util::toLower(File(firstPath).getBasename());
    if (job.segmentPaths.size() > 1 &&
        (util::endsWith(lowerFirst, ".mp4") ||
         util::endsWith(lowerFirst, ".m4s") ||
         lowerFirst.find("init") != std::string::npos)) {
      hasInitSegment = true;
      mediaStartIndex = 1;
    }
  }

  if (isTsSegmentBundle && outputMpegTs) {
    std::ofstream concatList(concatListPath.c_str(), std::ios::binary | std::ios::trunc);
    if (!concatList) {
      err = "could not write ffmpeg concat list";
      return false;
    }
    for (const auto& segmentPath : job.segmentPaths) {
      concatList << "file '" << segmentPath << "'\n";
    }
    concatList.flush();
  }
  else {
    std::ofstream list(listPath.c_str(), std::ios::binary | std::ios::trunc);
    if (!list) {
      err = "could not write ffmpeg hls playlist";
      return false;
    }

    list << "#EXTM3U\n";
    list << "#EXT-X-VERSION:7\n";
    list << "#EXT-X-TARGETDURATION:1\n";
    list << "#EXT-X-MEDIA-SEQUENCE:0\n";
    list << "#EXT-X-PLAYLIST-TYPE:VOD\n";
    if (hasInitSegment) {
      list << "#EXT-X-MAP:URI=\""
           << File(job.segmentPaths.front()).getBasename() << "\"\n";
    }
    for (size_t i = mediaStartIndex; i < job.segmentPaths.size(); ++i) {
      list << "#EXTINF:1.0,\n";
      list << File(job.segmentPaths[i]).getBasename() << "\n";
    }
    list << "#EXT-X-ENDLIST\n";
    list.flush();
  }

  std::vector<std::string> args = {"-y", "-hide_banner", "-loglevel", "error", "-xerror", "-err_detect",
                                   "crccheck+bitstream+buffer+explode"};
  if (isTsSegmentBundle && outputMpegTs) {
    args.insert(args.end(), {"-f", "concat", "-safe", "0", "-i", concatListPath, "-c", "copy", "-f", "mpegts"});
  }
  else {
    args.insert(args.end(), {"-allowed_extensions", "ALL", "-allowed_segment_extensions", "ALL", "-extension_picky",
                             "0", "-protocol_whitelist", "file,crypto,data,http,https,tcp,tls", "-f", "hls", "-i",
                             listPath, "-fflags", "+genpts", "-reset_timestamps", "1", "-c", "copy"});
    if (outputMpegTs) {
      args.insert(args.end(), {"-f", "mpegts"});
    }
    else {
      args.insert(args.end(), {"-movflags", "+faststart", "-f", "mp4"});
    }
  }
  args.push_back(partPath);

  static const char* const ffmpegBins[] = {"ffmpeg", "/usr/local/bin/ffmpeg", "/usr/bin/ffmpeg", nullptr};
  const auto timeout = fxRemuxTimeout(job);
  auto run = fxRunChild(ffmpegBins, args, STDERR_FILENO, timeout, job, "failed to exec ffmpeg (not installed?)\n");
  auto& ffmpegStderr = run.captured;
  while (!ffmpegStderr.empty() && (ffmpegStderr.back() == '\n' || ffmpegStderr.back() == '\r')) {
    ffmpegStderr.pop_back();
  }
  switch (run.outcome) {
  case FxChildRun::CANCELED:
    err = "merge canceled by user";
    return false;
  case FxChildRun::TIMED_OUT:
    err = fmt("ffmpeg did not finish within its %llds limit (%lu segments); stopped",
              static_cast<long long>(std::chrono::duration_cast<std::chrono::seconds>(timeout).count()),
              static_cast<unsigned long>(job.segmentPaths.size()));
    if (!ffmpegStderr.empty()) {
      err += ": " + ffmpegStderr;
    }
    return false;
  case FxChildRun::START_FAILED:
    err = fmt("failed to start ffmpeg: %s", run.error.c_str());
    return false;
  case FxChildRun::SIGNALED:
    err = "ffmpeg terminated abnormally";
    return false;
  case FxChildRun::EXITED:
    break;
  }

  if (run.exitCode != 0) {
    if (!ffmpegStderr.empty()) {
      A2_LOG_ERROR(fmt("[fxmerge] parent=%s ffmpeg stderr: %s",
                       GroupId::toHex(job.parentGid).c_str(),
                       ffmpegStderr.c_str()));
      err = fmt("ffmpeg exited with code %d: %s", run.exitCode,
                ffmpegStderr.c_str());
    }
    else {
      A2_LOG_ERROR(fmt("[fxmerge] parent=%s ffmpeg stderr: <empty>",
                       GroupId::toHex(job.parentGid).c_str()));
      err = fmt("ffmpeg exited with code %d", run.exitCode);
    }
    return false;
  }
  File(listPath).remove();
  return true;
}

// True when `path` names a video container (the primary save, or a video sent in concat mode). Posters,
// playlist text and preview clips (".trailer.") are never probed.
bool fxOutputIsProbedVideo(const FxMergeJob& job)
{
  const auto& out = job.outputPath;
  if (out.find(".trailer.") != std::string::npos) {
    return false;
  }
  if (job.remux) {
    return true;
  }
  static const char* exts[] = {".mp4", ".ts", ".m4v", ".mov", ".mkv", ".webm", nullptr};
  for (size_t i = 0; exts[i]; ++i) {
    const size_t n = strlen(exts[i]);
    if (out.size() >= n && out.compare(out.size() - n, n, exts[i]) == 0) {
      return true;
    }
  }
  return false;
}

// fxProbeMergedVideo fails a merged video shorter than FX_PROBE_MIN_DURATION_RATIO of the site's length, only when
// that length is at least FX_PROBE_MIN_EXPECTED_SECONDS (a short clip's rounding is not worth failing a save over).
const double FX_PROBE_MIN_DURATION_RATIO = 0.9;
const double FX_PROBE_MIN_EXPECTED_SECONDS = 30;

// 2026-11-14: check the merged part file with ffprobe before publishing it. Returns false (with `err`)
// when the file has no video stream, ffprobe cannot read it, or it is much shorter than
// job.expectedDurationSec. A missing ffprobe binary, a probe that runs out of time (FX_PROBE_TIMEOUT) or one
// that cannot be run only logs a warning and passes (never fail a save over the tool).
bool fxProbeMergedVideo(const FxMergeJob& job, const std::string& path, std::string& err)
{
  const auto parent = GroupId::toHex(job.parentGid);
  static const char* const bins[] = {"ffprobe", "/usr/local/bin/ffprobe", "/usr/bin/ffprobe", nullptr};
  const std::vector<std::string> args = {"-v", "error", "-show_entries", "stream=codec_type:format=duration",
                                         "-of", "default=noprint_wrappers=1", path};
  const auto run = fxRunChild(bins, args, STDOUT_FILENO, FX_PROBE_TIMEOUT, job, "");
  switch (run.outcome) {
  case FxChildRun::CANCELED:
    err = "merge canceled by user";
    return false;
  case FxChildRun::TIMED_OUT:
    A2_LOG_WARN(fmt("[fxmerge] parent=%s probe skipped: ffprobe timed out after %llds", parent.c_str(),
                    static_cast<long long>(FX_PROBE_TIMEOUT.count())));
    return true;
  case FxChildRun::START_FAILED:
    A2_LOG_WARN(fmt("[fxmerge] parent=%s probe skipped: %s", parent.c_str(), run.error.c_str()));
    return true;
  case FxChildRun::SIGNALED:
    A2_LOG_WARN(fmt("[fxmerge] parent=%s probe skipped: ffprobe terminated abnormally", parent.c_str()));
    return true;
  case FxChildRun::EXITED:
    break;
  }
  const int code = run.exitCode;
  if (code == 127 || code == 126) {
    A2_LOG_WARN(fmt("[fxmerge] parent=%s probe skipped: ffprobe not available (exit %d)", parent.c_str(), code));
    return true;
  }
  bool hasVideo = false;
  double duration = 0;
  {
    std::istringstream lines(run.captured);
    std::string line;
    while (std::getline(lines, line)) {
      if (!line.empty() && line.back() == '\r') {
        line.pop_back();
      }
      if (line == "codec_type=video") {
        hasVideo = true;
      }
      else if (line.compare(0, 9, "duration=") == 0) {
        char* end = nullptr;
        const double d = strtod(line.c_str() + 9, &end);
        if (end && end != line.c_str() + 9 && d > duration) {
          duration = d;
        }
      }
    }
  }
  const int64_t size = File(path).size();
  if (code != 0) {
    err = fmt("merged file is not a readable video (ffprobe exit %d, %" PRId64 " bytes)", code, size);
    return false;
  }
  if (!hasVideo) {
    err = fmt("merged file has no video stream (%" PRId64 " bytes)", size);
    return false;
  }
  const double expected = static_cast<double>(job.expectedDurationSec);
  if (expected >= FX_PROBE_MIN_EXPECTED_SECONDS && duration > 0 && duration < expected * FX_PROBE_MIN_DURATION_RATIO) {
    err = fmt("merged video is %.0fs long but %.0fs was expected", duration, expected);
    return false;
  }
  A2_LOG_INFO(fmt("[fxmerge] parent=%s probe ok duration=%.1fs expected=%" PRId64 "s bytes=%" PRId64,
                  parent.c_str(), duration, job.expectedDurationSec, size));
  return true;
}

// fxFinalizeMerge retries the atomic publish rename up to FX_PUBLISH_ATTEMPTS times, FX_PUBLISH_INTERVAL_US apart (~10 s).
const size_t FX_PUBLISH_ATTEMPTS = 100;
const useconds_t FX_PUBLISH_INTERVAL_US = 100000;

// Whether fxFinalizeMerge will run ffmpeg or ffprobe for `job`, i.e. needs the one-at-a-time slot
// (fxMergeSlotMutex). A plain concat of a non-video output (playlist text, thumbnail) runs neither.
bool fxFinalizeNeedsSlot(const FxMergeJob& job)
{
  return job.remux || fxOutputIsProbedVideo(job);
}

bool fxFinalizeMerge(FxMergeJob& job)
{
  if (job.cancelRequested()) {
    fxMarkFailed(job, FX_MERGE_ERR_CANCELED, "merge canceled by user", true);
    return false;
  }

  std::string preflightErr;
  if (!fxWaitForSegmentsReady(job, preflightErr)) {
    if (job.cancelRequested() || preflightErr == "merge canceled by user") {
      fxMarkFailed(job, FX_MERGE_ERR_CANCELED, "merge canceled by user", true);
    }
    else {
      fxMarkFailed(job, FX_MERGE_ERR_SEGMENT, preflightErr, true);
    }
    return false;
  }

  job.state = FX_MERGE_MERGING;
  job.mergeProgress = 0.0;
  // INFO, not WARN: the WARN-level start marker is fxMergeOnGroupStopped's "all segments downloaded;
  // starting merge" line, and the outcome is always logged at WARN ("finalize complete", with this
  // step's duration) or ERROR (fxMarkFailed) -- this line only added a third WARN per job.
  const auto finalizeStartedAt = std::chrono::steady_clock::now();
  A2_LOG_INFO(fmt("[fxmerge] parent=%s finalize begin mode=%s output=%s",
                  GroupId::toHex(job.parentGid).c_str(), job.mode.c_str(),
                  job.outputPath.c_str()));

  const auto outputDir = File(job.outputPath).getDirname();
  if (!outputDir.empty()) {
    File odir(outputDir);
    if (!odir.isDir() && !odir.mkdirs()) {
      fxMarkFailed(job, FX_MERGE_ERR_PATH,
                   "could not create output directory");
      return false;
    }
  }

  const auto partPath = fxMergePartPath(job);
  File(partPath).remove();

  std::string err;
  bool ok = false;
  if (job.remux) {
    for (size_t attempt = 1; attempt <= 2; ++attempt) {
      ok = fxRemuxSegments(job, partPath, err);
      if (!ok) {
        break;
      }

      File partFile(partPath);
      const int64_t partSize = partFile.size();
      if (partFile.isFile() && partSize > 0) {
        break;
      }

      ok = false;
      err = fmt("ffmpeg exited successfully but remux output is missing or empty: %s",
                partPath.c_str());
      if (attempt == 1) {
        A2_LOG_WARN(fmt("[fxmerge] parent=%s remux output missing after successful ffmpeg exit; retrying once path=%s size=%" PRId64,
                        GroupId::toHex(job.parentGid).c_str(),
                        partPath.c_str(), partSize));
        partFile.remove();
        err.clear();
      }
    }
    if (!ok) {
      if (job.cancelRequested() || err == "merge canceled by user") {
        fxMarkFailed(job, FX_MERGE_ERR_CANCELED, "merge canceled by user", true);
      }
      else {
        fxMarkFailed(job, FX_MERGE_ERR_REMUX, err);
      }
      return false;
    }
  }
  else {
    ok = fxConcatSegments(job, partPath, err);
    if (!ok) {
      if (job.cancelRequested() || err == "merge canceled by user") {
        fxMarkFailed(job, FX_MERGE_ERR_CANCELED, "merge canceled by user", true);
      }
      else {
        fxMarkFailed(job, FX_MERGE_ERR_CONCAT_IO, err);
      }
      return false;
    }
  }

  if (job.cancelRequested()) {
    fxMarkFailed(job, FX_MERGE_ERR_CANCELED, "merge canceled by user", true);
    return false;
  }

  if (fxOutputIsProbedVideo(job) && !fxProbeMergedVideo(job, partPath, err)) {
    if (job.cancelRequested() || err == "merge canceled by user") {
      fxMarkFailed(job, FX_MERGE_ERR_CANCELED, "merge canceled by user", true);
    }
    else {
      File(partPath).remove();
      fxMarkFailed(job, FX_MERGE_ERR_INVALID_OUTPUT, err);
    }
    return false;
  }

  if (!fxSyncFile(partPath, err)) {
    fxMarkFailed(job, FX_MERGE_ERR_CONCAT_IO, err);
    return false;
  }
  // The fsync of a multi-GB file can take many seconds (2026-11-14 review): a delete arriving meanwhile must not be
  // followed by publishing the deleted save's video.
  if (job.cancelRequested()) {
    fxMarkFailed(job, FX_MERGE_ERR_CANCELED, "merge canceled by user", true);
    return false;
  }

  // Keep a completed remux available through a short-lived publish failure.
  const size_t maxPublishAttempts = FX_PUBLISH_ATTEMPTS;
  int publishErrno = 0;
  std::string publishError;
  bool published = false;
  for (size_t attempt = 1; attempt <= maxPublishAttempts; ++attempt) {
    errno = 0;
    if (File(partPath).renameTo(job.outputPath)) {
      published = true;
      if (attempt > 1) {
        A2_LOG_WARN(fmt("[fxmerge] parent=%s atomic publish recovered attempt=%lu/%lu output=%s",
                        GroupId::toHex(job.parentGid).c_str(),
                        static_cast<unsigned long>(attempt),
                        static_cast<unsigned long>(maxPublishAttempts),
                        job.outputPath.c_str()));
      }
      break;
    }
    publishErrno = errno;
    publishError = std::strerror(publishErrno);
    if (job.cancelRequested()) {
      fxMarkFailed(job, FX_MERGE_ERR_CANCELED, "merge canceled by user", true);
      return false;
    }
    if (attempt == 1 || attempt == maxPublishAttempts || attempt % 10 == 0) {
      A2_LOG_WARN(fmt("[fxmerge] parent=%s atomic publish retry attempt=%lu/%lu errno=%d error=%s part=%s output=%s",
                      GroupId::toHex(job.parentGid).c_str(),
                      static_cast<unsigned long>(attempt),
                      static_cast<unsigned long>(maxPublishAttempts),
                            publishErrno, publishError.c_str(), partPath.c_str(),
                            job.outputPath.c_str()));
    }
    usleep(FX_PUBLISH_INTERVAL_US);
  }
  if (!published) {
    fxMarkFailed(job, FX_MERGE_ERR_RENAME,
                 fmt("atomic rename from part to output failed after %lu attempts: errno=%d error=%s part=%s output=%s",
                     static_cast<unsigned long>(maxPublishAttempts), publishErrno,
                     publishError.c_str(), partPath.c_str(),
                     job.outputPath.c_str()));
    return false;
  }

  if (!outputDir.empty()) {
    if (!fxSyncDir(outputDir, err)) {
      fxMarkFailed(job, FX_MERGE_ERR_RENAME, err);
      return false;
    }
  }

  for (const auto& segmentPath : job.segmentPaths) {
    File(segmentPath).remove();
  }
  fxRemoveTree(job.tmpDir);

  job.state = FX_MERGE_MERGED;
  job.errorCode = 0;
  job.errorMessage.clear();
  job.mergeProgress = 1.0;
  fxSetTerminalNow(job);
  A2_LOG_WARN(fmt("[fxmerge] parent=%s finalize complete mode=%s segments=%lu took=%.1fs output=%s",
                  GroupId::toHex(job.parentGid).c_str(), job.mode.c_str(),
                  static_cast<unsigned long>(job.segmentPaths.size()),
                  std::chrono::duration<double>(std::chrono::steady_clock::now() -
                                                finalizeStartedAt)
                      .count(),
                  job.outputPath.c_str()));
  return true;
}
} // namespace

namespace {
std::unique_ptr<ValueBase> createGIDResponse(a2_gid_t gid)
{
  return String::g(GroupId::toHex(gid));
}
} // namespace

namespace {
std::unique_ptr<ValueBase> createOKResponse() { return String::g("OK"); }
} // namespace

namespace {
std::unique_ptr<ValueBase>
addRequestGroup(const std::shared_ptr<RequestGroup>& group, DownloadEngine* e,
                bool posGiven, int pos)
{
  if (posGiven) {
    e->getRequestGroupMan()->insertReservedGroup(pos, group);
  }
  else {
    e->getRequestGroupMan()->addReservedGroup(group);
  }
  return createGIDResponse(group->getGID());
}
} // namespace

namespace {
bool checkPosParam(const Integer* posParam)
{
  if (posParam) {
    if (posParam->i() >= 0) {
      return true;
    }
    else {
      throw DL_ABORT_EX("Position must be greater than or equal to 0.");
    }
  }
  return false;
}
} // namespace

namespace {
a2_gid_t str2Gid(const String* str)
{
  assert(str);
  if (str->s().size() > sizeof(a2_gid_t) * 2) {
    throw DL_ABORT_EX(fmt("Invalid GID %s", str->s().c_str()));
  }
  a2_gid_t n;
  switch (GroupId::expandUnique(n, str->s().c_str())) {
  case GroupId::ERR_NOT_UNIQUE:
    throw DL_ABORT_EX(fmt("GID %s is not unique", str->s().c_str()));
  case GroupId::ERR_NOT_FOUND:
    throw DL_ABORT_EX(fmt("GID %s is not found", str->s().c_str()));
  case GroupId::ERR_INVALID:
    throw DL_ABORT_EX(fmt("Invalid GID %s", str->s().c_str()));
  }
  return n;
}
} // namespace

namespace {
template <typename OutputIterator>
void extractUris(OutputIterator out, const List* src)
{
  if (src) {
    for (auto& elem : *src) {
      const String* uri = downcast<String>(elem);
      if (uri) {
        out++ = uri->s();
      }
    }
  }
}
} // namespace

std::unique_ptr<ValueBase> AddUriRpcMethod::process(const RpcRequest& req,
                                                    DownloadEngine* e)
{
  const List* urisParam = checkRequiredParam<List>(req, 0);
  const Dict* optsParam = checkParam<Dict>(req, 1);
  const Integer* posParam = checkParam<Integer>(req, 2);

  std::vector<std::string> uris;
  extractUris(std::back_inserter(uris), urisParam);
  if (uris.empty()) {
    throw DL_ABORT_EX("URI is not provided.");
  }

  auto requestOption = std::make_shared<Option>(*e->getOption());
  gatherRequestOption(requestOption.get(), optsParam);

  bool posGiven = checkPosParam(posParam);
  size_t pos = posGiven ? posParam->i() : 0;

  std::vector<std::shared_ptr<RequestGroup>> result;
  createRequestGroupForUri(result, requestOption, uris,
                           /* ignoreForceSeq = */ true,
                           /* ignoreLocalPath = */ true);

  if (!result.empty()) {
    return addRequestGroup(result.front(), e, posGiven, pos);
  }
  else {
    throw DL_ABORT_EX("No URI to download.");
  }
}

namespace {
// 2026-11-14: whether an addMerge is the app's text fetch (WebService.swift fetchTextViaAria2): one URL, concat, to
// an output named "<key>.fetch-XXXXXXXX.tmp" (8 characters of a UUID). Keyed off what the app already sends, so
// readTextOutput can refuse -- and never delete -- any other job's output without an app change.
bool fxIsTextFetchJob(const std::string& outputPath, size_t segmentCount, bool remux)
{
  static const char marker[] = ".fetch-";
  static const char suffix[] = ".tmp";
  const size_t idLen = 8;
  const auto name = File(outputPath).getBasename();
  const size_t markerLen = sizeof(marker) - 1;
  const size_t suffixLen = sizeof(suffix) - 1;
  if (segmentCount != 1 || remux || name.size() < markerLen + idLen + suffixLen + 1) {
    return false;
  }
  const size_t markerPos = name.size() - suffixLen - idLen - markerLen;
  if (name.compare(markerPos, markerLen, marker) != 0 || name.compare(name.size() - suffixLen, suffixLen, suffix) != 0) {
    return false;
  }
  for (size_t i = markerPos + markerLen; i < name.size() - suffixLen; ++i) {
    const char c = name[i];
    if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'))) {
      return false;
    }
  }
  return true;
}
} // namespace

// Reclaim `outputPath`'s ownership for a fresh `addMerge` (the FXPlayer "retry failed download"
// button, and generally: overwrite=true means "start this exact output over, whatever else is
// there"). Two cases, depending on the existing owner's state:
//   TERMINAL (failed/merged): clean up its artifacts and erase it now, synchronously — no lingering
//   state, so the new job can reuse the default `outputPath + ".segments"` tmpDir safely.
//   NON-TERMINAL (still queued/downloading/merging): its children can't be torn down synchronously
//   within this single RPC call (aria2's engine only actually stops them on a later tick), so
//   erasing it now would leave them writing into the SAME tmpDir the new job is about to use —
//   racing to corrupt each other's segments. Instead: force-halt its children (mirrors
//   `aria2.forceRemove`), mark the OLD job failed/cleanup-pending so the EXISTING
//   `fxMergeOnGroupStopped` machinery cleans it up asynchronously once its children actually stop
//   (the normal path for any canceled job), and hand the NEW job a time-suffixed tmpDir so the two
//   can never collide. Only the output-path OWNERSHIP is reassigned here; the old job keeps existing
//   under its own GID until it naturally goes terminal.
// fxSupersedeTmpDir says (without changing anything) which tmpDir the new job gets: the kept segments' directory of a
// terminal owner that kept them, or a time-suffixed one when the owner is still live ("" = the caller's default).
// fxSupersedeOwnerForOverwrite then does the takeover. addMerge calls the first before validating its request and
// the second only after (2026-11-14): a request that fails validation must not already have deleted the old output,
// erased the old job or halted a live one.
std::string fxSupersedeTmpDir(const std::string& outputPath, std::time_t now)
{
  std::lock_guard<std::recursive_mutex> lock(fxMergeMutex);
  auto ownerItr = fxMergeOutputOwner.find(outputPath);
  if (ownerItr == fxMergeOutputOwner.end()) {
    return "";
  }
  auto existing = fxMergeJobs.find(ownerItr->second);
  if (existing == fxMergeJobs.end()) {
    return "";
  }
  const auto& job = existing->second;
  if (fxIsTerminalState(job.state)) {
    return fxKeptSegments(job) ? job.tmpDir : std::string();
  }
  return outputPath + ".segments." + std::to_string(now) + "." + GroupId::toHex(ownerItr->second);
}

void fxSupersedeOwnerForOverwrite(const std::string& outputPath, DownloadEngine* e)
{
  std::lock_guard<std::recursive_mutex> lock(fxMergeMutex);
  auto ownerItr = fxMergeOutputOwner.find(outputPath);
  if (ownerItr == fxMergeOutputOwner.end()) {
    return;
  }
  const auto oldGid = ownerItr->second;
  auto existing = fxMergeJobs.find(oldGid);
  if (existing == fxMergeJobs.end()) {
    // Stale owner entry (job already evicted) — just drop it.
    fxMergeOutputOwner.erase(ownerItr);
    return;
  }
  auto& job = existing->second;
  if (fxIsTerminalState(job.state)) {
    // A failed job that kept its segments (2026-11-08: a segment failure, expired links, or the app
    // stopping it) hands its directory to the new job, which reuses every segment already on disk.
    const bool keptSegments = fxKeptSegments(job);
    A2_LOG_WARN(fmt("[fxmerge] overwrite: reclaiming output=%s from terminal parent=%s state=%s%s",
                    outputPath.c_str(), GroupId::toHex(oldGid).c_str(),
                    fxMergeStateName(job.state), keptSegments ? " (resuming its kept segments)" : ""));
    if (job.state == FX_MERGE_FAILED && !keptSegments) {
      fxCleanupFailureArtifacts(job);
    }
    if (keptSegments) {
      fxFlushCompletedList(job); // the new job reads it
    }
    File(fxMergePartPath(job)).remove();
    File(job.outputPath).remove();
    fxEraseMergeJob(oldGid);
    e->getRequestGroupMan()->removeDownloadResult(oldGid);
    return;
  }

  A2_LOG_WARN(fmt("[fxmerge] overwrite: superseding LIVE parent=%s state=%s output=%s "
                  "(halting %lu child(ren) asynchronously)",
                  GroupId::toHex(oldGid).c_str(), fxMergeStateName(job.state),
                  outputPath.c_str(),
                  static_cast<unsigned long>(job.childGids.size())));
  // Tell a running merge worker to stop before it can publish over the new job's output.
  const bool wasMerging = job.state == FX_MERGE_MERGING;
  job.requestCancel();
  fxHaltRemainingChildren(job, e);
  fxMarkFailed(job, FX_MERGE_ERR_SUPERSEDED, "superseded by a new retry (overwrite=true)", true);
  if (!wasMerging && fxAllChildrenDone(job)) {
    fxCleanupFailureArtifacts(job);
  }
  fxMergeOutputOwner.erase(ownerItr);   // new job claims the path; old GID lives on until its
                                        // children report done, then self-cleans as usual.
}

std::unique_ptr<ValueBase> FxplayerAddMergeRpcMethod::process(
    const RpcRequest& req, DownloadEngine* e)
{
  std::lock_guard<std::recursive_mutex> lock(fxMergeMutex);
  fxPruneExpiredMergeJobs();

  const List* segmentsParam = checkRequiredParam<List>(req, 0);
  const Dict* aria2OptsParam = checkParam<Dict>(req, 1);
  const Dict* mergeOptsParam = checkParam<Dict>(req, 2);

  std::string outputPathForLog = "<unknown>";
  std::string tmpDirForLog = "<unknown>";
  std::string modeForLog = "concat";
  const auto segmentCountForLog = static_cast<unsigned long>(segmentsParam->size());

  try {

  const auto* outputParam = getStringField(mergeOptsParam, "output");
  if (!outputParam || outputParam->s().empty()) {
    throw DL_ABORT_EX("merge options must include non-empty 'output'.");
  }

  std::string outputPath = outputParam->s();
  outputPathForLog = outputPath;

  // overwrite=true (the FXPlayer "retry failed download" button): unconditionally reclaim
  // `outputPath`, whatever state its previous owner is in, instead of the normal idempotent-reuse-
  // or-collision-error behavior below. See fxSupersedeOwnerForOverwrite's doc comment.
  // 2026-11-14: only DECIDED here; the takeover (which can delete the old output, erase the old job or halt a
  // live one) happens after the whole request has been validated, below.
  const bool overwrite = getBoolField(mergeOptsParam, "overwrite", false);
  bool supersedeOwner = false;
  bool dropStaleOwner = false;
  {
    auto ownerItr = fxMergeOutputOwner.find(outputPath);
    if (ownerItr != fxMergeOutputOwner.end()) {
      auto existing = fxMergeJobs.find(ownerItr->second);
      if (overwrite || (existing != fxMergeJobs.end() && fxKeptSegments(existing->second))) {
        // Re-adding a failed save resumes it (2026-11-08) -- the same as overwrite=true for this case.
        supersedeOwner = true;
      }
      else if (existing != fxMergeJobs.end()) {
        if (existing->second.state != FX_MERGE_FAILED) {
          A2_LOG_WARN(fmt("[fxmerge] add idempotent reuse output=%s existingParent=%s state=%s",
                          outputPath.c_str(),
                          GroupId::toHex(ownerItr->second).c_str(),
                          fxMergeStateName(existing->second.state)));
          return createGIDResponse(ownerItr->second);
        }
        throw DL_ABORT_EX(fmt("merge output collision: output already claimed by GID#%s state=%s",
                              GroupId::toHex(ownerItr->second).c_str(),
                              fxMergeStateName(existing->second.state)));
      }
      else {
        dropStaleOwner = true;
      }
    }
  }
  std::string tmpDir = supersedeOwner ? fxSupersedeTmpDir(outputPath, std::time(nullptr)) : std::string();

  // A unique directory handed back by the supersede path must win over any caller-supplied one --
  // sharing a directory with the superseded job's still-running children is the race it exists to avoid.
  const bool tmpDirFromSupersede = !tmpDir.empty();
  const std::string defaultTmpDir = outputPath + ".segments";
  if (tmpDir.empty()) {
    tmpDir = defaultTmpDir;
  }
  if (!tmpDirFromSupersede) {
    if (const auto* tmpParam = getStringField(mergeOptsParam, "tmpDir")) {
      if (!tmpParam->s().empty()) {
        tmpDir = tmpParam->s();
      }
    }
  }
  // tmpDir is recursively deleted when the job finishes. 2026-11-14: only the default "<output>.segments" or a
  // sibling named "<output>.segments.<suffix>" (no further '/') is accepted -- the only names this daemon itself
  // uses (the app never sends a tmpDir) -- so it can never be the output, its directory, an ancestor of it, or
  // anything else on disk. Paths with parent-directory components are refused as before.
  {
    while (tmpDir.size() > 1 && tmpDir.back() == '/') {
      tmpDir.pop_back();
    }
    const bool traversal = tmpDir.find("/../") != std::string::npos ||
                           tmpDir.compare(0, 3, "../") == 0 ||
                           (tmpDir.size() >= 3 && tmpDir.compare(tmpDir.size() - 3, 3, "/..") == 0) ||
                           outputPath.find("/../") != std::string::npos ||
                           outputPath.compare(0, 3, "../") == 0;
    const bool ownName =
        tmpDir == defaultTmpDir ||
        (tmpDir.size() > defaultTmpDir.size() + 1 &&
         tmpDir.compare(0, defaultTmpDir.size() + 1, defaultTmpDir + ".") == 0 &&
         tmpDir.find('/', defaultTmpDir.size()) == std::string::npos &&
         tmpDir.compare(defaultTmpDir.size(), std::string::npos, ".") != 0);
    if (traversal || !ownName || File(outputPath).getBasename().empty()) {
      throw DL_ABORT_EX(fmt("Refusing unsafe merge paths: tmpDir=%s output=%s",
                            tmpDir.c_str(), outputPath.c_str()));
    }
  }
  tmpDirForLog = tmpDir;

  std::string mode = "concat";
  if (const auto* modeParam = getStringField(mergeOptsParam, "mode")) {
    mode = modeParam->s();
    util::lowercase(mode);
  }
  bool remux = getBoolField(mergeOptsParam, "remux", mode == "remux");
  if (!remux && mode == "remux") {
    remux = true;
  }
  if (!remux) {
    mode = "concat";
  }
  else {
    mode = "remux";
  }
  modeForLog = mode;
  const auto headersOptionValue =
      getHeadersFieldAsOptionValue(mergeOptsParam, "headers");
  const auto fxCookiesField = getFxCookiesFieldAsOptionValue(mergeOptsParam, "cookies");
  const auto fxAllowedHostSuffixesField =
      getFxAllowedHostSuffixesFieldAsOptionValue(mergeOptsParam, "allowedHostSuffixes");
  const auto fxDomainConnectionCaps =
      getFxDomainConnectionCaps(mergeOptsParam, "domainConnectionCaps");
  const auto fxDomainMinAdmissionIntervals =
      getFxDomainMinAdmissionIntervals(mergeOptsParam, "domainMinAdmissionIntervalMs");
  const auto fxDomainVolumeBudgets =
      getFxDomainVolumeBudgets(mergeOptsParam, "domainVolumeBudget");
  auto fxDomainVolumeBudgetExternal =
      getFxDomainVolumeBudgetExternal(mergeOptsParam, "domainVolumeBudgetExternal");
  const auto fxDomainVideoGaps = getFxDomainVideoGaps(mergeOptsParam, "domainVideoGapMs");

  // Do NOT touch tmpDir synchronously here. On SMB targets this can block JSON-RPC for
  // tens of seconds while a spun-down drive wakes up, causing client-side submit timeouts.
  // RequestGroup disk setup will create/use directories lazily when downloads actually start.

  std::vector<std::shared_ptr<RequestGroup>> groups;
  std::vector<std::string> segmentPaths;
  segmentPaths.reserve(segmentsParam->size());

  size_t index = 0;
  std::shared_ptr<Option> baseRequestOption;
  for (auto& elem : *segmentsParam) {
    std::string uri;
    if (const auto* s = downcast<String>(elem)) {
      uri = s->s();
    }
    else if (const auto* d = downcast<Dict>(elem)) {
      if (const auto* u = getStringField(d, "uri")) {
        uri = u->s();
      }
    }
    if (uri.empty()) {
      throw DL_ABORT_EX(fmt("segment at index %lu is missing uri",
                            static_cast<unsigned long>(index)));
    }

    // The per-job part of every segment's Option is identical, so it is parsed ONCE (on the first
    // segment, i.e. at the same point in the request as before, so validation errors surface in the
    // same order) and then copied per segment -- instead of re-copying the global Option and
    // re-running gatherRequestOption for each of a video's ~1000+ segments inside this RPC call.
    if (!baseRequestOption) {
      baseRequestOption = std::make_shared<Option>(*e->getOption());
      if (aria2OptsParam) {
        gatherRequestOption(baseRequestOption.get(), aria2OptsParam);
      }
      baseRequestOption->put(PREF_DIR, tmpDir);
      baseRequestOption->put(PREF_CONTINUE, "true");
      if (!headersOptionValue.empty()) {
        baseRequestOption->put(PREF_HEADER, headersOptionValue);
      }
      // put() even when fxCookiesField.encoded is empty (zero domains have a cookie) — `present`
      // alone must make the option `defined()`, since that's the signal that the app is the sole
      // cookie authority for this job (see PREF_FX_COOKIES's own doc comment).
      if (fxCookiesField.present) {
        baseRequestOption->put(PREF_FX_COOKIES, fxCookiesField.encoded);
      }
      // Same present-vs-absent reasoning as cookies above, but for the OPPOSITE default: an app
      // that never sends "allowedHostSuffixes" gets the pre-existing no-restriction behavior
      // (PREF_FX_ALLOWED_HOST_SUFFIXES stays undefined), while one that does is asserting a real
      // allowlist for this job — put() unconditionally so even a present-but-empty list still
      // makes the option defined() and fails closed (see prefs.h's own doc comment).
      if (fxAllowedHostSuffixesField.present) {
        baseRequestOption->put(PREF_FX_ALLOWED_HOST_SUFFIXES, fxAllowedHostSuffixesField.encoded);
      }
    }
    auto requestOption = std::make_shared<Option>(*baseRequestOption);
    const auto filename = fxSegmentFileName(index, uri);
    requestOption->put(PREF_OUT, filename);

    std::vector<std::shared_ptr<RequestGroup>> created;
    createRequestGroupForUri(created, requestOption, std::vector<std::string>{uri},
                             /* ignoreForceSeq = */ true,
                             /* ignoreLocalPath = */ true);
    if (created.empty()) {
      throw DL_ABORT_EX(fmt("could not create request group for segment %lu",
                            static_cast<unsigned long>(index)));
    }
    groups.push_back(created.front());
    segmentPaths.push_back(util::applyDir(tmpDir, filename));
    ++index;
  }

  if (groups.empty()) {
    throw DL_ABORT_EX("segments must contain at least one URI.");
  }

  // 2026-11-14: everything above only parsed and validated; nothing below throws. Now take the output over from
  // its previous owner (see fxSupersedeTmpDir) and apply this job's per-domain settings.
  if (supersedeOwner) {
    fxSupersedeOwnerForOverwrite(outputPath, e);
  }
  else if (dropStaleOwner) {
    fxMergeOutputOwner.erase(outputPath);
  }
  for (const auto& entry : fxDomainConnectionCaps) {
    e->getRequestGroupMan()->setDomainConnectionCapOverride(entry.first, entry.second);
  }
  for (const auto& entry : fxDomainMinAdmissionIntervals) {
    e->getRequestGroupMan()->setDomainMinAdmissionIntervalOverride(
        entry.first, std::chrono::milliseconds(entry.second));
  }
  for (const auto& entry : fxDomainVolumeBudgets) {
    e->getRequestGroupMan()->setDomainVolumeBudgetOverride(
        entry.domain, entry.count, std::chrono::seconds(entry.windowSeconds));
  }
  for (auto& entry : fxDomainVolumeBudgetExternal) {
    e->getRequestGroupMan()->setDomainVolumeBudgetExternal(entry.first, std::move(entry.second));
  }
  for (const auto& entry : fxDomainVideoGaps) {
    e->getRequestGroupMan()->setDomainVideoGapOverride(entry.domain, entry.minMs, entry.maxMs);
  }

  const auto parentGid = groups.front()->getGID();
  if (groups.size() > 1) {
    groups.front()->followedBy(groups.begin() + 1, groups.end());
    for (size_t i = 1; i < groups.size(); ++i) {
      groups[i]->following(parentGid);
      groups[i]->belongsTo(parentGid);
    }
  }

  FxMergeJob job;
  job.parentGid = parentGid;
  job.outputPath = outputPath;
  job.tmpDir = tmpDir;
  job.mode = mode;
  job.remux = remux;
  job.textFetch = fxIsTextFetchJob(outputPath, groups.size(), remux);
  if (const auto* expectedDuration = getIntegerField(mergeOptsParam, "expectedDurationSeconds")) {
    job.expectedDurationSec = std::max<int64_t>(0, expectedDuration->i());
  }
  job.state = FX_MERGE_DOWNLOADING;
  job.segmentPaths = segmentPaths;
  for (auto& g : groups) {
    auto gid = g->getGID();
    job.childGids.push_back(gid);
    job.childDone[gid] = false;
    job.childOK[gid] = true;
    job.childPath[gid] = segmentPaths[job.childGids.size() - 1];
    fxMergeChildToParent[gid] = parentGid;
    fxEverMergeChildren.insert(gid);
  }
  fxMergeJobs[parentGid] = job;
  fxMergeOutputOwner[outputPath] = parentGid;

  // FXPlayer (2026-11-07): one video identity across this video's jobs for the video-boundary gap.
  if (const auto* videoGroupParam = getStringField(mergeOptsParam, "videoGroup")) {
    if (videoGroupParam->s().size() <= 256) {
      e->getRequestGroupMan()->setFxVideoIdentity(parentGid, videoGroupParam->s());
    }
  }
  e->getRequestGroupMan()->addReservedGroup(groups);

  A2_LOG_WARN(fmt("[fxmerge] add parent=%s segments=%lu mode=%s remux=%s tmpDir=%s output=%s overwrite=%s",
                  GroupId::toHex(parentGid).c_str(),
                  static_cast<unsigned long>(groups.size()), mode.c_str(),
                  remux ? "true" : "false", tmpDir.c_str(),
                  outputPath.c_str(), overwrite ? "true" : "false"));

  return createGIDResponse(parentGid);
  }
  catch (RecoverableException& ex) {
    const auto op = fxMergeOperationFromOutputPath(outputPathForLog);
    A2_LOG_ERROR(fmt("[fxmerge] add failed operation=%s output=%s segments=%lu mode=%s tmpDir=%s reason=%s",
                     op.c_str(), outputPathForLog.c_str(), segmentCountForLog,
                     modeForLog.c_str(), tmpDirForLog.c_str(), ex.what()));
    throw DL_ABORT_EX(fmt("unable to %s with aria2: %s", op.c_str(), ex.what()));
  }
}

std::unique_ptr<ValueBase> FxplayerRetryMergeRpcMethod::process(
    const RpcRequest& req, DownloadEngine* e)
{
  // NOTE: still fully synchronous, unlike fxMergeOnGroupStopped's automatic path below — this holds
  // fxMergeMutex (and blocks the whole event loop, same as before this hardening pass) for the entire
  // ffmpeg remux. Deliberately NOT converted to the async pattern: the Swift RPC client
  // (Aria2RPCClient.retryMerge / WebService.swift's "retry merge-only" path) explicitly depends on this
  // call not returning until the file is actually merged on disk — see its own comment ("synchronous,
  // the file is already merged by the time the call returns without throwing"). Converting this path
  // too would need a coordinated client-side change (poll findMergeByOutput instead of awaiting this
  // call) that's out of scope here. This is a rare, explicit, user-initiated action (the "retry" button
  // on one failed download), unlike the automatic path, which fires on every single successful HLS
  // download and is the one that actually mattered for daemon-wide responsiveness.
  std::lock_guard<std::recursive_mutex> lock(fxMergeMutex);
  fxPruneExpiredMergeJobs();

  const String* gidParam = checkRequiredParam<String>(req, 0);
  a2_gid_t gid;
  if (GroupId::toNumericId(gid, gidParam->s().c_str()) != 0) {
    gid = str2Gid(gidParam);
  }

  auto itr = fxMergeJobs.find(gid);
  if (itr == fxMergeJobs.end()) {
    throw DL_ABORT_EX(fmt("No fx merge job for GID#%s",
                          GroupId::toHex(gid).c_str()));
  }
  auto& job = itr->second;

  if (!fxAllChildrenDone(job)) {
    throw DL_ABORT_EX("Cannot retry merge: some segments are still in progress.");
  }
  if (fxAnyChildFailed(job)) {
    throw DL_ABORT_EX("Cannot retry merge: one or more segments failed; re-add the job to resume downloads.");
  }

  if (job.state != FX_MERGE_FAILED) {
    throw DL_ABORT_EX(fmt("Cannot retry merge: job is in state '%s'; only failed merge jobs are retryable.",
                          fxMergeStateName(job.state)));
  }
  if (job.cleanupDone) {
    throw DL_ABORT_EX("Cannot retry merge: the job's segments were already cleaned up; re-add the job.");
  }

  job.errorCode = 0;
  job.errorMessage.clear();
  job.state = FX_MERGE_MERGING;
  bool finalizeOk;
  {
    // Same single remux slot as the automatic path (fxMergeSlotMutex's comment) -- if an automatic
    // merge for some OTHER download is mid-ffmpeg right now, this call waits for it rather than
    // forking a second concurrent ffmpeg. This call is already documented to block the whole daemon
    // for its own remux duration (rare, explicit user action); waiting for someone else's remux to
    // finish first extends that same accepted tradeoff, it doesn't introduce a new one.
    std::unique_lock<std::mutex> slotLock(fxMergeSlotMutex, std::defer_lock);
    if (fxFinalizeNeedsSlot(job)) {
      slotLock.lock();
    }
    finalizeOk = fxFinalizeMerge(job);
  }
  if (!finalizeOk) {
    fxSettleFailedJob(job);
    throw DL_ABORT_EX(fmt("Retry merge failed for GID#%s: %s",
                          GroupId::toHex(gid).c_str(),
                          job.errorMessage.c_str()));
  }

  A2_LOG_WARN(fmt("[fxmerge] retry complete parent=%s",
                  GroupId::toHex(gid).c_str()));
  return createOKResponse();
}

std::unique_ptr<ValueBase> FxplayerFindMergeByOutputRpcMethod::process(
    const RpcRequest& req, DownloadEngine* e)
{
  (void)e;
  std::lock_guard<std::recursive_mutex> lock(fxMergeMutex);
  fxPruneExpiredMergeJobs();

  const String* outputParam = checkRequiredParam<String>(req, 0);
  const auto outputPath = outputParam->s();

  auto result = Dict::g();
  auto ownerItr = fxMergeOutputOwner.find(outputPath);
  if (ownerItr == fxMergeOutputOwner.end()) {
    result->put("found", VLB_FALSE);
    return std::move(result);
  }

  auto jobItr = fxMergeJobs.find(ownerItr->second);
  if (jobItr == fxMergeJobs.end()) {
    fxMergeOutputOwner.erase(ownerItr);
    result->put("found", VLB_FALSE);
    return std::move(result);
  }

  const auto& job = jobItr->second;
  const bool retryable = fxIsRetryable(job);

  result->put("found", VLB_TRUE);
  result->put("gid", GroupId::toHex(job.parentGid));
  result->put("status", fxMergeStatusValue(job));
  result->put("state", fxMergeStateName(job.state));
  result->put("stage", fxMergeStateName(job.state));
  result->put("terminal", fxIsTerminalState(job.state) ? VLB_TRUE : VLB_FALSE);
  result->put("retryable", retryable ? VLB_TRUE : VLB_FALSE);
  result->put("outcome", fxMergeOutcome(job, retryable));
  result->put("output", job.outputPath);
  if (job.errorCode != 0) {
    result->put("errorCode", util::itos(job.errorCode));
  }
  if (!job.errorMessage.empty()) {
    result->put("errorMessage", job.errorMessage);
  }
  return std::move(result);
}

// FXPlayer extension (2026-11-07): fxplayer.getVolumeBudget(domain, windowSeconds) — the daemon's own
// volume-budget admissions for one budget key still inside the window, so the app can wait for room
// before resolving/submitting instead of guessing from its own mirror.
std::unique_ptr<ValueBase> FxplayerGetVolumeBudgetRpcMethod::process(
    const RpcRequest& req, DownloadEngine* e)
{
  const String* domainParam = checkRequiredParam<String>(req, 0);
  const Integer* windowParam = checkRequiredParam<Integer>(req, 1);
  const std::string& domain = domainParam->s();
  if (domain.empty() || domain.size() > 256) {
    throw DL_ABORT_EX("fxplayer.getVolumeBudget: domain must be 1-256 characters");
  }
  const auto windowSeconds = windowParam->i();
  if (windowSeconds <= 0 || windowSeconds > FX_MAX_BUDGET_WINDOW_SECONDS) {
    throw DL_ABORT_EX("fxplayer.getVolumeBudget: windowSeconds must be in (0, 604800]");
  }
  auto* rgman = e->getRequestGroupMan().get();
  const auto admissions =
      rgman->getDomainVolumeBudgetAdmissions(domain, std::chrono::seconds(windowSeconds));
  auto list = List::g();
  for (auto t : admissions) {
    list->append(Integer::g(t));
  }
  auto result = Dict::g();
  result->put("domain", domain);
  result->put("windowSeconds", Integer::g(windowSeconds));
  result->put("count", Integer::g(rgman->getDomainVolumeBudgetCount(domain)));
  result->put("admissions", std::move(list));
  return std::move(result);
}

// FXPlayer extension (item 6, 2026-09-27): reads back a completed job's output file as text, then
// deletes it and erases the job. Purpose: let the app fetch an HLS playlist through the SAME
// host/connection its segments come from (submitted as an ordinary single-URL fxplayer.addMerge
// concat job to a throwaway output path) rather than fetching it directly from the phone, which
// left playlist requests and segment requests looking like they came from two different clients
// for the same video. Deliberately reuses the merge-job machinery wholesale instead of adding a
// second, parallel "just fetch a URL" code path — a playlist fetch and a thumbnail/trailer aux
// fetch are the same shape of request (one small file, one URL), so there is nothing HLS-specific
// in this method; it only knows how to read a finished job's output back out.


// FXPlayer extension (2026-11-08): fxplayer.stopMerge(gid) -> {found, state}. Stops a running merge job the app
// has given up on (retry limit, stall) WITHOUT deleting anything: its queued segments are dropped, active ones
// halted, and every segment already downloaded stays on disk for the save's next attempt (an addMerge for the
// same output resumes from them). A job that is merging or already finished is left as it is.
// FXPlayer extension (2026-11-11): fxplayer.reportVolume({budgetKey: [unix seconds]}) -> {keys}. The app's own
// requests against a shared volume budget — its in-app downloads AND its playback — reported whenever they change,
// not only with a new merge job: a job can sit queued or paced for hours while the app plays. Replaces the app's
// list for each key given (an empty list clears it), the same store addMerge's "domainVolumeBudgetExternal" fills;
// the cap check before every segment counts it. Not persisted (the app re-sends after a restart).
std::unique_ptr<ValueBase> FxplayerReportVolumeRpcMethod::process(const RpcRequest& req, DownloadEngine* e)
{
  const Dict* param = checkRequiredParam<Dict>(req, 0);
  // Same validation as addMerge's "domainVolumeBudgetExternal": last 7 days only, bounded size.
  size_t keys = 0;
  for (const auto& elem : *param) {
    const auto* list = downcast<List>(elem.second.get());
    if (!list || elem.first.empty() || elem.first.size() > 512) {
      continue;
    }
    const int64_t nowSec = std::chrono::duration_cast<std::chrono::seconds>(
                               std::chrono::system_clock::now().time_since_epoch()).count();
    std::vector<std::chrono::system_clock::time_point> times;
    for (const auto& v : *list) {
      const auto* n = downcast<Integer>(v.get());
      if (!n) {
        continue;
      }
      const int64_t sec = n->i();
      if (sec < nowSec - FX_EXTERNAL_VOLUME_MAX_AGE_SECONDS || sec > nowSec + FX_EXTERNAL_VOLUME_MAX_SKEW_SECONDS) {
        continue;
      }
      times.push_back(std::chrono::system_clock::time_point(std::chrono::seconds(sec)));
      if (times.size() >= FX_EXTERNAL_VOLUME_MAX_ENTRIES) {
        break;
      }
    }
    e->getRequestGroupMan()->setDomainVolumeBudgetExternal(elem.first, std::move(times));
    ++keys;
  }
  auto result = Dict::g();
  result->put("keys", util::uitos(keys));
  return std::move(result);
}

std::unique_ptr<ValueBase> FxplayerStopMergeRpcMethod::process(const RpcRequest& req, DownloadEngine* e)
{
  std::lock_guard<std::recursive_mutex> lock(fxMergeMutex);
  fxPruneExpiredMergeJobs();
  const String* gidParam = checkRequiredParam<String>(req, 0);
  a2_gid_t gid;
  if (GroupId::toNumericId(gid, gidParam->s().c_str()) != 0) {
    gid = str2Gid(gidParam);
  }
  auto result = Dict::g();
  auto itr = fxMergeJobs.find(gid);
  if (itr == fxMergeJobs.end()) {
    result->put("found", VLB_FALSE);
    return std::move(result);
  }
  auto& job = itr->second;
  // A cancel/delete already in progress wins (forceRemove / removeMergeOutput): turning it into a "stop" here
  // would cancel its cleanup and leak the deleted save's files.
  if (!job.cancelRequested() && (job.state == FX_MERGE_DOWNLOADING || job.state == FX_MERGE_QUEUED)) {
    fxMarkFailed(job, FX_MERGE_ERR_STOPPED, "stopped by FXPlayer; downloaded segments kept",
                 !fxKeepsSegmentsOnFailure(job));
    fxHaltRemainingChildren(job, e);
    fxSettleFailedJob(job);
  }
  result->put("found", VLB_TRUE);
  result->put("state", fxMergeStateName(job.state));
  return std::move(result);
}

// FXPlayer extension (2026-11-08): fxplayer.removeMergeOutput(output) -> {removed, live}. The app deleted a
// save: drop its job and delete everything aria2 kept for `output` -- the segments directory (default and any
// superseded ones) and partial outputs. Never the finished output file itself (the app deletes that). Works
// without a job record too: the record expires 30 minutes after a job ends and doesn't survive a restart,
// while a failed save's kept segments stay until the save is deleted. A job still running is canceled like
// aria2.forceRemove, and its own cleanup deletes its files once its downloads have stopped (`live` = true).
std::unique_ptr<ValueBase> FxplayerRemoveMergeOutputRpcMethod::process(const RpcRequest& req,
                                                                        DownloadEngine* e)
{
  std::lock_guard<std::recursive_mutex> lock(fxMergeMutex);
  fxPruneExpiredMergeJobs();
  const String* outputParam = checkRequiredParam<String>(req, 0);
  const std::string outputPath = outputParam->s();
  if (outputPath.empty() || outputPath.back() == '/' || outputPath.find("/../") != std::string::npos ||
      outputPath.compare(0, 3, "../") == 0 || File(outputPath).getBasename().empty()) {
    throw DL_ABORT_EX(fmt("fxplayer.removeMergeOutput: unsafe output path: %s", outputPath.c_str()));
  }

  bool live = false;
  size_t removed = 0;
  auto ownerItr = fxMergeOutputOwner.find(outputPath);
  if (ownerItr != fxMergeOutputOwner.end()) {
    const auto gid = ownerItr->second;
    auto jobItr = fxMergeJobs.find(gid);
    if (jobItr != fxMergeJobs.end()) {
      auto& job = jobItr->second;
      if (fxIsTerminalState(job.state)) {
        if (job.state == FX_MERGE_FAILED) {
          fxCleanupFailureArtifacts(job);
          ++removed;
        }
        File(fxMergePartPath(job)).remove();
        fxEraseMergeJob(gid);
        e->getRequestGroupMan()->removeDownloadResult(gid);
      }
      else {
        job.requestCancel();
        fxHaltRemainingChildren(job, e);
        if (job.state != FX_MERGE_MERGING) {
          fxFinishCancelIfNothingRunning(job);
        }
        live = !job.cleanupDone;
      }
    }
    else {
      fxMergeOutputOwner.erase(ownerItr);
    }
  }

  if (!live) {
    const File output(outputPath);
    const auto dir = output.getDirname();
    const auto segmentsPrefix = output.getBasename() + ".segments";
    if (DIR* d = opendir(dir.c_str())) {
      std::vector<std::string> names;
      while (auto* entry = readdir(d)) {
        const std::string name(entry->d_name);
        if (name == segmentsPrefix || name.compare(0, segmentsPrefix.size() + 1, segmentsPrefix + ".") == 0) {
          names.push_back(name);
        }
      }
      closedir(d);
      for (const auto& name : names) {
        if (fxRemoveTree(util::applyDir(dir, name))) {
          ++removed;
        }
      }
    }
  }
  A2_LOG_WARN(fmt("[fxmerge] removeMergeOutput output=%s removed=%lu live=%s", outputPath.c_str(),
                  static_cast<unsigned long>(removed), live ? "true" : "false"));
  auto result = Dict::g();
  result->put("removed", util::uitos(removed));
  result->put("live", live ? VLB_TRUE : VLB_FALSE);
  return std::move(result);
}

std::unique_ptr<ValueBase> FxplayerReadTextOutputRpcMethod::process(
    const RpcRequest& req, DownloadEngine* e)
{
  std::lock_guard<std::recursive_mutex> lock(fxMergeMutex);
  fxPruneExpiredMergeJobs();

  const String* gidParam = checkRequiredParam<String>(req, 0);
  const a2_gid_t gid = str2Gid(gidParam);

  auto jobItr = fxMergeJobs.find(gid);
  if (jobItr == fxMergeJobs.end()) {
    throw DL_ABORT_EX(fmt("no fx merge job for GID#%s (already read, or never existed)",
                          GroupId::toHex(gid).c_str()));
  }
  auto& job = jobItr->second;
  // 2026-11-14: only a text fetch is read back and deleted here. Any other GID (a video, its kept segments) is
  // refused untouched: this used to delete whatever output a MERGED gid named, and the kept segments of a FAILED one.
  if (!job.textFetch) {
    throw DL_ABORT_EX(fmt("GID#%s is not a text fetch job; nothing read or deleted",
                          GroupId::toHex(gid).c_str()));
  }
  if (job.state == FX_MERGE_FAILED) {
    const std::string message = job.errorMessage;
    // A failed text-fetch job still needs its artifacts cleared and its GID freed the same as a
    // successful one -- the caller isn't going to retry it (it's ephemeral, single-attempt). Always: a
    // segment failure no longer sets cleanupPending (2026-11-08, kept segments), and this one is never resumed.
    fxCleanupFailureArtifacts(job);
    File(job.outputPath).remove();
    fxEraseMergeJob(gid);
    e->getRequestGroupMan()->removeDownloadResult(gid);
    throw DL_ABORT_EX(fmt("fetch failed: %s", message.c_str()));
  }
  if (job.state != FX_MERGE_MERGED) {
    throw DL_ABORT_EX(fmt("GID#%s is not finished yet (state=%s) -- poll fxplayer.tellStatus first",
                          GroupId::toHex(gid).c_str(), fxMergeStateName(job.state)));
  }

  // A playlist is at most a few tens of KB; this cap is generous headroom against a
  // misbehaving/compromised endpoint returning something enormous under an "m3u8" URL, not a
  // realistic size for the files this method is meant to serve.
  constexpr int64_t maxTextBytes = 8 * 1024 * 1024;
  File outputFile(job.outputPath);
  const int64_t size = outputFile.size();
  if (size < 0 || size > maxTextBytes) {
    // Nothing deleted (2026-11-14): something this large is not the playlist the app asked for.
    throw DL_ABORT_EX(fmt("output too large to read as text: %s bytes=%" PRId64,
                          job.outputPath.c_str(), size));
  }

  std::ifstream in(job.outputPath, std::ios::binary);
  std::string content;
  if (in) {
    content.assign(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
  }
  const bool readOk = static_cast<bool>(in) || in.eof();

  File(job.outputPath).remove();
  fxEraseMergeJob(gid);
  e->getRequestGroupMan()->removeDownloadResult(gid);

  if (!readOk) {
    throw DL_ABORT_EX(fmt("failed to read output: %s", job.outputPath.c_str()));
  }

  auto result = Dict::g();
  result->put("text", content);
  result->put("bytes", util::itos(content.size()));
  return std::move(result);
}

// Runs fxFinalizeMerge's blocking sequence (segment-ready wait, ffmpeg fork/wait, disk I/O, the
// publish-rename retry loop — collectively up to ~20+ seconds in the worst case, dominated by ffmpeg's
// own remux time) on a background thread instead of the calling (main event-loop) thread. Before this,
// EVERY successful HLS download's final segment landing synchronously froze the ENTIRE aria2 daemon —
// every other concurrent download, every RPC call (progress polls, pause/cancel) — for however long the
// remux took, since aria2's single-threaded reactor has nothing else to run while blocked in a usleep/
// waitpid polling loop. (FxplayerRetryMergeRpcMethod::process — the OTHER caller of fxFinalizeMerge —
// deliberately still calls it synchronously; see that function's own comment for why.)
//
// The worker operates on a VALUE COPY of the job, never touching fxMergeJobs/fxMergeChildToParent/
// fxMergeOutputOwner except in its own writeback at the very end (under fxMergeMutex, briefly) — every
// other step (fork/exec, waitpid, file I/O) works purely on the copy's data, so it needs no
// synchronization at all. A cancel request set on the LIVE map entry while the worker is running is
// still observed: FxMergeJob's cancelFlag is a shared_ptr to an atomic, so the copy and the original
// alias the SAME underlying flag (see FxMergeJob's own comment).
void fxFinalizeMergeAsync(a2_gid_t parentGid)
{
  FxMergeJob snapshot;
  {
    std::lock_guard<std::recursive_mutex> lock(fxMergeMutex);
    auto itr = fxMergeJobs.find(parentGid);
    if (itr == fxMergeJobs.end()) {
      return;
    }
    itr->second.state = FX_MERGE_MERGING;
    itr->second.mergeProgress = 0.0;
    snapshot = itr->second;
  }

  std::thread([parentGid, snapshot]() mutable {
    bool ok;
    {
      // Queue up for the single remux slot -- see fxMergeSlotMutex's own comment. Multiple finalize
      // threads spawned close together (the common case: several HLS downloads finishing around the
      // same time) block here in roughly arrival order and run their ffmpeg one at a time; the job
      // was already marked FX_MERGE_MERGING before this thread was spawned, so from the app's point
      // of view nothing distinguishes "waiting for the slot" from "ffmpeg is running" -- both are
      // legitimate non-stall merge-phase time. Only a job that runs ffmpeg/ffprobe takes the slot
      // (2026-11-14): a playlist fetch or thumbnail no longer waits behind another video's remux.
      std::unique_lock<std::mutex> slotLock(fxMergeSlotMutex, std::defer_lock);
      if (fxFinalizeNeedsSlot(snapshot)) {
        slotLock.lock();
      }
      ok = fxFinalizeMerge(snapshot);
    }

    std::lock_guard<std::recursive_mutex> lock(fxMergeMutex);
    auto itr = fxMergeJobs.find(parentGid);
    if (itr == fxMergeJobs.end()) {
      // Erased/pruned/superseded (TTL eviction, or an overwrite=true retry reclaiming the output
      // path) while the worker was running — nothing left to write the result into. Not a leak:
      // whatever the worker published or left as a failure artifact sits at paths
      // fxSupersedeOwnerForOverwrite/fxCleanupFailureArtifacts already know to look for.
      if (snapshot.state == FX_MERGE_MERGED) {
        A2_LOG_WARN(fmt("[fxmerge] parent=%s published output=%s after its job record was removed",
                        GroupId::toHex(parentGid).c_str(), snapshot.outputPath.c_str()));
      }
      return;
    }
    auto& job = itr->second;
    // 2026-11-14: the main thread may have decided this job's fate while the worker ran -- superseded by an
    // overwrite (marked failed, output handed to a new job). Its decision stands: copying the worker's result
    // over it would resurrect a superseded job as "merged" or as the output's owner-in-progress.
    auto ownerItr = fxMergeOutputOwner.find(job.outputPath);
    const bool ownerChanged = ownerItr == fxMergeOutputOwner.end() || ownerItr->second != parentGid;
    if (fxIsTerminalState(job.state) || ownerChanged) {
      if (snapshot.state == FX_MERGE_MERGED) {
        A2_LOG_WARN(fmt("[fxmerge] parent=%s published output=%s although it was %s meanwhile; keeping state=%s",
                        GroupId::toHex(parentGid).c_str(), job.outputPath.c_str(),
                        ownerChanged ? "superseded" : "finished", fxMergeStateName(job.state)));
      }
      fxSettleFailedJob(job);
      return;
    }
    job.state = snapshot.state;
    job.errorCode = snapshot.errorCode;
    job.errorMessage = snapshot.errorMessage;
    job.mergeProgress = snapshot.mergeProgress;
    job.cleanupPending = snapshot.cleanupPending;
    job.cleanupDone = snapshot.cleanupDone;
    job.terminalEpochSec = snapshot.terminalEpochSec;

    if (!ok) {
      fxSettleFailedJob(job);
    }
  }).detach();
}

bool fxIsMergeChild(a2_gid_t gid)
{
  std::lock_guard<std::recursive_mutex> lock(fxMergeMutex);
  return fxEverMergeChildren.count(gid) > 0;
}

void fxForgetMergeChild(a2_gid_t gid)
{
  std::lock_guard<std::recursive_mutex> lock(fxMergeMutex);
  fxEverMergeChildren.erase(gid);
}

void fxMergeOnAllChildrenDone(FxMergeJob& job, a2_gid_t parent);

namespace {
// The expiry XNXX's and XVideos' CDNs sign into a media URL's path: ".../<token>,<unix seconds>/...".
// 0 when the URI carries none. Allocation-free (2026-11-14): runs for every held segment on every admission pass.
int64_t fxSignedLinkExpiry(const std::string& uri)
{
  const size_t end = std::min(uri.find_first_of("?#"), uri.size());
  for (size_t pos = uri.find(','); pos != std::string::npos && pos < end; pos = uri.find(',', pos + 1)) {
    size_t i = pos + 1;
    int64_t value = 0;
    while (i < end && i - pos - 1 < 11 && uri[i] >= '0' && uri[i] <= '9') {
      value = value * 10 + (uri[i] - '0');
      ++i;
    }
    if (i - pos - 1 == 10 && i < end && uri[i] == '/') {
      return value;
    }
  }
  return 0;
}

// Same CDN list as RequestGroupMan.cc's isFxPacedCdnHost: only the sites the app paces.
bool fxIsPacedCdnUri(const std::string& uri)
{
  const auto scheme = uri.find("://");
  if (scheme == std::string::npos) {
    return false;
  }
  const size_t hostStart = scheme + 3;
  const size_t hostEnd = std::min(uri.find_first_of(":/?#", hostStart), uri.size());
  static const char* const suffixes[] = {".xnxx-cdn.com", ".xvideos-cdn.com", ".red-cdn.com"};
  for (const char* suffix : suffixes) {
    const size_t n = strlen(suffix);
    if (hostEnd - hostStart >= n && strncasecmp(uri.c_str() + hostEnd - n, suffix, n) == 0) {
      return true;
    }
  }
  return false;
}

const std::string& fxFirstUri(const RequestGroup& group)
{
  static const std::string none;
  const auto& dctx = group.getDownloadContext();
  if (!dctx) {
    return none;
  }
  const auto& fileEntry = dctx->getFirstFileEntry();
  if (!fileEntry) {
    return none;
  }
  const auto& spent = fileEntry->getSpentUris();
  const auto& remaining = fileEntry->getRemainingUris();
  if (!spent.empty()) {
    return spent.front();
  }
  return remaining.empty() ? none : remaining.front();
}
} // namespace

// FXPlayer (2026-11-08): RequestGroupMan asks this for every reserved group it is about to admit, BEFORE
// pacing, the video gap or the volume budget see it. True = the caller drops the group (it never starts):
//   - its job already failed or was canceled: a segment still queued (e.g. held in the admission pass's
//     own deferred list while the job failed) must never go out afterwards -- a failed 4K job once kept
//     sending its remaining segments for 35 minutes, every one answered 410 Gone;
//   - an earlier attempt of the same save already downloaded it completely (its file is on disk with no
//     .aria2 control file, see fxSegmentFileName): reused, no request. A job's first segment (its parent)
//     always downloads, since it carries the job's aria2 status. Checked once per segment, so a job held
//     by the volume budget doesn't re-stat its files on every admission pass;
//   - its signed link already expired (or does within a minute), e.g. after hours held by the volume
//     budget: nothing is sent, the job fails with FX_MERGE_ERR_LINK_EXPIRED keeping every downloaded
//     segment, and the app resubmits with fresh links.
bool fxMergeAdmissionCheck(const std::shared_ptr<RequestGroup>& group, DownloadEngine* e)
{
  std::lock_guard<std::recursive_mutex> lock(fxMergeMutex);
  const auto gid = group->getGID();
  a2_gid_t parent = 0;
  if (fxMergeJobs.count(gid) > 0) {
    parent = gid;
  }
  else {
    auto citr = fxMergeChildToParent.find(gid);
    if (citr == fxMergeChildToParent.end()) {
      return false;
    }
    parent = citr->second;
  }
  auto jobItr = fxMergeJobs.find(parent);
  if (jobItr == fxMergeJobs.end()) {
    return false;
  }
  auto& job = jobItr->second;

  if (job.state == FX_MERGE_FAILED || job.cancelRequested()) {
    if (!job.childDone[gid]) {
      fxMarkReservedChildRemoved(job, gid);
    }
    fxSettleFailedJob(job);
    return true;
  }
  if (job.state != FX_MERGE_DOWNLOADING && job.state != FX_MERGE_QUEUED) {
    return false;
  }

  const auto& uri = fxFirstUri(*group);
  if (fxIsPacedCdnUri(uri)) {
    const auto expiry = fxSignedLinkExpiry(uri);
    const auto now = static_cast<int64_t>(std::time(nullptr));
    if (expiry > 0 && expiry <= now + 60) {
      job.childDone[gid] = true;
      job.childOK[gid] = false;
      size_t kept = 0;
      for (auto childGid : job.childGids) {
        if (job.childDone[childGid] && job.childOK[childGid]) {
          ++kept;
        }
      }
      fxMarkFailed(job, FX_MERGE_ERR_LINK_EXPIRED,
                   fmt("signed links expired %llds ago, before segment gid=%s could be requested; nothing "
                       "sent, %lu downloaded segment(s) kept",
                       static_cast<long long>(now - expiry), GroupId::toHex(gid).c_str(),
                       static_cast<unsigned long>(kept)),
                   !fxKeepsSegmentsOnFailure(job));
      fxHaltRemainingChildren(job, e);
      fxSettleFailedJob(job);
      return true;
    }
  }

  if (gid == parent || !job.skipChecked.insert(gid).second) {
    return false;
  }
  auto pathItr = job.childPath.find(gid);
  if (pathItr == job.childPath.end()) {
    return false;
  }
  if (!job.completedListLoaded) {
    job.completedListLoaded = true;
    std::ifstream list(util::applyDir(job.tmpDir, FX_COMPLETED_LIST));
    std::string name;
    while (std::getline(list, name)) {
      if (!name.empty()) {
        job.completedOnDisk.insert(name);
      }
    }
  }
  const auto& path = pathItr->second;
  File file(path);
  if (job.completedOnDisk.count(file.getBasename()) == 0 || !file.isFile() || file.size() <= 0 ||
      File(path + ".aria2").exists()) {
    return false;
  }
  job.childDone[gid] = true;
  job.childOK[gid] = true;
  if (++job.skippedCount == 1) {
    A2_LOG_WARN(fmt("[fxmerge] parent=%s reusing segments an earlier attempt already downloaded tmpDir=%s",
                    GroupId::toHex(parent).c_str(), job.tmpDir.c_str()));
  }
  if (fxAllChildrenDone(job)) {
    fxMergeOnAllChildrenDone(job, parent);
  }
  return true;
}

void fxMergeOnGroupStopped(const std::shared_ptr<RequestGroup>& group,
                           DownloadEngine* e, error_code::Value result)
{
  std::lock_guard<std::recursive_mutex> lock(fxMergeMutex);
  fxPruneExpiredMergeJobs();

  const auto gid = group->getGID();
  a2_gid_t parent = 0;

  auto pitr = fxMergeJobs.find(gid);
  if (pitr != fxMergeJobs.end()) {
    parent = gid;
  }
  else {
    auto citr = fxMergeChildToParent.find(gid);
    if (citr == fxMergeChildToParent.end()) {
      return;
    }
    parent = citr->second;
  }

  auto jobItr = fxMergeJobs.find(parent);
  if (jobItr == fxMergeJobs.end()) {
    return;
  }
  auto& job = jobItr->second;
  if (job.state == FX_MERGE_MERGED) {
    return;
  }

  job.childDone[gid] = true;
  const bool ok = result == error_code::FINISHED;
  job.childOK[gid] = ok;
  if (ok && fxKeepsSegmentsOnFailure(job)) {
    auto pathItr = job.childPath.find(gid);
    if (pathItr != job.childPath.end()) {
      fxNoteSegmentCompleted(job, File(pathItr->second).getBasename());
    }
  }

  if (job.state == FX_MERGE_FAILED) {
    fxSettleFailedJob(job);
    return;
  }

  A2_LOG_INFO(fmt("[fxmerge] child-stop parent=%s child=%s result=%d",
                  GroupId::toHex(parent).c_str(), GroupId::toHex(gid).c_str(),
                  static_cast<int>(result)));

  if (!ok) {
    if (job.cancelRequested()) {
      fxMarkFailed(job, FX_MERGE_ERR_CANCELED,
                   fmt("merge canceled by user during download gid=%s code=%d",
                       GroupId::toHex(gid).c_str(), static_cast<int>(result)),
                   true);
    }
    else {
      // Segments kept (2026-11-08): the next attempt of this save resumes from them instead of
      // downloading the whole video again. Deleting the save in the app removes them.
      fxMarkFailed(
          job, FX_MERGE_ERR_SEGMENT,
          fmt("segment download failed gid=%s code=%d",
              GroupId::toHex(gid).c_str(), static_cast<int>(result)),
          !fxKeepsSegmentsOnFailure(job));
    }
    // One failed segment fails the whole video, so the rest are wasted requests (and with
    // expired links, a stream of 410s that also eats the volume budget). Stop them now.
    fxHaltRemainingChildren(job, e);
    fxSettleFailedJob(job);
    return;
  }

  if (!fxAllChildrenDone(job)) {
    job.state = FX_MERGE_DOWNLOADING;
    return;
  }

  fxMergeOnAllChildrenDone(job, parent);
}

// Every child of `job` has reported (downloaded, failed or skipped as already on disk): fail the job or
// start its merge. Caller holds fxMergeMutex.
void fxMergeOnAllChildrenDone(FxMergeJob& job, a2_gid_t parent)
{
  // A delete (forceRemove / removeMergeOutput) only requests cancel and halts the children (2026-11-14 review): when
  // the halted children then report back as failed, this used to record a segment failure — which KEEPS a
  // multi-segment job's files — so the deleted save's segments stayed on disk and a later add resumed from them.
  if (job.cancelRequested()) {
    if (job.state != FX_MERGE_FAILED) {
      fxMarkFailed(job, FX_MERGE_ERR_CANCELED, "merge canceled by user", true);
    }
    fxSettleFailedJob(job);
    return;
  }
  if (fxAnyChildFailed(job)) {
    fxMarkFailed(job, FX_MERGE_ERR_SEGMENT, "one or more segments failed", !fxKeepsSegmentsOnFailure(job));
    fxSettleFailedJob(job);
    return;
  }

  // A single-file concat job (playlist fetch, thumbnail, trailer) is a one-file copy that finishes
  // within milliseconds, and its "add" + "finalize complete" WARN lines already bracket it -- so its
  // start marker goes to INFO. Multi-segment and remux jobs keep the WARN start marker, since their
  // finalize can wait for the remux slot / run ffmpeg for tens of seconds.
  A2_LOG((job.childGids.size() == 1 && !job.remux) ? Logger::A2_INFO : Logger::A2_WARN,
         fmt("[fxmerge] parent=%s all segments downloaded (%lu reused from disk); starting merge (async) mode=%s tmpDir=%s output=%s",
             GroupId::toHex(parent).c_str(), static_cast<unsigned long>(job.skippedCount),
             job.mode.c_str(), job.tmpDir.c_str(), job.outputPath.c_str()));
  fxFinalizeMergeAsync(parent);
}

namespace {
std::string getHexSha1(const std::string& s)
{
  unsigned char hash[20];
  message_digest::digest(hash, sizeof(hash), MessageDigest::sha1().get(),
                         s.data(), s.size());
  return util::toHex(hash, sizeof(hash));
}
} // namespace

#ifdef ENABLE_BITTORRENT
std::unique_ptr<ValueBase> AddTorrentRpcMethod::process(const RpcRequest& req,
                                                        DownloadEngine* e)
{
  const String* torrentParam = checkRequiredParam<String>(req, 0);
  const List* urisParam = checkParam<List>(req, 1);
  const Dict* optsParam = checkParam<Dict>(req, 2);
  const Integer* posParam = checkParam<Integer>(req, 3);

  std::unique_ptr<String> tempTorrentParam;
  if (req.jsonRpc) {
    tempTorrentParam = String::g(
        base64::decode(torrentParam->s().begin(), torrentParam->s().end()));
    torrentParam = tempTorrentParam.get();
  }
  std::vector<std::string> uris;
  extractUris(std::back_inserter(uris), urisParam);

  auto requestOption = std::make_shared<Option>(*e->getOption());
  gatherRequestOption(requestOption.get(), optsParam);

  bool posGiven = checkPosParam(posParam);
  size_t pos = posGiven ? posParam->i() : 0;

  std::string filename;
  if (requestOption->getAsBool(PREF_RPC_SAVE_UPLOAD_METADATA)) {
    filename = util::applyDir(requestOption->get(PREF_DIR),
                              getHexSha1(torrentParam->s()) + ".torrent");
    // Save uploaded data in order to save this download in
    // --save-session file.
    if (util::saveAs(filename, torrentParam->s(), true)) {
      A2_LOG_INFO(
          fmt("Uploaded torrent data was saved as %s", filename.c_str()));
      requestOption->put(PREF_TORRENT_FILE, filename);
    }
    else {
      A2_LOG_INFO(fmt("Uploaded torrent data was not saved."
                      " Failed to write file %s",
                      filename.c_str()));
      filename.clear();
    }
  }
  std::vector<std::shared_ptr<RequestGroup>> result;
  createRequestGroupForBitTorrent(result, requestOption, uris, filename,
                                  torrentParam->s());

  if (!result.empty()) {
    return addRequestGroup(result.front(), e, posGiven, pos);
  }
  else {
    throw DL_ABORT_EX("No Torrent to download.");
  }
}
#endif // ENABLE_BITTORRENT

#ifdef ENABLE_METALINK
std::unique_ptr<ValueBase> AddMetalinkRpcMethod::process(const RpcRequest& req,
                                                         DownloadEngine* e)
{
  const String* metalinkParam = checkRequiredParam<String>(req, 0);
  const Dict* optsParam = checkParam<Dict>(req, 1);
  const Integer* posParam = checkParam<Integer>(req, 2);

  std::unique_ptr<String> tempMetalinkParam;
  if (req.jsonRpc) {
    tempMetalinkParam = String::g(
        base64::decode(metalinkParam->s().begin(), metalinkParam->s().end()));
    metalinkParam = tempMetalinkParam.get();
  }
  auto requestOption = std::make_shared<Option>(*e->getOption());
  gatherRequestOption(requestOption.get(), optsParam);

  bool posGiven = checkPosParam(posParam);
  size_t pos = posGiven ? posParam->i() : 0;

  std::vector<std::shared_ptr<RequestGroup>> result;
  std::string filename;
  if (requestOption->getAsBool(PREF_RPC_SAVE_UPLOAD_METADATA)) {
    // TODO RFC5854 Metalink has the extension .meta4 and Metalink
    // Version 3 uses .metalink extension. We use .meta4 for both
    // RFC5854 Metalink and Version 3. aria2 can detect which of which
    // by reading content rather than extension.
    filename = util::applyDir(requestOption->get(PREF_DIR),
                              getHexSha1(metalinkParam->s()) + ".meta4");
    // Save uploaded data in order to save this download in
    // --save-session file.
    if (util::saveAs(filename, metalinkParam->s(), true)) {
      A2_LOG_INFO(
          fmt("Uploaded metalink data was saved as %s", filename.c_str()));
      requestOption->put(PREF_METALINK_FILE, filename);
      createRequestGroupForMetalink(result, requestOption);
    }
    else {
      A2_LOG_INFO(fmt("Uploaded metalink data was not saved."
                      " Failed to write file %s",
                      filename.c_str()));
      createRequestGroupForMetalink(result, requestOption, metalinkParam->s());
    }
  }
  else {
    createRequestGroupForMetalink(result, requestOption, metalinkParam->s());
  }
  auto gids = List::g();
  if (!result.empty()) {
    if (posGiven) {
      e->getRequestGroupMan()->insertReservedGroup(pos, result);
    }
    else {
      e->getRequestGroupMan()->addReservedGroup(result);
    }
    for (auto& i : result) {
      gids->append(GroupId::toHex(i->getGID()));
    }
  }
  return std::move(gids);
}
#endif // ENABLE_METALINK

namespace {
std::unique_ptr<ValueBase> removeDownload(const RpcRequest& req,
                                          DownloadEngine* e, bool forceRemove)
{
  std::lock_guard<std::recursive_mutex> lock(fxMergeMutex);
  fxPruneExpiredMergeJobs();

  const String* gidParam = checkRequiredParam<String>(req, 0);

  a2_gid_t gid;
  if (GroupId::toNumericId(gid, gidParam->s().c_str()) != 0 ||
      fxMergeJobs.find(gid) == fxMergeJobs.end()) {
    gid = str2Gid(gidParam);
  }

  auto mergeItr = fxMergeJobs.find(gid);
  if (mergeItr != fxMergeJobs.end()) {
    auto& job = mergeItr->second;

    if (fxIsTerminalState(job.state)) {
      // Deleting a terminal merge job should always clear remaining artifacts.
      if (job.state == FX_MERGE_FAILED) {
        fxCleanupFailureArtifacts(job);
      }
      File(fxMergePartPath(job)).remove();
      {
        // Only delete the shared output path if this job still owns it; a superseded job's GID being
        // removed later must not delete the file its replacement is producing.
        auto ownerItr = fxMergeOutputOwner.find(job.outputPath);
        if ((ownerItr == fxMergeOutputOwner.end() || ownerItr->second == gid) && job.state == FX_MERGE_FAILED) {
          File(job.outputPath).remove();
        }
      }
      fxEraseMergeJob(gid);
      e->getRequestGroupMan()->removeDownloadResult(gid);
      return createGIDResponse(gid);
    }

    job.requestCancel();

    bool touched = false;
    for (auto childGid : job.childGids) {
      auto child = e->getRequestGroupMan()->findGroup(childGid);
      if (!child) {
        continue;
      }

      if (child->getState() == RequestGroup::STATE_ACTIVE) {
        if (forceRemove) {
          child->setForceHaltRequested(true, RequestGroup::USER_REQUEST);
        }
        else {
          child->setHaltRequested(true, RequestGroup::USER_REQUEST);
        }
        touched = true;
      }
      else if (child->isDependencyResolved()) {
        e->getRequestGroupMan()->removeReservedGroup(childGid);
        fxMarkReservedChildRemoved(job, childGid);
        touched = true;
      }
    }
    fxFinishCancelIfNothingRunning(job);

    if (touched) {
      e->setRefreshInterval(std::chrono::milliseconds(0));
    }
    return createGIDResponse(gid);
  }

  auto group = e->getRequestGroupMan()->findGroup(gid);
  if (group) {
    if (group->getState() == RequestGroup::STATE_ACTIVE) {
      if (forceRemove) {
        group->setForceHaltRequested(true, RequestGroup::USER_REQUEST);
      }
      else {
        group->setHaltRequested(true, RequestGroup::USER_REQUEST);
      }
      e->setRefreshInterval(std::chrono::milliseconds(0));
    }
    else {
      if (group->isDependencyResolved()) {
        e->getRequestGroupMan()->removeReservedGroup(gid);
      }
      else {
        throw DL_ABORT_EX(
            fmt("GID#%s cannot be removed now", GroupId::toHex(gid).c_str()));
      }
    }
  }
  else {
    throw DL_ABORT_EX(fmt("Active Download not found for GID#%s",
                          GroupId::toHex(gid).c_str()));
  }
  return createGIDResponse(gid);
}
} // namespace

std::unique_ptr<ValueBase> RemoveRpcMethod::process(const RpcRequest& req,
                                                    DownloadEngine* e)
{
  return removeDownload(req, e, false);
}

std::unique_ptr<ValueBase> ForceRemoveRpcMethod::process(const RpcRequest& req,
                                                         DownloadEngine* e)
{
  return removeDownload(req, e, true);
}

namespace {
std::unique_ptr<ValueBase> pauseDownload(const RpcRequest& req,
                                         DownloadEngine* e, bool forcePause)
{
  const String* gidParam = checkRequiredParam<String>(req, 0);

  a2_gid_t gid = str2Gid(gidParam);
  auto group = e->getRequestGroupMan()->findGroup(gid);
  if (group) {
    bool reserved = group->getState() == RequestGroup::STATE_WAITING;
    if (pauseRequestGroup(group, reserved, forcePause)) {
      e->setRefreshInterval(std::chrono::milliseconds(0));
      return createGIDResponse(gid);
    }
  }
  throw DL_ABORT_EX(
      fmt("GID#%s cannot be paused now", GroupId::toHex(gid).c_str()));
}
} // namespace

std::unique_ptr<ValueBase> PauseRpcMethod::process(const RpcRequest& req,
                                                   DownloadEngine* e)
{
  return pauseDownload(req, e, false);
}

std::unique_ptr<ValueBase> ForcePauseRpcMethod::process(const RpcRequest& req,
                                                        DownloadEngine* e)
{
  return pauseDownload(req, e, true);
}

namespace {
template <typename InputIterator>
void pauseRequestGroups(InputIterator first, InputIterator last, bool reserved,
                        bool forcePause)
{
  for (; first != last; ++first) {
    pauseRequestGroup(*first, reserved, forcePause);
  }
}
} // namespace

namespace {
std::unique_ptr<ValueBase> pauseAllDownloads(const RpcRequest& req,
                                             DownloadEngine* e, bool forcePause)
{
  auto& groups = e->getRequestGroupMan()->getRequestGroups();
  pauseRequestGroups(groups.begin(), groups.end(), false, forcePause);
  auto& reservedGroups = e->getRequestGroupMan()->getReservedGroups();
  pauseRequestGroups(reservedGroups.begin(), reservedGroups.end(), true,
                     forcePause);
  return createOKResponse();
}
} // namespace

std::unique_ptr<ValueBase> PauseAllRpcMethod::process(const RpcRequest& req,
                                                      DownloadEngine* e)
{
  return pauseAllDownloads(req, e, false);
}

std::unique_ptr<ValueBase>
ForcePauseAllRpcMethod::process(const RpcRequest& req, DownloadEngine* e)
{
  return pauseAllDownloads(req, e, true);
}

std::unique_ptr<ValueBase> UnpauseRpcMethod::process(const RpcRequest& req,
                                                     DownloadEngine* e)
{
  const String* gidParam = checkRequiredParam<String>(req, 0);

  a2_gid_t gid = str2Gid(gidParam);
  auto group = e->getRequestGroupMan()->findGroup(gid);
  if (!group || group->getState() != RequestGroup::STATE_WAITING ||
      !group->isPauseRequested()) {
    throw DL_ABORT_EX(
        fmt("GID#%s cannot be unpaused now", GroupId::toHex(gid).c_str()));
  }
  else {
    group->setPauseRequested(false);
    e->getRequestGroupMan()->requestQueueCheck();
  }
  return createGIDResponse(gid);
}

std::unique_ptr<ValueBase> UnpauseAllRpcMethod::process(const RpcRequest& req,
                                                        DownloadEngine* e)
{
  auto& groups = e->getRequestGroupMan()->getReservedGroups();
  for (auto& group : groups) {
    group->setPauseRequested(false);
  }
  e->getRequestGroupMan()->requestQueueCheck();
  return createOKResponse();
}

namespace {
template <typename InputIterator>
void createUriEntry(List* uriList, InputIterator first, InputIterator last,
                    const std::string& status)
{
  for (; first != last; ++first) {
    auto entry = Dict::g();
    entry->put(KEY_URI, *first);
    entry->put(KEY_STATUS, status);
    uriList->append(std::move(entry));
  }
}
} // namespace

namespace {
void createUriEntry(List* uriList, const std::shared_ptr<FileEntry>& file)
{
  createUriEntry(uriList, std::begin(file->getSpentUris()),
                 std::end(file->getSpentUris()), VLB_USED);
  createUriEntry(uriList, std::begin(file->getRemainingUris()),
                 std::end(file->getRemainingUris()), VLB_WAITING);
}
} // namespace

namespace {
template <typename InputIterator>
void createFileEntry(List* files, InputIterator first, InputIterator last,
                     const BitfieldMan* bf)
{
  size_t index = 1;
  for (; first != last; ++first, ++index) {
    auto entry = Dict::g();
    entry->put(KEY_INDEX, util::uitos(index));
    entry->put(KEY_PATH, (*first)->getPath());
    entry->put(KEY_SELECTED, (*first)->isRequested() ? VLB_TRUE : VLB_FALSE);
    entry->put(KEY_LENGTH, util::itos((*first)->getLength()));
    int64_t completedLength = bf->getOffsetCompletedLength(
        (*first)->getOffset(), (*first)->getLength());
    entry->put(KEY_COMPLETED_LENGTH, util::itos(completedLength));

    auto uriList = List::g();
    createUriEntry(uriList.get(), *first);
    entry->put(KEY_URIS, std::move(uriList));
    files->append(std::move(entry));
  }
}
} // namespace

namespace {
template <typename InputIterator>
void createFileEntry(List* files, InputIterator first, InputIterator last,
                     int64_t totalLength, int32_t pieceLength,
                     const std::string& bitfield)
{
  BitfieldMan bf(pieceLength, totalLength);
  bf.setBitfield(reinterpret_cast<const unsigned char*>(bitfield.data()),
                 bitfield.size());
  createFileEntry(files, first, last, &bf);
}
} // namespace

namespace {
template <typename InputIterator>
void createFileEntry(List* files, InputIterator first, InputIterator last,
                     int64_t totalLength, int32_t pieceLength,
                     const std::shared_ptr<PieceStorage>& ps)
{
  BitfieldMan bf(pieceLength, totalLength);
  if (ps) {
    bf.setBitfield(ps->getBitfield(), ps->getBitfieldLength());
  }
  createFileEntry(files, first, last, &bf);
}
} // namespace

namespace {
bool requested_key(const std::vector<std::string>& keys, const std::string& k)
{
  return keys.empty() || std::find(keys.begin(), keys.end(), k) != keys.end();
}
} // namespace

void gatherProgressCommon(Dict* entryDict,
                          const std::shared_ptr<RequestGroup>& group,
                          const std::vector<std::string>& keys)
{
  auto& ps = group->getPieceStorage();
  if (requested_key(keys, KEY_GID)) {
    entryDict->put(KEY_GID, GroupId::toHex(group->getGID()).c_str());
  }
  if (requested_key(keys, KEY_TOTAL_LENGTH)) {
    // This is "filtered" total length if --select-file is used.
    entryDict->put(KEY_TOTAL_LENGTH, util::itos(group->getTotalLength()));
  }
  if (requested_key(keys, KEY_COMPLETED_LENGTH)) {
    // This is "filtered" total length if --select-file is used.
    entryDict->put(KEY_COMPLETED_LENGTH,
                   util::itos(group->getCompletedLength()));
  }
  TransferStat stat = group->calculateStat();
  if (requested_key(keys, KEY_DOWNLOAD_SPEED)) {
    entryDict->put(KEY_DOWNLOAD_SPEED, util::itos(stat.downloadSpeed));
  }
  if (requested_key(keys, KEY_UPLOAD_SPEED)) {
    entryDict->put(KEY_UPLOAD_SPEED, util::itos(stat.uploadSpeed));
  }
  if (requested_key(keys, KEY_UPLOAD_LENGTH)) {
    entryDict->put(KEY_UPLOAD_LENGTH, util::itos(stat.allTimeUploadLength));
  }
  if (requested_key(keys, KEY_CONNECTIONS)) {
    entryDict->put(KEY_CONNECTIONS, util::itos(group->getNumConnection()));
  }
  if (requested_key(keys, KEY_BITFIELD)) {
    if (ps) {
      if (ps->getBitfieldLength() > 0) {
        entryDict->put(KEY_BITFIELD,
                       util::toHex(ps->getBitfield(), ps->getBitfieldLength()));
      }
    }
  }
  auto& dctx = group->getDownloadContext();
  if (requested_key(keys, KEY_PIECE_LENGTH)) {
    entryDict->put(KEY_PIECE_LENGTH, util::itos(dctx->getPieceLength()));
  }
  if (requested_key(keys, KEY_NUM_PIECES)) {
    entryDict->put(KEY_NUM_PIECES, util::uitos(dctx->getNumPieces()));
  }
  if (requested_key(keys, KEY_FOLLOWED_BY)) {
    if (!group->followedBy().empty()) {
      auto list = List::g();
      // The element is GID.
      for (auto& gid : group->followedBy()) {
        list->append(GroupId::toHex(gid));
      }
      entryDict->put(KEY_FOLLOWED_BY, std::move(list));
    }
  }
  if (requested_key(keys, KEY_FOLLOWING)) {
    if (group->following()) {
      entryDict->put(KEY_FOLLOWING, GroupId::toHex(group->following()));
    }
  }
  if (requested_key(keys, KEY_BELONGS_TO)) {
    if (group->belongsTo()) {
      entryDict->put(KEY_BELONGS_TO, GroupId::toHex(group->belongsTo()));
    }
  }
  if (requested_key(keys, KEY_FILES)) {
    auto files = List::g();
    createFileEntry(files.get(), std::begin(dctx->getFileEntries()),
                    std::end(dctx->getFileEntries()), dctx->getTotalLength(),
                    dctx->getPieceLength(), ps);
    entryDict->put(KEY_FILES, std::move(files));
  }
  if (requested_key(keys, KEY_DIR)) {
    entryDict->put(KEY_DIR, group->getOption()->get(PREF_DIR));
  }
}

#ifdef ENABLE_BITTORRENT
void gatherBitTorrentMetadata(Dict* btDict, TorrentAttribute* torrentAttrs)
{
  if (!torrentAttrs->comment.empty()) {
    btDict->put(KEY_COMMENT, torrentAttrs->comment);
  }
  if (torrentAttrs->creationDate) {
    btDict->put(KEY_CREATION_DATE, Integer::g(torrentAttrs->creationDate));
  }
  if (torrentAttrs->mode) {
    btDict->put(KEY_MODE, bittorrent::getModeString(torrentAttrs->mode));
  }
  auto destAnnounceList = List::g();
  for (auto& annlist : torrentAttrs->announceList) {
    auto destAnnounceTier = List::g();
    for (auto& ann : annlist) {
      destAnnounceTier->append(ann);
    }
    destAnnounceList->append(std::move(destAnnounceTier));
  }
  btDict->put(KEY_ANNOUNCE_LIST, std::move(destAnnounceList));
  if (!torrentAttrs->metadata.empty()) {
    auto infoDict = Dict::g();
    infoDict->put(KEY_NAME, torrentAttrs->name);
    btDict->put(KEY_INFO, std::move(infoDict));
  }
}

namespace {
void gatherProgressBitTorrent(Dict* entryDict,
                              const std::shared_ptr<RequestGroup>& group,
                              TorrentAttribute* torrentAttrs,
                              BtObject* btObject,
                              const std::vector<std::string>& keys)
{
  if (requested_key(keys, KEY_INFO_HASH)) {
    entryDict->put(KEY_INFO_HASH, util::toHex(torrentAttrs->infoHash));
  }
  if (requested_key(keys, KEY_BITTORRENT)) {
    auto btDict = Dict::g();
    gatherBitTorrentMetadata(btDict.get(), torrentAttrs);
    entryDict->put(KEY_BITTORRENT, std::move(btDict));
  }
  if (requested_key(keys, KEY_NUM_SEEDERS)) {
    if (!btObject) {
      entryDict->put(KEY_NUM_SEEDERS, VLB_ZERO);
    }
    else {
      auto& peerStorage = btObject->peerStorage;
      assert(peerStorage);
      auto& peers = peerStorage->getUsedPeers();
      entryDict->put(KEY_NUM_SEEDERS,
                     util::uitos(countSeeder(peers.begin(), peers.end())));
    }
  }
  if (requested_key(keys, KEY_SEEDER)) {
    entryDict->put(KEY_SEEDER, group->isSeeder() ? VLB_TRUE : VLB_FALSE);
  }
}
} // namespace

namespace {
void gatherPeer(List* peers, const std::shared_ptr<PeerStorage>& ps)
{
  auto& usedPeers = ps->getUsedPeers();
  for (auto& peer : usedPeers) {
    if (!peer->isActive()) {
      continue;
    }
    auto peerEntry = Dict::g();
    peerEntry->put(KEY_PEER_ID, util::torrentPercentEncode(peer->getPeerId(),
                                                           PEER_ID_LENGTH));
    peerEntry->put(KEY_IP, peer->getIPAddress());
    if (peer->isIncomingPeer()) {
      peerEntry->put(KEY_PORT, VLB_ZERO);
    }
    else {
      peerEntry->put(KEY_PORT, util::uitos(peer->getPort()));
    }
    peerEntry->put(KEY_BITFIELD,
                   util::toHex(peer->getBitfield(), peer->getBitfieldLength()));
    peerEntry->put(KEY_AM_CHOKING, peer->amChoking() ? VLB_TRUE : VLB_FALSE);
    peerEntry->put(KEY_PEER_CHOKING,
                   peer->peerChoking() ? VLB_TRUE : VLB_FALSE);
    peerEntry->put(KEY_DOWNLOAD_SPEED,
                   util::itos(peer->calculateDownloadSpeed()));
    peerEntry->put(KEY_UPLOAD_SPEED, util::itos(peer->calculateUploadSpeed()));
    peerEntry->put(KEY_SEEDER, peer->isSeeder() ? VLB_TRUE : VLB_FALSE);
    peers->append(std::move(peerEntry));
  }
}
} // namespace
#endif // ENABLE_BITTORRENT

namespace {
void gatherProgress(Dict* entryDict, const std::shared_ptr<RequestGroup>& group,
                    DownloadEngine* e, const std::vector<std::string>& keys)
{
  gatherProgressCommon(entryDict, group, keys);
#ifdef ENABLE_BITTORRENT
  if (group->getDownloadContext()->hasAttribute(CTX_ATTR_BT)) {
    gatherProgressBitTorrent(
        entryDict, group,
        bittorrent::getTorrentAttrs(group->getDownloadContext()),
        e->getBtRegistry()->get(group->getGID()), keys);
  }
#endif // ENABLE_BITTORRENT
  if (e->getCheckIntegrityMan()) {
    if (e->getCheckIntegrityMan()->isPicked(
            [&group](const CheckIntegrityEntry& ent) {
              return ent.getRequestGroup() == group.get();
            })) {
      entryDict->put(
          KEY_VERIFIED_LENGTH,
          util::itos(
              e->getCheckIntegrityMan()->getPickedEntry()->getCurrentLength()));
    }
    if (e->getCheckIntegrityMan()->isQueued(
            [&group](const CheckIntegrityEntry& ent) {
              return ent.getRequestGroup() == group.get();
            })) {
      entryDict->put(KEY_VERIFY_PENDING, VLB_TRUE);
    }
  }
}
} // namespace

void gatherStoppedDownload(Dict* entryDict,
                           const std::shared_ptr<DownloadResult>& ds,
                           const std::vector<std::string>& keys)
{
  if (requested_key(keys, KEY_GID)) {
    entryDict->put(KEY_GID, ds->gid->toHex());
  }
  if (requested_key(keys, KEY_ERROR_CODE)) {
    entryDict->put(KEY_ERROR_CODE, util::itos(static_cast<int>(ds->result)));
  }
  if (requested_key(keys, KEY_ERROR_MESSAGE)) {
    entryDict->put(KEY_ERROR_MESSAGE, ds->resultMessage);
  }
  if (requested_key(keys, KEY_STATUS)) {
    if (ds->result == error_code::REMOVED) {
      entryDict->put(KEY_STATUS, VLB_REMOVED);
    }
    else if (ds->result == error_code::FINISHED) {
      entryDict->put(KEY_STATUS, VLB_COMPLETE);
    }
    else {
      entryDict->put(KEY_STATUS, VLB_ERROR);
    }
  }
  if (requested_key(keys, KEY_FOLLOWED_BY)) {
    if (!ds->followedBy.empty()) {
      auto list = List::g();
      // The element is GID.
      for (auto gid : ds->followedBy) {
        list->append(GroupId::toHex(gid));
      }
      entryDict->put(KEY_FOLLOWED_BY, std::move(list));
    }
  }
  if (requested_key(keys, KEY_FOLLOWING)) {
    if (ds->following) {
      entryDict->put(KEY_FOLLOWING, GroupId::toHex(ds->following));
    }
  }
  if (requested_key(keys, KEY_BELONGS_TO)) {
    if (ds->belongsTo) {
      entryDict->put(KEY_BELONGS_TO, GroupId::toHex(ds->belongsTo));
    }
  }
  if (requested_key(keys, KEY_FILES)) {
    auto files = List::g();
    createFileEntry(files.get(), std::begin(ds->fileEntries),
                    std::end(ds->fileEntries), ds->totalLength, ds->pieceLength,
                    ds->bitfield);
    entryDict->put(KEY_FILES, std::move(files));
  }
  if (requested_key(keys, KEY_TOTAL_LENGTH)) {
    entryDict->put(KEY_TOTAL_LENGTH, util::itos(ds->totalLength));
  }
  if (requested_key(keys, KEY_COMPLETED_LENGTH)) {
    entryDict->put(KEY_COMPLETED_LENGTH, util::itos(ds->completedLength));
  }
  if (requested_key(keys, KEY_UPLOAD_LENGTH)) {
    entryDict->put(KEY_UPLOAD_LENGTH, util::itos(ds->uploadLength));
  }
  if (requested_key(keys, KEY_BITFIELD)) {
    if (!ds->bitfield.empty()) {
      entryDict->put(KEY_BITFIELD, util::toHex(ds->bitfield));
    }
  }
  if (requested_key(keys, KEY_DOWNLOAD_SPEED)) {
    entryDict->put(KEY_DOWNLOAD_SPEED, VLB_ZERO);
  }
  if (requested_key(keys, KEY_UPLOAD_SPEED)) {
    entryDict->put(KEY_UPLOAD_SPEED, VLB_ZERO);
  }
  if (!ds->infoHash.empty()) {
    if (requested_key(keys, KEY_INFO_HASH)) {
      entryDict->put(KEY_INFO_HASH, util::toHex(ds->infoHash));
    }
    if (requested_key(keys, KEY_NUM_SEEDERS)) {
      entryDict->put(KEY_NUM_SEEDERS, VLB_ZERO);
    }
  }
  if (requested_key(keys, KEY_PIECE_LENGTH)) {
    entryDict->put(KEY_PIECE_LENGTH, util::itos(ds->pieceLength));
  }
  if (requested_key(keys, KEY_NUM_PIECES)) {
    entryDict->put(KEY_NUM_PIECES, util::uitos(ds->numPieces));
  }
  if (requested_key(keys, KEY_CONNECTIONS)) {
    entryDict->put(KEY_CONNECTIONS, VLB_ZERO);
  }
  if (requested_key(keys, KEY_DIR)) {
    entryDict->put(KEY_DIR, ds->dir);
  }

#ifdef ENABLE_BITTORRENT
  if (ds->attrs.size() > CTX_ATTR_BT && ds->attrs[CTX_ATTR_BT]) {
    const auto attrs =
        static_cast<TorrentAttribute*>(ds->attrs[CTX_ATTR_BT].get());
    if (requested_key(keys, KEY_BITTORRENT)) {
      auto btDict = Dict::g();
      gatherBitTorrentMetadata(btDict.get(), attrs);
      entryDict->put(KEY_BITTORRENT, std::move(btDict));
    }
  }
#endif // ENABLE_BITTORRENT
}

std::unique_ptr<ValueBase> GetFilesRpcMethod::process(const RpcRequest& req,
                                                      DownloadEngine* e)
{
  const String* gidParam = checkRequiredParam<String>(req, 0);

  a2_gid_t gid = str2Gid(gidParam);
  auto files = List::g();
  auto group = e->getRequestGroupMan()->findGroup(gid);
  if (!group) {
    auto dr = e->getRequestGroupMan()->findDownloadResult(gid);
    if (!dr) {
      throw DL_ABORT_EX(fmt("No file data is available for GID#%s",
                            GroupId::toHex(gid).c_str()));
    }
    else {
      createFileEntry(files.get(), std::begin(dr->fileEntries),
                      std::end(dr->fileEntries), dr->totalLength,
                      dr->pieceLength, dr->bitfield);
    }
  }
  else {
    auto& dctx = group->getDownloadContext();
    createFileEntry(files.get(),
                    std::begin(group->getDownloadContext()->getFileEntries()),
                    std::end(group->getDownloadContext()->getFileEntries()),
                    dctx->getTotalLength(), dctx->getPieceLength(),
                    group->getPieceStorage());
  }
  return std::move(files);
}

std::unique_ptr<ValueBase> GetUrisRpcMethod::process(const RpcRequest& req,
                                                     DownloadEngine* e)
{
  const String* gidParam = checkRequiredParam<String>(req, 0);

  a2_gid_t gid = str2Gid(gidParam);
  auto group = e->getRequestGroupMan()->findGroup(gid);
  if (!group) {
    throw DL_ABORT_EX(fmt("No URI data is available for GID#%s",
                          GroupId::toHex(gid).c_str()));
  }
  auto uriList = List::g();
  // TODO Current implementation just returns first FileEntry's URIs.
  if (!group->getDownloadContext()->getFileEntries().empty()) {
    createUriEntry(uriList.get(),
                   group->getDownloadContext()->getFirstFileEntry());
  }
  return std::move(uriList);
}

#ifdef ENABLE_BITTORRENT
std::unique_ptr<ValueBase> GetPeersRpcMethod::process(const RpcRequest& req,
                                                      DownloadEngine* e)
{
  const String* gidParam = checkRequiredParam<String>(req, 0);

  a2_gid_t gid = str2Gid(gidParam);
  auto group = e->getRequestGroupMan()->findGroup(gid);
  if (!group) {
    throw DL_ABORT_EX(fmt("No peer data is available for GID#%s",
                          GroupId::toHex(gid).c_str()));
  }
  auto peers = List::g();
  auto btObject = e->getBtRegistry()->get(group->getGID());
  if (btObject) {
    assert(btObject->peerStorage);
    gatherPeer(peers.get(), btObject->peerStorage);
  }
  return std::move(peers);
}
#endif // ENABLE_BITTORRENT

namespace {
int64_t fxMedianSample(std::vector<int64_t> samples)
{
  if (samples.empty()) {
    return 0;
  }

  const size_t mid = samples.size() / 2;
  std::nth_element(samples.begin(), samples.begin() + mid, samples.end());
  int64_t median = samples[mid];
  if ((samples.size() % 2) == 0) {
    std::nth_element(samples.begin(), samples.begin() + mid - 1,
                     samples.begin() + mid);
    median = (median + samples[mid - 1]) / 2;
  }
  return median;
}

int64_t fxTypicalSegmentSize(const std::vector<int64_t>& samples,
                            int64_t firstSample)
{
  if (samples.empty()) {
    return 0;
  }

  int64_t median = fxMedianSample(samples);
  if (samples.size() >= 3 && firstSample > 0 && median > 0 &&
      firstSample * 4 < median) {
    std::vector<int64_t> filtered;
    filtered.reserve(samples.size());
    bool skippedFirst = false;
    for (auto sample : samples) {
      if (!skippedFirst && sample == firstSample) {
        skippedFirst = true;
        continue;
      }
      filtered.push_back(sample);
    }
    if (!filtered.empty()) {
      median = fxMedianSample(filtered);
    }
  }

  return median;
}

void gatherFxMergeStatus(Dict* entryDict, FxMergeJob& job,
                         DownloadEngine* e,
                         const std::vector<std::string>& keys)
{
  int64_t totalLength = 0;
  int64_t completedLength = 0;
  int64_t downloadSpeed = 0;
  int64_t finishedBytes = 0;
  int64_t activeBytes = 0;
  size_t completed = 0;
  size_t failed = 0;
  size_t active = 0;
  // Of the not-yet-done children, how many are still sitting in aria2's own admission queue (most
  // commonly deferred behind --max-concurrent-downloads-per-domain, RequestGroupMan.cc's per-domain
  // connection cap) rather than genuinely connected and transferring. This top-level `status` field
  // (below) can never distinguish the two on its own — it collapses every non-terminal FxMergeJob state
  // to VLB_ACTIVE regardless of what the underlying child is actually doing — so the app-side stall
  // watchdog (WebService.swift's pollAria2UntilTerminal, which reads exactly this "waiting" count) has
  // no way to tell "queued, waiting its turn" apart from "connected but stuck" without it. Root-caused
  // 2026-08-23: three downloads were force-removed by the app as stalled at 0 bytes while genuinely just
  // queued behind this cap; the app's first fix attempt checked a plain top-level `status == "waiting"`
  // that this RPC method can never actually produce, since it's synthesized from FxMergeJob state, not
  // passed through from the child RequestGroup's real aria2 state — this counter is what that fix needed.
  size_t waiting = 0;
  std::vector<int64_t> segmentSizeSamples;
  int64_t firstSegmentSample = 0;

  for (auto gid : job.childGids) {
    int64_t childTotalLength = 0;
    int64_t childCompletedLength = 0;
    auto group = e->getRequestGroupMan()->findGroup(gid);
    if (group) {
      childTotalLength = group->getTotalLength();
      childCompletedLength = group->getCompletedLength();
      totalLength += childTotalLength;
      completedLength += childCompletedLength;
      downloadSpeed += group->calculateStat().downloadSpeed;
    }
    else {
      auto dr = e->getRequestGroupMan()->findDownloadResult(gid);
      if (dr) {
        childTotalLength = dr->totalLength;
        childCompletedLength = dr->completedLength;
        totalLength += childTotalLength;
        completedLength += childCompletedLength;
      }
    }

    // Only a child still running has its file stat()ed; a finished one's size is looked up once and kept
    // (2026-11-14: every segment file was stat()ed on every poll, on the main thread).
    auto childFileSize = [&job, gid]() -> int64_t {
      const auto childPathItr = job.childPath.find(gid);
      return childPathItr == job.childPath.end() ? 0 : File(childPathItr->second).size();
    };

    auto doneItr = job.childDone.find(gid);
    if (doneItr != job.childDone.end() && doneItr->second) {
      auto okItr = job.childOK.find(gid);
      if (okItr != job.childOK.end() && okItr->second) {
        ++completed;
        auto sizeItr = job.finishedSize.find(gid);
        if (sizeItr == job.finishedSize.end()) {
          const int64_t observedProgress = std::max<int64_t>(childCompletedLength, childFileSize());
          sizeItr = job.finishedSize
                        .emplace(gid, std::max<int64_t>(observedProgress,
                                                        std::max<int64_t>(childTotalLength, childCompletedLength)))
                        .first;
        }
        const int64_t finishedSize = sizeItr->second;
        if (finishedSize > 0) {
          finishedBytes += finishedSize;
          segmentSizeSamples.push_back(finishedSize);
          if (gid == job.parentGid) {
            firstSegmentSample = finishedSize;
          }
        }
      }
      else {
        ++failed;
      }
    }
    else {
      ++active;
      const int64_t observedProgress = std::max<int64_t>(childCompletedLength, childFileSize());
      // `group` (found above, before this branch) is the same RequestGroup either way — STATE_WAITING
      // means it's sitting in RequestGroupMan's reserved-groups queue, not yet admitted (see this
      // function's own `waiting` doc comment above for why the app needs this distinction).
      if (group && group->getState() == RequestGroup::STATE_WAITING) {
        ++waiting;
      }
      if (observedProgress > 0) {
        activeBytes += observedProgress;
      }
      if (childTotalLength > 0) {
        segmentSizeSamples.push_back(childTotalLength);
        if (gid == job.parentGid && firstSegmentSample == 0) {
          firstSegmentSample = childTotalLength;
        }
      }
    }
  }

  double downloadProgress = 0.0;
  if (totalLength > 0) {
    downloadProgress = static_cast<double>(completedLength) /
                       static_cast<double>(totalLength);
  }
  else if (!job.childGids.empty()) {
    downloadProgress = static_cast<double>(completed + failed) /
                       static_cast<double>(job.childGids.size());
  }
  if (downloadProgress < 0.0) {
    downloadProgress = 0.0;
  }
  if (downloadProgress > 1.0) {
    downloadProgress = 1.0;
  }

  double estimatedDownloadProgress = downloadProgress;
  const int64_t typicalSegmentSize =
      fxTypicalSegmentSize(segmentSizeSamples, firstSegmentSample);
  if (!job.childGids.empty() && typicalSegmentSize > 0) {
    const int64_t estimatedTotalBytes =
        finishedBytes + (typicalSegmentSize * static_cast<int64_t>(active + job.childGids.size() - completed - failed - active));
    const int64_t estimatedCompletedBytes = finishedBytes + activeBytes;
    if (estimatedTotalBytes > 0) {
      estimatedDownloadProgress = static_cast<double>(estimatedCompletedBytes) /
                                  static_cast<double>(estimatedTotalBytes);
    }
  }
  else if (!job.childGids.empty()) {
    // FXPlayer fix (2026-09-22): `active` counts every not-yet-done child, but with a per-domain
    // connection cap of 1 (XNXX's forced-sequential hardening, RequestGroupMan.cc's
    // domainConnectionCapOverrides_) almost all of them are sitting in `waiting` (STATE_WAITING,
    // not yet admitted — see this function's own `waiting` doc comment above), not genuinely
    // transferring. The old formula gave every one of those a flat 0.5 credit regardless, so a
    // freshly-submitted 100-segment sequential job read (0 completed + 0.5*100 active) / 100 =
    // 50% instantly, before a single byte had moved — confirmed live (app-reported progress
    // jumped 0% -> 50% immediately, then sat there for many real segment completions, because the
    // app's own progress display is intentionally monotonic-non-decreasing and the ACCURATE,
    // byte-size-based estimate this branch is only a fallback for — see the `typicalSegmentSize`
    // branch above — reads much LOWER early on, so it got silently floored out by the inflated
    // first value). Only children actually connected and transferring (active minus waiting)
    // deserve partial credit; a merely-queued child is genuinely 0% done, same as this formula
    // already treats a not-yet-created child.
    const size_t transferring = active > waiting ? active - waiting : 0;
    estimatedDownloadProgress =
        (static_cast<double>(completed) + (0.5 * static_cast<double>(transferring))) /
        static_cast<double>(job.childGids.size());
  }
  if (estimatedDownloadProgress < 0.0) {
    estimatedDownloadProgress = 0.0;
  }
  if (estimatedDownloadProgress > 1.0) {
    estimatedDownloadProgress = 1.0;
  }

  double overallProgress = estimatedDownloadProgress;
  if (job.state == FX_MERGE_MERGING) {
    overallProgress = 0.95 + (0.05 * job.mergeProgress);
  }
  else if (job.state == FX_MERGE_MERGED) {
    overallProgress = 1.0;
  }
  if (overallProgress < 0.0) {
    overallProgress = 0.0;
  }
  if (overallProgress > 1.0) {
    overallProgress = 1.0;
  }

  const bool retryable = fxIsRetryable(job);

  if (requested_key(keys, KEY_GID)) {
    entryDict->put(KEY_GID, GroupId::toHex(job.parentGid));
  }
  if (requested_key(keys, KEY_TOTAL_LENGTH)) {
    entryDict->put(KEY_TOTAL_LENGTH, util::itos(totalLength));
  }
  if (requested_key(keys, KEY_COMPLETED_LENGTH)) {
    entryDict->put(KEY_COMPLETED_LENGTH, util::itos(completedLength));
  }
  if (requested_key(keys, KEY_DOWNLOAD_SPEED)) {
    entryDict->put(KEY_DOWNLOAD_SPEED, util::itos(downloadSpeed));
  }
  if (requested_key(keys, KEY_STATUS)) {
    entryDict->put(KEY_STATUS, fxMergeStatusValue(job));
  }
  if (requested_key(keys, KEY_ERROR_CODE)) {
    entryDict->put(KEY_ERROR_CODE, util::itos(job.errorCode));
  }
  if (requested_key(keys, KEY_ERROR_MESSAGE) && !job.errorMessage.empty()) {
    entryDict->put(KEY_ERROR_MESSAGE, job.errorMessage);
  }
  if (requested_key(keys, KEY_FOLLOWED_BY)) {
    auto list = List::g();
    for (auto gid : job.childGids) {
      if (gid != job.parentGid) {
        list->append(GroupId::toHex(gid));
      }
    }
    entryDict->put(KEY_FOLLOWED_BY, std::move(list));
  }

  if (requested_key(keys, KEY_MERGE)) {
    auto merge = Dict::g();
    merge->put("state", fxMergeStateName(job.state));
    merge->put("stage", fxMergeStateName(job.state));
    merge->put("total", util::uitos(job.childGids.size()));
    merge->put("completed", util::uitos(completed));
    merge->put("failed", util::uitos(failed));
    merge->put("waiting", util::uitos(waiting));
    // Of `completed`: reused from an earlier attempt's kept segments, not requested (2026-11-08).
    merge->put("skipped", util::uitos(job.skippedCount));
    merge->put("output", job.outputPath);
    merge->put("mode", job.mode);
    merge->put("mergeProgress", fmt("%.3f", job.mergeProgress));
    merge->put("downloadProgress", fmt("%.3f", downloadProgress));
    merge->put("estimatedDownloadProgress", fmt("%.3f", estimatedDownloadProgress));
    merge->put("overallProgress", fmt("%.3f", overallProgress));
    merge->put("cancelRequested", job.cancelRequested() ? VLB_TRUE : VLB_FALSE);
    merge->put("terminal", fxIsTerminalState(job.state) ? VLB_TRUE : VLB_FALSE);
    merge->put("retryable", retryable ? VLB_TRUE : VLB_FALSE);
    merge->put("outcome", fxMergeOutcome(job, retryable));
    entryDict->put(KEY_MERGE, std::move(merge));
  }
}
} // namespace

std::unique_ptr<ValueBase> TellStatusRpcMethod::process(const RpcRequest& req,
                                                        DownloadEngine* e)
{
  // Only the merge-job branch just below touches fxMergeJobs, but locking the whole function (matching
  // every other fxMerge* touchpoint) is simplest and cheap — an uncontended recursive_mutex lock/unlock
  // is microseconds, not worth a narrower, easier-to-get-wrong scope for an RPC call already dominated
  // by JSON building.
  std::lock_guard<std::recursive_mutex> lock(fxMergeMutex);
  fxPruneExpiredMergeJobs();

  const String* gidParam = checkRequiredParam<String>(req, 0);
  const List* keysParam = checkParam<List>(req, 1);

  a2_gid_t gid;
  const bool exactGid =
      GroupId::toNumericId(gid, gidParam->s().c_str()) == 0;
  std::vector<std::string> keys;
  toStringList(std::back_inserter(keys), keysParam);

  auto mergeItr = exactGid ? fxMergeJobs.find(gid) : fxMergeJobs.end();
  if (mergeItr != fxMergeJobs.end()) {
    auto entryDict = Dict::g();
    gatherFxMergeStatus(entryDict.get(), mergeItr->second, e, keys);
    return std::move(entryDict);
  }

  gid = str2Gid(gidParam);

  auto group = e->getRequestGroupMan()->findGroup(gid);
  auto entryDict = Dict::g();
  if (!group) {
    auto ds = e->getRequestGroupMan()->findDownloadResult(gid);
    if (!ds) {
      throw DL_ABORT_EX(
          fmt("No such download for GID#%s", GroupId::toHex(gid).c_str()));
    }
    gatherStoppedDownload(entryDict.get(), ds, keys);
  }
  else {
    if (requested_key(keys, KEY_STATUS)) {
      if (group->getState() == RequestGroup::STATE_ACTIVE) {
        entryDict->put(KEY_STATUS, VLB_ACTIVE);
      }
      else {
        if (group->isPauseRequested()) {
          entryDict->put(KEY_STATUS, VLB_PAUSED);
        }
        else {
          entryDict->put(KEY_STATUS, VLB_WAITING);
        }
      }
    }
    gatherProgress(entryDict.get(), group, e, keys);
  }
  return std::move(entryDict);
}

std::unique_ptr<ValueBase> TellActiveRpcMethod::process(const RpcRequest& req,
                                                        DownloadEngine* e)
{
  const List* keysParam = checkParam<List>(req, 0);
  std::vector<std::string> keys;
  toStringList(std::back_inserter(keys), keysParam);
  auto list = List::g();
  bool statusReq = requested_key(keys, KEY_STATUS);
  for (auto& group : e->getRequestGroupMan()->getRequestGroups()) {
    auto entryDict = Dict::g();
    if (statusReq) {
      entryDict->put(KEY_STATUS, VLB_ACTIVE);
    }
    gatherProgress(entryDict.get(), group, e, keys);
    list->append(std::move(entryDict));
  }
  return std::move(list);
}

const RequestGroupList& TellWaitingRpcMethod::getItems(DownloadEngine* e) const
{
  return e->getRequestGroupMan()->getReservedGroups();
}

void TellWaitingRpcMethod::createEntry(
    Dict* entryDict, const std::shared_ptr<RequestGroup>& item,
    DownloadEngine* e, const std::vector<std::string>& keys) const
{
  if (requested_key(keys, KEY_STATUS)) {
    if (item->isPauseRequested()) {
      entryDict->put(KEY_STATUS, VLB_PAUSED);
    }
    else {
      entryDict->put(KEY_STATUS, VLB_WAITING);
    }
  }
  gatherProgress(entryDict, item, e, keys);
}

const DownloadResultList&
TellStoppedRpcMethod::getItems(DownloadEngine* e) const
{
  return e->getRequestGroupMan()->getDownloadResults();
}

void TellStoppedRpcMethod::createEntry(
    Dict* entryDict, const std::shared_ptr<DownloadResult>& item,
    DownloadEngine* e, const std::vector<std::string>& keys) const
{
  gatherStoppedDownload(entryDict, item, keys);
}

std::unique_ptr<ValueBase>
PurgeDownloadResultRpcMethod::process(const RpcRequest& req, DownloadEngine* e)
{
  e->getRequestGroupMan()->purgeDownloadResult();
  return createOKResponse();
}

std::unique_ptr<ValueBase>
RemoveDownloadResultRpcMethod::process(const RpcRequest& req, DownloadEngine* e)
{
  const String* gidParam = checkRequiredParam<String>(req, 0);

  a2_gid_t gid = str2Gid(gidParam);
  if (!e->getRequestGroupMan()->removeDownloadResult(gid)) {
    throw DL_ABORT_EX(fmt("Could not remove download result of GID#%s",
                          GroupId::toHex(gid).c_str()));
  }
  return createOKResponse();
}

std::unique_ptr<ValueBase> ChangeOptionRpcMethod::process(const RpcRequest& req,
                                                          DownloadEngine* e)
{
  const String* gidParam = checkRequiredParam<String>(req, 0);
  const Dict* optsParam = checkRequiredParam<Dict>(req, 1);

  a2_gid_t gid = str2Gid(gidParam);
  auto group = e->getRequestGroupMan()->findGroup(gid);
  if (group) {
    Option option;
    std::shared_ptr<Option> pendingOption;
    if (group->getState() == RequestGroup::STATE_ACTIVE) {
      pendingOption = std::make_shared<Option>();
      gatherChangeableOption(&option, pendingOption.get(), optsParam);
      if (!pendingOption->emptyLocal()) {
        group->setPendingOption(pendingOption);
        // pauseRequestGroup() may fail if group has been told to
        // stop/pause already.  In that case, we can still apply the
        // pending options on pause.
        if (pauseRequestGroup(group, false, false)) {
          group->setRestartRequested(true);
          e->setRefreshInterval(std::chrono::milliseconds(0));
        }
      }
    }
    else {
      gatherChangeableOptionForReserved(&option, optsParam);
    }
    changeOption(group, option, e);
  }
  else {
    throw DL_ABORT_EX(
        fmt("Cannot change option for GID#%s", GroupId::toHex(gid).c_str()));
  }
  return createOKResponse();
}

std::unique_ptr<ValueBase>
ChangeGlobalOptionRpcMethod::process(const RpcRequest& req, DownloadEngine* e)
{
  const Dict* optsParam = checkRequiredParam<Dict>(req, 0);

  Option option;
  gatherChangeableGlobalOption(&option, optsParam);
  changeGlobalOption(option, e);
  return createOKResponse();
}

std::unique_ptr<ValueBase> GetVersionRpcMethod::process(const RpcRequest& req,
                                                        DownloadEngine* e)
{
  auto result = Dict::g();
  result->put(KEY_VERSION, PACKAGE_VERSION);
  auto featureList = List::g();
  for (int feat = 0; feat < MAX_FEATURE; ++feat) {
    const char* name = strSupportedFeature(feat);
    if (name) {
      featureList->append(name);
    }
  }
  result->put(KEY_ENABLED_FEATURES, std::move(featureList));
  return std::move(result);
}

namespace {
void pushRequestOption(Dict* dict, const std::shared_ptr<Option>& option,
                       const std::shared_ptr<OptionParser>& oparser)
{
  for (size_t i = 1, len = option::countOption(); i < len; ++i) {
    PrefPtr pref = option::i2p(i);
    const OptionHandler* h = oparser->find(pref);
    if (h && h->getInitialOption() && option->defined(pref)) {
      dict->put(pref->k, option->get(pref));
    }
  }
}
} // namespace

std::unique_ptr<ValueBase> GetOptionRpcMethod::process(const RpcRequest& req,
                                                       DownloadEngine* e)
{
  const String* gidParam = checkRequiredParam<String>(req, 0);

  a2_gid_t gid = str2Gid(gidParam);
  auto group = e->getRequestGroupMan()->findGroup(gid);
  auto result = Dict::g();
  if (!group) {
    auto dr = e->getRequestGroupMan()->findDownloadResult(gid);
    if (!dr) {
      throw DL_ABORT_EX(
          fmt("Cannot get option for GID#%s", GroupId::toHex(gid).c_str()));
    }
    pushRequestOption(result.get(), dr->option, getOptionParser());
  }
  else {
    pushRequestOption(result.get(), group->getOption(), getOptionParser());
  }
  return std::move(result);
}

std::unique_ptr<ValueBase>
GetGlobalOptionRpcMethod::process(const RpcRequest& req, DownloadEngine* e)
{
  auto result = Dict::g();
  for (size_t i = 0, len = e->getOption()->getTable().size(); i < len; ++i) {
    PrefPtr pref = option::i2p(i);
    if (pref == PREF_RPC_SECRET || !e->getOption()->defined(pref)) {
      continue;
    }
    const OptionHandler* h = getOptionParser()->find(pref);
    if (h) {
      result->put(pref->k, e->getOption()->get(pref));
    }
  }
  return std::move(result);
}

std::unique_ptr<ValueBase>
ChangePositionRpcMethod::process(const RpcRequest& req, DownloadEngine* e)
{
  const String* gidParam = checkRequiredParam<String>(req, 0);
  const Integer* posParam = checkRequiredParam<Integer>(req, 1);
  const String* howParam = checkRequiredParam<String>(req, 2);

  a2_gid_t gid = str2Gid(gidParam);
  int pos = posParam->i();
  const std::string& howStr = howParam->s();
  OffsetMode how;
  if (howStr == "POS_SET") {
    how = OFFSET_MODE_SET;
  }
  else if (howStr == "POS_CUR") {
    how = OFFSET_MODE_CUR;
  }
  else if (howStr == "POS_END") {
    how = OFFSET_MODE_END;
  }
  else {
    throw DL_ABORT_EX("Illegal argument.");
  }
  size_t destPos =
      e->getRequestGroupMan()->changeReservedGroupPosition(gid, pos, how);
  return Integer::g(destPos);
}

std::unique_ptr<ValueBase>
GetSessionInfoRpcMethod::process(const RpcRequest& req, DownloadEngine* e)
{
  auto result = Dict::g();
  result->put(KEY_SESSION_ID, util::toHex(e->getSessionId()));
  return std::move(result);
}

std::unique_ptr<ValueBase> GetServersRpcMethod::process(const RpcRequest& req,
                                                        DownloadEngine* e)
{
  const String* gidParam = checkRequiredParam<String>(req, 0);

  a2_gid_t gid = str2Gid(gidParam);
  auto group = e->getRequestGroupMan()->findGroup(gid);
  if (!group || group->getState() != RequestGroup::STATE_ACTIVE) {
    throw DL_ABORT_EX(
        fmt("No active download for GID#%s", GroupId::toHex(gid).c_str()));
  }
  auto result = List::g();
  size_t index = 1;
  for (auto& fe : group->getDownloadContext()->getFileEntries()) {
    auto fileEntry = Dict::g();
    fileEntry->put(KEY_INDEX, util::uitos(index++));
    auto servers = List::g();
    for (auto& req : fe->getInFlightRequests()) {
      auto ps = req->getPeerStat();
      if (ps) {
        auto serverEntry = Dict::g();
        serverEntry->put(KEY_URI, req->getUri());
        serverEntry->put(KEY_CURRENT_URI, req->getCurrentUri());
        serverEntry->put(KEY_DOWNLOAD_SPEED,
                         util::itos(ps->calculateDownloadSpeed()));
        servers->append(std::move(serverEntry));
      }
    }
    fileEntry->put(KEY_SERVERS, std::move(servers));
    result->append(std::move(fileEntry));
  }
  return std::move(result);
}

std::unique_ptr<ValueBase> ChangeUriRpcMethod::process(const RpcRequest& req,
                                                       DownloadEngine* e)
{
  const String* gidParam = checkRequiredParam<String>(req, 0);
  const Integer* indexParam = checkRequiredInteger(req, 1, IntegerGE(1));
  const List* delUrisParam = checkRequiredParam<List>(req, 2);
  const List* addUrisParam = checkRequiredParam<List>(req, 3);
  const Integer* posParam = checkParam<Integer>(req, 4);

  a2_gid_t gid = str2Gid(gidParam);
  bool posGiven = checkPosParam(posParam);
  size_t pos = posGiven ? posParam->i() : 0;
  size_t index = indexParam->i() - 1;
  auto group = e->getRequestGroupMan()->findGroup(gid);
  if (!group) {
    throw DL_ABORT_EX(
        fmt("Cannot remove URIs from GID#%s", GroupId::toHex(gid).c_str()));
  }
  auto& files = group->getDownloadContext()->getFileEntries();
  if (files.size() <= index) {
    throw DL_ABORT_EX(fmt("fileIndex is out of range"));
  }
  auto& s = files[index];
  size_t delcount = 0;
  for (auto& elem : *delUrisParam) {
    const String* uri = downcast<String>(elem);
    if (uri && s->removeUri(uri->s())) {
      ++delcount;
    }
  }
  size_t addcount = 0;
  if (posGiven) {
    for (auto& elem : *addUrisParam) {
      const String* uri = downcast<String>(elem);
      if (uri && s->insertUri(uri->s(), pos)) {
        ++addcount;
        ++pos;
      }
    }
  }
  else {
    for (auto& elem : *addUrisParam) {
      const String* uri = downcast<String>(elem);
      if (uri && s->addUri(uri->s())) {
        ++addcount;
      }
    }
  }
  if (addcount && group->getPieceStorage()) {
    std::vector<std::unique_ptr<Command>> commands;
    group->createNextCommand(commands, e);
    e->addCommand(std::move(commands));
    group->getSegmentMan()->recognizeSegmentFor(s);
  }
  auto res = List::g();
  res->append(Integer::g(delcount));
  res->append(Integer::g(addcount));
  return std::move(res);
}

namespace {
std::unique_ptr<ValueBase> goingShutdown(const RpcRequest& req,
                                         DownloadEngine* e, bool forceHalt)
{
  // Schedule shutdown after 3seconds to give time to client to
  // receive RPC response.
  e->addRoutineCommand(
      make_unique<TimedHaltCommand>(e->newCUID(), e, 3_s, forceHalt));
  A2_LOG_INFO("Scheduled shutdown in 3 seconds.");
  return createOKResponse();
}
} // namespace

std::unique_ptr<ValueBase> ShutdownRpcMethod::process(const RpcRequest& req,
                                                      DownloadEngine* e)
{
  return goingShutdown(req, e, false);
}

std::unique_ptr<ValueBase>
ForceShutdownRpcMethod::process(const RpcRequest& req, DownloadEngine* e)
{
  return goingShutdown(req, e, true);
}

std::unique_ptr<ValueBase>
GetGlobalStatRpcMethod::process(const RpcRequest& req, DownloadEngine* e)
{
  auto& rgman = e->getRequestGroupMan();
  auto ts = rgman->calculateStat();
  auto res = Dict::g();
  res->put(KEY_DOWNLOAD_SPEED, util::itos(ts.downloadSpeed));
  res->put(KEY_UPLOAD_SPEED, util::itos(ts.uploadSpeed));
  res->put(KEY_NUM_WAITING, util::uitos(rgman->getReservedGroups().size()));
  res->put(KEY_NUM_STOPPED, util::uitos(rgman->getDownloadResults().size()));
  res->put(KEY_NUM_STOPPED_TOTAL, util::uitos(rgman->getNumStoppedTotal()));
  res->put(KEY_NUM_ACTIVE, util::uitos(rgman->getRequestGroups().size()));
  return std::move(res);
}

std::unique_ptr<ValueBase> SaveSessionRpcMethod::process(const RpcRequest& req,
                                                         DownloadEngine* e)
{
  const std::string& filename = e->getOption()->get(PREF_SAVE_SESSION);
  if (filename.empty()) {
    throw DL_ABORT_EX("Filename is not given.");
  }
  SessionSerializer sessionSerializer(e->getRequestGroupMan().get());
  if (sessionSerializer.save(filename)) {
    A2_LOG_NOTICE(
        fmt(_("Serialized session to '%s' successfully."), filename.c_str()));
    return createOKResponse();
  }
  throw DL_ABORT_EX(
      fmt("Failed to serialize session to '%s'.", filename.c_str()));
}

std::unique_ptr<ValueBase>
SystemMulticallRpcMethod::process(const RpcRequest& req, DownloadEngine* e)
{
  // Should never get here, since SystemMulticallRpcMethod overrides execute().
  assert(false);
  return nullptr;
}

RpcResponse SystemMulticallRpcMethod::execute(RpcRequest req, DownloadEngine* e)
{
  auto authorized = RpcResponse::AUTHORIZED;
  try {
    const List* methodSpecs = checkRequiredParam<List>(req, 0);
    auto list = List::g();
    for (auto& methodSpec : *methodSpecs) {
      Dict* methodDict = downcast<Dict>(methodSpec);
      if (!methodDict) {
        list->append(createErrorResponse(
            DL_ABORT_EX("system.multicall expected struct."), req));
        continue;
      }
      const String* methodName =
          downcast<String>(methodDict->get(KEY_METHOD_NAME));
      if (!methodName) {
        list->append(
            createErrorResponse(DL_ABORT_EX("Missing methodName."), req));
        continue;
      }
      if (methodName->s() == getMethodName()) {
        list->append(createErrorResponse(
            DL_ABORT_EX("Recursive system.multicall forbidden."), req));
        continue;
      }
      // TODO what if params missing?
      auto tempParamsList = methodDict->get(KEY_PARAMS);
      std::unique_ptr<List> paramsList;
      if (downcast<List>(tempParamsList)) {
        paramsList.reset(
            static_cast<List*>(methodDict->popValue(KEY_PARAMS).release()));
      }
      else {
        paramsList = List::g();
      }
      RpcRequest r = {methodName->s(), std::move(paramsList), nullptr,
                      req.jsonRpc};
      RpcResponse res = getMethod(methodName->s())->execute(std::move(r), e);
      if (rpc::not_authorized(res)) {
        authorized = RpcResponse::NOTAUTHORIZED;
      }
      if (res.code == 0) {
        auto l = List::g();
        l->append(std::move(res.param));
        list->append(std::move(l));
      }
      else {
        list->append(std::move(res.param));
      }
    }
    return RpcResponse(0, authorized, std::move(list), std::move(req.id));
  }
  catch (RecoverableException& ex) {
    A2_LOG_DEBUG_EX(EX_EXCEPTION_CAUGHT, ex);
    return RpcResponse(1, authorized, createErrorResponse(ex, req),
                       std::move(req.id));
  }
}

std::unique_ptr<ValueBase>
SystemListMethodsRpcMethod::process(const RpcRequest& req, DownloadEngine* e)
{
  auto list = List::g();
  for (auto& s : allMethodNames()) {
    list->append(s);
  }

  return std::move(list);
}

RpcResponse SystemListMethodsRpcMethod::execute(RpcRequest req,
                                                DownloadEngine* e)
{
  auto r = process(req, e);
  return RpcResponse(0, RpcResponse::AUTHORIZED, std::move(r),
                     std::move(req.id));
}

std::unique_ptr<ValueBase>
SystemListNotificationsRpcMethod::process(const RpcRequest& req,
                                          DownloadEngine* e)
{
  auto list = List::g();
  for (auto& s : allNotificationsNames()) {
    list->append(s);
  }

  return std::move(list);
}

RpcResponse SystemListNotificationsRpcMethod::execute(RpcRequest req,
                                                      DownloadEngine* e)
{
  auto r = process(req, e);
  return RpcResponse(0, RpcResponse::AUTHORIZED, std::move(r),
                     std::move(req.id));
}

std::unique_ptr<ValueBase> NoSuchMethodRpcMethod::process(const RpcRequest& req,
                                                          DownloadEngine* e)
{
  throw DL_ABORT_EX(fmt("No such method: %s", req.methodName.c_str()));
}

} // namespace rpc

bool pauseRequestGroup(const std::shared_ptr<RequestGroup>& group,
                       bool reserved, bool forcePause)
{
  if ((reserved && !group->isPauseRequested()) ||
      (!reserved && !group->isForceHaltRequested() &&
       ((forcePause && group->isHaltRequested() && group->isPauseRequested()) ||
        (!group->isHaltRequested() && !group->isPauseRequested())))) {
    if (!reserved) {
      // Call setHaltRequested before setPauseRequested because
      // setHaltRequested calls setPauseRequested(false) internally.
      if (forcePause) {
        group->setForceHaltRequested(true, RequestGroup::NONE);
      }
      else {
        group->setHaltRequested(true, RequestGroup::NONE);
      }
    }
    group->setPauseRequested(true);
    return true;
  }
  else {
    return false;
  }
}

void changeOption(const std::shared_ptr<RequestGroup>& group,
                  const Option& option, DownloadEngine* e)
{
  const std::shared_ptr<DownloadContext>& dctx = group->getDownloadContext();
  const std::shared_ptr<Option>& grOption = group->getOption();
  grOption->merge(option);
  if (option.defined(PREF_CHECKSUM)) {
    const std::string& checksum = grOption->get(PREF_CHECKSUM);
    auto p = util::divide(std::begin(checksum), std::end(checksum), '=');
    std::string hashType(p.first.first, p.first.second);
    util::lowercase(hashType);
    dctx->setDigest(hashType, util::fromHex(p.second.first, p.second.second));
  }
  if (option.defined(PREF_SELECT_FILE)) {
    auto sgl = util::parseIntSegments(grOption->get(PREF_SELECT_FILE));
    sgl.normalize();
    dctx->setFileFilter(std::move(sgl));
  }
  if (option.defined(PREF_SPLIT)) {
    group->setNumConcurrentCommand(grOption->getAsInt(PREF_SPLIT));
  }
  if (option.defined(PREF_MAX_CONNECTION_PER_SERVER)) {
    int maxConn = grOption->getAsInt(PREF_MAX_CONNECTION_PER_SERVER);
    const std::vector<std::shared_ptr<FileEntry>>& files =
        dctx->getFileEntries();
    for (auto& file : files) {
      (file)->setMaxConnectionPerServer(maxConn);
    }
  }
  if (option.defined(PREF_DIR) || option.defined(PREF_OUT)) {
    if (!group->getMetadataInfo()) {

      assert(dctx->getFileEntries().size() == 1);

      auto& fileEntry = dctx->getFirstFileEntry();

      if (!grOption->blank(PREF_OUT)) {
        fileEntry->setPath(
            util::applyDir(grOption->get(PREF_DIR), grOption->get(PREF_OUT)));
        fileEntry->setSuffixPath(A2STR::NIL);
      }
      else if (fileEntry->getSuffixPath().empty()) {
        fileEntry->setPath(A2STR::NIL);
      }
      else {
        fileEntry->setPath(util::applyDir(grOption->get(PREF_DIR),
                                          fileEntry->getSuffixPath()));
      }
    }
    else if (group->getMetadataInfo()
#ifdef ENABLE_BITTORRENT
             && !dctx->hasAttribute(CTX_ATTR_BT)
#endif // ENABLE_BITTORRENT
    ) {
      // In case of Metalink
      for (auto& fileEntry : dctx->getFileEntries()) {
        // PREF_OUT is not applicable to Metalink.  We have always
        // suffixPath set.
        fileEntry->setPath(util::applyDir(grOption->get(PREF_DIR),
                                          fileEntry->getSuffixPath()));
      }
    }
  }
#ifdef ENABLE_BITTORRENT
  if (option.defined(PREF_DIR) || option.defined(PREF_INDEX_OUT)) {
    if (dctx->hasAttribute(CTX_ATTR_BT)) {
      std::istringstream indexOutIn(grOption->get(PREF_INDEX_OUT));
      std::vector<std::pair<size_t, std::string>> indexPaths =
          util::createIndexPaths(indexOutIn);
      for (std::vector<std::pair<size_t, std::string>>::const_iterator
               i = indexPaths.begin(),
               eoi = indexPaths.end();
           i != eoi; ++i) {
        dctx->setFilePathWithIndex(
            (*i).first, util::applyDir(grOption->get(PREF_DIR), (*i).second));
      }
    }
  }
#endif // ENABLE_BITTORRENT
  if (option.defined(PREF_MAX_DOWNLOAD_LIMIT)) {
    group->setMaxDownloadSpeedLimit(
        grOption->getAsInt(PREF_MAX_DOWNLOAD_LIMIT));
  }
  if (option.defined(PREF_MAX_UPLOAD_LIMIT)) {
    group->setMaxUploadSpeedLimit(grOption->getAsInt(PREF_MAX_UPLOAD_LIMIT));
  }
#ifdef ENABLE_BITTORRENT
  auto btObject = e->getBtRegistry()->get(group->getGID());
  if (btObject) {
    if (option.defined(PREF_BT_MAX_PEERS)) {
      btObject->btRuntime->setMaxPeers(grOption->getAsInt(PREF_BT_MAX_PEERS));
    }
  }
#endif // ENABLE_BITTORRENT
}

void changeGlobalOption(const Option& option, DownloadEngine* e)
{
  e->getOption()->merge(option);
  if (option.defined(PREF_MAX_OVERALL_DOWNLOAD_LIMIT)) {
    e->getRequestGroupMan()->setMaxOverallDownloadSpeedLimit(
        option.getAsInt(PREF_MAX_OVERALL_DOWNLOAD_LIMIT));
  }
  if (option.defined(PREF_MAX_OVERALL_UPLOAD_LIMIT)) {
    e->getRequestGroupMan()->setMaxOverallUploadSpeedLimit(
        option.getAsInt(PREF_MAX_OVERALL_UPLOAD_LIMIT));
  }
  if (option.defined(PREF_MAX_CONCURRENT_DOWNLOADS)) {
    e->getRequestGroupMan()->setMaxConcurrentDownloads(
        option.getAsInt(PREF_MAX_CONCURRENT_DOWNLOADS));
    e->getRequestGroupMan()->requestQueueCheck();
  }
  if (option.defined(PREF_MAX_CONCURRENT_DOWNLOADS_PER_DOMAIN)) {
    e->getRequestGroupMan()->setMaxConcurrentDownloadsPerDomain(
        option.getAsInt(PREF_MAX_CONCURRENT_DOWNLOADS_PER_DOMAIN));
    e->getRequestGroupMan()->requestQueueCheck();
  }
  if (option.defined(PREF_OPTIMIZE_CONCURRENT_DOWNLOADS)) {
    e->getRequestGroupMan()->setupOptimizeConcurrentDownloads();
    e->getRequestGroupMan()->requestQueueCheck();
  }
  if (option.defined(PREF_MAX_DOWNLOAD_RESULT)) {
    e->getRequestGroupMan()->setMaxDownloadResult(
        option.getAsInt(PREF_MAX_DOWNLOAD_RESULT));
  }
  if (option.defined(PREF_LOG_LEVEL)) {
    LogFactory::setLogLevel(option.get(PREF_LOG_LEVEL));
  }
  if (option.defined(PREF_LOG)) {
    LogFactory::setLogFile(option.get(PREF_LOG));
    try {
      LogFactory::reconfigure();
    }
    catch (RecoverableException& e) {
      // TODO no exception handling
    }
  }
  if (option.defined(PREF_BT_MAX_OPEN_FILES)) {
    auto& openedFileCounter = e->getRequestGroupMan()->getOpenedFileCounter();
    openedFileCounter->setMaxOpenFiles(option.getAsInt(PREF_BT_MAX_OPEN_FILES));
  }
}

} // namespace aria2
