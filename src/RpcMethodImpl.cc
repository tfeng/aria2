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
#include <ctime>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dirent.h>
#include <sys/wait.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <fcntl.h>

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
  bool cancelRequested = false;
  std::time_t terminalEpochSec = 0;
};

std::map<a2_gid_t, FxMergeJob> fxMergeJobs;
std::map<a2_gid_t, a2_gid_t> fxMergeChildToParent;
std::map<std::string, a2_gid_t> fxMergeOutputOwner;

const int FX_MERGE_ERR_SEGMENT = 2000;
const int FX_MERGE_ERR_CONCAT_IO = 2001;
const int FX_MERGE_ERR_REMUX = 2002;
const int FX_MERGE_ERR_RENAME = 2003;
const int FX_MERGE_ERR_PATH = 2004;
const int FX_MERGE_ERR_INIT = 2005;
const int FX_MERGE_ERR_CANCELED = 2007;

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

void fxPruneExpiredMergeJobs()
{
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

bool fxWaitForSegmentsReady(FxMergeJob& job, std::string& err)
{
  // aria2 may still be finalizing segment files after child-stop callbacks fire.
  const size_t maxAttempts = 120; // ~6 seconds at 50ms interval
  std::string lastErr;

  for (size_t attempt = 0; attempt < maxAttempts; ++attempt) {
    if (job.cancelRequested) {
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
    usleep(50000);
  }

  err = lastErr.empty() ? "segment files not fully finalized" : lastErr;
  return false;
}

bool fxRemoveTree(const std::string& path)
{
  File file(path);
  if (file.isFile()) {
    return file.remove();
  }
  if (!file.isDir()) {
    return true;
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

  if (!file.remove()) {
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
  A2_LOG_ERROR(fmt("[fxmerge] parent=%s failed code=%d message=%s",
                   GroupId::toHex(job.parentGid).c_str(), code,
                   msg.c_str()));
}

void fxCleanupFailureArtifacts(FxMergeJob& job)
{
  if (job.cleanupDone) {
    return;
  }
  const auto listPath = util::applyDir(job.tmpDir, "ffconcat.list");
  const auto playlistPath = util::applyDir(job.tmpDir, "remux.m3u8");
  File(fxMergePartPath(job)).remove();
  File(job.outputPath + ".part").remove();
  File(listPath).remove();
  File(playlistPath).remove();
  for (const auto& segmentPath : job.segmentPaths) {
    File(segmentPath).remove();
  }
  fxRemoveTree(job.tmpDir);
  A2_LOG_WARN(fmt("[fxmerge] parent=%s cleanup complete tmpDir=%s",
                  GroupId::toHex(job.parentGid).c_str(), job.tmpDir.c_str()));
  job.cleanupDone = true;
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
    if (job.cancelRequested) {
      err = "merge canceled by user";
      return false;
    }

    std::ifstream in(segmentPath.c_str(), std::ios::binary);
    if (!in) {
      err = fmt("could not open segment: %s", segmentPath.c_str());
      return false;
    }
    out << in.rdbuf();
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

  A2_LOG_WARN(fmt("[fxmerge] parent=%s remux command start",
                  GroupId::toHex(job.parentGid).c_str()));

  // aria2 may run with SIGCHLD ignored; temporarily restore default so waitpid works.
  struct sigaction oldAct;
  struct sigaction dflAct;
  memset(&dflAct, 0, sizeof(dflAct));
  dflAct.sa_handler = SIG_DFL;
  sigemptyset(&dflAct.sa_mask);
  if (sigaction(SIGCHLD, &dflAct, &oldAct) != 0) {
    err = fmt("failed to configure SIGCHLD for ffmpeg: errno=%d", errno);
    return false;
  }

  int errPipe[2] = {-1, -1};
  if (pipe(errPipe) != 0) {
    int e = errno;
    sigaction(SIGCHLD, &oldAct, nullptr);
    err = fmt("failed to create ffmpeg error pipe: errno=%d", e);
    return false;
  }
  fcntl(errPipe[1], F_SETFD, FD_CLOEXEC);
  fcntl(errPipe[0], F_SETFL, O_NONBLOCK);

  pid_t cpid = fork();
  if (cpid == -1) {
    int e = errno;
    close(errPipe[0]);
    close(errPipe[1]);
    sigaction(SIGCHLD, &oldAct, nullptr);
    err = fmt("failed to fork ffmpeg process: errno=%d", e);
    return false;
  }

  if (cpid == 0) {
    close(errPipe[0]);
    dup2(errPipe[1], STDERR_FILENO);
    const char* ffmpegBins[] = {"ffmpeg", "/usr/local/bin/ffmpeg",
                                "/usr/bin/ffmpeg", nullptr};
    const auto lowerOutput = util::toLower(job.outputPath);
    const bool outputMpegTs = util::endsWith(lowerOutput, ".ts");
    for (size_t i = 0; ffmpegBins[i]; ++i) {
      if (isTsSegmentBundle && outputMpegTs) {
        execl(ffmpegBins[i], ffmpegBins[i], "-y", "-hide_banner", "-loglevel",
              "error", "-xerror", "-err_detect",
              "crccheck+bitstream+buffer+explode", "-f", "concat", "-safe",
              "0", "-i", concatListPath.c_str(), "-c", "copy", "-f",
              "mpegts", partPath.c_str(), static_cast<char*>(nullptr));
      }
      else if (outputMpegTs) {
        execl(ffmpegBins[i], ffmpegBins[i], "-y", "-hide_banner", "-loglevel",
              "error", "-xerror", "-err_detect",
              "crccheck+bitstream+buffer+explode", "-allowed_extensions", "ALL",
              "-allowed_segment_extensions", "ALL", "-extension_picky", "0",
              "-protocol_whitelist", "file,crypto,data,http,https,tcp,tls", "-f",
              "hls", "-i", listPath.c_str(), "-fflags", "+genpts",
              "-reset_timestamps", "1", "-c", "copy", "-f", "mpegts",
              partPath.c_str(), static_cast<char*>(nullptr));
      }
      else {
        execl(ffmpegBins[i], ffmpegBins[i], "-y", "-hide_banner", "-loglevel",
              "error", "-xerror", "-err_detect",
              "crccheck+bitstream+buffer+explode", "-allowed_extensions", "ALL",
              "-allowed_segment_extensions", "ALL", "-extension_picky", "0",
              "-protocol_whitelist", "file,crypto,data,http,https,tcp,tls", "-f",
              "hls", "-i", listPath.c_str(), "-fflags", "+genpts",
              "-reset_timestamps", "1", "-c", "copy", "-movflags",
              "+faststart", "-f", "mp4", partPath.c_str(),
              static_cast<char*>(nullptr));
      }
      if (errno != ENOENT) {
        break;
      }
    }
    std::string execErr = fmt("failed to exec ffmpeg: errno=%d\n", errno);
    (void)::write(errPipe[1], execErr.c_str(), execErr.size());
    close(errPipe[1]);
    _exit(127);
  }

  close(errPipe[1]);

  int status = 0;
  std::string ffmpegStderr;
  auto drainFfmpegStderr = [&ffmpegStderr, &errPipe]() {
    char buf[1024];
    while (true) {
      ssize_t nread = ::read(errPipe[0], buf, sizeof(buf));
      if (nread > 0) {
        ffmpegStderr.append(buf, static_cast<size_t>(nread));
        continue;
      }
      if (nread == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        break;
      }
      if (nread <= 0) {
        break;
      }
    }
  };
  auto drainFfmpegStderrUntilEof = [&ffmpegStderr, &errPipe]() {
    char buf[1024];
    while (true) {
      ssize_t nread = ::read(errPipe[0], buf, sizeof(buf));
      if (nread > 0) {
        ffmpegStderr.append(buf, static_cast<size_t>(nread));
        continue;
      }
      if (nread == 0) {
        break;
      }
      if (errno == EINTR) {
        continue;
      }
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        continue;
      }
      break;
    }
  };
  while (true) {
    pid_t w = waitpid(cpid, &status, WNOHANG);
    if (w == cpid) {
      drainFfmpegStderr();
      break;
    }
    if (w == 0) {
      drainFfmpegStderr();
      if (job.cancelRequested) {
        kill(cpid, SIGTERM);
        for (size_t i = 0; i < 20; ++i) {
          w = waitpid(cpid, &status, WNOHANG);
          if (w == cpid) {
            drainFfmpegStderr();
            break;
          }
          drainFfmpegStderr();
          usleep(50000);
        }
        if (w == 0) {
          kill(cpid, SIGKILL);
          (void)waitpid(cpid, &status, 0);
          drainFfmpegStderr();
        }

        close(errPipe[0]);
        sigaction(SIGCHLD, &oldAct, nullptr);
        err = "merge canceled by user";
        return false;
      }

      usleep(50000);
      continue;
    }
    if (w == -1) {
      if (errno == EINTR) {
        drainFfmpegStderr();
        continue;
      }
      int e = errno;
      close(errPipe[0]);
      sigaction(SIGCHLD, &oldAct, nullptr);
      err = fmt("waitpid failed for ffmpeg: errno=%d", e);
      return false;
    }
  }
  sigaction(SIGCHLD, &oldAct, nullptr);

  int flags = fcntl(errPipe[0], F_GETFL, 0);
  if (flags != -1) {
    fcntl(errPipe[0], F_SETFL, flags & ~O_NONBLOCK);
  }
  drainFfmpegStderrUntilEof();
  close(errPipe[0]);

  if (!WIFEXITED(status)) {
    err = "ffmpeg terminated abnormally";
    return false;
  }

  int exitCode = WEXITSTATUS(status);
  if (exitCode != 0) {
    while (!ffmpegStderr.empty() &&
           (ffmpegStderr.back() == '\n' || ffmpegStderr.back() == '\r')) {
      ffmpegStderr.pop_back();
    }
    if (!ffmpegStderr.empty()) {
      A2_LOG_ERROR(fmt("[fxmerge] parent=%s ffmpeg stderr: %s",
                       GroupId::toHex(job.parentGid).c_str(),
                       ffmpegStderr.c_str()));
      err = fmt("ffmpeg exited with code %d: %s", exitCode,
                ffmpegStderr.c_str());
    }
    else {
      A2_LOG_ERROR(fmt("[fxmerge] parent=%s ffmpeg stderr: <empty>",
                       GroupId::toHex(job.parentGid).c_str()));
      err = fmt("ffmpeg exited with code %d", exitCode);
    }
    return false;
  }
  File(listPath).remove();
  return true;
}

bool fxFinalizeMerge(FxMergeJob& job)
{
  if (job.cancelRequested) {
    fxMarkFailed(job, FX_MERGE_ERR_CANCELED, "merge canceled by user", true);
    return false;
  }

  std::string preflightErr;
  if (!fxWaitForSegmentsReady(job, preflightErr)) {
    if (job.cancelRequested || preflightErr == "merge canceled by user") {
      fxMarkFailed(job, FX_MERGE_ERR_CANCELED, "merge canceled by user", true);
    }
    else {
      fxMarkFailed(job, FX_MERGE_ERR_SEGMENT, preflightErr, true);
    }
    return false;
  }

  job.state = FX_MERGE_MERGING;
  job.mergeProgress = 0.0;
  A2_LOG_WARN(fmt("[fxmerge] parent=%s finalize begin mode=%s output=%s",
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
      if (job.cancelRequested || err == "merge canceled by user") {
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
      if (job.cancelRequested || err == "merge canceled by user") {
        fxMarkFailed(job, FX_MERGE_ERR_CANCELED, "merge canceled by user", true);
      }
      else {
        fxMarkFailed(job, FX_MERGE_ERR_CONCAT_IO, err);
      }
      return false;
    }
  }

  if (job.cancelRequested) {
    fxMarkFailed(job, FX_MERGE_ERR_CANCELED, "merge canceled by user", true);
    return false;
  }

  if (!fxSyncFile(partPath, err)) {
    fxMarkFailed(job, FX_MERGE_ERR_CONCAT_IO, err);
    return false;
  }

  // Keep a completed remux available through a short-lived publish failure.
  const size_t maxPublishAttempts = 100; // 10 seconds at 100ms intervals
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
    if (job.cancelRequested) {
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
    usleep(100000);
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
  A2_LOG_WARN(fmt("[fxmerge] parent=%s finalize complete output=%s",
                  GroupId::toHex(job.parentGid).c_str(),
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

std::unique_ptr<ValueBase> FxplayerAddMergeRpcMethod::process(
    const RpcRequest& req, DownloadEngine* e)
{
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

  auto ownerItr = fxMergeOutputOwner.find(outputPath);
  if (ownerItr != fxMergeOutputOwner.end()) {
    auto existing = fxMergeJobs.find(ownerItr->second);
    if (existing != fxMergeJobs.end()) {
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
    fxMergeOutputOwner.erase(ownerItr);
  }

  std::string tmpDir = outputPath + ".segments";
  if (const auto* tmpParam = getStringField(mergeOptsParam, "tmpDir")) {
    if (!tmpParam->s().empty()) {
      tmpDir = tmpParam->s();
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

  // Do NOT touch tmpDir synchronously here. On SMB targets this can block JSON-RPC for
  // tens of seconds while a spun-down drive wakes up, causing client-side submit timeouts.
  // RequestGroup disk setup will create/use directories lazily when downloads actually start.

  std::vector<std::shared_ptr<RequestGroup>> groups;
  std::vector<std::string> segmentPaths;
  segmentPaths.reserve(segmentsParam->size());

  size_t index = 0;
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

    auto requestOption = std::make_shared<Option>(*e->getOption());
    if (aria2OptsParam) {
      gatherRequestOption(requestOption.get(), aria2OptsParam);
    }
    const auto filename = fxZeroPadIndex(index);
    requestOption->put(PREF_DIR, tmpDir);
    requestOption->put(PREF_OUT, filename);
    requestOption->put(PREF_CONTINUE, "true");
    if (!headersOptionValue.empty()) {
      requestOption->put(PREF_HEADER, headersOptionValue);
    }

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
  job.state = FX_MERGE_DOWNLOADING;
  job.segmentPaths = segmentPaths;
  for (auto& g : groups) {
    auto gid = g->getGID();
    job.childGids.push_back(gid);
    job.childDone[gid] = false;
    job.childOK[gid] = true;
    job.childPath[gid] = util::applyDir(tmpDir, fxZeroPadIndex(job.childGids.size() - 1));
    fxMergeChildToParent[gid] = parentGid;
  }
  fxMergeJobs[parentGid] = job;
  fxMergeOutputOwner[outputPath] = parentGid;

  e->getRequestGroupMan()->addReservedGroup(groups);

  A2_LOG_WARN(fmt("[fxmerge] add parent=%s segments=%lu mode=%s remux=%s tmpDir=%s output=%s",
                  GroupId::toHex(parentGid).c_str(),
                  static_cast<unsigned long>(groups.size()), mode.c_str(),
                  remux ? "true" : "false", tmpDir.c_str(),
                  outputPath.c_str()));

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

  job.errorCode = 0;
  job.errorMessage.clear();
  job.state = FX_MERGE_MERGING;
  if (!fxFinalizeMerge(job)) {
    if (job.cleanupPending && fxAllChildrenDone(job)) {
      fxCleanupFailureArtifacts(job);
    }
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
  const bool retryable =
      job.state == FX_MERGE_FAILED && fxAllChildrenDone(job) &&
      !fxAnyChildFailed(job) && job.errorCode != FX_MERGE_ERR_CANCELED;

  const char* outcome = "running";
  if (job.state == FX_MERGE_MERGED) {
    outcome = "succeeded";
  }
  else if (job.state == FX_MERGE_FAILED) {
    outcome = retryable ? "retryable-failure" : "failed";
  }

  const char* status = VLB_ACTIVE;
  if (job.state == FX_MERGE_MERGED) {
    status = VLB_COMPLETE;
  }
  else if (job.state == FX_MERGE_FAILED) {
    status = VLB_ERROR;
  }

  result->put("found", VLB_TRUE);
  result->put("gid", GroupId::toHex(job.parentGid));
  result->put("status", status);
  result->put("state", fxMergeStateName(job.state));
  result->put("stage", fxMergeStateName(job.state));
  result->put("terminal", fxIsTerminalState(job.state) ? VLB_TRUE : VLB_FALSE);
  result->put("retryable", retryable ? VLB_TRUE : VLB_FALSE);
  result->put("outcome", outcome);
  result->put("output", job.outputPath);
  if (job.errorCode != 0) {
    result->put("errorCode", util::itos(job.errorCode));
  }
  if (!job.errorMessage.empty()) {
    result->put("errorMessage", job.errorMessage);
  }
  return std::move(result);
}

void fxMergeOnGroupStopped(const std::shared_ptr<RequestGroup>& group,
                           DownloadEngine* e, error_code::Value result)
{
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

  if (job.state == FX_MERGE_FAILED) {
    if (job.cleanupPending && fxAllChildrenDone(job)) {
      fxCleanupFailureArtifacts(job);
    }
    return;
  }

  A2_LOG_INFO(fmt("[fxmerge] child-stop parent=%s child=%s result=%d",
                  GroupId::toHex(parent).c_str(), GroupId::toHex(gid).c_str(),
                  static_cast<int>(result)));

  if (!ok) {
    if (job.cancelRequested) {
      fxMarkFailed(job, FX_MERGE_ERR_CANCELED,
                   fmt("merge canceled by user during download gid=%s code=%d",
                       GroupId::toHex(gid).c_str(), static_cast<int>(result)),
                   true);
    }
    else {
      fxMarkFailed(
          job, FX_MERGE_ERR_SEGMENT,
          fmt("segment download failed gid=%s code=%d",
              GroupId::toHex(gid).c_str(), static_cast<int>(result)),
          true);
    }
    if (fxAllChildrenDone(job)) {
      fxCleanupFailureArtifacts(job);
    }
    return;
  }

  if (!fxAllChildrenDone(job)) {
    job.state = FX_MERGE_DOWNLOADING;
    return;
  }

  if (fxAnyChildFailed(job)) {
    fxMarkFailed(job, FX_MERGE_ERR_SEGMENT,
                 "one or more segments failed", true);
    fxCleanupFailureArtifacts(job);
    return;
  }

  A2_LOG_WARN(fmt("[fxmerge] parent=%s all segments downloaded; starting merge mode=%s tmpDir=%s output=%s",
                  GroupId::toHex(parent).c_str(), job.mode.c_str(),
                  job.tmpDir.c_str(), job.outputPath.c_str()));
  if (!fxFinalizeMerge(job)) {
    if (job.cleanupPending && fxAllChildrenDone(job)) {
      fxCleanupFailureArtifacts(job);
    }
  }
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
      File(job.outputPath + ".part").remove();
      if (job.state == FX_MERGE_FAILED) {
        File(job.outputPath).remove();
      }
      fxEraseMergeJob(gid);
      e->getRequestGroupMan()->removeDownloadResult(gid);
      return createGIDResponse(gid);
    }

    job.cancelRequested = true;

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
        touched = true;
      }
    }

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

void gatherFxMergeStatus(Dict* entryDict, const FxMergeJob& job,
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

    const auto childPathItr = job.childPath.find(gid);
    const std::string childPath =
        childPathItr != job.childPath.end() ? childPathItr->second : std::string();
    const int64_t observedFileSize = childPath.empty() ? 0 : File(childPath).size();
    const int64_t observedProgress =
        std::max<int64_t>(childCompletedLength, observedFileSize);

    auto doneItr = job.childDone.find(gid);
    if (doneItr != job.childDone.end() && doneItr->second) {
      auto okItr = job.childOK.find(gid);
      if (okItr != job.childOK.end() && okItr->second) {
        ++completed;
        const int64_t finishedSize = std::max<int64_t>(
            observedProgress, std::max<int64_t>(childTotalLength, childCompletedLength));
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
    estimatedDownloadProgress =
        (static_cast<double>(completed) + (0.5 * static_cast<double>(active))) /
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

  const bool retryable =
      job.state == FX_MERGE_FAILED && fxAllChildrenDone(job) &&
      !fxAnyChildFailed(job) && job.errorCode != FX_MERGE_ERR_CANCELED;

  const char* outcome = "running";
  if (job.state == FX_MERGE_MERGED) {
    outcome = "succeeded";
  }
  else if (job.state == FX_MERGE_FAILED) {
    outcome = retryable ? "retryable-failure" : "failed";
  }

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
    if (job.state == FX_MERGE_MERGED) {
      entryDict->put(KEY_STATUS, VLB_COMPLETE);
    }
    else if (job.state == FX_MERGE_FAILED) {
      entryDict->put(KEY_STATUS, VLB_ERROR);
    }
    else {
      entryDict->put(KEY_STATUS, VLB_ACTIVE);
    }
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
    merge->put("output", job.outputPath);
    merge->put("mode", job.mode);
    merge->put("mergeProgress", fmt("%.3f", job.mergeProgress));
    merge->put("downloadProgress", fmt("%.3f", downloadProgress));
    merge->put("estimatedDownloadProgress", fmt("%.3f", estimatedDownloadProgress));
    merge->put("overallProgress", fmt("%.3f", overallProgress));
    merge->put("cancelRequested", job.cancelRequested ? VLB_TRUE : VLB_FALSE);
    merge->put("terminal", fxIsTerminalState(job.state) ? VLB_TRUE : VLB_FALSE);
    merge->put("retryable", retryable ? VLB_TRUE : VLB_FALSE);
    merge->put("outcome", outcome);
    entryDict->put(KEY_MERGE, std::move(merge));
  }
}
} // namespace

std::unique_ptr<ValueBase> TellStatusRpcMethod::process(const RpcRequest& req,
                                                        DownloadEngine* e)
{
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
