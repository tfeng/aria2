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
#ifndef D_REQUEST_GROUP_MAN_H
#define D_REQUEST_GROUP_MAN_H

#include "common.h"

#include <string>
#include <deque>
#include <vector>
#include <map>
#include <set>
#include <memory>
#include <chrono>

#include "DownloadResult.h"
#include "TransferStat.h"
#include "RequestGroup.h"
#include "NetStat.h"
#include "IndexedList.h"
#include "TimerA2.h"

namespace aria2 {

class DownloadEngine;
class Command;
struct DownloadResult;
class ServerStatMan;
class ServerStat;
class Option;
class OutputFile;
class UriListParser;
class WrDiskCache;
class OpenedFileCounter;

typedef IndexedList<a2_gid_t, std::shared_ptr<RequestGroup>> RequestGroupList;
typedef IndexedList<a2_gid_t, std::shared_ptr<DownloadResult>>
    DownloadResultList;

class RequestGroupMan {
private:
  RequestGroupList requestGroups_;
  RequestGroupList reservedGroups_;
  DownloadResultList downloadResults_;
  // This includes download result which did not finish, and deleted
  // from downloadResults_.  This is used to save them in
  // SessionSerializer.
  std::vector<std::shared_ptr<DownloadResult>> unfinishedDownloadResults_;

  int maxConcurrentDownloads_;

  // Maximum number of concurrent HTTP connections aria2 will hold open to
  // any single domain, summed across all active downloads whose first URI
  // shares that domain (not just a count of downloads: a single download
  // can itself use several connections via split/max-connection-per-server,
  // and that gets counted too -- see getRequestGroupConnectionWeight).
  // 0 means no per-domain limit.
  int maxConcurrentDownloadsPerDomain_;

  // Sum of connection weights (see getRequestGroupConnectionWeight) of
  // currently active downloads, keyed by the domain of each RequestGroup's
  // first URI. Only domains with at least one active download are present.
  std::map<std::string, int> activeConnectionsByDomain_;

  // FXPlayer extension: per-domain override of maxConcurrentDownloadsPerDomain_, set via
  // fxplayer.addMerge's "domainConnectionCaps" option (see RpcMethodImpl.cc's
  // FxplayerAddMergeRpcMethod::process and setDomainConnectionCapOverride below). Motivation: an
  // XNXX account suspension made the global per-domain cap (one value for every domain, whatever
  // service it belongs to) too coarse — the app wants XNXX's CDN specifically clamped much
  // tighter (e.g. 1, fully sequential) than the shared default used for other services, without
  // lowering that shared default for everyone. A domain present here takes priority over
  // maxConcurrentDownloadsPerDomain_ in fillRequestGroupFromReserver's admission check; a domain
  // absent here (the common case) uses the global default unchanged. Entries persist for the
  // daemon's lifetime once set (there's no "unset" — a domain that stops being downloaded just
  // sits unused in the map), same lifetime as maxConcurrentDownloadsPerDomain_ itself.
  std::map<std::string, int> domainConnectionCapOverrides_;

  // FXPlayer extension: per-domain minimum interval between successive connection admissions, set
  // via fxplayer.addMerge's "domainMinAdmissionIntervalMs" option (see RpcMethodImpl.cc's
  // FxplayerAddMergeRpcMethod::process and setDomainMinAdmissionIntervalOverride below).
  // Motivation: domainConnectionCapOverrides_ above makes XNXX segment fetches fully sequential,
  // but sequential alone doesn't stop them being requested flat-out, back to back -- a live
  // XNXX account-suspension investigation (2026-09-23) found sustained streaks of 500+ segment
  // requests at sub-1.2s intervals, a shape no real HLS player produces regardless of connection
  // speed. This is a time-based sibling to the capacity-based cap above: unlike that cap, an
  // interval floor has no natural mechanism to re-trigger admission once it's cleared (a
  // capacity slot frees itself and its own completion path calls requestQueueCheck(); a clock
  // just ticks with nothing watching it), so FillRequestGroupCommand also gained a dedicated poll
  // gated on hasActiveDomainMinAdmissionIntervalOverrides() below -- see its own comment. Same
  // lifetime/semantics as domainConnectionCapOverrides_: persists for the daemon's lifetime once
  // set, 0 (or absent) means no floor.
  std::map<std::string, std::chrono::milliseconds> domainMinAdmissionIntervalOverrides_;

  // Wall-clock time of the most recent connection admission for each domain with an active
  // entry in domainMinAdmissionIntervalOverrides_ above. Absent until the first admission.
  std::map<std::string, Timer> domainLastAdmissionAt_;

  // FXPlayer extension: per-domain rolling admission-volume budget, set via fxplayer.addMerge's
  // "domainVolumeBudget" option. Motivation: pacing (above) stops segments being requested
  // flat-out, but two real XNXX account suspensions both landed at almost the same CUMULATIVE
  // segment count (~1,300) regardless of how long that took to reach (24 min vs 58 min) --
  // pointing at a fair-use/volume quota on top of (not instead of) the rate limit pacing already
  // addresses. `count` is the max admissions allowed inside the trailing `window`; `window` uses
  // wall-clock (system_clock), NOT Timer/steady_clock, specifically so the budget survives a
  // daemon restart correctly (steady_clock's epoch is only meaningful within one process
  // lifetime -- see loadDomainVolumeBudgetState's own comment). `count` <= 0 removes the override.
  struct VolumeBudget {
    int count = 0;
    std::chrono::seconds window{0};
  };
  std::map<std::string, VolumeBudget> domainVolumeBudgetOverrides_;

  // Wall-clock (system_clock) timestamps of every admission counted against a domain's volume
  // budget, oldest first. Entries older than that domain's current window are purged lazily on
  // each check. Loaded from disk once per domain (see loadedVolumeBudgetDomains_) the first time
  // an override is set for it, so a daemon restart doesn't silently reset the window.
  std::map<std::string, std::deque<std::chrono::system_clock::time_point>>
      domainVolumeBudgetLog_;

  // Domains whose persisted volume-budget log has already been loaded from disk this process
  // lifetime -- loadDomainVolumeBudgetState() is a no-op after the first call per domain, so a
  // later setDomainVolumeBudgetOverride() call for the same domain (e.g. a second job in the same
  // session) doesn't re-read stale on-disk state over newer in-memory entries.
  std::set<std::string> loadedVolumeBudgetDomains_;

  // FXPlayer extension: per-domain random pause inserted specifically at a VIDEO boundary (the
  // first admission whose parent RequestGroup differs from the previous admission's), set via
  // fxplayer.addMerge's "domainVideoGapMs" option -- distinct from domainMinAdmissionIntervalOverrides_,
  // which paces every segment uniformly regardless of which video it belongs to. Real playback has
  // a human pause between videos, not a metronome; `maxMs` <= 0 removes the override.
  struct VideoGap {
    int minMs = 0;
    int maxMs = 0;
  };
  std::map<std::string, VideoGap> domainVideoGapOverrides_;

  // Per-domain video-boundary-gap state: which parent this domain's last-admitted group belonged
  // to (to detect the NEXT boundary), and — once a boundary is being waited out — the single
  // randomly-chosen deadline for it, generated once per boundary (not re-rolled every recheck)
  // so the wait doesn't drift shorter, and cleared once satisfied.
  struct VideoGapState {
    a2_gid_t lastParent = 0;
    bool gapPending = false;
    Timer deadline;
  };
  std::map<std::string, VideoGapState> domainVideoGapState_;

  // Domains for which we have already logged a per-domain throttle
  // notice since they last dropped below the limit. Used to emit at
  // most one log line per throttle episode instead of one per
  // deferred activation attempt.
  std::set<std::string> loggedThrottledDomains_;

  bool optimizeConcurrentDownloads_;
  double optimizeConcurrentDownloadsCoeffA_;
  double optimizeConcurrentDownloadsCoeffB_;
  int optimizationSpeed_;
  Timer optimizationSpeedTimer_;

  // The number of simultaneous active downloads, excluding seed only
  // item if PREF_BT_DETACH_SEED_ONLY is true.  We rely on this
  // variable to maintain the number of concurrent downloads.  If
  // PREF_BT_DETACH_SEED_ONLY is false, this variable is equal to
  // requestGroups_.size().
  size_t numActive_;

  const Option* option_;

  std::shared_ptr<ServerStatMan> serverStatMan_;

  int maxOverallDownloadSpeedLimit_;

  int maxOverallUploadSpeedLimit_;

  NetStat netStat_;

  // true if download engine should keep running even if there is no
  // download to perform.
  bool keepRunning_;

  bool queueCheck_;

  // The number of error DownloadResult removed because of upper limit
  // of the queue
  int removedErrorResult_;

  // The last error of removed DownloadResult
  error_code::Value removedLastErrorResult_;

  size_t maxDownloadResult_;

  // UriListParser for deferred input.
  std::shared_ptr<UriListParser> uriListParser_;

  std::unique_ptr<WrDiskCache> wrDiskCache_;

  std::shared_ptr<OpenedFileCounter> openedFileCounter_;

  // The number of stopped downloads so far in total, including
  // evicted DownloadResults.
  size_t numStoppedTotal_;

  // SHA1 hash value of the content of last session serialization.
  std::string lastSessionHash_;

  void formatDownloadResultFull(
      OutputFile& out, const char* status,
      const std::shared_ptr<DownloadResult>& downloadResult) const;

  std::string formatDownloadResult(
      const char* status,
      const std::shared_ptr<DownloadResult>& downloadResult) const;

  void configureRequestGroup(
      const std::shared_ptr<RequestGroup>& requestGroup) const;

  void addRequestGroupIndex(const std::shared_ptr<RequestGroup>& group);
  void addRequestGroupIndex(
      const std::vector<std::shared_ptr<RequestGroup>>& groups);

  int optimizeConcurrentDownloads();

  // FXPlayer extension: path of the small text file domainVolumeBudgetLog_ persists to, derived
  // from the daemon's own --save-session path (same directory, sibling file) so it needs no new
  // daemon option -- empty if --save-session isn't configured, in which case the budget still
  // works for the daemon's uptime, it just resets on restart (logged once, not fatal).
  std::string volumeBudgetStatePath() const;

  // Reads any persisted entries for `domain` from volumeBudgetStatePath() into
  // domainVolumeBudgetLog_[domain], skipping entries already older than the window passed in (no
  // point loading what would be purged immediately). No-op if already loaded this process
  // lifetime (loadedVolumeBudgetDomains_) or if there's nothing on disk for `domain`.
  void loadDomainVolumeBudgetState(const std::string& domain, std::chrono::seconds window);

  // Rewrites volumeBudgetStatePath() from the current in-memory domainVolumeBudgetLog_ for every
  // domain that has one. Called after every admission that consumes budget -- cheap (the log per
  // domain is capped at its own `count`, at most a few hundred timestamps) and guarantees the
  // file is never more than one admission stale, so a SIGKILL between writes loses at most one
  // entry rather than the whole session's worth.
  void persistDomainVolumeBudgetState() const;

public:
  // Returns the lower-cased host of group's first URI (spent or
  // remaining), or an empty string if group has no URI at all.
  static std::string getRequestGroupDomain(const RequestGroup* group);

  // Returns how many concurrent HTTP connections group is configured to
  // use against a single server: min(split, max-connection-per-server),
  // clamped to at least 1. This reflects whatever is currently set on
  // group's own Option -- fillRequestGroupFromReserver may lower these
  // values at admission time to fit the domain's remaining connection
  // budget, in which case a later call here (e.g. on completion, to know
  // how much to give back) returns that same clamped figure.
  static int getRequestGroupConnectionWeight(const RequestGroup* group);

  // FXPlayer extension: set (or update) a per-domain override for the connection cap normally
  // governed globally by maxConcurrentDownloadsPerDomain_ — see domainConnectionCapOverrides_'s
  // own doc comment. `cap` <= 0 removes any existing override for `domain` (reverting it to the
  // global default), matching how `max-concurrent-downloads-per-domain=0` means "unlimited" for
  // the global setting.
  void setDomainConnectionCapOverride(const std::string& domain, int cap);

  // Effective per-domain connection cap fillRequestGroupFromReserver should use for `domain`:
  // the override if one is set, else maxConcurrentDownloadsPerDomain_ (which itself may be 0,
  // meaning unlimited).
  int effectiveDomainConnectionCap(const std::string& domain) const;

  // FXPlayer extension: maps a request's host to the key its connection cap / pacing budget is
  // tracked under. An override key containing '*' is a glob pattern (e.g. "hls*.xnxx-cdn.com") that
  // makes every matching host SHARE one budget -- needed because a site's CDN can serve different
  // videos from different hostnames, and a per-hostname cap would let those download in parallel
  // (and each with its own pacing clock). A host matching no pattern maps to itself.
  std::string resolveDomainBudgetKey(const std::string& domain) const;

  // FXPlayer extension: set (or update) a per-domain minimum interval between successive
  // connection admissions -- see domainMinAdmissionIntervalOverrides_'s own doc comment.
  // `interval` <= 0 removes any existing override for `domain` (reverting to no floor).
  void setDomainMinAdmissionIntervalOverride(const std::string& domain,
                                              std::chrono::milliseconds interval);

  // Effective minimum admission interval fillRequestGroupFromReserver should use for `domain`:
  // the override if one is set (and positive), else std::chrono::milliseconds(0) (no floor).
  std::chrono::milliseconds
  effectiveDomainMinAdmissionInterval(const std::string& domain) const;

  // True if any domain currently has a positive minimum admission interval override -- gates
  // FillRequestGroupCommand's dedicated pacing-recheck poll so it costs nothing for any
  // daemon/job that never uses this feature.
  bool hasActiveDomainMinAdmissionIntervalOverrides() const;

  // FXPlayer extension: set (or update) a per-domain rolling volume budget -- see
  // domainVolumeBudgetOverrides_'s own doc comment. `count` <= 0 removes the override. Lazily
  // loads any persisted admission log for `domain` from disk the first time it's called for that
  // domain this process lifetime.
  void setDomainVolumeBudgetOverride(const std::string& domain, int count,
                                      std::chrono::seconds window);

  // True if any domain currently has an active volume budget override -- gates
  // FillRequestGroupCommand's recheck poll, same reasoning as the pacing gate above (a volume
  // budget's release event -- an old entry aging out of the window -- is also purely time-based).
  bool hasActiveDomainVolumeBudgetOverrides() const;

  // FXPlayer extension: set (or update) a per-domain random pause at video boundaries -- see
  // domainVideoGapOverrides_'s own doc comment. `maxMs` <= 0 removes the override.
  void setDomainVideoGapOverride(const std::string& domain, int minMs, int maxMs);

  // True if any domain currently has an active video-gap override -- same recheck-poll gating
  // reasoning as the other two time-based overrides above.
  bool hasActiveDomainVideoGapOverrides() const;

  RequestGroupMan(std::vector<std::shared_ptr<RequestGroup>> requestGroups,
                  int maxConcurrentDownloads, const Option* option);

  ~RequestGroupMan();

  bool downloadFinished();

  void save();

  void closeFile();

  void halt();

  void forceHalt();

  void removeStoppedGroup(DownloadEngine* e);

  void fillRequestGroupFromReserver(DownloadEngine* e);

  // Note that this method does not call addRequestGroupIndex(). This
  // method should be considered as private, but exposed for unit
  // testing purpose.
  void addRequestGroup(const std::shared_ptr<RequestGroup>& group);

  void
  addReservedGroup(const std::vector<std::shared_ptr<RequestGroup>>& groups);

  void addReservedGroup(const std::shared_ptr<RequestGroup>& group);

  void
  insertReservedGroup(size_t pos,
                      const std::vector<std::shared_ptr<RequestGroup>>& groups);

  void insertReservedGroup(size_t pos,
                           const std::shared_ptr<RequestGroup>& group);

  size_t countRequestGroup() const;

  const RequestGroupList& getRequestGroups() const { return requestGroups_; }

  const RequestGroupList& getReservedGroups() const { return reservedGroups_; }

  // Returns RequestGroup object whose gid is gid. This method returns
  // RequestGroup either in requestGroups_ or reservedGroups_.
  std::shared_ptr<RequestGroup> findGroup(a2_gid_t gid) const;

  // Changes the position of download denoted by gid.  If how is
  // POS_SET, it moves the download to a position relative to the
  // beginning of the queue.  If how is POS_CUR, it moves the download
  // to a position relative to the current position. If how is
  // POS_END, it moves the download to a position relative to the end
  // of the queue. If the destination position is less than 0 or
  // beyond the end of the queue, it moves the download to the
  // beginning or the end of the queue respectively.  Returns the
  // destination position.
  size_t changeReservedGroupPosition(a2_gid_t gid, int pos, OffsetMode how);

  bool removeReservedGroup(a2_gid_t gid);

  bool getOptimizeConcurrentDownloads() const
  {
    return optimizeConcurrentDownloads_;
  }

  bool setupOptimizeConcurrentDownloads();

  void showDownloadResults(OutputFile& o, bool full) const;

  bool isSameFileBeingDownloaded(RequestGroup* requestGroup) const;

  TransferStat calculateStat();

  class DownloadStat {
  private:
    int error_;
    int inProgress_;
    int waiting_;
    error_code::Value lastErrorResult_;

  public:
    DownloadStat(int error, int inProgress, int waiting,
                 error_code::Value lastErrorResult = error_code::FINISHED)
        : error_(error),
          inProgress_(inProgress),
          waiting_(waiting),
          lastErrorResult_(lastErrorResult)
    {
    }

    error_code::Value getLastErrorResult() const { return lastErrorResult_; }

    bool allCompleted() const
    {
      return error_ == 0 && inProgress_ == 0 && waiting_ == 0;
    }

    int getInProgress() const { return inProgress_; }
  };

  DownloadStat getDownloadStat() const;

  const DownloadResultList& getDownloadResults() const
  {
    return downloadResults_;
  }

  std::shared_ptr<DownloadResult> findDownloadResult(a2_gid_t gid) const;

  // Removes all download results.
  void purgeDownloadResult();

  // Removes download result of given gid. Returns true if download
  // result was removed. Otherwise returns false.
  bool removeDownloadResult(a2_gid_t gid);

  void addDownloadResult(const std::shared_ptr<DownloadResult>& downloadResult);

  const std::vector<std::shared_ptr<DownloadResult>>&
  getUnfinishedDownloadResult() const
  {
    return unfinishedDownloadResults_;
  }

  std::shared_ptr<ServerStat> findServerStat(const std::string& hostname,
                                             const std::string& protocol) const;

  std::shared_ptr<ServerStat>
  getOrCreateServerStat(const std::string& hostname,
                        const std::string& protocol);

  bool addServerStat(const std::shared_ptr<ServerStat>& serverStat);

  bool loadServerStat(const std::string& filename);

  bool saveServerStat(const std::string& filename) const;

  void removeStaleServerStat(const std::chrono::seconds& timeout);

  // Returns true if current download speed exceeds
  // maxOverallDownloadSpeedLimit_.  Always returns false if
  // maxOverallDownloadSpeedLimit_ == 0.  Otherwise returns false.
  bool doesOverallDownloadSpeedExceed();

  void setMaxOverallDownloadSpeedLimit(int speed)
  {
    maxOverallDownloadSpeedLimit_ = speed;
  }

  int getMaxOverallDownloadSpeedLimit() const
  {
    return maxOverallDownloadSpeedLimit_;
  }

  // Returns true if current upload speed exceeds
  // maxOverallUploadSpeedLimit_. Always returns false if
  // maxOverallUploadSpeedLimit_ == 0. Otherwise returns false.
  bool doesOverallUploadSpeedExceed();

  void setMaxOverallUploadSpeedLimit(int speed)
  {
    maxOverallUploadSpeedLimit_ = speed;
  }

  int getMaxOverallUploadSpeedLimit() const
  {
    return maxOverallUploadSpeedLimit_;
  }

  void setMaxConcurrentDownloads(int max) { maxConcurrentDownloads_ = max; }

  void setMaxConcurrentDownloadsPerDomain(int max)
  {
    maxConcurrentDownloadsPerDomain_ = max;
  }

  int getMaxConcurrentDownloadsPerDomain() const
  {
    return maxConcurrentDownloadsPerDomain_;
  }

  // Call this function if requestGroups_ queue should be maintained.
  // This function is added to reduce the call of maintenance, but at
  // the same time, it provides fast maintenance reaction.
  void requestQueueCheck() { queueCheck_ = true; }

  void clearQueueCheck() { queueCheck_ = false; }

  bool queueCheckRequested() const { return queueCheck_; }

  // Returns currently used hosts and its use count.
  void getUsedHosts(std::vector<std::pair<size_t, std::string>>& usedHosts);

  const std::shared_ptr<ServerStatMan>& getServerStatMan() const
  {
    return serverStatMan_;
  }

  void setMaxDownloadResult(size_t v) { maxDownloadResult_ = v; }

  void setUriListParser(const std::shared_ptr<UriListParser>& uriListParser);

  NetStat& getNetStat() { return netStat_; }

  WrDiskCache* getWrDiskCache() const { return wrDiskCache_.get(); }

  // Initializes WrDiskCache according to PREF_DISK_CACHE option.  If
  // its value is 0, cache storage will not be initialized.
  void initWrDiskCache();

  void setKeepRunning(bool flag) { keepRunning_ = flag; }

  bool getKeepRunning() const { return keepRunning_; }

  size_t getNumStoppedTotal() const { return numStoppedTotal_; }

  void setLastSessionHash(std::string lastSessionHash)
  {
    lastSessionHash_ = std::move(lastSessionHash);
  }

  const std::string& getLastSessionHash() const { return lastSessionHash_; }

  const std::shared_ptr<OpenedFileCounter>& getOpenedFileCounter() const
  {
    return openedFileCounter_;
  }

  // If domain is non-empty, also subtracts connectionWeight from
  // activeConnectionsByDomain_[domain] (erasing the entry once it reaches
  // 0). Callers should pass getRequestGroupConnectionWeight(group) for the
  // group that just stopped, so the exact amount reserved at admission
  // time is given back.
  void decreaseNumActive(const std::string& domain = std::string(),
                         int connectionWeight = 1);
};

} // namespace aria2

#endif // D_REQUEST_GROUP_MAN_H
