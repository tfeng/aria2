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

#include "DownloadResult.h"
#include "TransferStat.h"
#include "RequestGroup.h"
#include "NetStat.h"
#include "IndexedList.h"

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
