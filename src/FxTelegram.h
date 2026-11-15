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
#ifndef D_FX_TELEGRAM_H
#define D_FX_TELEGRAM_H

// FXPlayer extension (2026-11-14): Telegram downloads for fxplayer.addMerge jobs.
//
// aria2c cannot link TDLib (it links curl-impersonate's BoringSSL; TDLib needs OpenSSL, and the two export the same
// symbols), so TDLib runs in a helper process, aria2c-tg (tg-helper/, built by fetch-tdlib-deps.sh), that only relays
// TDLib's JSON interface over a pipe. This module starts and restarts that helper and speaks TDLib's protocol: the
// account's sign-in and, per download, "message -> file -> downloadFile -> progress -> done". It knows nothing about
// merge jobs: RpcMethodImpl.cc runs a Telegram job as an ordinary FxMergeJob whose one "segment" is the downloaded
// file, and reads this module's results with takeEvents() on the main thread.
//
// Thread safety: every function may be called from any thread; RpcMethodImpl calls them from the main thread, with
// fxMergeMutex held. The helper's output is read on a thread of this module, which only ever takes this module's own
// lock: nothing here calls back into the merge code, so the lock order is always fxMergeMutex -> this module's.

#include "common.h"
#include "GroupId.h"

#include <string>
#include <vector>

namespace aria2 {

namespace fxtg {

enum class Result {
  OK,             // downloaded; Event::path is TDLib's file
  CANCELED,       // stop() was called
  UNAVAILABLE,    // the message is gone, not accessible, or has no video/document
  NOT_AUTHORIZED, // the helper's Telegram account isn't signed in
  FAILED,         // anything else (TDLib error, helper trouble); worth retrying
};

struct Event {
  a2_gid_t gid = 0;
  Result result = Result::FAILED;
  std::string message;
  std::string path;
  int64_t size = 0;
  int64_t downloaded = 0; // bytes TDLib had of the file when the download ended (0 = it never started)
};

struct Progress {
  int64_t downloaded = 0; // contiguous bytes from the start of the file
  int64_t total = 0;      // the file's size (0 = not known yet)
  int64_t speed = 0;      // bytes/s, averaged over the last few ticks
  bool queued = false;    // waiting for a download slot or for the account to be ready
};

struct AuthState {
  bool configured = false; // --fx-telegram-helper set
  // disabled | starting | wait-credentials | wait-phone | wait-qr | wait-code | wait-password | unsupported | ready |
  // closing | helper-failed
  std::string state = "disabled";
  std::string detail; // e.g. the password hint, or the unsupported TDLib state
  std::string error;  // the last sign-in error TDLib reported
};

// Called once at startup. `helperPath` empty = Telegram downloads disabled. `dataDir` holds the TDLib database,
// TDLib's downloads (partial and complete) and the saved API credentials. A finished file is only read from there
// (ffmpeg's input), never renamed, so any filesystem works; the downloads' one just keeps the space accounting in one
// place. The helper starts on the first tick().
void configure(const std::string& helperPath, const std::string& dataDir);
bool configured();

// Starts downloading message `msgId` of chat `chatId` (TDLib ids) for job `gid`. Its result arrives as an Event.
void start(a2_gid_t gid, int64_t chatId, int64_t msgId);
// Stops `gid`'s download, keeping TDLib's partial file. A CANCELED Event follows. No-op for an unknown gid.
void stop(a2_gid_t gid);
// Deletes TDLib's copy (partial or complete) of the message's file, unless a download of this module still uses it.
// Runs once the account is ready (looking the file up again if needed); a request still waiting is lost to a restart.
void discard(int64_t chatId, int64_t msgId);
// False for an unknown gid (finished, failed, stopped or never started).
bool progress(a2_gid_t gid, Progress& out);
std::vector<Event> takeEvents();

// Main thread, about once a second: (re)starts the helper, admits queued downloads, updates speeds, watches for
// stalled downloads, and fails downloads that can't run (the account needs signing in, the helper keeps exiting).
void tick();
// aria2 is shutting down: closes the helper (TDLib saves its state) and waits a few seconds for it to exit, so a
// quick restart doesn't find TDLib's database still in use. The helper isn't restarted after this.
void shutdown();

AuthState authState();
// Sign-in steps, sent to TDLib in whatever state it is in (a step that doesn't fit is reported as an error).
void setCredentials(int64_t apiId, const std::string& apiHash);
void submitPhone(const std::string& phone);
void submitCode(const std::string& code);
void submitPassword(const std::string& password);
// QR-code login instead of a phone code: the state becomes wait-qr with the link in `detail`, for another signed-in
// client of the same account to confirm.
void requestQrLogin();
void logOut();

} // namespace fxtg

} // namespace aria2

#endif // D_FX_TELEGRAM_H
