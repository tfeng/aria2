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
#include "FxTickCommand.h"

#include "DownloadEngine.h"
#include "FxTelegram.h"
#include "RequestGroupMan.h"
#include "RpcMethodImpl.h"

namespace aria2 {

FxTickCommand::FxTickCommand(cuid_t cuid, DownloadEngine* e)
    : TimeBasedCommand(cuid, e, std::chrono::seconds(1), true)
{
}

void FxTickCommand::preProcess()
{
  // Like aria2's other routine commands: let a non-RPC run exit once its downloads are done.
  if (getDownloadEngine()->getRequestGroupMan()->downloadFinished() ||
      getDownloadEngine()->isHaltRequested()) {
    // The Telegram helper goes with it: closed now (TDLib saves its state), not killed with the process.
    fxtg::shutdown();
    enableExit();
  }
}

void FxTickCommand::process() { rpc::fxTick(getDownloadEngine()); }

} // namespace aria2
