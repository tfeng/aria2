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
#ifndef D_FX_TICK_COMMAND_H
#define D_FX_TICK_COMMAND_H

#include "TimeBasedCommand.h"

namespace aria2 {

// FXPlayer extension (2026-11-14): runs rpc::fxTick once a second on the main thread -- the jobs file (restore at
// startup, rewrite on change) and Telegram downloads' results (FxTelegram.h).
class FxTickCommand : public TimeBasedCommand {
public:
  FxTickCommand(cuid_t cuid, DownloadEngine* e);

  virtual void preProcess() CXX11_OVERRIDE;

  virtual void process() CXX11_OVERRIDE;
};

} // namespace aria2

#endif // D_FX_TICK_COMMAND_H
