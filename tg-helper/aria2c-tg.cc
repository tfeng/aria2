// aria2c-tg — relays TDLib's JSON interface over stdin/stdout for aria2c's Telegram jobs.
//
// aria2c can't link TDLib itself: aria2c links the curl-impersonate BoringSSL and TDLib needs OpenSSL, and the
// two export the same symbols. Running TDLib here also keeps a TDLib crash away from aria2c's web downloads
// (aria2c restarts this helper). It holds no download logic — aria2c drives TDLib's protocol itself:
//   stdin : one TDLib request per line (JSON, no raw newlines), sent with td_send to this process's one client;
//   stdout: one TDLib response/update per line, as td_receive returns it.
// End of stdin, or SIGTERM/SIGINT/SIGHUP (e.g. systemd stopping the service, which signals every process of it),
// closes the client and exits once TDLib reports authorizationStateClosed (or after 10 s), so TDLib's database is
// always closed properly. On a signal it first writes {"@type":"fxHelperExiting","reason":"SIGTERM"} so aria2c
// logs the exit as expected rather than as a crash.
//
// Usage: aria2c-tg [--log-verbosity N]   (TDLib's own log goes to stderr; default 1 = errors)
#include <td/telegram/td_json_client.h>

#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string>
#include <thread>

namespace {

std::mutex outMutex;
int signalPipe[2] = {-1, -1};

void writeLine(const char* line)
{
  // Write errors are ignored (SIGPIPE too): once aria2c is gone nobody reads this, but TDLib must still close.
  std::lock_guard<std::mutex> lock(outMutex);
  std::fputs(line, stdout);
  std::fputc('\n', stdout);
  std::fflush(stdout);
}

void onSignal(int sig)
{
  const int saved = errno;
  const char c = static_cast<char>(sig);
  if (::write(signalPipe[1], &c, 1) < 0) {
    // the pipe is full: a signal is already pending
  }
  errno = saved;
}

} // namespace

int main(int argc, char** argv)
{
  int verbosity = 1;
  for (int i = 1; i < argc; ++i) {
    if (std::strcmp(argv[i], "--log-verbosity") == 0 && i + 1 < argc) {
      verbosity = std::atoi(argv[++i]);
    }
    else {
      std::fprintf(stderr, "usage: %s [--log-verbosity N]\n", argv[0]);
      return 2;
    }
  }

  // Signals only write to a pipe the main loop polls (whichever thread they land on), before any thread starts.
  if (::pipe2(signalPipe, O_CLOEXEC | O_NONBLOCK) != 0) {
    std::perror("aria2c-tg: pipe");
    return 1;
  }
  ::signal(SIGPIPE, SIG_IGN);
  struct sigaction sa;
  std::memset(&sa, 0, sizeof(sa));
  sa.sa_handler = onSignal;
  sa.sa_flags = SA_RESTART;
  sigemptyset(&sa.sa_mask);
  for (int sig : {SIGTERM, SIGINT, SIGHUP}) {
    ::sigaction(sig, &sa, nullptr);
  }

  const std::string logRequest =
      "{\"@type\":\"setLogVerbosityLevel\",\"new_verbosity_level\":" + std::to_string(verbosity) + "}";
  td_execute(logRequest.c_str());

  const int client = td_create_client_id();
  std::atomic<bool> closed{false};

  // td_receive serves every client in the process; there is only ours.
  std::thread receiver([&] {
    while (!closed.load()) {
      const char* response = td_receive(1.0);
      if (response == nullptr) {
        continue;
      }
      writeLine(response);
      if (std::strstr(response, "\"authorizationStateClosed\"") != nullptr) {
        closed.store(true);
      }
    }
  });

  // Announce the client right away: TDLib only starts once a request arrives, and aria2c's first request
  // (getAuthorizationState) doubles as a handshake.
  writeLine(("{\"@type\":\"fxHelperReady\",\"client_id\":" + std::to_string(client) + "}").c_str());

  // stdin with plain read(): one request per line.
  std::string pending;
  char buf[65536];
  const char* signalName = nullptr;
  while (!closed.load() && signalName == nullptr) {
    struct pollfd fds[2] = {{STDIN_FILENO, POLLIN, 0}, {signalPipe[0], POLLIN, 0}};
    if (::poll(fds, 2, 1000) < 0) { // a timeout only to notice `closed` (TDLib closed itself, e.g. a log out)
      if (errno == EINTR) {
        continue;
      }
      break;
    }
    if (fds[1].revents & POLLIN) {
      char sig = 0;
      if (::read(signalPipe[0], &sig, 1) == 1) {
        signalName = sig == SIGTERM ? "SIGTERM" : sig == SIGINT ? "SIGINT" : "SIGHUP";
      }
      continue;
    }
    if (fds[0].revents == 0) {
      continue;
    }
    const ssize_t n = ::read(STDIN_FILENO, buf, sizeof(buf));
    if (n < 0 && (errno == EINTR || errno == EAGAIN)) {
      continue;
    }
    if (n <= 0) {
      break; // aria2c went away (or closed our stdin to end us)
    }
    pending.append(buf, static_cast<size_t>(n));
    size_t start = 0;
    for (size_t nl; (nl = pending.find('\n', start)) != std::string::npos; start = nl + 1) {
      if (nl > start) {
        td_send(client, pending.substr(start, nl - start).c_str());
      }
    }
    pending.erase(0, start);
  }

  if (signalName != nullptr) {
    std::fprintf(stderr, "aria2c-tg: %s, closing TDLib\n", signalName);
    writeLine((std::string("{\"@type\":\"fxHelperExiting\",\"reason\":\"") + signalName + "\"}").c_str());
  }
  // Close the client so TDLib flushes its database, then exit.
  if (!closed.load()) {
    td_send(client, "{\"@type\":\"close\"}");
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!closed.load() && std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    closed.store(true);
  }
  receiver.join();
  return 0;
}
