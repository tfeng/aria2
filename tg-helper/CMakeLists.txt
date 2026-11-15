# aria2c-tg — the Telegram helper aria2c starts for Telegram jobs (see aria2c-tg.cc). Built by
# ../fetch-tdlib-deps.sh, which passes TD_SOURCE_DIR = its pinned TDLib checkout. TDLib is pulled in with
# add_subdirectory (TDLib's own documented way to embed it), so only what Td::TdJsonStatic needs is compiled —
# not tg_cli, the examples or the shared library.
cmake_minimum_required(VERSION 3.10 FATAL_ERROR)
project(aria2c_tg LANGUAGES CXX C)

if(NOT TD_SOURCE_DIR)
  message(FATAL_ERROR "TD_SOURCE_DIR is not set — build this through ../fetch-tdlib-deps.sh")
endif()
add_subdirectory(${TD_SOURCE_DIR} td EXCLUDE_FROM_ALL)

add_executable(aria2c-tg aria2c-tg.cc)
set_property(TARGET aria2c-tg PROPERTY CXX_STANDARD 17)
target_link_libraries(aria2c-tg PRIVATE Td::TdJsonStatic)
