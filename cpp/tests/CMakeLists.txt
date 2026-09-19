# C++ tests for the virtualshell core. Built only when VIRTUALSHELL_BUILD_TESTS=ON;
# the pybind11 bindings (binder.cpp / py_proxy.cpp) are deliberately excluded so
# the test binary needs no Python runtime.

add_executable(vs_core_tests
  test_main.cpp
  test_helpers.cpp
  test_virtual_shell.cpp
  ${CMAKE_SOURCE_DIR}/cpp/src/io_pump.cpp
  ${CMAKE_SOURCE_DIR}/cpp/src/powershell_process.cpp
  ${CMAKE_SOURCE_DIR}/cpp/src/virtual_shell.cpp
)

target_include_directories(vs_core_tests PRIVATE ${CMAKE_SOURCE_DIR}/cpp/include)

if(MSVC)
  target_compile_options(vs_core_tests PRIVATE /permissive- /Zc:__cplusplus /EHsc /utf-8 /bigobj)
else()
  find_package(Threads REQUIRED)
  target_link_libraries(vs_core_tests PRIVATE Threads::Threads)
  if(UNIX AND NOT APPLE)
    target_link_libraries(vs_core_tests PRIVATE dl)
  endif()
endif()

add_test(NAME vs_core_tests COMMAND vs_core_tests)
