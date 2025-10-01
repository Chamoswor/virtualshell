cmake_minimum_required(VERSION 3.20)
project(virtualshell LANGUAGES CXX)

find_package(pybind11 CONFIG QUIET)
if(NOT pybind11_FOUND)
  find_package(Python3 COMPONENTS Interpreter Development REQUIRED)
  execute_process(
    COMMAND ${Python3_EXECUTABLE} -c "import pybind11; print(pybind11.get_cmake_dir())"
    OUTPUT_VARIABLE pybind11_DIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
    RESULT_VARIABLE pybind11_RESULT
  )
  if(pybind11_RESULT EQUAL 0)
    find_package(pybind11 CONFIG REQUIRED PATHS ${pybind11_DIR} NO_DEFAULT_PATH)
  else()
    message(FATAL_ERROR "pybind11 not found. Install with: pip install pybind11")
  endif()
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)


if (WIN32 AND CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  message(FATAL_ERROR "MinGW is not supported on Windows for this project. Use MSVC.")
endif()


set(SRC
  cpp/src/binder.cpp
  cpp/src/virtual_shell.cpp
)

# Output: site-packages/virtualshell (wheel) vs src/virtualshell (editable)
if(DEFINED ENV{SKBUILD_STATE} AND "$ENV{SKBUILD_STATE}" STREQUAL "editable")
  set(_VS_OUTDIR "${PROJECT_SOURCE_DIR}/src/virtualshell")
else()
  set(_VS_OUTDIR "${SKBUILD_PLATLIB_DIR}/virtualshell")
endif()

pybind11_add_module(_core MODULE ${SRC})
target_include_directories(_core PRIVATE cpp/include)

if(UNIX)
  find_package(Threads REQUIRED)
  target_link_libraries(_core PRIVATE Threads::Threads dl)
endif()

set_target_properties(_core PROPERTIES
  OUTPUT_NAME "_core"
  LIBRARY_OUTPUT_DIRECTORY                "${_VS_OUTDIR}"
  RUNTIME_OUTPUT_DIRECTORY                "${_VS_OUTDIR}"
  LIBRARY_OUTPUT_DIRECTORY_RELEASE        "${_VS_OUTDIR}"
  RUNTIME_OUTPUT_DIRECTORY_RELEASE        "${_VS_OUTDIR}"
  LIBRARY_OUTPUT_DIRECTORY_RELWITHDEBINFO "${_VS_OUTDIR}"
  RUNTIME_OUTPUT_DIRECTORY_RELWITHDEBINFO "${_VS_OUTDIR}"
  CXX_VISIBILITY_PRESET hidden
  VISIBILITY_INLINES_HIDDEN ON
)

if(MSVC)
  target_compile_options(_core PRIVATE /permissive- /Zc:__cplusplus /EHsc /utf-8 /bigobj)
else()
  target_compile_options(_core PRIVATE -O3 -fvisibility=hidden)
endif()

install(TARGETS _core
  LIBRARY DESTINATION virtualshell
  RUNTIME DESTINATION virtualshell
  ARCHIVE DESTINATION virtualshell
)

