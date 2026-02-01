cmake_minimum_required(VERSION 3.20)
project(virtualshell LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Enforce 64-bit build on Windows
if(WIN32 AND CMAKE_SIZEOF_VOID_P EQUAL 4)
  message(FATAL_ERROR
    "32-bit build detected (x86). virtualshell requires 64-bit (x64).\n"
    "Open 'x64 Native Tools Command Prompt for VS' and retry."
  )
endif()

# Find Python and pybind11
find_package(Python3 REQUIRED COMPONENTS Interpreter Development.Module)
find_package(pybind11 CONFIG QUIET)
if(NOT pybind11_FOUND)
    execute_process(
      COMMAND ${Python3_EXECUTABLE} -c "import pybind11; print(pybind11.get_cmake_dir())"
      OUTPUT_VARIABLE pybind11_DIR
      OUTPUT_STRIP_TRAILING_WHITESPACE
      RESULT_VARIABLE pybind11_RESULT
    )
    file(TO_CMAKE_PATH "${pybind11_DIR}" pybind11_DIR)
  if(pybind11_RESULT EQUAL 0)
    find_package(pybind11 CONFIG REQUIRED PATHS ${pybind11_DIR} NO_DEFAULT_PATH)
  else()
    message(FATAL_ERROR "pybind11 not found. Install with: pip install pybind11")
  endif()
endif()



# Source files
set(SRC
  cpp/src/binder.cpp
  cpp/src/io_pump.cpp
  cpp/src/powershell_process.cpp
  cpp/src/virtual_shell.cpp
  cpp/src/py_proxy.cpp
)

file(GLOB_RECURSE VS_HEADERS CONFIGURE_DEPENDS
  "${CMAKE_CURRENT_SOURCE_DIR}/cpp/include/*.h"
  "${CMAKE_CURRENT_SOURCE_DIR}/cpp/include/*.hpp"
)

pybind11_add_module(_core MODULE ${SRC} ${VS_HEADERS})
target_include_directories(_core PRIVATE cpp/include)

# Link platform-specific libraries
if(WIN32)
  target_link_libraries(_core PRIVATE Python3::Module)
elseif(UNIX)
  find_package(Threads REQUIRED)
  target_link_libraries(_core PRIVATE Threads::Threads dl)
endif()

# Set output directory for the built module
if(DEFINED ENV{SKBUILD_STATE} AND "$ENV{SKBUILD_STATE}" STREQUAL "editable")
  set(_VS_OUTDIR "${PROJECT_SOURCE_DIR}/src/virtualshell")
elseif(DEFINED SKBUILD_PLATLIB_DIR)
  set(_VS_OUTDIR "${SKBUILD_PLATLIB_DIR}/virtualshell")
else()
  set(_VS_OUTDIR "${CMAKE_CURRENT_BINARY_DIR}/virtualshell")
endif()

# Configure target properties
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

# Compiler-specific options
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

# Build and link the vs_shm library on Windows
if(WIN32 AND MSVC)
  add_subdirectory(cpp/lib/vs_shm)

  set_target_properties(_vs_shm PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY                "${_VS_OUTDIR}"
    RUNTIME_OUTPUT_DIRECTORY_RELEASE        "${_VS_OUTDIR}"
    RUNTIME_OUTPUT_DIRECTORY_RELWITHDEBINFO "${_VS_OUTDIR}"
    RUNTIME_OUTPUT_DIRECTORY_DEBUG          "${_VS_OUTDIR}"
    LIBRARY_OUTPUT_DIRECTORY                "${_VS_OUTDIR}"
    LIBRARY_OUTPUT_DIRECTORY_RELEASE        "${_VS_OUTDIR}"
    LIBRARY_OUTPUT_DIRECTORY_RELWITHDEBINFO "${_VS_OUTDIR}"
    LIBRARY_OUTPUT_DIRECTORY_DEBUG          "${_VS_OUTDIR}"
  )

  add_dependencies(_core _vs_shm)

  add_custom_command(TARGET _core POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E make_directory "${_VS_OUTDIR}"
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
            "$<TARGET_FILE:_vs_shm>"
            "${_VS_OUTDIR}/$<TARGET_FILE_NAME:_vs_shm>"
    VERBATIM
  )

  install(TARGETS _vs_shm
    RUNTIME DESTINATION virtualshell
    LIBRARY DESTINATION virtualshell
    ARCHIVE DESTINATION virtualshell
  )
endif()


