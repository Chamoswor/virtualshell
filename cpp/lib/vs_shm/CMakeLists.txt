cmake_minimum_required(VERSION 3.20)

project(vs_shm LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if (NOT MSVC)
  message(FATAL_ERROR "vs_shm requires MSVC (/clr). MinGW/Clang on Windows is not supported for this target.")
endif()

if(MSVC)
  foreach(lang C CXX)
    foreach(cfg DEBUG RELWITHDEBINFO)
      string(REPLACE "/RTC1" "" CMAKE_${lang}_FLAGS_${cfg} "${CMAKE_${lang}_FLAGS_${cfg}}")
      string(REPLACE "/RTC"  "" CMAKE_${lang}_FLAGS_${cfg} "${CMAKE_${lang}_FLAGS_${cfg}}")
    endforeach()
  endforeach()
endif()

# Zero-copy bridge library with clean API
add_library(_vs_shm SHARED
  src/vs_shm.cpp
  src/object_serializer.cpp
)

target_include_directories(_vs_shm PRIVATE
  "${CMAKE_CURRENT_SOURCE_DIR}/include"
)

target_compile_definitions(_vs_shm PRIVATE
  WIN32_LEAN_AND_MEAN
  NOMINMAX
)

if (MSVC)
  target_compile_options(_vs_shm PRIVATE
    /EHa
    /clr
    /Zc:twoPhase-
    /wd4793
    /wd4267
    $<$<CONFIG:Debug>:/RTC->
  )

  # --- Locate System.Management.Automation.dll in Windows PowerShell GAC ---
  set(SMA_PATH "")
  file(GLOB_RECURSE SMA_DLL_GAC
    "C:/Windows/Microsoft.NET/assembly/GAC_MSIL/System.Management.Automation/*/System.Management.Automation.dll"
  )

  if (SMA_DLL_GAC)
    list(GET SMA_DLL_GAC 0 SMA_PATH)
  endif()

  if (SMA_PATH)
    get_filename_component(SMA_DIR "${SMA_PATH}" DIRECTORY)
    message(STATUS "Using GAC System.Management.Automation.dll: ${SMA_PATH}")
    message(STATUS "Assembly search path: ${SMA_DIR}")

    # Let CMake handle quoting; /FU forces the reference
    target_compile_options(_vs_shm PRIVATE
      "/AI${SMA_DIR}"
      "/FU${SMA_PATH}"
    )
  else()
    message(FATAL_ERROR
      "Could not locate System.Management.Automation.dll in GAC.\n"
      "Ensure Windows PowerShell (5.1) is present on this machine."
    )
  endif()

  # Optional: avoid LNK1146 (/implib: without value) in some setups
  set_target_properties(_vs_shm PROPERTIES
    IMPORT_LIBRARY "${CMAKE_CURRENT_BINARY_DIR}/_vs_shm.lib"
  )
else()
  target_compile_options(_vs_shm PRIVATE -Wall -Wextra -Wpedantic)
endif()

set_target_properties(_vs_shm PROPERTIES
  OUTPUT_NAME "_vs_shm"
  WINDOWS_EXPORT_ALL_SYMBOLS OFF
)

install(TARGETS _vs_shm
  RUNTIME DESTINATION virtualshell
  LIBRARY DESTINATION virtualshell
  ARCHIVE DESTINATION virtualshell
)
