cmake_minimum_required(VERSION 3.20)

project(win_pwsh_dll LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_BUILD_TYPE "Release")

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

# Zero-copy bridge library with clean API
add_library(win_pwsh SHARED 
	src/vs_shm.cpp
	src/object_serializer.cpp
)

target_compile_definitions(win_pwsh PRIVATE
	WIN32_LEAN_AND_MEAN
	NOMINMAX
)

if (MSVC)
	# Enable C++/CLI for the entire target
	set_target_properties(win_pwsh PROPERTIES
		COMMON_LANGUAGE_RUNTIME ""  # Enable CLR
	)
	
	# Set /clr for the entire target
	target_compile_options(win_pwsh PRIVATE 
		/EHa           # C++ exception handling with SEH
		/clr           # Enable C++/CLI compilation
		/Zc:twoPhase-  # Required for C++/CLI
		/wd4793        # Disable warning about native/managed mixing
		/wd4267        # Disable size_t conversion warnings
	)
	
	# Add assembly search path for System.Management.Automation
	file(GLOB_RECURSE SMA_DLL 
		"C:/Windows/Microsoft.NET/assembly/GAC_MSIL/System.Management.Automation/*/System.Management.Automation.dll"
	)
	
	if(SMA_DLL)
		list(GET SMA_DLL 0 SMA_PATH)
		get_filename_component(SMA_DIR "${SMA_PATH}" DIRECTORY)
		message(STATUS "Found System.Management.Automation: ${SMA_PATH}")
		target_compile_options(win_pwsh PRIVATE "/AI\"${SMA_DIR}\"")
	else()
		message(WARNING "Could not locate System.Management.Automation.dll automatically")
		# Fallback: try PowerShell 7
		file(GLOB_RECURSE SMA_DLL_PS7 
			"C:/Program Files/PowerShell/*/System.Management.Automation.dll"
		)
		if(SMA_DLL_PS7)
			list(GET SMA_DLL_PS7 0 SMA_PATH)
			get_filename_component(SMA_DIR "${SMA_PATH}" DIRECTORY)
			message(STATUS "Using PowerShell 7 System.Management.Automation: ${SMA_PATH}")
			target_compile_options(win_pwsh PRIVATE "/AI\"${SMA_DIR}\"")
		endif()
	endif()
	
	# Link .NET Framework assemblies
	set_target_properties(win_pwsh PROPERTIES
		VS_DOTNET_REFERENCES "System;System.Core;System.Management.Automation"
	)
	
else ()
	target_compile_options(win_pwsh PRIVATE -Wall -Wextra -Wpedantic)
endif ()

set_target_properties(win_pwsh PROPERTIES
	OUTPUT_NAME "win_pwsh"
	WINDOWS_EXPORT_ALL_SYMBOLS OFF
)

install(TARGETS win_pwsh
	RUNTIME DESTINATION bin
	LIBRARY DESTINATION bin
)
