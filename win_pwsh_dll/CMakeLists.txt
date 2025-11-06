cmake_minimum_required(VERSION 3.20)

project(win_pwsh_dll LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
add_library(win_pwsh SHARED src/main.cpp)

target_compile_definitions(win_pwsh PRIVATE
	WIN32_LEAN_AND_MEAN
	NOMINMAX
)

if (MSVC)
	target_compile_options(win_pwsh PRIVATE /W4 /permissive- /EHsc)
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
