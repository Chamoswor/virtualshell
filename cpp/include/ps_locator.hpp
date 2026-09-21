#pragma once

/**
 * @file ps_locator.hpp
 * @brief Resolve which PowerShell executable to launch for a Config.
 *
 * Supported editions (Config::powershellEdition):
 *   - "core":    PowerShell 7+ (`pwsh`), cross-platform.
 *   - "desktop": Windows PowerShell 5.1 (`powershell.exe`), Windows only.
 *   - "auto":    prefer pwsh; fall back to Windows PowerShell on Windows.
 *
 * An explicit Config::powershellPath always wins over the edition.
 */

#include <cctype>
#include <initializer_list>
#include <optional>
#include <string>
#include <string_view>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#else
#include <cstdlib>
#include <unistd.h>
#endif

namespace virtualshell::helpers::locator {

inline constexpr std::string_view kEditionAuto    = "auto";
inline constexpr std::string_view kEditionCore    = "core";
inline constexpr std::string_view kEditionDesktop = "desktop";

/// Canonical edition name (lower-case, no whitespace), or "" when unrecognised.
inline std::string normalizeEdition(std::string_view raw) {
    std::string s;
    s.reserve(raw.size());
    for (char ch : raw) {
        const unsigned char c = static_cast<unsigned char>(ch);
        if (!std::isspace(c)) s.push_back(static_cast<char>(std::tolower(c)));
    }
    if (s == kEditionAuto || s == kEditionCore || s == kEditionDesktop) return s;
    return {};
}

#ifdef _WIN32
namespace detail {

inline std::string wideToUtf8(const std::wstring& w) {
    if (w.empty()) return {};
    const int n = ::WideCharToMultiByte(CP_UTF8, 0, w.data(), static_cast<int>(w.size()),
                                        nullptr, 0, nullptr, nullptr);
    if (n <= 0) return {};
    std::string out(static_cast<size_t>(n), '\0');
    ::WideCharToMultiByte(CP_UTF8, 0, w.data(), static_cast<int>(w.size()),
                          out.data(), n, nullptr, nullptr);
    return out;
}

inline bool isFile(const std::wstring& path) {
    const DWORD attrs = ::GetFileAttributesW(path.c_str());
    return attrs != INVALID_FILE_ATTRIBUTES && !(attrs & FILE_ATTRIBUTE_DIRECTORY);
}

inline std::optional<std::wstring> envVar(const wchar_t* name) {
    const DWORD needed = ::GetEnvironmentVariableW(name, nullptr, 0);
    if (needed == 0) return std::nullopt;
    std::wstring value(needed, L'\0');
    const DWORD written = ::GetEnvironmentVariableW(name, value.data(), needed);
    if (written == 0 || written >= needed) return std::nullopt;
    value.resize(written);
    return value;
}

/// Search the application dir, CWD, system dirs and PATH like CreateProcess would.
inline std::optional<std::wstring> searchPath(const wchar_t* exe) {
    wchar_t buffer[MAX_PATH];
    const DWORD n = ::SearchPathW(nullptr, exe, L".exe", MAX_PATH, buffer, nullptr);
    if (n == 0 || n >= MAX_PATH) return std::nullopt;
    return std::wstring(buffer, n);
}

} // namespace detail
#endif

/// Windows PowerShell 5.1 (`powershell.exe`) when installed. Always nullopt off Windows.
inline std::optional<std::string> findWindowsPowerShell() {
#ifdef _WIN32
    if (auto root = detail::envVar(L"SystemRoot")) {
        const std::wstring candidate =
            *root + L"\\System32\\WindowsPowerShell\\v1.0\\powershell.exe";
        if (detail::isFile(candidate)) return detail::wideToUtf8(candidate);
    }
    if (auto found = detail::searchPath(L"powershell")) return detail::wideToUtf8(*found);
#endif
    return std::nullopt;
}

/// PowerShell 7+ (`pwsh`) from PATH or its default install location.
inline std::optional<std::string> findPwsh() {
#ifdef _WIN32
    if (auto found = detail::searchPath(L"pwsh")) return detail::wideToUtf8(*found);
    if (auto pf = detail::envVar(L"ProgramFiles")) {
        const std::wstring candidate = *pf + L"\\PowerShell\\7\\pwsh.exe";
        if (detail::isFile(candidate)) return detail::wideToUtf8(candidate);
    }
    return std::nullopt;
#else
    if (const char* path = std::getenv("PATH")) {
        std::string_view rest(path);
        while (!rest.empty()) {
            const auto sep = rest.find(':');
            const std::string_view dir = rest.substr(0, sep);
            if (!dir.empty()) {
                std::string candidate(dir);
                candidate += "/pwsh";
                if (::access(candidate.c_str(), X_OK) == 0) return candidate;
            }
            if (sep == std::string_view::npos) break;
            rest.remove_prefix(sep + 1);
        }
    }
    for (const char* candidate : {"/usr/bin/pwsh", "/usr/local/bin/pwsh",
                                  "/opt/microsoft/powershell/7/pwsh", "/snap/bin/pwsh"}) {
        if (::access(candidate, X_OK) == 0) return std::string(candidate);
    }
    return std::nullopt;
#endif
}

/**
 * @brief Pick the executable for the given explicit path and edition.
 *
 * - A non-empty explicit path is returned untouched (bare names are resolved
 *   by the OS at spawn time, as before).
 * - "core": pwsh; falls back to the bare name so the spawn reports the failure.
 * - "desktop": Windows PowerShell; nullopt off Windows.
 * - "auto": pwsh when found, else Windows PowerShell on Windows, else bare "pwsh".
 * - Unknown edition: nullopt.
 */
inline std::optional<std::string> resolveExecutable(const std::string& explicitPath,
                                                    std::string_view editionRaw) {
    if (!explicitPath.empty()) return explicitPath;

    const std::string edition = normalizeEdition(editionRaw);
    if (edition.empty()) return std::nullopt;

    if (edition == kEditionCore) {
        return findPwsh().value_or(std::string("pwsh"));
    }
    if (edition == kEditionDesktop) {
#ifdef _WIN32
        return findWindowsPowerShell().value_or(std::string("powershell.exe"));
#else
        return std::nullopt;
#endif
    }
    if (auto pwsh = findPwsh()) return pwsh;
#ifdef _WIN32
    if (auto desktop = findWindowsPowerShell()) return desktop;
#endif
    return std::string("pwsh");
}

} // namespace virtualshell::helpers::locator
