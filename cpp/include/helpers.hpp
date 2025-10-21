#pragma once

#include <string>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#else
#include <sstream>
#endif

#include <limits>


/**
 * @brief Helper utilities shared across the VirtualShell C++ implementation.
 */
namespace virtualshell::helpers {
#ifdef _WIN32

namespace win {

/**
 * @brief Convert a wide string to UTF-8 using Win32 conversion routines.
 */
static std::string wstring_to_utf8(const std::wstring& w) {
    if (w.empty()) return {};
    int n = ::WideCharToMultiByte(CP_UTF8, 0, w.data(), (int)w.size(), nullptr, 0, nullptr, nullptr);
    if (n <= 0) return {};
    std::string out(n, '\0');
    ::WideCharToMultiByte(CP_UTF8, 0, w.data(), (int)w.size(), out.data(), n, nullptr, nullptr);
    return out;
}
} // namespace win

#endif

namespace parsers {
/**
 * @brief Trim ASCII whitespace from both ends of a string in-place.
 */
static inline void trim_inplace(std::string& s) {
    // Remove leading/trailing whitespace (space, tab, CR, LF) in-place.
    auto is_space = [](unsigned char ch){ return ch==' '||ch=='\t'||ch=='\r'||ch=='\n'; };
    size_t a = 0, b = s.size();
    while (a < b && is_space(static_cast<unsigned char>(s[a]))) ++a;
    while (b > a && is_space(static_cast<unsigned char>(s[b-1]))) --b;

    // Only reassign if trimming actually changes the view.
    if (a==0 && b==s.size()) return;
    s.assign(s.begin()+a, s.begin()+b);
}

/**
 * @brief Quote a string as a PowerShell single-quoted literal.
 */
static inline std::string ps_quote(const std::string& s) {

    std::string t;
    t.reserve(s.size() + 2);
    t.push_back('\'');
    for (char c : s) {
        if (c == '\'') t += "''"; 
        else t.push_back(c);
    }
    t.push_back('\'');
    return t;
}
} // namespace parsers

#ifdef _WIN32
inline bool isValidUtf8(const std::string& data) {
    const unsigned char* ptr = reinterpret_cast<const unsigned char*>(data.data());
    size_t len = data.size();
    size_t i = 0;
    while (i < len) {
        unsigned char c = ptr[i];
        if (c < 0x80) {
            ++i;
            continue;
        }
        size_t extra = 0;
        if ((c >> 5) == 0x6) {
            extra = 1;
        } else if ((c >> 4) == 0xE) {
            extra = 2;
        } else if ((c >> 3) == 0x1E) {
            extra = 3;
        } else {
            return false;
        }
        if (i + extra >= len) {
            return false;
        }
        for (size_t j = 1; j <= extra; ++j) {
            if ((ptr[i + j] >> 6) != 0x2) {
                return false;
            }
        }
        i += extra + 1;
    }
    return true;
}

inline std::string latin1Fallback(const std::string& payload) {
    std::string out;
    out.reserve(payload.size() * 2);
    for (unsigned char c : payload) {
        if (c < 0x80) {
            out.push_back(static_cast<char>(c));
        } else {
            out.push_back(static_cast<char>(0xC0 | (c >> 6)));
            out.push_back(static_cast<char>(0x80 | (c & 0x3F)));
        }
    }
    return out;
}

inline std::string normalizeToUtf8(std::string&& payload) {
    if (payload.empty() || isValidUtf8(payload)) {
        return std::move(payload);
    }

    if (payload.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
        return std::move(payload);
    }

    int wideLen = MultiByteToWideChar(CP_ACP,
                                      0,
                                      payload.data(),
                                      static_cast<int>(payload.size()),
                                      nullptr,
                                      0);
    if (wideLen > 0) {
        std::wstring wide(static_cast<size_t>(wideLen), L'\0');
        MultiByteToWideChar(CP_ACP,
                            0,
                            payload.data(),
                            static_cast<int>(payload.size()),
                            wide.data(),
                            wideLen);

        int utf8Len = WideCharToMultiByte(CP_UTF8,
                                          0,
                                          wide.data(),
                                          wideLen,
                                          nullptr,
                                          0,
                                          nullptr,
                                          nullptr);
        if (utf8Len > 0) {
            std::string utf8(static_cast<size_t>(utf8Len), '\0');
            WideCharToMultiByte(CP_UTF8,
                                0,
                                wide.data(),
                                wideLen,
                                utf8.data(),
                                utf8Len,
                                nullptr,
                                nullptr);
            return utf8;
        }
    }

    return latin1Fallback(payload);
}
#else
inline std::string normalizeToUtf8(std::string&& payload) {
    return std::move(payload);
}
#endif


} // namespace virtualshell::helpers