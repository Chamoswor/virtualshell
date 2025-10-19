#pragma once
#if not defined(_WIN32)
#include <sstream>
#endif


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



} // namespace virtualshell::helpers