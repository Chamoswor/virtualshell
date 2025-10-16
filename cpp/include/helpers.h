#pragma once
#include "../include/virtual_shell.hpp"
#if not defined(_WIN32)
#include <sstream>
#endif


namespace _VirtualShellHelpers {
// ==============================
// Windows helpers / shims
// ==============================
#ifdef _WIN32

/**
 * Convert UTF-16 (Windows wide string) to UTF-8.
 * Note: allocates once using the exact required byte count.
 * Failure returns empty string.
 */
static std::string wstring_to_utf8(const std::wstring& w) {
    if (w.empty()) return {};
    int n = ::WideCharToMultiByte(CP_UTF8, 0, w.data(), (int)w.size(), nullptr, 0, nullptr, nullptr);
    if (n <= 0) return {};
    std::string out(n, '\0');
    ::WideCharToMultiByte(CP_UTF8, 0, w.data(), (int)w.size(), out.data(), n, nullptr, nullptr);
    return out;
}

/**
 * Complete an overlapped I/O and report bytes read/written.
 * @internal
 * @param h        I/O handle
 * @param ov       OVERLAPPED used for the operation
 * @param bytes    OUT: number of bytes transferred (0 on error)
 * @param blocking If true, wait until completion; if false, return immediately if incomplete
 * 
 * @return true  => either completed successfully or encountered a terminal error
 *         false => still in progress (ERROR_IO_INCOMPLETE)
 *
 * Rationale: We treat any status other than INCOMPLETE as "done", and let callers
 *            decide how to handle errors (bytes=0). This keeps read loops simpler.
 */
static bool complete_overlapped(HANDLE h, OVERLAPPED& ov, DWORD& bytes, bool blocking) {
    if (::GetOverlappedResult(h, &ov, &bytes, blocking ? TRUE : FALSE)) {
        return true;
    }
    DWORD err = ::GetLastError();
    if (err == ERROR_IO_INCOMPLETE) {
        return false; // still pending
    }
    bytes = 0;        // error path: normalize to zero bytes
    return true;      // signal "done" so caller can handle/exit
}

static std::string read_overlapped_once(OverlappedPipe& P, bool blocking) {
    std::string out;

    // Guard: invalid handle => no data
    if (P.h == nullptr || P.h == INVALID_HANDLE_VALUE) return out;

    // 1) If a previous ReadFile was pending, try to complete it first.
    //    - non-blocking: if still INCOMPLETE, just return whatever we have (empty).
    //    - blocking: complete_overlapped(..., blocking=true) will wait.
    if (P.pending) {
        DWORD br = 0;
        if (!complete_overlapped(P.h, P.ov, br, blocking)) {
            // Still pending and caller requested non-blocking => bail
            return out;
        }
        P.pending = false;
        ::ResetEvent(P.ov.hEvent);

        if (br > 0) {
            out.append(P.buf.data(), P.buf.data() + br);
        }
    }

    // 2) Issue fresh reads until:
    //    - we hit EOF / broken pipe,
    //    - we transition to a pending async read (and caller is non-blocking),
    //    - or we complete a read (possibly multiple times) and need to loop again.
    for (;;) {
        ::ResetEvent(P.ov.hEvent);
        DWORD br = 0;

        // Overlapped read:
        // - ok==TRUE => immediate completion; 'br' bytes available (0==EOF)
        // - ok==FALSE && ERROR_IO_PENDING => async in-flight
        // - ok==FALSE && other error => treat as terminal
        BOOL ok = ::ReadFile(P.h, P.buf.data(), static_cast<DWORD>(P.buf.size()), &br, &P.ov);
        if (ok) {
            if (br == 0) break; // EOF
            out.append(P.buf.data(), P.buf.data() + br);
            continue;           // try to read more in this pass
        }

        DWORD err = ::GetLastError();
        if (err == ERROR_IO_PENDING) {
            P.pending = true;

            if (!blocking) {
                // Non-blocking mode: leave the async op in-flight; return what we have.
                break;
            }

            // Blocking mode: wait for completion of this overlapped read.
            DWORD done = 0;
            if (complete_overlapped(P.h, P.ov, done, /*blocking*/true)) {
                P.pending = false;
                ::ResetEvent(P.ov.hEvent);
                if (done > 0) out.append(P.buf.data(), P.buf.data() + done);
                continue; // try another read immediately
            } else {
                // Still incomplete (shouldn't happen with blocking=true) — exit loop.
                break;
            }
        } else if (err == ERROR_BROKEN_PIPE || err == ERROR_HANDLE_EOF) {
            // Producer closed the pipe: normal termination.
            break;
        } else {
            // Any other error: treat as terminal; caller will decide next steps.
            break;
        }
    }

    return out;
}

#endif

static constexpr std::string_view INTERNAL_TIMEOUT_SENTINEL = "__VS_INTERNAL_TIMEOUT__";

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

static inline std::string ps_quote(const std::string& s) {
    // Quote a string for PowerShell literal context.
    // - Encloses with single quotes.
    // - Internal single quotes are doubled (' -> '').
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

} // namespace _VirtualShellHelpers