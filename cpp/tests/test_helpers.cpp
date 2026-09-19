// Unit tests for pure helpers and value types: no PowerShell process needed.
#include "test_framework.hpp"

#include "config.hpp"
#include "execution_result.hpp"
#include "helpers.hpp"

using virtualshell::helpers::parsers::ps_quote;
using virtualshell::helpers::parsers::trim_inplace;

// ---------- ps_quote ----------

TEST_CASE("ps_quote wraps plain text in single quotes") {
    CHECK_EQ(ps_quote(""), "''");
    CHECK_EQ(ps_quote("hello"), "'hello'");
}

TEST_CASE("ps_quote doubles embedded single quotes") {
    CHECK_EQ(ps_quote("it's"), "'it''s'");
    CHECK_EQ(ps_quote("''"), "''''''");
}

TEST_CASE("ps_quote leaves interpolation characters untouched") {
    CHECK_EQ(ps_quote("$env:PATH `n \"x\""), "'$env:PATH `n \"x\"'");
}

TEST_CASE("ps_quote doubles unicode smart quotes") {
    // U+2019 (right single quotation mark) = E2 80 99: PowerShell treats the
    // smart-quote family as quote characters, so they must be doubled too.
    const std::string smart = "\xE2\x80\x99";
    CHECK_EQ(ps_quote("a" + smart + "b"), "'a" + smart + smart + "b'");
    CHECK_EQ(ps_quote(smart), "'" + smart + smart + "'");
}

// ---------- trim_inplace ----------

TEST_CASE("trim_inplace strips surrounding whitespace") {
    std::string s = " \t\r\n hei \r\n";
    trim_inplace(s);
    CHECK_EQ(s, "hei");
}

TEST_CASE("trim_inplace keeps interior whitespace and handles edge cases") {
    std::string mid = "a b";
    trim_inplace(mid);
    CHECK_EQ(mid, "a b");

    std::string untouched = "abc";
    trim_inplace(untouched);
    CHECK_EQ(untouched, "abc");

    std::string empty;
    trim_inplace(empty);
    CHECK_EQ(empty, "");

    std::string only_ws = " \t\r\n";
    trim_inplace(only_ws);
    CHECK_EQ(only_ws, "");
}

// ---------- UTF-8 normalization (Windows-only helpers) ----------

#ifdef _WIN32
TEST_CASE("isValidUtf8 accepts valid sequences and rejects invalid ones") {
    using virtualshell::helpers::isValidUtf8;
    CHECK(isValidUtf8(""));
    CHECK(isValidUtf8("plain ascii"));
    CHECK(isValidUtf8("\xC3\xA6\xC3\xB8\xC3\xA5"));      // æøå
    CHECK(isValidUtf8("\xE2\x80\x99"));                    // U+2019
    CHECK(!isValidUtf8("\xE9"));                           // lone latin-1 byte
    CHECK(!isValidUtf8("\xC3"));                           // truncated sequence
    CHECK(!isValidUtf8("\xC3\x28"));                       // bad continuation
}

TEST_CASE("latin1Fallback maps high bytes to two-byte UTF-8") {
    using virtualshell::helpers::latin1Fallback;
    CHECK_EQ(latin1Fallback("abc"), "abc");
    CHECK_EQ(latin1Fallback("\xE9"), "\xC3\xA9");          // é
}

TEST_CASE("normalizeToUtf8 passes valid UTF-8 through and repairs the rest") {
    using virtualshell::helpers::isValidUtf8;
    using virtualshell::helpers::normalizeToUtf8;

    std::string valid = "r\xC3\xA5tekst";                  // råtekst
    CHECK_EQ(normalizeToUtf8(std::string(valid)), valid);

    // Non-UTF-8 input (ANSI bytes): the exact result depends on the system
    // code page, but the output must always be valid UTF-8 and non-empty.
    std::string repaired = normalizeToUtf8(std::string("caf\xE9"));
    CHECK(!repaired.empty());
    CHECK(isValidUtf8(repaired));
}
#endif

// ---------- value types ----------

TEST_CASE("Config defaults match the documented contract") {
    virtualshell::core::Config cfg;
    CHECK_EQ(cfg.powershellPath, "pwsh");
    CHECK_EQ(cfg.workingDirectory, "");
    CHECK(cfg.captureOutput);
    CHECK(cfg.captureError);
    CHECK(cfg.autoRestartOnTimeout);
    CHECK_EQ(cfg.timeoutSeconds, 30);
    CHECK(cfg.environment.empty());
    CHECK(cfg.initialCommands.empty());
    CHECK_EQ(cfg.stdin_buffer_size, static_cast<size_t>(64 * 1024));
}

TEST_CASE("ExecutionResult default-constructs to an empty, unsuccessful result") {
    virtualshell::core::ExecutionResult res;
    CHECK_EQ(res.out, "");
    CHECK_EQ(res.err, "");
    CHECK_EQ(res.exitCode, 0);
    CHECK(!res.success);
    CHECK_EQ(res.executionTime, 0.0);
}

TEST_CASE("BatchProgress default-constructs empty") {
    virtualshell::core::BatchProgress prog;
    CHECK_EQ(prog.currentCommand, static_cast<size_t>(0));
    CHECK_EQ(prog.totalCommands, static_cast<size_t>(0));
    CHECK(!prog.isComplete);
    CHECK(prog.allResults.empty());
}
