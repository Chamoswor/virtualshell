// Integration tests: VirtualShell against a real PowerShell process (pwsh, or
// Windows PowerShell 5.1 when pwsh is missing on Windows; Config default "auto").
// Each case skips itself if no PowerShell can be started on this machine.
#include "test_framework.hpp"

#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "helpers.hpp"
#include "virtual_shell.hpp"

using virtualshell::core::Config;

namespace {

Config test_config() {
    Config cfg;
    cfg.initialCommands = {
        "$OutputEncoding = [Console]::OutputEncoding = [Text.UTF8Encoding]::new()"
    };
    return cfg;
}

// Starting a process per case keeps the cases independent; a shared static
// shell would let one failure cascade into the rest.
std::shared_ptr<VirtualShell> start_shell(Config cfg) {
    auto shell = std::make_shared<VirtualShell>(cfg);
    if (!shell->start()) {
        SKIP("could not start PowerShell (is pwsh on PATH, or Windows PowerShell installed?)");
    }
    return shell;
}

bool contains(const std::string& haystack, const std::string& needle) {
    return haystack.find(needle) != std::string::npos;
}

} // namespace

TEST_CASE("start/isAlive/stop lifecycle") {
    auto shell = start_shell(test_config());
    CHECK(shell->isAlive());
    CHECK(shell->getProcessId() > 0);
    shell->stop();
    CHECK(!shell->isAlive());
}

TEST_CASE("start resolves an executable and reports the running edition") {
    auto shell = start_shell(test_config());
    CHECK(!shell->getResolvedPowerShellPath().empty());
    const std::string edition = shell->getPowerShellEdition();
    CHECK(edition == "core" || edition == "desktop");
    CHECK(!shell->getPowerShellVersion().empty());
    shell->stop();
}

#ifdef _WIN32
TEST_CASE("desktop edition hosts Windows PowerShell 5.1") {
    auto cfg = test_config();
    cfg.powershellEdition = "desktop";
    auto shell = std::make_shared<VirtualShell>(cfg);
    if (!shell->start()) {
        SKIP("Windows PowerShell 5.1 could not be started");
    }
    CHECK(contains(shell->getResolvedPowerShellPath(), "powershell"));
    CHECK_EQ(shell->getPowerShellEdition(), "desktop");
    CHECK(shell->getPowerShellVersion().rfind("5.", 0) == 0);

    // The command protocol (markers, $? status, multi-line blocks, UTF-8) is edition-independent.
    const std::string aoa = "\xC3\xA6\xC3\xB8\xC3\xA5";  // \u00e6\u00f8\u00e5
    auto res = shell->execute("foreach ($i in 1..2) {\n  \"rad-$i-" + aoa + "\"\n}");
    CHECK(res.success);
    CHECK(contains(res.out, "rad-1-" + aoa));
    CHECK(contains(res.out, "rad-2-" + aoa));
    auto failing = shell->execute("Get-Item 'C:/definitely/not/here.xyz'");
    CHECK(!failing.success);
    shell->stop();
}
#else
TEST_CASE("desktop edition cannot start off Windows") {
    auto cfg = test_config();
    cfg.powershellEdition = "desktop";
    VirtualShell shell(cfg);
    CHECK(!shell.start());
    CHECK(!shell.isAlive());
}
#endif

TEST_CASE("execute returns stdout, success and timing") {
    auto shell = start_shell(test_config());
    auto res = shell->execute("Write-Output 'hello'");
    CHECK(res.success);
    CHECK_EQ(res.exitCode, 0);
    CHECK(contains(res.out, "hello"));
    CHECK(res.executionTime >= 0.0);
    shell->stop();
}

TEST_CASE("execute round-trips UTF-8 output") {
    auto shell = start_shell(test_config());
    auto res = shell->execute("Write-Output '\xC3\xA6\xC3\xB8\xC3\xA5'"); // æøå
    CHECK(res.success);
    if (!contains(res.out, "\xC3\xA6\xC3\xB8\xC3\xA5")) {
        std::cout << "    out bytes:";
        for (unsigned char c : res.out) std::printf(" %02X", c);
        std::cout << "\n    err: " << res.err << "\n";
    }
    CHECK(contains(res.out, "\xC3\xA6\xC3\xB8\xC3\xA5"));
    shell->stop();
}

TEST_CASE("non-ASCII command text arrives intact (stdin decoded as UTF-8)") {
    auto shell = start_shell(test_config());
    // If stdin were decoded with the OEM code page, 'æøå' would arrive as six
    // mangled characters and .Length would report 6.
    auto res = shell->execute("'\xC3\xA6\xC3\xB8\xC3\xA5'.Length");
    CHECK(res.success);
    CHECK(contains(res.out, "3"));
    shell->stop();
}

TEST_CASE("session state persists between commands") {
    auto shell = start_shell(test_config());
    CHECK(shell->execute("$vs_cpp_state = 41").success);
    auto res = shell->execute("$vs_cpp_state + 1");
    CHECK(res.success);
    CHECK(contains(res.out, "42"));
    shell->stop();
}

TEST_CASE("failing command reports error output and failure") {
    auto shell = start_shell(test_config());
    auto res = shell->execute("Get-Item 'C:/definitely/not/a/real/path.xyz'");
    CHECK(!res.success);
    CHECK(!res.err.empty());
    shell->stop();
}

TEST_CASE("execute_batch preserves order") {
    auto shell = start_shell(test_config());
    auto results = shell->execute_batch({
        "Write-Output 'one'",
        "Write-Output 'two'",
        "Write-Output 'three'",
    });
    REQUIRE(results.size() == 3);
    CHECK(contains(results[0].out, "one"));
    CHECK(contains(results[1].out, "two"));
    CHECK(contains(results[2].out, "three"));
    for (const auto& r : results) CHECK(r.success);
    shell->stop();
}

TEST_CASE("executeAsync resolves future and invokes callback") {
    auto shell = start_shell(test_config());

    std::promise<std::string> seen;
    auto seen_future = seen.get_future();
    auto fut = shell->executeAsync(
        "Write-Output 'async'",
        [&seen](const VirtualShell::ExecutionResult& r) { seen.set_value(r.out); });

    REQUIRE(fut.wait_for(std::chrono::seconds(30)) == std::future_status::ready);
    auto res = fut.get();
    CHECK(res.success);
    CHECK(contains(res.out, "async"));

    REQUIRE(seen_future.wait_for(std::chrono::seconds(5)) == std::future_status::ready);
    CHECK(contains(seen_future.get(), "async"));
    shell->stop();
}

TEST_CASE("environment variables can be set and read back") {
    auto shell = start_shell(test_config());
    CHECK(shell->setEnvironmentVariable("VS_CPP_TEST_ENV", "hello-env"));
    CHECK_EQ(shell->getEnvironmentVariable("VS_CPP_TEST_ENV"), "hello-env");
    shell->stop();
}

TEST_CASE("getPowerShellVersion returns a version string") {
    auto shell = start_shell(test_config());
    auto version = shell->getPowerShellVersion();
    CHECK(!version.empty());
    CHECK(version[0] >= '0' && version[0] <= '9');
    shell->stop();
}

TEST_CASE("timeout without auto-restart yields exit code -1") {
    auto cfg = test_config();
    cfg.autoRestartOnTimeout = false;
    auto shell = start_shell(cfg);
    auto res = shell->execute("Start-Sleep -Seconds 10", 1.0);
    CHECK(!res.success);
    CHECK_EQ(res.exitCode, -1);
    shell->stop(true);
}

TEST_CASE("timeout with auto-restart recovers and accepts new work") {
    auto cfg = test_config();
    cfg.autoRestartOnTimeout = true;
    auto shell = start_shell(cfg);

    auto res = shell->execute("Start-Sleep -Seconds 10", 1.0);
    CHECK(!res.success);
    CHECK_EQ(res.exitCode, -1);

    bool recovered = false;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (std::chrono::steady_clock::now() < deadline) {
        if (!shell->isRestarting() && shell->isAlive()) {
            auto follow_up = shell->execute("40 + 2");
            if (follow_up.success && contains(follow_up.out, "42")) {
                recovered = true;
                break;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    CHECK(recovered);
    shell->stop(true);
}

// ---- Paths the Python proxy layer (ps_proxy.py) depends on ----

TEST_CASE("large output survives the pipe pump intact") {
    auto shell = start_shell(test_config());
    // 300 KB on a single line: crosses several read-chunk boundaries.
    auto res = shell->execute("'ab' * 150000");
    CHECK(res.success);
    std::string payload = res.out;
    while (!payload.empty() && (payload.back() == '\n' || payload.back() == '\r'))
        payload.pop_back();
    CHECK_EQ(payload.size(), static_cast<size_t>(300000));
    CHECK(payload.find_first_not_of("ab") == std::string::npos);

    // The next command must not see any residue of the previous payload.
    auto marker = shell->execute("Write-Output 'marker-after-large'");
    CHECK(marker.success);
    CHECK(contains(marker.out, "marker-after-large"));
    CHECK(!contains(marker.out, "ababab"));
    shell->stop();
}

TEST_CASE("multi-line script blocks execute as one command") {
    auto shell = start_shell(test_config());
    std::string script =
        "$__t_list = @()\n"
        "foreach ($i in 1..3) {\n"
        "  if ($i -gt 1) { $__t_list += ,($i * 10) }\n"
        "}\n"
        "[pscustomobject]@{ total = ($__t_list | Measure-Object -Sum).Sum } | ConvertTo-Json -Compress";
    auto res = shell->execute(script);
    CHECK(res.success);
    CHECK(contains(res.out, "\"total\":50"));

    // Variables defined in one execute persist into the next.
    auto next = shell->execute("$__t_list.Count");
    CHECK(next.success);
    CHECK(contains(next.out, "2"));
    shell->stop();
}

TEST_CASE("ps_quote round-trips interpolation-hostile strings") {
    auto shell = start_shell(test_config());
    const std::string hostile = "a'b `n $env:PATH \"quoted\" ; & | %";
    auto res = shell->execute(
        "Write-Output " + virtualshell::helpers::parsers::ps_quote(hostile));
    CHECK(res.success);
    CHECK(contains(res.out, hostile));
    shell->stop();
}

TEST_CASE("success reflects only the final statement of a packet") {
    // The proxy layer sends an assignment and reads $?-based success for it,
    // then follows up with a reader command. This contract requires that a
    // packet's success mirrors its LAST statement.
    auto shell = start_shell(test_config());

    auto res = shell->execute(
        "Get-Item 'C:/definitely/not/here.xyz' -ErrorAction SilentlyContinue\n"
        "Write-Output 'still-ok'");
    CHECK(res.success);   // final statement succeeded
    CHECK(contains(res.out, "still-ok"));

    auto fail = shell->execute("Write-Output 'x'\nGet-Item 'C:/definitely/not/here.xyz'");
    CHECK(!fail.success); // final statement failed
    shell->stop();
}

TEST_CASE("json emitted by ConvertTo-Json arrives unmangled") {
    auto shell = start_shell(test_config());
    auto res = shell->execute(
        "@{ s = 'x''y\"z'; i = 42; f = 1.5; b = $true; u = '\xC3\xA6\xC3\xB8\xC3\xA5' } | ConvertTo-Json -Compress");
    CHECK(res.success);
    CHECK(contains(res.out, "\"i\":42"));
    CHECK(contains(res.out, "\"b\":true"));
    CHECK(contains(res.out, "\xC3\xA6\xC3\xB8\xC3\xA5"));
    shell->stop();
}

TEST_CASE("updateConfig is rejected while running and applied when stopped") {
    auto shell = start_shell(test_config());
    auto cfg = shell->getConfig();
    cfg.timeoutSeconds = 7;
    CHECK(!shell->updateConfig(cfg));  // running: must refuse
    shell->stop();
    CHECK(shell->updateConfig(cfg));
    CHECK_EQ(shell->getDefaultTimeout(), 7);
}
