// Integration tests: VirtualShell against a real PowerShell (pwsh) process.
// Each case skips itself if pwsh cannot be started on this machine.
#include "test_framework.hpp"

#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>

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
        SKIP("could not start pwsh (is PowerShell 7 on PATH?)");
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

TEST_CASE("updateConfig is rejected while running and applied when stopped") {
    auto shell = start_shell(test_config());
    auto cfg = shell->getConfig();
    cfg.timeoutSeconds = 7;
    CHECK(!shell->updateConfig(cfg));  // running: must refuse
    shell->stop();
    CHECK(shell->updateConfig(cfg));
    CHECK_EQ(shell->getDefaultTimeout(), 7);
}
