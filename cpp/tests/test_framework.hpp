// Minimal, dependency-free test harness for the virtualshell C++ core.
//
// Usage:
//   TEST_CASE("name") { CHECK(cond); CHECK_EQ(a, b); REQUIRE(cond); }
//
// REQUIRE aborts the current test case on failure; CHECK records the failure
// and continues. SKIP("reason") marks the case as skipped (e.g. when pwsh is
// not installed). The exit code of run_all() is non-zero iff any case failed.
#pragma once

#include <functional>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

namespace vstest {

struct TestCase {
    std::string name;
    std::function<void()> fn;
};

inline std::vector<TestCase>& registry() {
    static std::vector<TestCase> r;
    return r;
}

struct Registrar {
    Registrar(std::string name, std::function<void()> fn) {
        registry().push_back({std::move(name), std::move(fn)});
    }
};

struct require_error {};
struct skip_error { std::string reason; };

inline int& failures() { static int f = 0; return f; }

inline void report_failure(const char* file, int line, const std::string& msg) {
    ++failures();
    std::cout << "    FAILED " << file << ":" << line << ": " << msg << "\n";
}

template <class A, class B>
void check_eq(const char* file, int line, const A& a, const B& b, const char* expr) {
    if (!(a == b)) {
        std::ostringstream os;
        os << expr << "  (lhs=" << a << ", rhs=" << b << ")";
        report_failure(file, line, os.str());
    }
}

inline int run_all() {
    int failed_cases = 0;
    int skipped_cases = 0;
    for (auto& tc : registry()) {
        const int before = failures();
        std::cout << "[ RUN  ] " << tc.name << "\n";
        bool aborted = false;
        try {
            tc.fn();
        } catch (const skip_error& s) {
            ++skipped_cases;
            std::cout << "[ SKIP ] " << tc.name << " (" << s.reason << ")\n";
            continue;
        } catch (const require_error&) {
            aborted = true;
        } catch (const std::exception& e) {
            report_failure("(uncaught exception)", 0, e.what());
        } catch (...) {
            report_failure("(uncaught exception)", 0, "unknown exception type");
        }
        (void)aborted;
        if (failures() == before) {
            std::cout << "[  OK  ] " << tc.name << "\n";
        } else {
            ++failed_cases;
            std::cout << "[ FAIL ] " << tc.name << "\n";
        }
    }
    std::cout << "\n" << registry().size() << " test case(s): "
              << (registry().size() - failed_cases - skipped_cases) << " passed, "
              << failed_cases << " failed, " << skipped_cases << " skipped\n";
    return failed_cases == 0 ? 0 : 1;
}

} // namespace vstest

#define VS_CONCAT2(a, b) a##b
#define VS_CONCAT(a, b) VS_CONCAT2(a, b)

#define TEST_CASE(name)                                                            \
    static void VS_CONCAT(vs_test_fn_, __LINE__)();                                \
    static ::vstest::Registrar VS_CONCAT(vs_test_reg_, __LINE__)(                  \
        name, &VS_CONCAT(vs_test_fn_, __LINE__));                                  \
    static void VS_CONCAT(vs_test_fn_, __LINE__)()

#define CHECK(cond)                                                                \
    do {                                                                           \
        if (!(cond)) ::vstest::report_failure(__FILE__, __LINE__,                  \
                                              "CHECK failed: " #cond);             \
    } while (0)

#define CHECK_EQ(a, b) ::vstest::check_eq(__FILE__, __LINE__, (a), (b), #a " == " #b)

#define REQUIRE(cond)                                                              \
    do {                                                                           \
        if (!(cond)) {                                                             \
            ::vstest::report_failure(__FILE__, __LINE__,                           \
                                     "REQUIRE failed: " #cond);                    \
            throw ::vstest::require_error{};                                       \
        }                                                                          \
    } while (0)

#define SKIP(reason) throw ::vstest::skip_error{reason}
