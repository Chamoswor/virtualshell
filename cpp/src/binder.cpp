#include <Python.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/functional.h>
#include <mutex>
#include <vector>
#include <thread>
#include <memory>
#include "../include/virtual_shell.hpp"

namespace py = pybind11;

#if defined(_WIN32)
  #include <windows.h>
#else
  #include <dlfcn.h>
#endif
#include <atomic>
#include <mutex>



// -------------------- Interpreter state check --------------------
static inline bool interpreter_down() noexcept {
  // If the interpreter is not initialized, it's definitely down
  if (!Py_IsInitialized()) return true;

  // Try to find Py_IsFinalizing dynamically and cache the result
#if defined(_WIN32)
      // Windows: keep __cdecl calling convention
    using Fn = int(__cdecl*)(void);
#else
// POSIX: normal C calling convention, and we need dlsym()

    using Fn = int(*)(void);
#endif
  static std::atomic<Fn> fn_cached{nullptr};
  Fn fn = fn_cached.load(std::memory_order_acquire);
  if (!fn) {
    // one-time resolve
    static std::once_flag once;
    std::call_once(once, [] {
      Fn found = nullptr;
    #if defined(_WIN32)
      // Try both pythonXY.dll and python3.dll
      wchar_t dllname[32];
      if (swprintf_s(dllname, L"python%u%u.dll", PY_MAJOR_VERSION, PY_MINOR_VERSION) > 0) {
        if (HMODULE h = GetModuleHandleW(dllname)) {
          found = reinterpret_cast<Fn>(GetProcAddress(h, "Py_IsFinalizing"));
        }
      }
      if (!found) {
        if (HMODULE h2 = GetModuleHandleW(L"python3.dll")) {
          found = reinterpret_cast<Fn>(GetProcAddress(h2, "Py_IsFinalizing"));
        }
      }
    #else
      void* sym = dlsym(RTLD_DEFAULT, "Py_IsFinalizing");
      if (sym) found = reinterpret_cast<Fn>(sym);
    #endif
      fn_cached.store(found, std::memory_order_release);
    });
    fn = fn_cached.load(std::memory_order_acquire);
  }

  // If the symbol is not found in this Python build → assume "not finalizing"
  return fn ? (fn() != 0) : false;
}

// -------------------- Async helpers (thread mgmt + GIL safety) --------------------
namespace {
std::mutex g_thr_mx;
std::vector<std::thread> g_thr;

void register_atexit_joiner() {
    static bool registered = false;
    if (registered) return;
    registered = true;
    // Join all helper threads before Python finalization completes
    // This ensures proper cleanup of background threads during interpreter shutdown
    py::module_::import("atexit").attr("register")(py::cpp_function([](){
        std::lock_guard<std::mutex> lk(g_thr_mx);
        for (auto &t : g_thr) {
            if (t.joinable()) t.join();
        }
        g_thr.clear();
    }));
}

py::object make_py_future_from_std_future(std::future<VirtualShell::ExecutionResult> fut,
                                          py::object py_callback /* may be None */) {
    py::object futures = py::module_::import("concurrent.futures");
    py::object py_future = futures.attr("Future")();

    auto shared_future = std::make_shared<std::future<VirtualShell::ExecutionResult>>(std::move(fut));

    register_atexit_joiner();
    {
        std::lock_guard<std::mutex> lk(g_thr_mx);
        // Capture MUTABLE copies so we can null them in GIL zone at the end
        // Dev note: This prevents DECREF without GIL during thread destruction
        g_thr.emplace_back(
            [shared_future,
             py_future = py::object(py_future),
             py_callback = py::object(py_callback)
            ]() mutable {
                VirtualShell::ExecutionResult r{};
                try {
                    r = shared_future->get();
                } catch (const std::exception& e) {
                    if (!interpreter_down()) {
                        py::gil_scoped_acquire ag;
                        try {
                            py_future.attr("set_exception")(py::cast(std::runtime_error(e.what())));
                        } catch (...) {}
                    }
                    if (!interpreter_down()) { py::gil_scoped_acquire ag; py_future = py::none(); py_callback = py::none(); }
                    return;
                } catch (...) {
                    if (!interpreter_down()) {
                        py::gil_scoped_acquire ag;
                        try {
                            py_future.attr("set_exception")(py::cast(std::runtime_error("unknown async error")));
                        } catch (...) {}
                    }
                    if (!interpreter_down()) { py::gil_scoped_acquire ag; py_future = py::none(); py_callback = py::none(); }
                    return;
                }

                if (!interpreter_down()) {
                    py::gil_scoped_acquire ag;
                    try {
                        py::object py_res = py::cast(r);
                        py_future.attr("set_result")(py_res);
                        if (!py_callback.is_none()) {
                            try { py_callback(py_res); }
                            catch (py::error_already_set& e) { e.discard_as_unraisable(__func__); }
                            catch (...) {}
                        }
                    } catch (const py::error_already_set& e) {
                        try { py_future.attr("set_exception")(e.type()); } catch (...) {}
                    } catch (...) {
                        try {
                            py_future.attr("set_exception")(py::cast(std::runtime_error("callback/future set_result failed")));
                        } catch (...) {}
                    }
                }
                // GIL-held destruction of py-objects (avoid DECREF without GIL)
                // Dev note: Critical for thread safety - all Python object cleanup must happen with GIL acquired
                if (!interpreter_down()) { py::gil_scoped_acquire ag; py_future = py::none(); py_callback = py::none(); }
            }
        );
    }
    return py_future;
}

py::object make_py_future_from_std_future_vec(
    std::future<std::vector<VirtualShell::ExecutionResult>> fut) {

    py::object futures = py::module_::import("concurrent.futures");
    py::object py_future = futures.attr("Future")();

    auto shared_future = std::make_shared<std::future<std::vector<VirtualShell::ExecutionResult>>>(std::move(fut));

    register_atexit_joiner();
    {
        std::lock_guard<std::mutex> lk(g_thr_mx);
        g_thr.emplace_back(
            [shared_future,
             py_future = py::object(py_future)
            ]() mutable {
                std::vector<VirtualShell::ExecutionResult> vec;
                try {
                    vec = shared_future->get();
                } catch (const std::exception& e) {
                    if (!interpreter_down()) {
                        py::gil_scoped_acquire ag;
                        try {
                            py_future.attr("set_exception")(py::cast(std::runtime_error(e.what())));
                        } catch (...) {}
                    }
                    if (!interpreter_down()) { py::gil_scoped_acquire ag; py_future = py::none(); }
                    return;
                } catch (...) {
                    if (!interpreter_down()) {
                        py::gil_scoped_acquire ag;
                        try {
                            py_future.attr("set_exception")(py::cast(std::runtime_error("unknown async batch error")));
                        } catch (...) {}
                    }
                    if (!interpreter_down()) { py::gil_scoped_acquire ag; py_future = py::none(); }
                    return;
                }

                if (!interpreter_down()) {
                    py::gil_scoped_acquire ag;
                    try {
                        py::list lst;
                        for (auto &r : vec) lst.append(py::cast(r));
                        py_future.attr("set_result")(lst);
                    } catch (const py::error_already_set& e) {
                        try { py_future.attr("set_exception")(e.type()); } catch (...) {}
                    } catch (...) {
                        try {
                            py_future.attr("set_exception")(py::cast(std::runtime_error("setting batch result failed")));
                        } catch (...) {}
                    }
                }
                if (!interpreter_down()) { py::gil_scoped_acquire ag; py_future = py::none(); }
            }
        );
    }
    return py_future;
}
} // namespace

// -------------------- Module --------------------
PYBIND11_MODULE(_core, m) {

    m.doc() = "High-performance Python facade over a C++ PowerShell runner";

    // ExecutionResult
    py::class_<VirtualShell::ExecutionResult>(m, "ExecutionResult")
        .def_readwrite("out", &VirtualShell::ExecutionResult::out)
        .def_readwrite("err", &VirtualShell::ExecutionResult::err)
        .def_readwrite("exit_code", &VirtualShell::ExecutionResult::exitCode)
        .def_readwrite("success", &VirtualShell::ExecutionResult::success)
        .def_readwrite("execution_time", &VirtualShell::ExecutionResult::executionTime)
        .def("__repr__", [](const VirtualShell::ExecutionResult& r) {
            return "<ExecutionResult success=" + std::to_string(r.success) +
                   " exit_code=" + std::to_string(r.exitCode) +
                   " execution_time=" + std::to_string(r.executionTime) + "s>";
        });

    // BatchProgress
    py::class_<VirtualShell::BatchProgress>(m, "BatchProgress")
        .def_readwrite("current_command", &VirtualShell::BatchProgress::currentCommand)
        .def_readwrite("total_commands",  &VirtualShell::BatchProgress::totalCommands)
        .def_readwrite("last_result",     &VirtualShell::BatchProgress::lastResult)
        .def_readwrite("is_complete",     &VirtualShell::BatchProgress::isComplete)
        .def_readwrite("all_results",     &VirtualShell::BatchProgress::allResults)
        // camelCase alias
        .def_readwrite("currentCommand",  &VirtualShell::BatchProgress::currentCommand)
        .def_readwrite("totalCommands",   &VirtualShell::BatchProgress::totalCommands)
        .def_readwrite("lastResult",      &VirtualShell::BatchProgress::lastResult)
        .def_readwrite("isComplete",      &VirtualShell::BatchProgress::isComplete)
        .def_readwrite("allResults",      &VirtualShell::BatchProgress::allResults)
        .def("__repr__", [](const VirtualShell::BatchProgress& p) {
            return "<BatchProgress current_command=" + std::to_string(p.currentCommand) +
                   " total_commands=" + std::to_string(p.totalCommands) +
                   " is_complete=" + std::to_string(p.isComplete) +
                   " last_result_success=" + std::to_string(p.lastResult.success) +
                   " last_result_exit_code=" + std::to_string(p.lastResult.exitCode) +
                   " last_result_execution_time=" + std::to_string(p.lastResult.executionTime) + "s>";
        });

    // Config
    py::class_<VirtualShell::Config>(m, "Config")
        .def(py::init<>())
        .def_readwrite("powershell_path",    &VirtualShell::Config::powershellPath)
        .def_readwrite("working_directory",  &VirtualShell::Config::workingDirectory)
        .def_readwrite("capture_output",     &VirtualShell::Config::captureOutput)
        .def_readwrite("capture_error",      &VirtualShell::Config::captureError)
        .def_readwrite("timeout_seconds",    &VirtualShell::Config::timeoutSeconds)
        .def_readwrite("environment",        &VirtualShell::Config::environment)
        .def_readwrite("initial_commands",   &VirtualShell::Config::initialCommands)
        .def("__repr__", [](const VirtualShell::Config& c) {
            return "<Config powershell_path='" + c.powershellPath +
                   "' timeout=" + std::to_string(c.timeoutSeconds) + "s>";
        });

    // VirtualShell
    py::class_<VirtualShell, std::shared_ptr<VirtualShell>>(m, "VirtualShell")
        .def(py::init<>())
        .def(py::init<const VirtualShell::Config&>())

        // Process control
        .def("start",    &VirtualShell::start, "Start the PowerShell process")
        .def("stop",     &VirtualShell::stop,  py::arg("force") = false, "Stop the PowerShell process")
        .def("is_alive", &VirtualShell::isAlive, "Check if the PowerShell process is running")

        // Sync commands
        .def("execute", &VirtualShell::execute,
             py::arg("command"), py::arg("timeout_seconds") = 0.0,
             "Execute a PowerShell command synchronously")
        .def("execute_batch", &VirtualShell::execute_batch,
             py::arg("commands"), py::arg("timeout_seconds") = 0.0,
             "Execute a batch of PowerShell commands synchronously")
        .def("execute_script", &VirtualShell::execute_script,
             py::arg("script_path"),
             py::arg("args") = std::vector<std::string>{},
             py::arg("timeout_seconds") = 0.0,
             py::arg("dot_source") = false,
             py::arg("raise_on_error") = false,
             "Execute a PowerShell script file synchronously")
        .def("execute_script_kv", &VirtualShell::execute_script_kv,
             py::arg("script_path"),
             py::arg("named_args"),
             py::arg("timeout_seconds") = 0.0,
             py::arg("dot_source") = false,
             py::arg("raise_on_error") = false,
             "Execute script with named parameters via hashtable splatting")

        // Async: single
        .def("execute_async",
             [](std::shared_ptr<VirtualShell> self,
                std::string command,
                py::object callback /* = None */) {
                 auto fut = self->executeAsync(std::move(command), /*cb*/ nullptr);
                 return make_py_future_from_std_future(std::move(fut), std::move(callback));
             },
             py::arg("command"),
             py::arg("callback") = py::none(),
             "Execute a PowerShell command asynchronously and return a Python Future")

        // Async: batch (snake_case) + camelCase alias
        .def("execute_async_batch",
             [](std::shared_ptr<VirtualShell> self,
                std::vector<std::string> commands,
                py::object progress_cb /* = None */,
                bool stop_on_first_error,
                double per_command_timeout_seconds) {

                 std::function<void(const VirtualShell::BatchProgress&)> cpp_cb;
                 if (!progress_cb.is_none()) {
                     // GIL-safe lifecycle for callback object
                     // Dev note: Custom deleter ensures proper Python object cleanup during shutdown
                     auto pcb = std::shared_ptr<py::object>(
                         new py::object(progress_cb),
                         [](py::object* p){
                             if (!p) return;
                             //if (!Py_IsFinalizing()) { py::gil_scoped_acquire ag; *p = py::none(); }
                             if (interpreter_down()) { p->release(); delete p; return; }
                             delete p;
                         }
                     );
                     cpp_cb = [pcb](const VirtualShell::BatchProgress& p) {
                         if (interpreter_down()) return;
                         py::gil_scoped_acquire ag;
                         try { py::object py_p = py::cast(p); (*pcb)(py_p); }
                         catch (py::error_already_set& e) { e.discard_as_unraisable(__func__); }
                         catch (...) {}
                     };
                 }

                 auto fut = self->executeAsync_batch(
                     std::move(commands),
                     cpp_cb,
                     stop_on_first_error,
                     per_command_timeout_seconds
                 );
                 return make_py_future_from_std_future_vec(std::move(fut));
             },
             py::arg("commands"),
             py::arg("progress_callback") = py::none(),
             py::arg("stop_on_first_error") = true,
             py::arg("per_command_timeout_seconds") = 0.0,
             "Execute a batch asynchronously (returns Future[List[ExecutionResult]])")

        .def("executeAsync_batch",
             [](std::shared_ptr<VirtualShell> self,
                std::vector<std::string> commands,
                py::object progress_cb /* = None */,
                bool stop_on_first_error,
                double per_command_timeout_seconds) {

                 std::function<void(const VirtualShell::BatchProgress&)> cpp_cb;
                 if (!progress_cb.is_none()) {
                     auto pcb = std::shared_ptr<py::object>(
                         new py::object(progress_cb),
                         [](py::object* p){
                             if (!p) return;
                             //if (!Py_IsFinalizing()) { py::gil_scoped_acquire ag; *p = py::none(); }
                             if (interpreter_down()) { p->release(); delete p; return; }
                             delete p;
                         }
                     );
                     cpp_cb = [pcb](const VirtualShell::BatchProgress& p) {
                         if (interpreter_down()) return;
                         py::gil_scoped_acquire ag;
                         try { py::object py_p = py::cast(p); (*pcb)(py_p); }
                         catch (py::error_already_set& e) { e.discard_as_unraisable(__func__); }
                         catch (...) {}
                     };
                 }

                 auto fut = self->executeAsync_batch(
                     std::move(commands),
                     cpp_cb,
                     stop_on_first_error,
                     per_command_timeout_seconds
                 );
                 return make_py_future_from_std_future_vec(std::move(fut));
             },
             py::arg("commands"),
             py::arg("progress_callback") = py::none(),
             py::arg("stop_on_first_error") = true,
             py::arg("per_command_timeout_seconds") = 0.0)

        // Async: script
        .def("execute_async_script",
             [](std::shared_ptr<VirtualShell> self,
                std::string script_path,
                std::vector<std::string> args,
                py::object callback /* = None */,
                double timeout_seconds,
                bool dot_source,
                bool /*raise_on_error*/) {
                 auto fut = self->executeAsync_script(
                     std::move(script_path),
                     std::move(args),
                     timeout_seconds,
                     dot_source,
                     /*raiseOnError*/ false,
                     /*cb*/ nullptr
                 );
                 return make_py_future_from_std_future(std::move(fut), std::move(callback));
             },
             py::arg("script_path"),
             py::arg("args") = std::vector<std::string>{},
             py::arg("callback") = py::none(),
             py::arg("timeout_seconds") = 0.0,
             py::arg("dot_source") = false,
             py::arg("raise_on_error") = false)

        .def("executeAsync_script",
             [](std::shared_ptr<VirtualShell> self,
                std::string script_path,
                std::vector<std::string> args,
                double timeout_seconds,
                bool dot_source,
                bool /*raise_on_error*/) {
                 auto fut = self->executeAsync_script(
                     std::move(script_path),
                     std::move(args),
                     timeout_seconds,
                     dot_source,
                     /*raiseOnError*/ false,
                     /*cb*/ nullptr
                 );
                 return make_py_future_from_std_future(std::move(fut), py::none());
             },
             py::arg("script_path"),
             py::arg("args") = std::vector<std::string>{},
             py::arg("timeout_seconds") = 0.0,
             py::arg("dot_source") = false,
             py::arg("raise_on_error") = false)

        // Async: script_kv
        .def("execute_async_script_kv",
             [](std::shared_ptr<VirtualShell> self,
                std::string script_path,
                std::map<std::string,std::string> named_args,
                double timeout_seconds,
                bool dot_source,
                bool /*raise_on_error*/) {
                 auto fut = self->executeAsync_script_kv(
                     std::move(script_path),
                     std::move(named_args),
                     timeout_seconds,
                     dot_source,
                     /*raiseOnError*/ false
                 );
                 return make_py_future_from_std_future(std::move(fut), py::none());
             },
             py::arg("script_path"),
             py::arg("named_args"),
             py::arg("timeout_seconds") = 0.0,
             py::arg("dot_source") = false,
             py::arg("raise_on_error") = false)

        .def("executeAsync_script_kv",
             [](std::shared_ptr<VirtualShell> self,
                std::string script_path,
                std::map<std::string,std::string> named_args,
                double timeout_seconds,
                bool dot_source,
                bool /*raise_on_error*/) {
                 auto fut = self->executeAsync_script_kv(
                     std::move(script_path),
                     std::move(named_args),
                     timeout_seconds,
                     dot_source,
                     /*raiseOnError*/ false
                 );
                 return make_py_future_from_std_future(std::move(fut), py::none());
             },
             py::arg("script_path"),
             py::arg("named_args"),
             py::arg("timeout_seconds") = 0.0,
             py::arg("dot_source") = false,
             py::arg("raise_on_error") = false)

        // Direct I/O / env / modules
        .def("send_input",  &VirtualShell::sendInput,  py::arg("input"))
        .def("read_output", &VirtualShell::readOutput, py::arg("blocking") = false)
        .def("read_error",  &VirtualShell::readError,  py::arg("blocking") = false)

        .def("set_working_directory", &VirtualShell::setWorkingDirectory, py::arg("directory"))
        .def("get_working_directory", &VirtualShell::getWorkingDirectory)
        .def("set_environment_variable", &VirtualShell::setEnvironmentVariable, py::arg("name"), py::arg("value"))
        .def("get_environment_variable", &VirtualShell::getEnvironmentVariable, py::arg("name"))

        .def("is_module_available", &VirtualShell::isModuleAvailable, py::arg("module_name"))
        .def("import_module",      &VirtualShell::importModule,      py::arg("module_name"))
        .def("get_powershell_version", &VirtualShell::getPowerShellVersion)
        .def("get_available_modules",  &VirtualShell::getAvailableModules)

        .def("get_config",   &VirtualShell::getConfig, py::return_value_policy::reference_internal)
        .def("update_config",&VirtualShell::updateConfig, py::arg("config"))

        .def("__repr__", [](const VirtualShell& shell) {
            return std::string("<VirtualShell running=") + (shell.isAlive() ? "1" : "0") + ">";
        })
        .def("__enter__", [](VirtualShell& shell) -> VirtualShell& { shell.start(); return shell; })
        .def("__exit__",  [](VirtualShell& shell, py::object, py::object, py::object) { shell.stop(); });

    // Utility
    m.def("create_config", []() { return VirtualShell::Config{}; }, "Create a new Config object with default values");
    m.def("create_shell",  [](const VirtualShell::Config& config) {
        return std::make_unique<VirtualShell>(config);
    }, "Create a new VirtualShell instance", py::arg("config"));

    // Metadata
    m.attr("__version__") = "1.0.0";
    m.attr("__author__")  = "Kim-Andre Myrvold";
}


