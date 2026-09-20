#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/functional.h>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>
#include "../include/virtual_shell.hpp"
#include "../include/py_bridge.hpp"

namespace py = pybind11;

namespace {

// The async bindings are plain free functions bound by pointer rather than
// lambdas: MSVC 14.51 (VS 2026) hits an internal compiler error (C1001) when
// instantiating pybind11::cpp_function over complex lambdas with
// default-argument packs. Function pointers take a simpler instantiation
// path, and keeping the callback machinery in ordinary function bodies keeps
// it out of the template tree entirely.

py::object execute_async_py(std::shared_ptr<VirtualShell> self,
                            std::string command,
                            py::object callback /* may be None */,
                            double timeout_seconds) {
    auto fut = self->executeAsync(std::move(command), /*cb*/ nullptr, timeout_seconds);
    return virtualshell::pybridge::make_py_future_from_std_future(std::move(fut), std::move(callback));
}

std::function<void(const VirtualShell::BatchProgress&)>
make_progress_dispatcher(py::object progress_cb /* may be None */) {
    if (progress_cb.is_none()) {
        return {};
    }
    // GIL-safe lifecycle for callback object
    // Dev note: Custom deleter ensures proper Python object cleanup during shutdown
    auto pcb = std::shared_ptr<py::object>(
        new py::object(std::move(progress_cb)),
        [](py::object* p) {
            if (!p) return;
            if (virtualshell::pybridge::interpreter_down()) { p->release(); delete p; return; }
            delete p;
        });
    return [pcb](const VirtualShell::BatchProgress& p) {
        if (virtualshell::pybridge::interpreter_down()) return;
        virtualshell::pybridge::PyDispatcher::inst().post(
            [pcb, p]() mutable {
                try {
                    py::object py_p = py::cast(p);
                    (*pcb)(py_p);
                } catch (py::error_already_set& e) {
                    e.discard_as_unraisable("progress_callback");
                } catch (...) {
                    // swallow
                }
            });
    };
}

py::object execute_async_batch_py(std::shared_ptr<VirtualShell> self,
                                  std::vector<std::string> commands,
                                  py::object progress_cb /* may be None */,
                                  bool stop_on_first_error,
                                  double per_command_timeout_seconds) {
    auto fut = self->executeAsync_batch(
        std::move(commands),
        make_progress_dispatcher(std::move(progress_cb)),
        stop_on_first_error,
        per_command_timeout_seconds);
    return virtualshell::pybridge::make_py_future_from_std_future(std::move(fut), py::none());
}

py::object execute_async_script_py(std::shared_ptr<VirtualShell> self,
                                   std::string script_path,
                                   std::vector<std::string> args,
                                   py::object callback /* may be None */,
                                   double timeout_seconds,
                                   bool dot_source,
                                   bool /*raise_on_error*/) {
    auto fut = self->executeAsync_script(
        std::move(script_path),
        std::move(args),
        timeout_seconds,
        dot_source,
        /*raiseOnError*/ false,
        /*cb*/ nullptr);
    return virtualshell::pybridge::make_py_future_from_std_future(std::move(fut), std::move(callback));
}

py::object execute_async_script_kv_py(std::shared_ptr<VirtualShell> self,
                                      std::string script_path,
                                      std::map<std::string, std::string> named_args,
                                      double timeout_seconds,
                                      bool dot_source,
                                      bool /*raise_on_error*/) {
    auto fut = self->executeAsync_script_kv(
        std::move(script_path),
        std::move(named_args),
        timeout_seconds,
        dot_source,
        /*raiseOnError*/ false);
    return virtualshell::pybridge::make_py_future_from_std_future(std::move(fut), py::none());
}

} // namespace

// -------------------- Module --------------------
PYBIND11_MODULE(_core, m) {
    virtualshell::pybridge::install_atexit_guard();
    (void)virtualshell::pybridge::PyDispatcher::inst();
    m.doc() = "Internal module for VirtualShell bindings";
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
        .def_readwrite("powershell_path",      &VirtualShell::Config::powershellPath)
        .def_readwrite("working_directory",    &VirtualShell::Config::workingDirectory)
        .def_readwrite("capture_output",       &VirtualShell::Config::captureOutput)
        .def_readwrite("capture_error",        &VirtualShell::Config::captureError)
        .def_readwrite("auto_restart_on_timeout", &VirtualShell::Config::autoRestartOnTimeout)
        .def_readwrite("timeout_seconds",      &VirtualShell::Config::timeoutSeconds)
        .def_readwrite("environment",          &VirtualShell::Config::environment)
        .def_readwrite("initial_commands",     &VirtualShell::Config::initialCommands)
        .def_readwrite("restore_script_path",  &VirtualShell::Config::restoreScriptPath)
        .def_readwrite("session_snapshot_path", &VirtualShell::Config::sessionSnapshotPath)
        .def_readwrite("stdin_buffer_size",    &VirtualShell::Config::stdin_buffer_size)
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
        .def("execute_async", &execute_async_py,
             py::arg("command"),
             py::arg("callback") = py::none(),
             py::arg("timeout_seconds") = 0.0,
             "Execute a PowerShell command asynchronously and return a Python Future")

        // Async: batch
        .def("execute_async_batch", &execute_async_batch_py,
             py::arg("commands"),
             py::arg("progress_callback") = py::none(),
             py::arg("stop_on_first_error") = true,
             py::arg("per_command_timeout_seconds") = 0.0,
             "Execute a batch asynchronously (returns Future[List[ExecutionResult]])")

        // Async: script
        .def("execute_async_script", &execute_async_script_py,
             py::arg("script_path"),
             py::arg("args") = std::vector<std::string>{},
             py::arg("callback") = py::none(),
             py::arg("timeout_seconds") = 0.0,
             py::arg("dot_source") = false,
             py::arg("raise_on_error") = false)

        // Async: script_kv
        .def("execute_async_script_kv", &execute_async_script_kv_py,
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

        .def("get_process_id", &VirtualShell::getProcessId)

        .def("is_restarting", &VirtualShell::isRestarting)
        .def("get_shared_ptr", &VirtualShell::getSharedPtr)

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
    m.attr("__version__") = "1.2.1";
    m.attr("__author__")  = "Kim-Andre Myrvold";
}


