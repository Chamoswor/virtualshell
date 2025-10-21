#include "../include/py_proxy.hpp"
#include "../include/helpers.hpp"
#include <pybind11/stl.h>
#include <pybind11/functional.h>
#include <pybind11/pytypes.h>
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <regex>
#include <format>
#include "dev_debug.hpp"


namespace py = pybind11;

namespace virtualshell::pybridge {

namespace {
constexpr long kPropertyFlags[] = {1, 2, 4, 16, 32, 512};
constexpr long kMethodFlags[]   = {64, 128, 256};

bool matches_flag(long value, const long* begin, const long* end) {
    for (auto it = begin; it != end; ++it) {
        if (*it == value) return true;
    }
    return false;
}

py::object dump_members(VirtualShell& shell, std::string objRef, int depth) {
    auto ref = std::move(objRef);
    auto result = shell.execute("$" + ref + " | Get-Member | ConvertTo-Json -Depth " + std::to_string(depth) + " -Compress");

    if (!result.success) {
        std::cerr << "PowerShell failed: " << result.err << '\n';
        return py::none();
    }
    if (result.out.empty()) {
        return py::none();
    }

    virtualshell::helpers::parsers::trim_inplace(result.out);
    try {
        return py::module_::import("json").attr("loads")(py::str(result.out));
    } catch (const py::error_already_set& e) {
        std::cerr << "Failed to parse JSON from PowerShell output: " << e.what() << '\n';
        return py::none();
    }
}

py::object coerce_scalar(std::string value) {
    virtualshell::helpers::parsers::trim_inplace(value);
    if (value.empty()) return py::none();
    if (value == "True" || value == "$true")  return py::bool_(true);
    if (value == "False" || value == "$false") return py::bool_(false);

    char* end = nullptr;
    long long asInt = std::strtoll(value.c_str(), &end, 10);
    if (end != value.c_str() && *end == '\0') return py::int_(asInt);

    char* endd = nullptr;
    double asDouble = std::strtod(value.c_str(), &endd);
    if (endd != value.c_str() && *endd == '\0') return py::float_(asDouble);

    return py::str(value);
}

} // namespace

PsProxy::PsProxy(VirtualShell& shell,
                 std::string typeName,
                 std::string objectRef,
                 int depth)
    : shell_(shell),
      typeName_(std::move(typeName)),
      objRef_(std::move(objectRef)),
      dynamic_(py::dict()),
      methodCache_(py::dict()) {
    if (!objRef_.empty() && objRef_[0] == '$') {
        objRef_ = objRef_.substr(1);
    } 
    ingest_members(depth);
}

py::object PsProxy::getattr(const std::string& name) {
    py::str key(name);

    if (name == "__dict__") {
        return dynamic_;
    }
    if (name == "__members__") {
        return schema();
    }
    if (name == "__type_name__") {
        return py::str(typeName_);
    }

    if (methodCache_.contains(key)) {
        return methodCache_[key];
    }

    if (auto mit = methods_.find(name); mit != methods_.end()) {
        auto callable = bind_method(name, mit->second);
        methodCache_[key] = callable;
        return callable;
    }

    if (auto pit = properties_.find(name); pit != properties_.end()) {
        return read_property(name);
    }

    if (dynamic_.contains(key)) {
        return dynamic_[key];
    }

    throw py::attribute_error(typeName_ + " proxy has no attribute '" + name + "'");
}

void PsProxy::setattr(const std::string& name, py::object value) {
    if (name == "__dict__") {
        if (!py::isinstance<py::dict>(value)) {
            throw py::type_error("__dict__ must be a mapping");
        }
        dynamic_ = value.cast<py::dict>();
        return;
    }

    if (auto mit = methods_.find(name); mit != methods_.end()) {
        throw py::attribute_error("Cannot overwrite proxied method '" + name + "'");
    }

    if (auto pit = properties_.find(name); pit != properties_.end()) {
        if (!pit->second.writable) {
            throw py::attribute_error("Property '" + name + "' is read-only");
        }
        write_property(name, pit->second, value);
        return;
    }

    dynamic_[py::str(name)] = std::move(value);
}

py::list PsProxy::dir() const {
    py::set seen;
    py::list out;

    auto push = [&](const std::string& value) {
        py::str key(value);
        if (!seen.contains(key)) {
            seen.add(key);
            out.append(key);
        }
    };

    push("__members__");
    push("__type_name__");
    for (const auto& kv : methods_)    push(kv.first);
    for (const auto& kv : properties_) push(kv.first);

    auto extras = dynamic_.attr("keys")();
    for (auto item : extras) {
        push(py::cast<std::string>(item));
    }

    return out;
}

py::dict PsProxy::schema() const {
    py::dict out;
    py::list methods;
    py::list props;

    for (const auto& kv : methods_) {
        py::dict entry;
        entry["Name"] = kv.first;
        entry["Awaitable"] = kv.second.awaitable;
        methods.append(entry);
    }
    for (const auto& kv : properties_) {
        py::dict entry;
        entry["Name"] = kv.first;
        entry["Writable"] = kv.second.writable;
        props.append(entry);
    }

    out["Methods"] = methods;
    out["Properties"] = props;
    return out;
}

void PsProxy::ingest_members(int depth) {
    auto consume_entry = [&](py::dict entry) {
        py::object get = entry.attr("get");
        py::object nameObj = get("Name", py::none());
        if (nameObj.is_none()) return;
        const std::string name = py::cast<std::string>(nameObj);

        py::object memberTypeObj = get("MemberType", py::none());
        bool isMethod = false;
        bool isProperty = false;

        if (py::isinstance<py::int_>(memberTypeObj)) {
            long flag = py::cast<long>(memberTypeObj);
            if (matches_flag(flag, std::begin(kMethodFlags), std::end(kMethodFlags))) {
                isMethod = true;
            } else if (matches_flag(flag, std::begin(kPropertyFlags), std::end(kPropertyFlags))) {
                isProperty = true;
            }
        } else if (py::isinstance<py::str>(memberTypeObj)) {
            const std::string text = py::cast<std::string>(memberTypeObj);
            if (text.find("Method") != std::string::npos) {
                isMethod = true;
            } else if (text.find("Property") != std::string::npos ||
                       text == "NoteProperty" || text == "AliasProperty") {
                isProperty = true;
            }
        }

        if (!isMethod && !isProperty) {
            py::object def = get("Definition", py::none());
            if (py::isinstance<py::str>(def)) {
                const std::string definition = py::cast<std::string>(def);
                if (definition.find("(") != std::string::npos &&
                    definition.find(")") != std::string::npos) {
                    isMethod = true;
                }
            }
        }

        if (isMethod) {
            methods_.emplace(name, decode_method(std::move(entry)));
        } else {
            properties_.emplace(name, decode_property(std::move(entry)));
        }
    };

    py::object members = dump_members(shell_, objRef_, depth);

    if (members.is_none()) return;

    if (py::isinstance<py::dict>(members)) {
        py::dict d = members.cast<py::dict>();
        bool specialized = false;
        if (d.contains("Methods")) {
            for (auto item : py::list(d["Methods"])) {
                if (py::isinstance<py::dict>(item)) {
                    consume_entry(item.cast<py::dict>());
                }
            }
            specialized = true;
        }
        if (d.contains("Properties")) {
            for (auto item : py::list(d["Properties"])) {
                if (py::isinstance<py::dict>(item)) {
                    consume_entry(item.cast<py::dict>());
                }
            }
            specialized = true;
        }
        if (specialized) return;

        for (auto item : d) {
            if (py::isinstance<py::dict>(item.second)) {
                consume_entry(item.second.cast<py::dict>());
            }
        }
        return;
    }

    for (auto item : py::list(members)) {
        if (py::isinstance<py::dict>(item)) {
            consume_entry(item.cast<py::dict>());
        }
    }
}

PsProxy::MethodMeta PsProxy::decode_method(py::dict entry) const {
    MethodMeta meta{};
    py::object get = entry.attr("get");

    py::object nameObj = get("Name", py::none());
    if (py::isinstance<py::str>(nameObj)) {
        const std::string name = py::cast<std::string>(nameObj);
        if (name.size() >= 5 && name.rfind("Async") == name.size() - 5) {
            meta.awaitable = true;
        }
    }

    py::object definitionObj = get("Definition", py::none());
    if (py::isinstance<py::str>(definitionObj)) {
        const std::string def = py::cast<std::string>(definitionObj);
        if (def.find("System.Threading.Tasks.Task") != std::string::npos ||
            def.find("ValueTask") != std::string::npos) {
            meta.awaitable = true;
        }
    }

    return meta;
}

PsProxy::PropertyMeta PsProxy::decode_property(py::dict entry) const {
    PropertyMeta meta{};
    py::object get = entry.attr("get");

    py::object definitionObj = get("Definition", py::none());
    if (py::isinstance<py::str>(definitionObj)) {
        const std::string def = py::cast<std::string>(definitionObj);
        if (def.find("set;") != std::string::npos || def.find(" set ") != std::string::npos) {
            meta.writable = true;
        }
    }

    py::object setter = get("SetMethod", py::none());
    if (!setter.is_none()) {
        meta.writable = true;
    }

    return meta;
}

std::string PsProxy::format_argument(py::handle value) const {
    if (value.is_none()) {
        return "$null";
    }

    if (py::isinstance<py::bool_>(value)) {
        return py::cast<bool>(value) ? "$true" : "$false";
    }

    if (py::isinstance<py::str>(value)) {
        return virtualshell::helpers::parsers::ps_quote(py::cast<std::string>(value));
    }

    if (py::isinstance<py::int_>(value)) {
        return py::cast<std::string>(py::str(value));
    }

    if (py::isinstance<py::float_>(value)) {
        return py::cast<std::string>(py::str(value));
    }

    if (py::hasattr(value, "_ps_literal")) {
        auto literal = value.attr("_ps_literal")();
        return py::cast<std::string>(py::str(literal));
    }

    if (py::hasattr(value, "to_pwsh")) {
        auto literal = value.attr("to_pwsh")();
        return py::cast<std::string>(py::str(literal));
    }

    if (py::isinstance<py::list>(value) || py::isinstance<py::tuple>(value)) {
        std::string payload = "@(";
        bool first = true;
        py::sequence seq = py::reinterpret_borrow<py::sequence>(value);
        for (auto item : seq) {
            if (!first) payload += ", ";
            first = false;
            payload += format_argument(item);
        }
        payload += ")";
        return payload;
    }

    if (py::isinstance<py::dict>(value)) {
        std::string payload = "@{";
        bool first = true;
        py::dict mapping = py::reinterpret_borrow<py::dict>(value);
        for (auto item : mapping) {
            if (!first) payload += "; ";
            first = false;
            payload += py::cast<std::string>(item.first);
            payload += "=";
            payload += format_argument(item.second);
        }
        payload += "}";
        return payload;
    }

    return py::cast<std::string>(py::str(value));
}

inline bool is_simple_ident(const std::string& s) {
    if (s.empty()) return false;
    auto isAlpha = [](unsigned char c){ return (c>='A'&&c<='Z')||(c>='a'&&c<='z'); };
    auto isNum   = [](unsigned char c){ return (c>='0'&&c<='9'); };
    auto isUnd   = [](unsigned char c){ return c=='_'; };

    if (!(isAlpha((unsigned char)s[0]) || isUnd((unsigned char)s[0]))) return false;
    for (size_t i=1;i<s.size();++i){
        unsigned char c = (unsigned char)s[i];
        if (!(isAlpha(c) || isNum(c) || isUnd(c))) return false;
    }
    return true;
}

inline std::string escape_single_quotes(const std::string& name) {
    std::string out;
    out.reserve(name.size());
    for (char c : name) {
        out.push_back(c);
        if (c == '\'') out.push_back('\'');
    }
    return out;
}

inline std::string build_property_expr(const std::string& objRef_, const std::string& name) {
    if (is_simple_ident(name)) {
        return "$" + objRef_ + "." + name;
    }
    std::string escaped = escape_single_quotes(name);
    return "$" + objRef_ + ".PSObject.Properties['" + escaped + "'].Value";
}


inline std::string build_method_invocation(const std::string& objRef_,
                                           const std::string& name,
                                           const std::vector<std::string>& args) {
    std::string base;
    if (is_simple_ident(name)) {
        base = "$" + objRef_ + "." + name;
    } else {
        std::string escaped = escape_single_quotes(name);
        base = "$" + objRef_ + ".PSObject.Methods['" + escaped + "'].Invoke";
    }

    std::string command;
    std::size_t estimated = base.size() + 2; // account for "()"
    for (const auto& arg : args) {
        estimated += arg.size() + 2; // comma and space
    }
    command.reserve(estimated);
    command.append(base);
    command.push_back('(');
    for (std::size_t i = 0; i < args.size(); ++i) {
        if (i) command.append(", ");
        command.append(args[i]);
    }
    command.push_back(')');
    return command;
}


inline void rstrip_newlines(std::string& s) {
    while (!s.empty() && (s.back() == '\n' || s.back() == '\r')) s.pop_back();
}

py::object PsProxy::bind_method(const std::string& name, const MethodMeta& meta) {
    auto formatter = [this](py::handle h) { return format_argument(h); };
    auto result_name = typeName_ + "." + name;

    return py::cpp_function(
        [this, meta, formatter, result_name, name](py::args args, py::kwargs kwargs) -> py::object {
            if (kwargs && kwargs.size() != 0) {
                throw py::type_error("Proxy methods do not support keyword arguments");
            }

            std::vector<std::string> psArgs;
            psArgs.reserve(args.size());
            for (auto item : args) {
                psArgs.emplace_back(formatter(item));
            }

            std::string command = build_method_invocation(objRef_, name, psArgs);

            if ( meta.awaitable) {
                command = "(" + command + ").GetAwaiter().GetResult()";
            }

            auto exec = shell_.execute(command);
            if (!exec.success) {
                throw py::value_error("PowerShell method '" + result_name + "' failed: " + exec.err);
            }

            return coerce_scalar(exec.out);
        },
        py::name(name.c_str()));
}

py::object PsProxy::read_property(const std::string& name) const {
    std::string cmd = build_property_expr(objRef_, name);
    auto exec = shell_.execute(cmd);
    if (!exec.success) {
        throw py::value_error("Failed to read property '" + name + "': " + exec.err);
    }
    rstrip_newlines(exec.out);
    return coerce_scalar(exec.out);
}

void PsProxy::write_property(const std::string& name, const PropertyMeta&, py::handle value) {
    std::string lhs = build_property_expr(objRef_, name);
    std::string command = lhs + " = " + format_argument(value);
    auto exec = shell_.execute(command);
    if (!exec.success) {
        throw py::value_error("Failed to set property '" + name + "': " + exec.err);
    }
}

std::shared_ptr<PsProxy> make_ps_proxy(VirtualShell& shell,
                                       std::string typeName,
                                       std::string objectRef, int depth) {
    return std::make_shared<PsProxy>(shell,
                                     std::move(typeName),
                                     std::move(objectRef), depth);
}
} // namespace virtualshell::pybridge