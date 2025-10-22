#include "../include/py_proxy.hpp"
#include "../include/helpers.hpp"
#include "../include/dev_debug.hpp"
#include "../include/execution_result.hpp"
#include "../include/helpers.hpp"
#include <pybind11/stl.h>
#include <pybind11/functional.h>
#include <pybind11/pytypes.h>
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <regex>
#include <format>
#include <atomic>
#include <unordered_set>
#include "dev_debug.hpp"


namespace py = pybind11;

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

// Key type for schema cache
struct CacheKey {
    uintptr_t shell;
    std::string typeName;
    int depth;
    bool operator==(const CacheKey& o) const noexcept {
        return shell == o.shell && depth == o.depth && typeName == o.typeName;
    }
};
struct KeyHash {
    size_t operator()(CacheKey const& k) const noexcept {
        size_t h1 = std::hash<uintptr_t>{}(k.shell);
        size_t h2 = std::hash<int>{}(k.depth);
        size_t h3 = std::hash<std::string>{}(k.typeName);
        return ((h1 ^ (h2 << 1)) >> 1) ^ (h3 << 1);
    }
};

class SchemaCache {
public:
    explicit SchemaCache(size_t maxEntries = 128) : max_(maxEntries) {}

    std::shared_ptr<const virtualshell::pybridge::PsProxy::SchemaRecord> get(const CacheKey& key) {
        std::scoped_lock lk(mx_);
        auto it = map_.find(key);
        if (it == map_.end()) return {};
        // move to front
        lru_.splice(lru_.begin(), lru_, it->second.second);
        return it->second.first;
    }

    void put(const CacheKey& key, std::shared_ptr<virtualshell::pybridge::PsProxy::SchemaRecord> schema) {
        std::scoped_lock lk(mx_);
        auto it = map_.find(key);
        if (it != map_.end()) {
            it->second.first = std::move(schema);
            lru_.splice(lru_.begin(), lru_, it->second.second);
            return;
        }
        lru_.push_front(key);
        map_[key] = { std::move(schema), lru_.begin() };
        if (map_.size() > max_) {
            auto& victim = lru_.back();
            map_.erase(victim);
            lru_.pop_back();
        }
    }

    bool track_shell(uintptr_t shellId) {
        std::scoped_lock lk(mx_);
        return registered_shells_.insert(shellId).second;
    }

    void clear_for(uintptr_t shellId) {
        std::scoped_lock lk(mx_);
        for (auto it = lru_.begin(); it != lru_.end(); ) {
            if (it->shell == shellId) {
                map_.erase(*it);
                it = lru_.erase(it);
            } else {
                ++it;
            }
        }
        registered_shells_.erase(shellId);
    }

private:
    size_t max_;
    std::mutex mx_;
    std::list<CacheKey> lru_;
    
    std::unordered_map<CacheKey, std::pair<std::shared_ptr<const virtualshell::pybridge::PsProxy::SchemaRecord>, std::list<CacheKey>::iterator>, KeyHash> map_;
    std::unordered_set<uintptr_t> registered_shells_;
    
};

static SchemaCache g_schema_cache{128};

static std::string get_real_ps_type(VirtualShell& shell, const std::string& objRefFallback, const std::string& providedTypeName) {
    // Build "$X.PSObject.TypeNames[0]"
    std::string expr = "$" + objRefFallback + ".PSObject.TypeNames[0]";
    auto r = shell.execute(expr);
    if (r.success && !r.out.empty()) {
        auto s = r.out;
        virtualshell::helpers::parsers::trim_inplace(s);
        return s;
    }
    return providedTypeName;
}

static std::shared_ptr<virtualshell::pybridge::PsProxy::SchemaRecord> build_schema_for(VirtualShell& shell, const std::string& objRef, int depth, virtualshell::pybridge::PsProxy const& decoderSelf) {
    auto members = dump_members(shell, objRef, depth); // existing helper you already have
    auto sch = std::make_shared<virtualshell::pybridge::PsProxy::SchemaRecord>();

    auto consume_entry_into = [&](py::dict entry){
        py::object get = entry.attr("get");
        py::object nameObj = get("Name", py::none());
        if (nameObj.is_none()) return;
        const std::string name = py::cast<std::string>(nameObj);

        py::object memberTypeObj = get("MemberType", py::none());
        bool isMethod = false, isProperty = false;

        if (py::isinstance<py::int_>(memberTypeObj)) {
            long flag = py::cast<long>(memberTypeObj);
            if (matches_flag(flag, std::begin(kMethodFlags), std::end(kMethodFlags))) isMethod = true;
            else if (matches_flag(flag, std::begin(kPropertyFlags), std::end(kPropertyFlags))) isProperty = true;
        } else if (py::isinstance<py::str>(memberTypeObj)) {
            const std::string text = py::cast<std::string>(memberTypeObj);
            if (text.find("Method") != std::string::npos) isMethod = true;
            else if (text.find("Property") != std::string::npos) isProperty = true;
        }

        if (isMethod) {
            sch->methods[name] = decoderSelf.decode_method(entry);
        } else if (isProperty) {
            sch->properties[name] = decoderSelf.decode_property(entry);
        }
    };

    if (!members.is_none()) {
        if (py::isinstance<py::dict>(members)) {
            py::dict d = members.cast<py::dict>();
            bool specialized = false;
            if (d.contains("Methods")) {
                for (auto item : py::list(d["Methods"])) {
                    if (py::isinstance<py::dict>(item)) consume_entry_into(item.cast<py::dict>());
                }
                specialized = true;
            }
            if (d.contains("Properties")) {
                for (auto item : py::list(d["Properties"])) {
                    if (py::isinstance<py::dict>(item)) consume_entry_into(item.cast<py::dict>());
                }
                specialized = true;
            }
            if (specialized) return sch;

            // fallback: iterate plain dict-of-dicts
            for (auto item : d) {
                if (py::isinstance<py::dict>(item.second)) {
                    consume_entry_into(item.second.cast<py::dict>());
                }
            }
        } else {
            for (auto item : py::list(members)) {
                if (py::isinstance<py::dict>(item)) {
                    consume_entry_into(item.cast<py::dict>());
                }
            }
        }
    }

    return sch;
}
} // namespace

namespace virtualshell::pybridge {


PsProxy::PsProxy(VirtualShell& shell,
                 std::string typeName,
                 std::string objectRef, int depth)
  : shell_(shell),
    typeName_(std::move(typeName)),
    objRef_(std::move(objectRef)),
    dynamic_(py::dict()),
    methodCache_(py::dict())
{
    if (objRef_.empty() || objRef_[0] != '$') {
        // If objectRef is not a variable, assume it's a type name and create it.
        objRef_ = create_ps_object(objRef_);
    } else {
        // If it is a variable, strip the leading '$'
        objRef_ = objRef_.substr(1);
    }

    const uintptr_t shellId = reinterpret_cast<uintptr_t>(&shell_);
    const bool shouldRegisterStopCallback = g_schema_cache.track_shell(shellId);
    if (shouldRegisterStopCallback) {
        VSHELL_DBG("PROXY","Registering schema cache cleanup for shell %p", (void*)shellId);
        shell_.registerStopCallback([shellId]() {
            g_schema_cache.clear_for(shellId);
        });
    }

    // 1) Try cache with provided type name first
    CacheKey key1{ shellId, typeName_, depth };
    if (auto cached = g_schema_cache.get(key1)) {
        schema_ = cached;
        VSHELL_DBG("PROXY","Cache hit for key1: %s", typeName_.c_str());
        return;
    }

    // 2) Miss: try cache with "real" type name
    const std::string realType = get_real_ps_type(shell_, objRef_, typeName_);
    CacheKey key2{ shellId, realType, depth };
    if (auto cached = g_schema_cache.get(key2)) {
        schema_ = cached;
        VSHELL_DBG("PROXY","Cache hit for key2: %s", realType.c_str());
        return;
    }

    // 3) Full miss: try cache with "real" type name
    auto sch = build_schema_for(shell_, objRef_, depth, *this); // returns shared_ptr<Schema>
    schema_ = sch;
    g_schema_cache.put(key2, sch);
    if (key1.typeName != key2.typeName) {
        g_schema_cache.put(key1, sch);
    }
    VSHELL_DBG("PROXY","Schema built and cached for type: %s (real type: %s)", typeName_.c_str(), realType.c_str());
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
    
    if (auto mit = schema_ref().methods.find(name); mit != schema_ref().methods.end()) {
        auto callable = bind_method(name, mit->second);
        methodCache_[key] = callable;
        return callable;
    }

    if (auto pit = schema_ref().properties.find(name); pit != schema_ref().properties.end()) {
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

    if (auto mit = schema_ref().methods.find(name); mit != schema_ref().methods.end()) {
        throw py::attribute_error("Cannot overwrite proxied method '" + name + "'");
    }

    if (auto pit = schema_ref().properties.find(name); pit != schema_ref().properties.end()) {
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
    for (const auto& kv : schema_ref().methods) push(kv.first);
    for (const auto& kv : schema_ref().properties) push(kv.first);

    auto extras = dynamic_.attr("keys")();
    for (auto item : extras) {
        push(py::cast<std::string>(item));
    }

    return out;
}

const PsProxy::SchemaRecord& PsProxy::schema_ref() const {
    return *schema_;
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

py::dict PsProxy::schema() const {
    py::dict out;
    py::list methods;
    py::list props;

    for (const auto& kv : schema_ref().methods) {
        py::dict entry;
        entry["Name"] = kv.first;
        entry["Awaitable"] = kv.second.awaitable;
        methods.append(entry);
    }
    for (const auto& kv : schema_ref().properties) {
        py::dict entry;
        entry["Name"] = kv.first;
        entry["Writable"] = kv.second.writable;
        props.append(entry);
    }

    out["Methods"] = methods;
    out["Properties"] = props;
    return out;
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

std::string PsProxy::create_ps_object(const std::string& typeNameWithArgs) {
    // 1. Generate a unique variable name for the proxy object
    static std::atomic<uint32_t> counter = 0;
    std::string varName = "proxy_obj_" + std::to_string(counter++);
    std::string psVar = "$" + varName;

    // 2. Parse type name and arguments
    std::string typeName = typeNameWithArgs;
    std::string args;
    size_t parenPos = typeNameWithArgs.find('(');
    if (parenPos != std::string::npos) {
        typeName = typeNameWithArgs.substr(0, parenPos);
        virtualshell::helpers::parsers::trim_inplace(typeName);
        size_t endParenPos = typeNameWithArgs.rfind(')');
        if (endParenPos != std::string::npos && endParenPos > parenPos) {
            args = typeNameWithArgs.substr(parenPos + 1, endParenPos - parenPos - 1);
        }
    }

    std::string bracketedType = typeName;
    if (bracketedType.front() != '[' || bracketedType.back() != ']') {
        bracketedType = "[" + bracketedType + "]";
    }

    // 3. Build a list of creation strategies
    std::vector<std::string> strategies;
    if (!args.empty()) {
        strategies.push_back(psVar + " = New-Object -TypeName '" + typeName + "' -ArgumentList " + args + " -ErrorAction Stop");
        strategies.push_back(psVar + " = " + bracketedType + "::new(" + args + ")");
        strategies.push_back(psVar + " = " + bracketedType + "::New(" + args + ")");
    } else {
        strategies.push_back(psVar + " = New-Object -TypeName '" + typeName + "' -ErrorAction Stop");
        strategies.push_back(psVar + " = " + bracketedType + "::new()");
        strategies.push_back(psVar + " = " + bracketedType + "::New()");
    }
    // Add COM object strategy if type name looks like it could be one
    if (typeName.find('.') != std::string::npos) {
        strategies.push_back(psVar + " = New-Object -ComObject '" + typeName + "' -ErrorAction Stop");
    }

    // 4. Try strategies until one succeeds
    virtualshell::core::ExecutionResult result;
    bool success = false;
    for (const auto& cmd : strategies) {
        result = shell_.execute(cmd);
        if (result.success) {
            success = true;
            VSHELL_DBG("PROXY", "Object creation succeeded with command: %s", cmd.c_str());
            break;
        }
    }

    if (!success) {
        throw std::runtime_error("Failed to create PowerShell object for type '" + typeNameWithArgs + "'. Last error: " + result.err);
    }

    return varName; // Return the variable name without the '$'
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