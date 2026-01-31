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
#include <fstream>
#include <regex>
// NOTE:
// <format> is required on some platforms (MSVC/libstdc++)
// even in C++17, due to STL/pybind11 internal dependencies.
#include <format>

#include <atomic>
#include <unordered_set>
#include <algorithm>
#include <cstring>
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

static std::string ensure_dollar(const std::string& nameOrVar) {
    if (!nameOrVar.empty() && nameOrVar[0] == '$') {
        return nameOrVar;  // allerede "$foo"
    }
    return "$" + nameOrVar; // gjør "foo" -> "$foo"
}

auto escape_single_quotes = [](const std::string& s) {
    std::string out;
    out.reserve(s.size()*2);
    for (char c : s) {
        out.push_back(c);
        if (c == '\'') out.push_back('\''); // PS single-quote escape
    }
    return out;
};


py::object dump_members(VirtualShell& shell, std::string objRef, int depth) {
    auto ref = std::move(objRef);
    auto result = shell.execute(ensure_dollar(ref) + " | Get-Member | ConvertTo-Json -Depth " + std::to_string(depth) + " -Compress");

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

static std::string make_byte_array_literal_from_py(py::handle h) {
    std::vector<unsigned int> vals;

    if (PyByteArray_Check(h.ptr())) {
        auto len = (size_t)PyByteArray_GET_SIZE(h.ptr());
        const unsigned char* data = (const unsigned char*)PyByteArray_AS_STRING(h.ptr());
        vals.reserve(len);
        for (size_t i = 0; i < len; ++i) {
            vals.push_back((unsigned int)data[i]);
        }
    } else if (py::isinstance<py::bytes>(h)) {
        py::bytes b = h.cast<py::bytes>();
        std::string s = b; // copies
        vals.reserve(s.size());
        for (unsigned char c : s) {
            vals.push_back((unsigned int)c);
        }
    } else if (py::isinstance<py::list>(h)) {
        py::list lst = h.cast<py::list>();
        vals.reserve(py::len(lst));
        for (auto item : lst) {
            vals.push_back((unsigned int)py::cast<int>(item)); // assume 0..255
        }
    } else {
        
        std::string s = py::cast<std::string>(py::str(h));
        vals.reserve(s.size());
        for (unsigned char c : s) {
            vals.push_back((unsigned int)c);
        }
    }

    std::string ps = "[byte[]](";
    for (size_t i = 0; i < vals.size(); ++i) {
        if (i) ps += ",";
        ps += std::to_string(vals[i]);
    }
    ps += ")";
    return ps;
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

struct SignatureInfo {
    std::string returnType;
    bool returnsVoid{false};
    std::vector<virtualshell::pybridge::PsProxy::ParamMeta> params;
    int expectedArgCount() const {
        return static_cast<int>(params.size());
    }
};


static std::atomic<uint32_t> sigCounter{0};

static SignatureInfo reflect_signature_for(
    VirtualShell& shell,
    const std::string& objRef,      // uten '$', f.eks. "proxy_obj_3"
    const std::string& methodName)  // f.eks. "ReadArray"
{
    SignatureInfo sig;

    std::string safeName = escape_single_quotes(methodName);
    std::string ps;
    ps += "$__vs_obj = $" + ensure_dollar(objRef) + ";\n";
    ps += "$__vs_sigs = @();\n";
    ps += "foreach ($mi in ($__vs_obj.GetType().GetMethods() | Where-Object { $_.Name -eq '" + safeName + "' })) {\n";
    ps += "  $params = @();\n";
    ps += "  foreach ($p in $mi.GetParameters()) {\n";
    ps += "    $params += [pscustomobject]@{\n";
    ps += "      ParamName = $p.Name;\n";
    ps += "      TypeName  = $p.ParameterType.FullName;\n";
    ps += "      IsOut     = $p.IsOut;\n";
    ps += "      IsByRef   = $p.ParameterType.IsByRef;\n";
    ps += "      IsArray   = $p.ParameterType.IsArray;\n";
    ps += "    };\n";
    ps += "  }\n";
    ps += "  $__vs_sigs += [pscustomobject]@{\n";
    ps += "    ReturnType = $mi.ReturnType.FullName;\n";
    ps += "    Params     = $params;\n";
    ps += "  };\n";
    ps += "}\n";
    ps += "$__vs_sigs | ConvertTo-Json -Depth 8 -Compress\n";

    std::string tempPath = "C:\\temp\\ps_sig_" + std::to_string(sigCounter++) + ".ps1";
    std::ofstream out(tempPath, std::ios::out);
    out.write(ps.data(), ps.size());
    out.close();

    auto exec = shell.execute_script(tempPath, std::vector<std::string>{}, 10.0, true);
    if (!exec.success || exec.out.empty()) {
        return sig;
    }

    virtualshell::helpers::parsers::trim_inplace(exec.out);

    try {
        py::object json_mod = py::module_::import("json");
        py::object parsed   = json_mod.attr("loads")(py::str(exec.out));

        py::object first;
        if (py::isinstance<py::list>(parsed)) {
            py::list lst = parsed.cast<py::list>();
            if (lst.empty()) return sig;
            first = lst[0];
        } else {
            first = parsed;
        }

        // ReturnType
        {
            py::object rt = first.attr("get")("ReturnType", py::none());
            if (!rt.is_none()) {
                sig.returnType = py::cast<std::string>(rt);
            }
        }

        // Params
        {
            py::object pListObj = first.attr("get")("Params", py::list());
            py::list pList      = pListObj.cast<py::list>();
            for (auto pitem : pList) {
                py::object get = pitem.attr("get");
                virtualshell::pybridge::PsProxy::ParamMeta pm;
                pm.name     = py::cast<std::string>(get("ParamName", py::str("")));
                pm.typeName = py::cast<std::string>(get("TypeName",  py::str("")));
                pm.isOut    = py::cast<bool>(get("IsOut",    py::bool_(false)));
                pm.isByRef  = py::cast<bool>(get("IsByRef",  py::bool_(false)));
                pm.isArray  = py::cast<bool>(get("IsArray",  py::bool_(false)));
                sig.params.push_back(std::move(pm));
            }
        }
        {
            std::string lower = sig.returnType;
            for (auto &c : lower) c = (char)std::tolower((unsigned char)c);
            if (lower == "void" || lower == "system.void") {
                sig.returnsVoid = true;
            }
        }

    } catch (const py::error_already_set&) {
    }

    return sig;
}



static std::string get_real_ps_type(VirtualShell& shell, const std::string& objRefFallback, const std::string& providedTypeName) {
    // Build "$X.PSObject.TypeNames[0]"
    std::string expr = ensure_dollar(objRefFallback) + ".PSObject.TypeNames[0]";
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
            auto mm = decoderSelf.decode_method(entry);

            SignatureInfo sig = reflect_signature_for(shell, objRef, name);
            if (!sig.returnType.empty()) {
                mm.returnType   = sig.returnType;
                mm.returnsVoid  = sig.returnsVoid;
            }
            if (!sig.params.empty()) {
                mm.params = std::move(sig.params);
            }
            

            sch->methods[name] = std::move(mm);
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

static bool is_ps_scalar_type(const std::string& t)
{
    std::string lower = t;
    for (auto &c : lower) c = (char)std::tolower((unsigned char)c);

    if (lower == "string"          || lower == "system.string")          return true;
    if (lower == "bool"            || lower == "system.boolean")         return true;
    if (lower == "int"             || lower == "int32" ||
        lower == "system.int32")                                       return true;
    if (lower == "long"            || lower == "int64" ||
        lower == "system.int64")                                       return true;
    if (lower == "double"          || lower == "system.double")          return true;
    if (lower == "single"          || lower == "float" ||
        lower == "system.single")                                     return true;
    if (lower == "decimal"         || lower == "system.decimal")         return true;

    return false;
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
    if (objRef_[0] != '$') {
        objRef_ = create_ps_object(objRef_);
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

inline std::string qualify_objref(const std::string& objRef) {
    // Ensure we always end up with something like "$foo", not "$$foo"
    if (!objRef.empty() && objRef[0] == '$') {
        return objRef;
    }
    return "$" + objRef;
}


inline std::string build_property_expr(const std::string& objRef_, const std::string& name) {
    std::string baseRef = qualify_objref(objRef_);

    if (is_simple_ident(name)) {
        return baseRef + "." + name;
    }
    std::string escaped = escape_single_quotes(name);
    return baseRef + ".PSObject.Properties['" + escaped + "'].Value";
}


inline std::string build_method_invocation(const std::string& objRef_,
                                           const std::string& name,
                                           const std::vector<std::string>& args) {
    std::string baseRef = qualify_objref(objRef_);
    std::string base;

    if (is_simple_ident(name)) {
        base = baseRef + "." + name;
    } else {
        std::string escaped = escape_single_quotes(name);
        base = baseRef + ".PSObject.Methods['" + escaped + "'].Invoke";
    }

    std::string command;
    std::size_t estimated = base.size() + 2;
    for (const auto& arg : args) {
        estimated += arg.size() + 2;
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

    // Hint om awaitable via navn som slutter på "Async"
    py::object nameObj = get("Name", py::none());
    if (py::isinstance<py::str>(nameObj)) {
        const std::string nm = py::cast<std::string>(nameObj);
        if (nm.size() >= 5 && nm.rfind("Async") == nm.size() - 5) {
            meta.awaitable = true;
        }
    }

    py::object defObj = get("Definition", py::none());
    if (py::isinstance<py::str>(defObj)) {
        const std::string def = py::cast<std::string>(defObj);

        // Return type = alt før første space
        auto firstSpace = def.find(' ');
        if (firstSpace != std::string::npos) {
            meta.returnType = def.substr(0, firstSpace);
        }

        if (def.find("System.Threading.Tasks.Task") != std::string::npos ||
            def.find("ValueTask") != std::string::npos) {
            meta.awaitable = true;
        }

        // Void?
        {
            std::string lower = meta.returnType;
            for (auto &c : lower) c = (char)std::tolower((unsigned char)c);
            if (lower == "void" || lower == "system.void") {
                meta.returnsVoid = true;
            }
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
        strategies.push_back(psVar + " = $" + typeName + "' -ArgumentList " + args);
        strategies.push_back(psVar + " = New-Object -TypeName '" + typeName + "' -ArgumentList " + args + " -ErrorAction Stop");
        strategies.push_back(psVar + " = " + bracketedType + "::new(" + args + ")");
        strategies.push_back(psVar + " = " + bracketedType + "::New(" + args + ")");
    } else {
        strategies.push_back(psVar + " = $" + typeName);
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
            std::string checkCmd = psVar + " | Get-Member -ErrorAction Stop";
            auto checkResult = shell_.execute(checkCmd);
            if (checkResult.out.empty() || !checkResult.success) {
                continue; // Creation command succeeded but object is invalid
            }
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

py::list PsProxy::multi_call(const py::function& func, py::args args) {
    // Check if func is a method of this proxy
    std::string methodName = func.attr("__name__").cast<std::string>();
    auto method_it = schema_ref().methods.find(methodName);
    if (method_it == schema_ref().methods.end()) {
        throw py::type_error("Function is not a method of this proxy");
    }

    const auto& meta = method_it->second;
    py::list final_results;
    constexpr size_t BATCH_SIZE = 1000;

    auto process_batch = [&](const std::string& command_batch) {
        if (command_batch.empty()) return;

        std::string full_command = "$batch_result = @();\n" + command_batch + "$batch_result | ConvertTo-Json -Compress -Depth 5\n";
        auto result = shell_.execute(full_command);

        if (!result.success) {
            throw py::value_error("PowerShell multi-call batch for method '" + methodName + "' failed: " + result.err);
        }
        if (result.out.empty() || result.out == "null") return;

        try {
            py::object json_module = py::module_::import("json");
            py::object batch_results = json_module.attr("loads")(result.out);
            if (py::isinstance<py::list>(batch_results)) {
                for (auto item : batch_results.cast<py::list>()) {
                    final_results.append(item);
                }
            } else {
                final_results.append(batch_results);
            }
        } catch (const py::error_already_set& e) {
            throw std::runtime_error("Failed to parse JSON from batch result: " + std::string(e.what()));
        }
    };

    // Case 1: A single integer is passed to repeat the call.
    if (args.size() == 1 && py::isinstance<py::int_>(args[0])) {
        size_t total_calls = py::cast<size_t>(args[0]);
        std::vector<std::string> psArgsForCall; // Empty vector for no-argument call
        std::string single_invocation = build_method_invocation(objRef_, methodName, psArgsForCall);
        if (meta.awaitable) {
            single_invocation = "(" + single_invocation + ").GetAwaiter().GetResult()";
        }

        std::string command_batch;
        for (size_t i = 0; i < total_calls; ++i) {
            command_batch += "$batch_result += " + single_invocation + ";\n";
            if ((i + 1) % BATCH_SIZE == 0 || i == total_calls - 1) {
                process_batch(command_batch);
                command_batch.clear();
            }
        }
    }
    // Case 2: A single list is passed, iterate over its items.
    else if (args.size() == 1 && py::isinstance<py::list>(args[0])) {
        py::list arg_list = py::reinterpret_borrow<py::list>(args[0]);
        size_t total_calls = arg_list.size();
        std::string command_batch;

        for (size_t i = 0; i < total_calls; ++i) {
            std::vector<std::string> psArgsForCall;
            psArgsForCall.push_back(format_argument(arg_list[i]));
            std::string single_invocation = build_method_invocation(objRef_, methodName, psArgsForCall);
            if (meta.awaitable) {
                single_invocation = "(" + single_invocation + ").GetAwaiter().GetResult()";
            }
            command_batch += "$batch_result += " + single_invocation + ";\n";

            if ((i + 1) % BATCH_SIZE == 0 || i == total_calls - 1) {
                process_batch(command_batch);
                command_batch.clear();
            }
        }
    }
    // Case 3: Fallback for a sequence of arguments (e.g., multi_call(method, arg1, arg2, ...))
    else {
        size_t total_calls = args.size();
        std::string command_batch;

        for (size_t i = 0; i < total_calls; ++i) {
            std::vector<std::string> psArgsForCall;
            psArgsForCall.push_back(format_argument(args[i]));
            std::string single_invocation = build_method_invocation(objRef_, methodName, psArgsForCall);
            if (meta.awaitable) {
                single_invocation = "(" + single_invocation + ").GetAwaiter().GetResult()";
            }
            command_batch += "$batch_result += " + single_invocation + ";\n";

            if ((i + 1) % BATCH_SIZE == 0 || i == total_calls - 1) {
                process_batch(command_batch);
                command_batch.clear();
            }
        }
    }

    return final_results;
}

py::object PsProxy::bind_method(const std::string& name, const MethodMeta& meta) {
    auto formatter   = [this](py::handle h) { return format_argument(h); };
    auto result_name = typeName_ + "." + name;

    static std::atomic<uint32_t> globalCounter{0};

    return py::cpp_function(
        [this, meta, formatter, result_name, name](py::args args, py::kwargs kwargs) -> py::object {
            if (kwargs && kwargs.size() != 0) {
                throw py::type_error("Proxy methods do not support keyword arguments");
            }

            struct OutBufInfo {
                size_t      pyIndex; // index in args[]
                std::string psVar;   // "__vs_buf_42"
            };

            std::vector<OutBufInfo> outBufs;
            std::string allocLines;          // PS lines that declare temp vars BEFORE call
            std::vector<std::string> finalPsArgs;
            finalPsArgs.reserve(args.size());

            auto calc_py_buf_len = [](py::handle h) -> size_t {
                if (PyByteArray_Check(h.ptr())) {
                    return (size_t)PyByteArray_GET_SIZE(h.ptr());
                }
                if (py::isinstance<py::bytes>(h)) {
                    py::bytes b = h.cast<py::bytes>();
                    std::string s = b; // copies
                    return s.size();
                }
                if (py::isinstance<py::list>(h)) {
                    return (size_t)py::len(h);
                }
                if (h.is_none()) return 0;
                try { return (size_t)py::len(h); }
                catch (...) { return 0; }
            };

            size_t nCallArgs = args.size();
            for (size_t i = 0; i < nCallArgs; ++i) {
                bool handled = false;

                if (i < meta.params.size()) {
                    bool handled = false;
                    py::handle h = args[i]; // Hent Python-objektet

                    // Refleksjons-flagg (vi trenger fortsatt pm.isArray)
                    bool paramIsArray = false;
                    bool paramIsOutOrRef = false;
                    bool paramIsKnownByteIn = false;

                    if (i < meta.params.size()) {
                        const auto& pm = meta.params[i];
                        paramIsArray = pm.isArray;
                        paramIsOutOrRef = (pm.isOut || pm.isByRef);
                        paramIsKnownByteIn = pm.isArray && !paramIsOutOrRef &&
                            (pm.typeName == "System.Byte[]" || pm.typeName == "System.Byte[]" || pm.typeName == "System.Byte[]");
                    }


                    // ---- CASE A: out/ref buffer (Python 'bytearray') ----
                    // Heuristikk: Hvis Python sender 'bytearray' og PS-metoden
                    // forventer en array, antar vi at det er en out-buffer.
                    if (paramIsArray && PyByteArray_Check(h.ptr()))
                    {
                        size_t bufLen = calc_py_buf_len(h);

                        // fallback: hent size hint fra siste arg (ofte "count")
                        if (bufLen == 0 && nCallArgs > 0) {
                            py::handle lastArg = args[nCallArgs - 1];
                            if (py::isinstance<py::int_>(lastArg)) {
                                bufLen = (size_t)py::cast<int>(lastArg);
                            }
                        }

                        uint32_t id = globalCounter++;
                        std::string psVarBase = "__vs_buf_" + std::to_string(id);

                        allocLines += "$" + psVarBase + " = New-Object byte[] " +
                                    std::to_string(bufLen) + ";\n";

                        finalPsArgs.push_back("$" + psVarBase);

                        outBufs.push_back(OutBufInfo{
                            i,              // which python arg maps to this buffer
                            psVarBase       // powershell var name (no $)
                        });

                        handled = true;
                    }
                    // ---- CASE B: in buffer (Python 'bytes') ----
                    // Heuristikk: Hvis Python sender 'bytes' og PS-metoden
                    // forventer en 'in'-array, antar vi at det er en in-buffer.
                    else if (paramIsArray && !paramIsOutOrRef && py::isinstance<py::bytes>(h))
                    {
                        // Bygg en statisk [byte[]](...) literal fra Python-argumentet
                        std::string psLiteral = make_byte_array_literal_from_py(h);

                        uint32_t id = globalCounter++;
                        std::string psVarBase = "__vs_in_" + std::to_string(id);

                        // $__vs_in_X = [byte[]](72,101,108,...)
                        allocLines += "$" + psVarBase + " = " + psLiteral + ";\n";

                        finalPsArgs.push_back("$" + psVarBase);
                        handled = true;
                    }
                    // ---- CASE B (refleksjon): Fallback for 'in' array ----
                    else if (!handled && paramIsKnownByteIn)
                    {
                        // (Samme som over, men trigget av refleksjon i stedet for py::bytes)
                        std::string psLiteral = make_byte_array_literal_from_py(h);
                        uint32_t id = globalCounter++;
                        std::string psVarBase = "__vs_in_" + std::to_string(id);
                        allocLines += "$" + psVarBase + " = " + psLiteral + ";\n";
                        finalPsArgs.push_back("$" + psVarBase);
                        handled = true;
                    }
                    // ---- CASE A (refleksjon): Fallback for 'out' list/etc ----
                    else if (!handled && paramIsOutOrRef && py::isinstance<py::list>(h))
                    {
                        // (Logikk for [out] list[], etc. - duplisert fra CASE A over)
                        size_t bufLen = calc_py_buf_len(h);
                        // ... (samme logikk som CASE A)
                        // ... (NB: Denne antar fortsatt New-Object byte[]...)
                        // ... (Denne logikken var ufullstendig i din opprinnelige kode også)

                        // For nå, la oss anta at [out] array alltid er byte[]
                        uint32_t id = globalCounter++;
                        std::string psVarBase = "__vs_buf_" + std::to_string(id);
                        allocLines += "$" + psVarBase + " = New-Object byte[] " +
                                    std::to_string(bufLen) + ";\n";
                        finalPsArgs.push_back("$" + psVarBase);
                        outBufs.push_back(OutBufInfo{ i, psVarBase });

                        handled = true;
                    }


                    // ---- CASE C: alt annet (int, string, bool, etc.) ----
                    if (!handled) {
                        finalPsArgs.push_back(formatter(args[i]));
                    }
                } // slutt på if (i < meta.params.size())
                else if (!handled) {
                    // Fallback for args utenfor param-listen (CASE C)
                    finalPsArgs.push_back(formatter(args[i]));
                }
            }

            // --- bygg selve metodekallet ( "$obj.Method(arg1,...)" ) ---
            std::string callExpr = build_method_invocation(objRef_, name, finalPsArgs);
            if (meta.awaitable) {
                callExpr = "(" + callExpr + ").GetAwaiter().GetResult()";
            }

            // --- unikt returvariabelnavn ---
            uint32_t retId = globalCounter++;
            std::string retVarBase = "__vs_ret_" + std::to_string(retId);
            std::string retVarPS   = "$" + retVarBase;

            // --- script som kjøres i PowerShell ---
            // allocLines:
            //   $__vs_in_12 = [byte[]](72,101,...)
            //   $__vs_buf_13 = New-Object byte[] 30
            // call + ret:
            //   $__vs_ret_99 = $obj.Method($__vs_in_12, $__vs_buf_13, 0, 30);
            std::string psScript;
            psScript.reserve(allocLines.size() + callExpr.size() + 64);
            psScript.append(allocLines);
            psScript.append(retVarPS);
            psScript.append(" = ");
            psScript.append(callExpr);
            psScript.append(";\n");

            auto execMain = shell_.execute(psScript);
            if (!execMain.success) {
                throw py::value_error(
                    "PowerShell method '" + result_name + "' failed: " + execMain.err);
            }

            // --- finn python-returverdi ---
            py::object pyReturn = py::none();

            if (!meta.returnsVoid) {
                if (is_ps_scalar_type(meta.returnType)) {
                    // f.eks. ReadArray(...) returnerer int (# of bytes read)
                    auto execRet = shell_.execute(retVarPS);
                    if (!execRet.success) {
                        throw py::value_error(
                            "PowerShell method '" + result_name + "' failed (read ret): " + execRet.err);
                    }
                    pyReturn = coerce_scalar(execRet.out);
                } else {
                    // kompleks .NET-objekt → ny proxy til retVarPS
                    auto newProxyPtr = make_ps_proxy(shell_, meta.returnType, retVarPS, 4);
                    pyReturn = py::cast(newProxyPtr);
                }
            }

            // --- sync tilbake out/ref byte[] (ReadArray-scenario) ---
            if (!outBufs.empty()) {
                // Bygg en liten liste av { Id = 0; Data = $__vs_buf_x }
                std::string dumpScript;
                dumpScript += "@(";
                for (size_t idx = 0; idx < outBufs.size(); ++idx) {
                    if (idx) dumpScript += ",";
                    dumpScript += "[pscustomobject]@{ Id=" + std::to_string(idx)
                                + "; Data=$" + outBufs[idx].psVar + " }";
                }
                dumpScript += ") | ConvertTo-Json -Compress -Depth 6\n";

                auto execDump = shell_.execute(dumpScript);
                if (!execDump.success) {
                    throw py::value_error(
                        "PowerShell dump of out buffers failed: " + execDump.err);
                }

                if (!execDump.out.empty()) {
                    try {
                        py::object json_mod = py::module_::import("json");
                        py::object parsed   = json_mod.attr("loads")(py::str(execDump.out));

                        py::list bufList;
                        if (py::isinstance<py::list>(parsed)) {
                            bufList = parsed.cast<py::list>();
                        } else {
                            bufList = py::list();
                            bufList.append(parsed);
                        }

                        for (size_t idx = 0; idx < outBufs.size(); ++idx) {
                            if (idx >= (size_t)py::len(bufList)) break;

                            const auto& info = outBufs[idx];
                            py::object elem   = bufList[idx];

                            py::object dataListObj = elem.attr("__getitem__")("Data");
                            py::list   dataList    = dataListObj.cast<py::list>();

                            std::vector<uint8_t> vec;
                            vec.reserve(py::len(dataList));
                            for (auto v : dataList) {
                                vec.push_back((uint8_t)py::cast<int>(v));
                            }

                            py::handle targetPyBuf = args[info.pyIndex];

                            // skriv tilbake til kallers mutable buffer
                            if (PyByteArray_Check(targetPyBuf.ptr())) {
                                if (PyByteArray_Resize(
                                        targetPyBuf.ptr(),
                                        (Py_ssize_t)vec.size()) != 0)
                                {
                                    throw py::error_already_set();
                                }
                                char* pyBufPtr = PyByteArray_AS_STRING(targetPyBuf.ptr());
                                std::memcpy(pyBufPtr, vec.data(), vec.size());
                            }
                            else if (py::isinstance<py::list>(targetPyBuf)) {
                                py::list pyList = targetPyBuf.cast<py::list>();
                                pyList.attr("clear")();
                                for (uint8_t b : vec) {
                                    pyList.append(py::int_(b));
                                }
                            }
                            else {
                                std::ostringstream oss;
                                oss << "Argument at index " << info.pyIndex
                                    << " for method '" << name
                                    << "' was used as an out/ref buffer "
                                       "but is not a mutable bytearray/list.";
                                throw py::type_error(oss.str());
                            }
                        }
                    } catch (const py::error_already_set& e) {
                        throw std::runtime_error(
                            std::string("Python error parsing out-buffers: ") + e.what());
                    } catch (const std::exception& e) {
                        throw std::runtime_error(
                            std::string("C++ error parsing out-buffers: ") + e.what());
                    }
                }
            }

            return pyReturn;
        }
    );
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
