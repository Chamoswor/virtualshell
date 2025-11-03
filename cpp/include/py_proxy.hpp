#pragma once
#include <memory>
#include <string>
#include <unordered_map>
#include <pybind11/pybind11.h>
#include "../include/virtual_shell.hpp"

namespace virtualshell::pybridge {


class PsProxy {

public:
    PsProxy(VirtualShell& shell,
            std::string typeName,
            std::string objectRef, int depth = 4);

    pybind11::object getattr(const std::string& name);
    void setattr(const std::string& name, pybind11::object value);
    pybind11::list dir() const;
    pybind11::dict schema() const;

    const std::string& type_name() const noexcept { return typeName_; }

        struct ParamMeta {
        std::string name;
        std::string typeName;
        bool isOut{false};
        bool isByRef{false};
        bool isArray{false};
    };

    struct MethodMeta {
        bool awaitable{false};
        bool returnsVoid{false};
        std::string returnType;
        std::vector<ParamMeta> params;
    };

    struct PropertyMeta { 
        bool writable{false}; 
    };
    struct SchemaRecord {
        std::unordered_map<std::string, MethodMeta> methods;
        std::unordered_map<std::string, PropertyMeta> properties;
    };
    MethodMeta decode_method(pybind11::dict entry) const;
    PropertyMeta decode_property(pybind11::dict entry) const;
    pybind11::list multi_call(const pybind11::function& func, pybind11::args args);

private:
    const SchemaRecord& schema_ref() const;
    std::string format_argument(pybind11::handle value) const;
    pybind11::object bind_method(const std::string& name, const MethodMeta& meta);
    pybind11::object read_property(const std::string& name) const;
    void write_property(const std::string& name, const PropertyMeta& meta, pybind11::handle value);
    std::string create_ps_object(const std::string& typeNameWithArgs);
    

    VirtualShell& shell_;
    std::string typeName_;
    std::string objRef_;

    std::shared_ptr<const SchemaRecord> schema_;

    pybind11::dict dynamic_{};
    pybind11::dict methodCache_{};
};

std::shared_ptr<PsProxy> make_ps_proxy(VirtualShell& shell,
                                       std::string typeName,
                                       std::string objectRef, int depth = 4);



} // namespace virtualshell::pybridge