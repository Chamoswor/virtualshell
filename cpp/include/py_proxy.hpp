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

private:
    struct MethodMeta { bool awaitable{false}; };
    struct PropertyMeta { bool writable{false}; };
    
    void ingest_members(int depth);
    MethodMeta decode_method(pybind11::dict entry) const;
    PropertyMeta decode_property(pybind11::dict entry) const;
    std::string format_argument(pybind11::handle value) const;
    pybind11::object bind_method(const std::string& name, const MethodMeta& meta);
    pybind11::object read_property(const std::string& name) const;
    void write_property(const std::string& name, const PropertyMeta& meta, pybind11::handle value);

    VirtualShell& shell_;
    std::string typeName_;
    std::string objRef_;

    std::unordered_map<std::string, MethodMeta> methods_;
    std::unordered_map<std::string, PropertyMeta> properties_;
    pybind11::dict dynamic_{};
    pybind11::dict methodCache_{};
};

std::shared_ptr<PsProxy> make_ps_proxy(VirtualShell& shell,
                                       std::string typeName,
                                       std::string objectRef, int depth = 4);



} // namespace virtualshell::pybridge