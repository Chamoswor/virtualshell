"""Tests for generate_psobject(follow=True): a package of cross-annotated stubs.

Unit tests cover type-name mapping, class-name allocation and module
rendering; the integration part compiles a small .NET type graph with
Add-Type -OutputAssembly and round-trips a generated package into a fresh
session (both editions).
"""
from __future__ import annotations

import importlib
import sys

import pytest

from virtualshell.errors import ExecutionError
from virtualshell.generate_package import (
    Annotator,
    allocate_class_names,
    parse_type_graph,
    render_type_module,
    split_generic,
)

from conftest import integration


class TestSplitGeneric:
    def test_non_generic(self):
        assert split_generic("System.Int32") is None

    def test_simple_and_nested(self):
        assert split_generic("System.Collections.Generic.List`1[Vs.Project]") == \
            ("System.Collections.Generic.List`1", ["Vs.Project"])
        assert split_generic(
            "System.Collections.Generic.Dictionary`2[System.String,"
            "System.Collections.Generic.List`1[Vs.Project]]"
        ) == ("System.Collections.Generic.Dictionary`2",
              ["System.String", "System.Collections.Generic.List`1[Vs.Project]"])


class TestAnnotator:
    def test_protocols_enums_collections_and_scalars(self):
        names = {"Vs.Project": "Project", "Vs.ProjectSet": "ProjectSet"}
        a = Annotator(names, enums={"Vs.Mode"})
        assert a("Vs.Project") == "Project"
        assert a("Vs.Mode") == "str"
        # Collections are proxies with len()/[]/iter/in: read-only protocols.
        assert a("Vs.Project[]") == "Sequence[Project]"
        assert a("System.Collections.Generic.IEnumerable`1[Vs.Project]") == "Sequence[Project]"
        assert a("System.Collections.ObjectModel.ReadOnlyCollection`1[Vs.Project]") == \
            "Sequence[Project]"
        assert a("System.Collections.Generic.Dictionary`2[System.String,Vs.Project]") == \
            "Mapping[str, Project]"
        assert a("System.Collections.Generic.HashSet`1[System.String]") == "AbstractSet[str]"
        assert a("System.Byte[]") == "bytes"
        assert a("System.Char[]") == "str"
        assert a("System.Int32[]") == "Sequence[int]"
        assert a("System.Nullable`1[System.Int32]") == "Optional[int]"
        assert a("System.String") == "str"
        assert a("System.Void") == "None"
        assert a("System.IO.FileInfo") == "Any"          # runtime type: not followed
        assert a.imports == {"Project"}
        assert {"Sequence", "Mapping", "AbstractSet", "Optional"} <= a.typing_bits


class TestAllocateClassNames:
    def test_disambiguates_collisions(self):
        names = allocate_class_names(["A.Root", "A.HW.Device", "A.SW.Device", "A.Nested+Inner"])
        assert names["A.Root"] == "Root"
        assert names["A.HW.Device"] == "HW_Device"
        assert names["A.SW.Device"] == "SW_Device"
        assert names["A.Nested+Inner"] == "Nested_Inner"
        assert len(set(names.values())) == 4


class TestRenderTypeModule:
    DESC = {
        "tn": "Vs.Portal", "asm": "C:/sdk/Vs.dll", "asmName": "Vs, Version=1.0", "ext": True,
        "props": [
            {"n": "Projects", "t": "Vs.ProjectSet", "w": False, "ip": []},
            {"n": "Mode", "t": "Vs.Mode", "w": True, "ip": []},
            {"n": "Item", "t": "Vs.Project", "w": False,
             "ip": [{"n": "index", "t": "System.Int32"}]},
        ],
        "meths": [
            {"n": "GetProcess", "o": [{"r": "Vs.Proc", "p": []}]},
            {"n": "Open", "o": [
                {"r": "Vs.Project", "p": [{"n": "path", "t": "System.String"}]},
                {"r": "Vs.Project", "p": [{"n": "path", "t": "System.IO.FileInfo"}]},
            ]},
        ],
    }
    NAMES = {"Vs.Portal": "Portal", "Vs.ProjectSet": "ProjectSet",
             "Vs.Project": "Project", "Vs.Proc": "Proc"}

    def test_cross_references_and_metadata(self):
        source = render_type_module(self.DESC, "Portal", self.NAMES, {"Vs.Mode"},
                                    expression="[Vs.Portal]::new()")
        compile(source, "<Portal>", "exec")
        assert "if TYPE_CHECKING:" in source
        assert "    from .ProjectSet import ProjectSet" in source
        assert "    from .Proc import Proc" in source
        assert "    from .Portal import Portal" not in source     # never imports itself
        assert "    def Projects(self) -> ProjectSet: ..." in source
        assert "    def Mode(self) -> str: ..." in source
        assert "    def Mode(self, value: str) -> None: ..." in source
        assert "    def Item(self, index: int) -> Project: ..." in source
        assert "    def GetProcess(self) -> Proc: ..." in source
        assert "    def Open(self, path: str) -> Project: ..." in source
        assert "    def Open(self, path: Any) -> Project: ..." in source
        assert "__ps_expression__: ClassVar[str] = '[Vs.Portal]::new()'" in source
        assert "__ps_assembly__: ClassVar[str] = 'C:/sdk/Vs.dll'" in source
        assert "def ps_origin(self) -> str: ..." in source

    def test_non_root_has_no_expression(self):
        source = render_type_module(self.DESC, "Portal", self.NAMES, set())
        assert "__ps_expression__" not in source

    def test_parse_type_graph_skips_noise(self):
        out = 'noise\n{"tn": "A", "props": []}\n\n{"tn": "B", "isEnum": true}\n{bad json'
        parsed = parse_type_graph(out)
        assert [d["tn"] for d in parsed] == ["A", "B"]


_GRAPH_CS = (
    "using System.Collections.Generic; "
    "namespace VsOther { public class Extra { public int X { get { return 1; } } } } "
    "namespace VsGraph { "
    "public enum Mode { A, B } "
    "public class Proc { public int Id { get { return 42; } } } "
    "public class Project { public string Name; public ProjectSet Parent; "
    "  public Project(string n) { Name = n; } } "
    "public class ProjectSet { List<Project> _items = new List<Project>(); "
    '  public ProjectSet() { _items.Add(new Project("first")); } '
    "  public int Count { get { return _items.Count; } } "
    "  public Project this[int index] { get { return _items[index]; } } "
    "  public Project Open(string path) { var p = new Project(path); _items.Add(p); return p; } "
    "  public IEnumerable<Project> All() { return _items; } } "
    "public class Portal { ProjectSet _set = new ProjectSet(); "
    "  public ProjectSet Projects { get { return _set; } } "
    "  public Proc GetProcess() { return new Proc(); } "
    "  public static Proc[] GetProcesses() { return new Proc[] { new Proc() }; } "
    "  public static int Version { get { return 7; } } "
    "  public T GetService<T>() where T : new() { return new T(); } "
    "  public T Echo<T>(T value) { return value; } "
    "  public static T Default<T>() { return default(T); } "
    "  public VsOther.Extra Extra { get { return new VsOther.Extra(); } } "
    "  public Mode Mode { get { return Mode.B; } } } }"
)


@pytest.fixture(scope="module")
def shell(edition):
    from virtualshell import Shell

    sh = Shell(timeout_seconds=60, powershell_edition=edition).start()
    yield sh
    sh.stop(force=True)


@pytest.fixture(scope="module")
def graph_dll(shell, tmp_path_factory):
    dll = tmp_path_factory.mktemp("vsgraph") / "VsGraph.dll"
    res = shell.run(f"Add-Type -TypeDefinition '{_GRAPH_CS}' -OutputAssembly '{dll}'", timeout=120)
    if not res.success or not dll.exists():
        pytest.skip(f"Add-Type -OutputAssembly unavailable here: {res.err[:200]}")
    shell.run(f"[Reflection.Assembly]::LoadFrom('{dll}') | Out-Null", raise_on_error=True)
    return dll


def _import_package(tmp_path, name):
    sys.path.insert(0, str(tmp_path))
    try:
        return importlib.import_module(name)
    finally:
        sys.path.remove(str(tmp_path))


def _forget_package(name):
    for key in [k for k in sys.modules if k == name or k.startswith(name + ".")]:
        sys.modules.pop(key, None)


@integration
class TestGeneratePackage:
    def test_package_is_cross_annotated_and_usable(self, shell, edition, graph_dll, tmp_path):
        from virtualshell import Shell

        shell.run("$vs_portal = [VsGraph.Portal]::new()", raise_on_error=True)
        pkg = tmp_path / "vsgraph_sdk"
        written = shell.generate_psobject("$vs_portal", pkg, follow=True)
        assert {p.name for p in written} == \
            {"Portal.py", "ProjectSet.py", "Project.py", "Proc.py", "Extra.py", "__init__.py"}

        portal_src = (pkg / "Portal.py").read_text(encoding="utf-8")
        assert "    def Projects(self) -> ProjectSet: ..." in portal_src
        assert "    def GetProcess(self) -> Proc: ..." in portal_src
        assert "    def Mode(self) -> str: ..." in portal_src                 # enum
        # Static members are emitted too, tagged, and callable on the instance proxy.
        assert "    def GetProcesses(self) -> Sequence[Proc]: ...  # static" in portal_src
        assert "    def Version(self) -> int: ...  # static" in portal_src
        assert "ReferenceEquals" not in portal_src                          # Object's statics skipped
        assert "__ps_expression__: ClassVar[str] = '[VsGraph.Portal]::new()'" in portal_src
        set_src = (pkg / "ProjectSet.py").read_text(encoding="utf-8")
        assert "    def Item(self, index: int) -> Project: ..." in set_src
        assert "    def Open(self, path: str) -> Project: ..." in set_src
        assert "    def All(self) -> Sequence[Project]: ..." in set_src
        project_src = (pkg / "Project.py").read_text(encoding="utf-8")
        assert "    def Parent(self) -> ProjectSet: ..." in project_src
        assert "__ps_expression__" not in project_src                          # via a parent

        sdk = _import_package(tmp_path, "vsgraph_sdk")
        try:
            assert sdk.Portal.__ps_type_name__ == "VsGraph.Portal"
            assert sdk.Project.__ps_assembly__.lower() == str(graph_dll).lower()
            with Shell(timeout_seconds=60, powershell_edition=edition) as fresh:
                portal = fresh.make_proxy(sdk.Portal)             # loads VsGraph.dll, creates
                assert portal.GetProcess().Id == 42
                assert portal.Projects.Item(0).Name == "first"
                assert portal.Projects.Open("second").Parent is None
                assert portal.Projects.Count == 2
                assert portal.Mode == "B"
                # Sequence protocol on the proxy: what Sequence[Project] promises.
                projects = portal.Projects
                assert len(projects) == 2
                assert [p.Name for p in projects] == ["first", "second"]
                assert projects[-1].Name == "second"
                assert projects[1].ps_origin.endswith(".Item(1)")
                everything = portal.Projects.All()             # IEnumerable<Project>
                assert [p.Name for p in everything] == ["first", "second"]
                # Statics through the instance proxy, Python style.
                assert portal.Version == 7
                processes = portal.GetProcesses()
                assert len(processes) == 1 and processes[0].Id == 42

                # Generic methods: one round trip, MethodInfo cached per type.
                proc = portal.generic("GetService", sdk.Proc)()      # GetService<Proc>()
                assert proc.type_name == "VsGraph.Proc" and proc.Id == 42
                assert portal.generic("Echo", "System.String")("hei") == "hei"
                assert portal.generic("Echo", "[System.Int32]")(5) == 5
                assert portal.generic("Default", "System.Int32")() == 0   # static generic
                key = ("VsGraph.Portal", "GetService", ("VsGraph.Proc",), 0)
                assert key in fresh._generic_methods
                cached = fresh._generic_methods[key]
                assert portal.generic("GetService", sdk.Proc)().Id == 42
                assert fresh._generic_methods[key] == cached            # reused, not re-resolved
                with pytest.raises(ExecutionError, match="No generic method"):
                    portal.generic("Nope", "System.String")()
                with pytest.raises(ExecutionError, match="Type argument not found"):
                    portal.generic("Echo", "No.Such.Type")(1)

                # Bulk read of a collection in one round trip.
                rows = portal.Projects.proxy_select("Name", type="GetType().FullName")
                assert rows == [{"Name": "first", "type": "VsGraph.Project"},
                                {"Name": "second", "type": "VsGraph.Project"}]
                with pytest.raises(TypeError, match="parent"):
                    fresh.make_proxy(sdk.Project)                 # no creation expression
                bound = fresh.make_proxy(sdk.ProjectSet, f"{portal.ps_ref}.Projects")
                assert bound.Count == 2
        finally:
            _forget_package("vsgraph_sdk")

    def test_type_literal_root_max_types_and_include(self, shell, graph_dll, tmp_path):
        pkg = tmp_path / "vsgraph_root_only"
        written = shell.generate_psobject("[VsGraph.Portal]", pkg, follow=True, max_types=1,
                                          expression="[VsGraph.Portal]::new()")
        assert {p.name for p in written} == {"Portal.py", "__init__.py"}
        src = (pkg / "Portal.py").read_text(encoding="utf-8")
        assert "    def Projects(self) -> Any: ..." in src          # not followed -> Any
        assert "__ps_expression__: ClassVar[str] = '[VsGraph.Portal]::new()'" in src

        # include_namespaces: VsGraph (and nested namespaces) yes, VsOther no.
        pkg2 = tmp_path / "vsgraph_filtered"
        written2 = shell.generate_psobject("[VsGraph.Portal]", pkg2, follow=True,
                                           include_namespaces=["VsGraph"])
        names = {p.name for p in written2}
        assert names == {"Portal.py", "Project.py", "ProjectSet.py", "Proc.py", "__init__.py"}
        portal_src = (pkg2 / "Portal.py").read_text(encoding="utf-8")
        assert "    def Extra(self) -> Any: ..." in portal_src          # filtered out -> Any
        assert "__ps_expression__" not in portal_src

        # The root's own namespace is not implicitly included.
        pkg3 = tmp_path / "vsgraph_other_only"
        written3 = shell.generate_psobject("[VsGraph.Portal]", pkg3, follow=True,
                                           include_namespaces=["VsOther"])
        assert {p.name for p in written3} == {"Portal.py", "Extra.py", "__init__.py"}

    def test_generate_from_derived_proxy_records_origin(self, shell, graph_dll, tmp_path):
        shell.run("$vs_portal2 = [VsGraph.Portal]::new()", raise_on_error=True)
        portal = shell.make_proxy("", "$vs_portal2")
        pkg = tmp_path / "vsgraph_from_projects"
        shell.generate_psobject(portal.Projects, pkg, follow=True)
        src = (pkg / "ProjectSet.py").read_text(encoding="utf-8")
        assert "__ps_expression__: ClassVar[str] = '$vs_portal2.Projects'" in src
        assert "    def Item(self, index: int) -> Project: ..." in src
