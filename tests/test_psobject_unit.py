"""Unit tests for PSObject CliXml parsing/serialization (pure Python)."""
from __future__ import annotations

from datetime import datetime

import pytest

from virtualshell.ps_object import PSObject

CLIXML_NS = 'xmlns="http://schemas.microsoft.com/powershell/2004/04"'


def _clixml(body: str) -> bytes:
    return (
        '<?xml version="1.0" encoding="utf-8"?>\n'
        f'<Objs Version="1.1.0.1" {CLIXML_NS}>{body}</Objs>'
    ).encode("utf-8")


SAMPLE = _clixml(
    '<Obj RefId="0">'
    '<TN RefId="0"><T>System.Management.Automation.PSCustomObject</T>'
    "<T>System.Object</T></TN>"
    "<MS>"
    '<S N="Name">test-æøå</S>'
    '<I32 N="Id">42</I32>'
    '<B N="Enabled">true</B>'
    '<Db N="Score">3.5</Db>'
    '<Nil N="Missing" />'
    '<BA N="Blob">AAEC</BA>'
    "</MS>"
    "</Obj>"
)


class TestFromBytes:
    def test_parses_scalar_properties(self):
        obj = PSObject.from_bytes(SAMPLE)
        assert obj.type_name == "System.Management.Automation.PSCustomObject"
        assert obj["Name"] == "test-æøå"
        assert obj["Id"] == 42
        assert obj["Enabled"] is True
        assert obj["Score"] == 3.5
        assert obj["Missing"] is None
        assert obj["Blob"] == b"\x00\x01\x02"

    def test_property_types(self):
        obj = PSObject.from_bytes(SAMPLE)
        assert obj.get_property("Id").type is int
        assert obj.get_property("Name").type is str
        assert obj.get_property("Enabled").type is bool

    def test_missing_property_returns_none(self):
        obj = PSObject.from_bytes(SAMPLE)
        assert obj["DoesNotExist"] is None
        assert obj.get_property("DoesNotExist") is None

    def test_nested_list(self):
        data = _clixml(
            '<Obj RefId="0"><TN RefId="0"><T>PSCustomObject</T></TN><MS>'
            '<Obj N="Values" RefId="1"><LST>'
            "<I32>1</I32><I32>2</I32><S>three</S>"
            "</LST></Obj>"
            "</MS></Obj>"
        )
        obj = PSObject.from_bytes(data)
        assert obj["Values"] == [1, 2, "three"]

    def test_nested_dict(self):
        data = _clixml(
            '<Obj RefId="0"><TN RefId="0"><T>PSCustomObject</T></TN><MS>'
            '<Obj N="Map" RefId="1"><DCT><En>'
            '<S N="Key">answer</S><I32 N="Value">42</I32>'
            "</En></DCT></Obj>"
            "</MS></Obj>"
        )
        obj = PSObject.from_bytes(data)
        assert obj["Map"] == {"answer": 42}

    def test_datetime(self):
        data = _clixml(
            '<Obj RefId="0"><TN RefId="0"><T>PSCustomObject</T></TN><MS>'
            '<DT N="When">2026-01-02T03:04:05</DT>'
            "</MS></Obj>"
        )
        obj = PSObject.from_bytes(data)
        assert obj["When"] == datetime(2026, 1, 2, 3, 4, 5)

    def test_invalid_xml_raises(self):
        with pytest.raises(ValueError):
            PSObject.from_bytes(b"not xml at all")

    def test_no_object_raises(self):
        with pytest.raises(ValueError):
            PSObject.from_bytes(_clixml("<S>just a string</S>"))


class TestRoundTrip:
    def test_to_bytes_from_bytes_round_trip(self):
        original = PSObject.from_bytes(SAMPLE)
        recycled = PSObject.from_bytes(original.to_bytes())
        assert recycled["Name"] == "test-æøå"
        assert recycled["Id"] == 42
        assert recycled["Enabled"] is True
        assert recycled["Score"] == 3.5
        assert recycled["Blob"] == b"\x00\x01\x02"

    def test_constructed_object_round_trip(self):
        obj = PSObject("System.Management.Automation.PSCustomObject", [
            PSObject.Property("Name", str, "fra-python"),
            PSObject.Property("Count", int, 3),
            PSObject.Property("Tags", list, ["a", "b"]),
        ])
        parsed = PSObject.from_bytes(obj.to_bytes())
        assert parsed["Name"] == "fra-python"
        assert parsed["Count"] == 3
        assert parsed["Tags"] == ["a", "b"]

    def test_setitem_updates_and_adds(self):
        obj = PSObject("PSCustomObject", [PSObject.Property("A", int, 1)])
        obj["A"] = 2
        obj["B"] = "new"
        assert obj["A"] == 2
        assert obj["B"] == "new"


class TestToDict:
    def test_flat_mode(self):
        obj = PSObject.from_bytes(SAMPLE)
        d = obj.to_dict()
        assert d["Name"] == "test-æøå"
        assert d["Id"] == 42
        assert d["Blob"] == "AAEC"  # bytes as base64 by default

    def test_typed_mode(self):
        obj = PSObject.from_bytes(SAMPLE)
        d = obj.to_dict(mode="typed")
        assert d["__type"] == "System.Management.Automation.PSCustomObject"
        assert d["props"]["Id"] == 42

    def test_exclude_none(self):
        obj = PSObject.from_bytes(SAMPLE)
        d = obj.to_dict(include_none=False)
        assert "Missing" not in d

    def test_bytes_as_list(self):
        obj = PSObject.from_bytes(SAMPLE)
        d = obj.to_dict(bytes_as="list")
        assert d["Blob"] == [0, 1, 2]


class TestPackageExports:
    def test_psobject_importable_from_package(self):
        import virtualshell
        assert virtualshell.PSObject is PSObject
