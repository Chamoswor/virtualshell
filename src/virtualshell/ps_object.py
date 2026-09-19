"""CliXml parsing and serialization for PowerShell objects.

PSObject represents a PowerShell object parsed from CliXml
(the format produced by [System.Management.Automation.PSSerializer]).
"""
from __future__ import annotations

import base64
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Type

__all__ = ["PSObject"]


class PSObject:
    """Dataclass representing a PowerShell object.
    
    Supports parsing PowerShell CliXml-serialized objects.
    
    Example:
        ps_bytes = bridge.receive()
        ps_obj = PSObject.from_bytes(ps_bytes)
        name = ps_obj.get_property("Name").value
    """
    
    @dataclass
    class Property:
        name: str
        type: Type[Any]
        value: Any

    def __setitem__(self, name: str, value: Any) -> None:
        """Set property value by name (shorthand)."""
        if name in self.properties:
            self.properties[name].value = value
        else:
            self.properties[name] = PSObject.Property(name, type(value), value)

    def __init__(self, type_name: str, properties: List["PSObject.Property"]):
        self.type_name = type_name
        self.properties = {prop.name: prop for prop in properties}

    def get_property(self, name: str) -> Optional["PSObject.Property"]:
        """Get property by name."""
        return self.properties.get(name)
    
    def __getitem__(self, name: str) -> Any:
        """Get property value by name (shorthand)."""
        prop = self.properties.get(name)
        return prop.value if prop else None
    
    def __repr__(self) -> str:
        props = ", ".join(f"{k}={v.value!r}" for k, v in self.properties.items())
        return f"PSObject({self.type_name}, {props})"
    
    def to_bytes(self) -> bytes:
        """Serialize PSObject to CliXml bytes.
        
        Converts the PSObject back to PowerShell CliXml format.
        The resulting bytes can be sent to PowerShell and deserialized with:
        [System.Management.Automation.PSSerializer]::Deserialize($bytes)
        
        Returns:
            bytes: CliXml-serialized representation of the object
        
        Example:
            >>> obj = PSObject.from_bytes(data)
            >>> # Modify object...
            >>> obj_bytes = obj.to_bytes()
            >>> bridge.send(obj_bytes, "restoredObject")
        """
        import xml.etree.ElementTree as ET
        
        # Create root element
        root = ET.Element("Objs", {
            "Version": "1.1.0.1",
            "xmlns": "http://schemas.microsoft.com/powershell/2004/04"
        })
        
        # Create main object element
        obj_elem = ET.SubElement(root, "Obj", {"RefId": "0"})
        
        # Add type name(s)
        tn_elem = ET.SubElement(obj_elem, "TN", {"RefId": "0"})
        
        # For arrays, add all standard type hierarchy
        if "[]" in self.type_name or "Array" in self.type_name:
            ET.SubElement(tn_elem, "T").text = self.type_name
            if "Array" not in self.type_name:
                ET.SubElement(tn_elem, "T").text = "System.Array"
            if self.type_name != "System.Object":
                ET.SubElement(tn_elem, "T").text = "System.Object"
        else:
            # Regular object - just add the type name
            ET.SubElement(tn_elem, "T").text = self.type_name
            if self.type_name not in ("System.Object", "PSCustomObject"):
                ET.SubElement(tn_elem, "T").text = "System.Object"
        
        # Check if this is an array type with Items property
        if ("[]" in self.type_name or "Array" in self.type_name) and "Items" in self.properties:
            # Array type - serialize as LST directly under Obj
            lst_elem = ET.SubElement(obj_elem, "LST")
            items = self.properties["Items"].value
            
            if isinstance(items, list):
                for item in items:
                    item_type = type(item)
                    self._serialize_value(lst_elem, item, "", item_type)
        else:
            # Regular object - add properties in MS
            ms_elem = ET.SubElement(obj_elem, "MS")
            
            for prop_name, prop in self.properties.items():
                self._serialize_value(ms_elem, prop.value, prop_name, prop.type)
        
        # Convert to string with XML declaration
        xml_str = '<?xml version="1.0" encoding="utf-8"?>\n'
        xml_str += ET.tostring(root, encoding='unicode')
        
        return xml_str.encode('utf-8')
    
    @staticmethod
    def _serialize_value(parent: Any, value: Any, name: str, value_type: Type[Any]) -> None:
        """Serialize a single value to XML element.
        
        Args:
            parent: Parent XML element
            value: Value to serialize
            name: Property name
            value_type: Python type of the value
        """
        import xml.etree.ElementTree as ET
        from datetime import datetime
        
        # String
        if value_type == str or isinstance(value, str):
            ET.SubElement(parent, "S", {"N": name}).text = str(value) if value is not None else ""
        
        # Boolean
        elif value_type == bool or isinstance(value, bool):
            ET.SubElement(parent, "B", {"N": name}).text = "true" if value else "false"
        
        # Integer
        elif value_type == int or isinstance(value, int):
            if -2147483648 <= value <= 2147483647:
                ET.SubElement(parent, "I32", {"N": name}).text = str(value)
            else:
                ET.SubElement(parent, "I64", {"N": name}).text = str(value)
        
        # Float
        elif value_type == float or isinstance(value, float):
            ET.SubElement(parent, "Db", {"N": name}).text = str(value)
        
        # DateTime
        elif value_type == datetime or isinstance(value, datetime):
            # Format datetime to PowerShell format
            dt_str = value.isoformat()
            obj_elem = ET.SubElement(parent, "Obj", {"N": name, "RefId": "1"})
            ET.SubElement(obj_elem, "DT").text = dt_str
        
        # None/null
        elif value is None:
            ET.SubElement(parent, "Nil", {"N": name})
        
        # List/Array
        elif value_type == list or isinstance(value, list):
            obj_elem = ET.SubElement(parent, "Obj", {"N": name, "RefId": "1"})
            lst_elem = ET.SubElement(obj_elem, "LST")
            
            for item in value:
                item_type = type(item)
                PSObject._serialize_value(lst_elem, item, "", item_type)
        
        # Dict/Hashtable
        elif value_type == dict or isinstance(value, dict):
            obj_elem = ET.SubElement(parent, "Obj", {"N": name, "RefId": "1"})
            dct_elem = ET.SubElement(obj_elem, "DCT")
            
            for key, val in value.items():
                en_elem = ET.SubElement(dct_elem, "En")
                PSObject._serialize_value(en_elem, str(key), "Key", str)
                PSObject._serialize_value(en_elem, val, "Value", type(val))
        
        # Nested PSObject
        elif isinstance(value, PSObject):
            obj_elem = ET.SubElement(parent, "Obj", {"N": name, "RefId": "1"})
            
            # Add type name
            tn_elem = ET.SubElement(obj_elem, "TN", {"RefId": "0"})
            ET.SubElement(tn_elem, "T").text = value.type_name
            
            # Add properties
            ms_elem = ET.SubElement(obj_elem, "MS")
            for prop_name, prop in value.properties.items():
                PSObject._serialize_value(ms_elem, prop.value, prop_name, prop.type)
        
        # Bytes (Base64)
        elif value_type == bytes or isinstance(value, bytes):
            import base64
            b64_str = base64.b64encode(value).decode('ascii')
            ET.SubElement(parent, "BA", {"N": name}).text = b64_str
        
        # Fallback to string
        else:
            ET.SubElement(parent, "S", {"N": name}).text = str(value)
    
    @staticmethod
    def from_bytes(data: bytes) -> "PSObject":
        """Parse PowerShell CliXml-serialized bytes into PSObject.
        
        Args:
            data: Bytes from PowerShell (CliXml format)
        
        Returns:
            PSObject with parsed properties
        
        Raises:
            ValueError: If data is not valid CliXml
        """
        import xml.etree.ElementTree as ET
        
        try:
            # CliXml uses UTF-8 encoding
            xml_str = data.decode('utf-8')
            root = ET.fromstring(xml_str)
        except Exception as e:
            raise ValueError(f"Failed to parse CliXml: {e}") from e
        
        # CliXml structure: <Objs><Obj>...</Obj></Objs>
        # PowerShell uses namespace, so we need to handle it
        ns = {'ps': 'http://schemas.microsoft.com/powershell/2004/04'}
        
        # Find first object (with or without namespace)
        obj_elem = root.find(".//ps:Obj", ns)
        if obj_elem is None:
            # Try without namespace
            obj_elem = root.find(".//Obj")
        
        if obj_elem is None:
            raise ValueError("No PowerShell object found in CliXml")
        
        return PSObject._parse_object(obj_elem, ns)
    
    @staticmethod
    def _parse_object(obj_elem, ns: Optional[dict] = None) -> "PSObject":
        """Parse a single <Obj> element."""
        import xml.etree.ElementTree as ET
        
        if ns is None:
            ns = {}
        
        # Get type name from TN (TypeName) element
        type_name = "PSCustomObject"
        tn_elem = obj_elem.find("ps:TN", ns)
        if tn_elem is None:
            tn_elem = obj_elem.find("TN")
        if tn_elem is not None:
            # Get all type names (first one is the most specific)
            type_names = [t.text for t in tn_elem.findall("ps:T", ns) or tn_elem.findall("T") if t.text]
            if type_names:
                type_name = type_names[0]  # Use first (most specific) type
        
        # Check if this is an array type (has LST directly under Obj)
        lst_elem = obj_elem.find("ps:LST", ns)
        if lst_elem is None:
            lst_elem = obj_elem.find("LST")
        
        if lst_elem is not None and ("[]" in type_name or "Array" in type_name):
            # This is an array - parse items from LST and return as a special property
            items = []
            for item_elem in lst_elem:
                value, _ = PSObject._parse_value(item_elem, ns)
                items.append(value)
            
            # Return PSObject with special "Items" property containing the array
            properties = [PSObject.Property(
                name="Items",
                type=list,
                value=items
            )]
            return PSObject(type_name, properties)
        
        # Find <Props> or <MS> (MemberSet) element for regular objects
        props_elem = None
        for query in ("ps:MS", "MS", "ps:Props", "Props"):
            props_elem = obj_elem.find(query, ns)
            if props_elem is not None:
                break
        
        properties = []
        
        if props_elem is not None:
            # Parse each property
            for prop_elem in props_elem:
                # Remove namespace from tag
                tag = prop_elem.tag.split('}')[-1] if '}' in prop_elem.tag else prop_elem.tag
                
                prop_name = prop_elem.get("N")  # Property name
                if not prop_name:
                    continue
                
                # Parse value based on tag type
                prop_value, prop_type = PSObject._parse_value(prop_elem, ns)
                
                properties.append(PSObject.Property(
                    name=prop_name,
                    type=prop_type,
                    value=prop_value
                ))
        
        return PSObject(type_name, properties)
    
    @staticmethod
    def _parse_value(elem, ns: Optional[dict] = None) -> tuple[Any, Type[Any]]:
        """Parse property value from XML element.
        
        Returns:
            (value, type) tuple
        """
        from datetime import datetime
        
        if ns is None:
            ns = {}
        
        # Remove namespace from tag
        tag = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
        text = elem.text or ""
        
        # String
        if tag == "S":
            return text, str
        
        # Boolean
        elif tag == "B":
            return text.lower() == "true", bool
        
        # Integer types
        elif tag in ("I32", "I16", "I64", "U32", "U16", "U64", "By", "SByte"):
            try:
                return int(text), int
            except ValueError:
                return 0, int
        
        # Floating point
        elif tag in ("Sg", "Db", "D"):
            try:
                return float(text), float
            except ValueError:
                return 0.0, float
        
        # DateTime
        elif tag == "DT":
            try:
                # PowerShell DateTime format: 2025-11-08T10:30:45.1234567-05:00
                dt = datetime.fromisoformat(text.replace('Z', '+00:00'))
                return dt, datetime
            except (ValueError, AttributeError):
                return text, str
        
        # TimeSpan (duration)
        elif tag == "TS":
            try:
                # PowerShell TimeSpan format: P0DT0H0M5.123S or PT5.123S
                # For now, return as string (could parse to timedelta)
                return text, str
            except Exception:
                return text, str
        
        # Char
        elif tag == "C":
            return text[0] if text else '', str
        
        # Null
        elif tag == "Nil":
            return None, type(None)
        
        # Array/List
        elif tag == "LST":
            items = []
            for item_elem in elem:
                value, _ = PSObject._parse_value(item_elem, ns)
                items.append(value)
            return items, list
        
        # Nested object
        elif tag == "Obj":
            # Check if it's a DateTime with <DT> child (must check with namespace)
            dt_elem = elem.find("{http://schemas.microsoft.com/powershell/2004/04}DT")
            if dt_elem is None:
                dt_elem = elem.find("DT")
            
            if dt_elem is not None and dt_elem.text:
                try:
                    dt = datetime.fromisoformat(dt_elem.text.replace('Z', '+00:00'))
                    return dt, datetime
                except (ValueError, AttributeError):
                    pass  # Fall through to object parsing
            
            # Check if it's an array by looking for LST child
            lst_elem = elem.find("ps:LST", ns)
            if lst_elem is None:
                lst_elem = elem.find("LST")
            if lst_elem is not None:
                # It's an array - parse items from LST
                items = []
                for item_elem in lst_elem:
                    value, _ = PSObject._parse_value(item_elem, ns)
                    items.append(value)
                return items, list
            
            # Check if it's a dictionary/hashtable by looking for DCT child
            dct_elem = elem.find("ps:DCT", ns)
            if dct_elem is None:
                dct_elem = elem.find("DCT")
            if dct_elem is not None:
                # It's a hashtable - parse key-value pairs
                result = {}
                for entry in dct_elem.findall("ps:En", ns) or dct_elem.findall("En"):
                    key_elem = None
                    val_elem = None
                    
                    # Find key and value elements
                    for child in entry:
                        child_name = child.get("N")
                        if child_name == "Key":
                            key_elem = child
                        elif child_name == "Value":
                            val_elem = child
                    
                    if key_elem is not None and val_elem is not None:
                        key_value, _ = PSObject._parse_value(key_elem, ns)
                        value, _ = PSObject._parse_value(val_elem, ns)
                        result[str(key_value)] = value
                
                return result, dict
            
            # Regular nested object
            return PSObject._parse_object(elem, ns), PSObject
        
        # Reference (to another object)
        elif tag == "Ref":
            ref_id = elem.get("RefId", "")
            return f"<Ref:{ref_id}>", str
        
        # URI
        elif tag == "URI":
            return text, str
        
        # Version
        elif tag == "Version":
            return text, str
        
        # Guid
        elif tag == "G":
            return text, str
        
        # Base64 binary data
        elif tag == "BA":
            try:
                import base64
                return base64.b64decode(text), bytes
            except Exception:
                return text, str
        
        # ScriptBlock
        elif tag == "SBK":
            return text, str
        
        # SecureString (can't decrypt, return placeholder)
        elif tag == "SS":
            return "<SecureString>", str
        
        # Unknown type - return as string
        else:
            return text, str
        
    def to_dict(
        self,
        *,
        mode: str = "flat",
        include_none: bool = True,
        bytes_as: str = "base64",  # "base64" | "list"
        include_type: bool = False # adds __type per nested PSObject when mode="flat"
    ) -> Dict[str, Any]:
        """
        Convert PSObject to a JSON-friendly dict.

        modes:
          - "flat":   {"Name": "...", "Id": 1, ...}
          - "typed":  {"__type": "X.Y.Z", "props": { ... }}

        bytes_as:
          - "base64" (default): "AAEC..." string
          - "list":   [0,1,2,...]  (slightly larger JSON)
        """
        def jsonify_scalar(val: Any) -> Any:
            if isinstance(val, PSObject):
                return val.to_dict(
                    mode=mode, include_none=include_none,
                    bytes_as=bytes_as, include_type=include_type
                )
            if isinstance(val, datetime):
                return val.isoformat()
            if isinstance(val, bytes):
                if bytes_as == "list":
                    return list(val)
                return base64.b64encode(val).decode("ascii")
            if isinstance(val, (list, tuple, set)):
                return [jsonify_scalar(v) for v in val]
            if isinstance(val, dict):
                return {str(k): jsonify_scalar(v) for k, v in val.items()}
            # JSON doesn't support NaN/Inf well; convert to string if needed
            if isinstance(val, float) and (val != val or val in (float("inf"), float("-inf"))):
                return str(val)
            # basic JSON types pass through
            return val

        # Special-case array-like PSObject parsed as {"Items": [...]}
        is_array_like = ("[]" in getattr(self, "type_name", "")) or ("Array" in getattr(self, "type_name", ""))
        if is_array_like and "Items" in self.properties:
            arr = jsonify_scalar(self.properties["Items"].value)
            if mode == "typed":
                return {"__type": self.type_name, "items": arr}
            else:
                # flat mode returns bare list to embed nicely in JSON
                return arr

        # Regular object: build props dict
        props: Dict[str, Any] = {}
        for name, prop in self.properties.items():
            val = jsonify_scalar(prop.value)
            if val is None and not include_none:
                continue
            props[name] = val

        if mode == "typed":
            return {"__type": self.type_name, "props": props}
        else:
            if include_type:
                # helpful when you still want a flat dict but keep type info
                props["__type"] = self.type_name
            return props
