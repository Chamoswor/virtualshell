"""Zero-Copy Bridge - Integrated with Shell class

Design:
- Python owns channel lifecycle AND PowerShell session
- Shell class is used to control PowerShell side
- All transfers are chunked (configurable chunk size)
- Zero-copy via memoryview of shared memory
"""
import ctypes
import os
import time
from concurrent.futures import Future
from pathlib import Path
from typing import Any, List, Optional, TYPE_CHECKING, Type, Dict
from datetime import datetime
import base64
import json
from dataclasses import dataclass
from . import _globals as _g

if TYPE_CHECKING:
    from .shell import Shell

__all__ = ["ZeroCopyBridge", "PSObject"]

_IS_WINDOWS = os.name == "nt"

# =============================================================================
# DLL LOADING
# =============================================================================

def _load_win_dll() -> tuple[Optional[Path], Optional[ctypes.CDLL]]:
    """Attempt to load the Windows DLL, returning path and handle when available."""
    if not _IS_WINDOWS:
        return None, None

    try:
        return _g._VS_SHM_CPP_MODULE_PATH, ctypes.CDLL(str(_g._VS_SHM_CPP_MODULE_PATH))
    except OSError as exc:
        raise RuntimeError(f"Failed to load module from {_g._VS_SHM_CPP_MODULE_PATH}: {exc}") from exc

# Load DLL (Windows only)
_dll_path, _dll = _load_win_dll()

# Status codes
VS_OK = 0
VS_TIMEOUT = 1
VS_WOULD_BLOCK = 2
VS_ERR_INVALID = -1
VS_ERR_SYSTEM = -2
VS_ERR_BAD_STATE = -3
VS_ERR_TOO_LARGE = -4

# Function signatures
if _dll is not None:
    _dll.VS_CreateChannel.argtypes = [ctypes.c_wchar_p, ctypes.c_uint64]
    _dll.VS_CreateChannel.restype = ctypes.c_void_p

    _dll.VS_DestroyChannel.argtypes = [ctypes.c_void_p]
    _dll.VS_DestroyChannel.restype = None

    _dll.VS_BeginPy2PsTransfer.argtypes = [ctypes.c_void_p, ctypes.c_uint64, ctypes.c_uint64]
    _dll.VS_BeginPy2PsTransfer.restype = ctypes.c_int32

    _dll.VS_SendPy2PsChunk.argtypes = [
        ctypes.c_void_p, ctypes.c_uint32, ctypes.POINTER(ctypes.c_ubyte),
        ctypes.c_uint64, ctypes.c_uint32
    ]
    _dll.VS_SendPy2PsChunk.restype = ctypes.c_int32

    _dll.VS_WaitPy2PsAck.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
    _dll.VS_WaitPy2PsAck.restype = ctypes.c_int32

    _dll.VS_FinishPy2PsTransfer.argtypes = [ctypes.c_void_p]
    _dll.VS_FinishPy2PsTransfer.restype = ctypes.c_int32

    _dll.VS_WaitPs2PyChunk.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_uint32),
        ctypes.POINTER(ctypes.c_uint64),
        ctypes.POINTER(ctypes.c_uint64),
        ctypes.c_uint32
    ]
    _dll.VS_WaitPs2PyChunk.restype = ctypes.c_int32

    _dll.VS_AckPs2PyChunk.argtypes = [ctypes.c_void_p]
    _dll.VS_AckPs2PyChunk.restype = ctypes.c_int32

    _dll.VS_IsPs2PyComplete.argtypes = [ctypes.c_void_p]
    _dll.VS_IsPs2PyComplete.restype = ctypes.c_int32

    _dll.VS_GetMemoryBase.argtypes = [ctypes.c_void_p]
    _dll.VS_GetMemoryBase.restype = ctypes.c_void_p

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
        tn_elem = obj_elem.find("ps:TN", ns) or obj_elem.find("TN")
        if tn_elem is not None:
            # Get all type names (first one is the most specific)
            type_names = [t.text for t in tn_elem.findall("ps:T", ns) or tn_elem.findall("T") if t.text]
            if type_names:
                type_name = type_names[0]  # Use first (most specific) type
        
        # Check if this is an array type (has LST directly under Obj)
        lst_elem = obj_elem.find("ps:LST", ns) or obj_elem.find("LST")
        
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
        props_elem = obj_elem.find("ps:MS", ns) or obj_elem.find("MS") or obj_elem.find("ps:Props", ns) or obj_elem.find("Props")
        
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
            lst_elem = elem.find("ps:LST", ns) or elem.find("LST")
            if lst_elem is not None:
                # It's an array - parse items from LST
                items = []
                for item_elem in lst_elem:
                    value, _ = PSObject._parse_value(item_elem, ns)
                    items.append(value)
                return items, list
            
            # Check if it's a dictionary/hashtable by looking for DCT child
            dct_elem = elem.find("ps:DCT", ns) or elem.find("DCT")
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

# =============================================================================
# INTEGRATED BRIDGE WITH SHELL
# =============================================================================

class ZeroCopyBridge:
    """Zero-copy bridge that controls both Python and PowerShell sides.
    
    Uses Shell class to manage PowerShell session.
    
    Example:
        from virtualshell import Shell
        shell = Shell()
        bridge = ZeroCopyBridge(shell, channel_name="my_channel")
        
        # Send from PowerShell to Python
        bridge.send_from_powershell("$myvar")  # PowerShell sends in background
        data = bridge.receive()  # Python receives
        
        # Send from Python to PowerShell
        bridge.send(b"hello")  # Python sends in background
        bridge.receive_to_powershell("$result")  # PowerShell receives
    """
    
    def __init__(
        self,
        shell: "Shell",
        frame_mb: int = 64,
        chunk_mb: int = 4,
        scope: str = "Local"
    ):
        """Initialize bridge with Shell instance.
        
        Args:
            shell: Shell instance to use for PowerShell side
            channel_name: Channel name (without prefix)
            frame_mb: Frame size in MB (per direction)
            chunk_mb: Default chunk size in MB
            scope: "Local" or "Global"
        """
        dll = _dll
        if dll is None:
            raise RuntimeError("ZeroCopyBridge requires win_pwsh.dll and is only available on Windows")
        channel_name = f"vs_{os.getpid()}_{int(time.time())}"
        self.shell = shell
        self.channel_name = f"{scope}\\{channel_name}"
        self.channel_name_short = channel_name  # Without prefix
        self._scope = scope
        self.frame_bytes = frame_mb * 1024 * 1024
        self.default_chunk_bytes = chunk_mb * 1024 * 1024
        self._active_jobs = []  # Track active PowerShell jobs
        self._active_futures: List[Future[Any]] = []
        self._dll = dll
        
        # Create channel (Python owns it)
        self._handle = dll.VS_CreateChannel(self.channel_name, self.frame_bytes)
        if not self._handle:
            raise RuntimeError(f"Failed to create channel: {self.channel_name}")
        
        # Get shared memory base
        self._mem_base = dll.VS_GetMemoryBase(self._handle)
        if not self._mem_base:
            dll.VS_DestroyChannel(self._handle)
            raise RuntimeError("Failed to get shared memory base")
        
        self._mem_base_addr = self._mem_base if isinstance(self._mem_base, int) else self._mem_base.value
        
        # Load PowerShell bridge module
        bridge_script = Path(__file__).parent / "zero_copy_bridge.ps1"
        dll_path = _dll_path
        if dll_path is None:
            raise RuntimeError("win_pwsh.dll path is unavailable")
        
        # Store paths for job execution
        self._ps_script_path = str(bridge_script.absolute())
        self._ps_dll_path = str(dll_path.absolute())
        
        # Load PowerShell bridge script (set execution policy bypass for this session)
        self.shell.run("Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force")
        self.shell.run(f". '{bridge_script}'")
        init_cmd = f"Initialize-VSNative -PreferredPath '{self._ps_dll_path}'"
        self.shell.run(init_cmd)
    
    def __del__(self):
        # Clean up any active jobs
        if hasattr(self, '_active_jobs'):
            for job_id in self._active_jobs:
                try:
                    self.shell.run(f"Stop-Job -Id {job_id} -ErrorAction SilentlyContinue; Remove-Job -Id {job_id} -Force -ErrorAction SilentlyContinue")
                except:
                    pass
        
        if hasattr(self, '_active_futures'):
            for future in list(self._active_futures):
                future.cancel()
            self._active_futures.clear()

        if hasattr(self, '_handle') and self._handle and hasattr(self, "_dll"):
            try:
                self._dll.VS_DestroyChannel(self._handle)
            except Exception:
                pass
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        # Clean up jobs
        for job_id in self._active_jobs:
            try:
                self.shell.run(f"Stop-Job -Id {job_id} -ErrorAction SilentlyContinue; Remove-Job -Id {job_id} -Force -ErrorAction SilentlyContinue")
            except:
                pass
        self._active_jobs.clear()

        for future in self._active_futures:
            future.cancel()
        self._active_futures.clear()
        
        if self._handle:
            self._dll.VS_DestroyChannel(self._handle)
            self._handle = None

    def _track_future(self, future: Future[Any]) -> Future[Any]:
        self._active_futures.append(future)

        def _remove(_fut: Future[Any]) -> None:
            try:
                self._active_futures.remove(_fut)
            except ValueError:
                pass

        future.add_done_callback(_remove)
        return future
    
    def serialize(
        self,
        variable: str,
        *,
        depth: int = 1,
        out_var: Optional[str] = None,
        timeout: float = 30.0,
    ) -> bool:
        """Serialize PowerShell variable to CliXml bytes.
        
        Converts a PowerShell variable to CliXml format (as bytes) in-place.
        After calling this, the variable will contain byte[] instead of the original object.
        
        Args:
            variable: PowerShell variable name (with or without $)
            depth: Serialization depth (default: 1). Higher values capture more nested objects.
            timeout: Timeout in seconds
        
        Returns:
            Future that completes when serialization is done
        
        Raises:
            ValueError: If variable doesn't exist or cannot be serialized (when validate=True)
            RuntimeError: If serialization fails during execution
        
        Example:
            >>> bridge.serialize("mydata", depth=2)
            >>> future = bridge.send_from_powershell("$mydata")
        
        Note:
            Not all PowerShell objects can be serialized. The following types will fail:
            - COM objects (New-Object -ComObject)
            - Live .NET objects (FileStream, Thread, etc.)
            - PSCredential objects
            - Runspace/PSSession objects
        """
        var_name = variable.lstrip('$')
        out_name = out_var.lstrip('$') if out_var is not None else var_name
        
        cmd = [
            f"$xml_ = [System.Management.Automation.PSSerializer]::Serialize(${var_name}, {depth})",
            f"${out_name} = [System.Text.Encoding]::UTF8.GetBytes($xml_)",
        ]

        reses = self.shell.run(cmd, timeout=timeout)
        for res in reses:
            if res.exit_code != 0 and not res.err:
                return False
        return True
    
    def deserialize(
        self,
        variable: str,
        *,
        out_var: Optional[str] = None,
        timeout: float = 30.0,
    ) -> bool:
        """Deserialize CliXml bytes in PowerShell variable back to object.
        
        Converts a PowerShell variable containing CliXml bytes back to the original object.
        
        Args:
            variable: PowerShell variable name (with or without $)
            out_var: Output variable name (if None, overwrites input variable)
            timeout: Timeout in seconds
        Returns:
            True if successful, False otherwise
        Example:
            >>> bridge.deserialize("mydata")
            >>> # Now $mydata contains the original object
        """
        var_name = variable.lstrip('$')
        out_name = out_var.lstrip('$') if out_var is not None else var_name
        
        cmd = [
            f"$xml_ = [System.Text.Encoding]::UTF8.GetString(${var_name})",
            f"${out_name} = [System.Management.Automation.PSSerializer]::Deserialize($xml_)"
        ]

        reses = self.shell.run(cmd, timeout=timeout)
        for res in reses:
            if res.exit_code != 0 or res.err:
                return False
        return True
    # =========================================================================
    # POWERSHELL → PYTHON
    # =========================================================================

    def receive(
        self,
        variable: str,
        *,
        timeout: float = 30.0,
        return_memoryview: bool = False,
    ) -> bytes | memoryview:
        """Send variable from PowerShell to Python (all-in-one).
        
        Starts PowerShell send operation, receives data in Python, and waits for completion.
        This is a complete transfer operation - no need to call receive() separately.
        
        Args:
            variable: PowerShell variable to send (e.g., "$mydata" or "mydata")
            timeout: Timeout in seconds
            return_memoryview: If True, return memoryview (zero-copy), else bytes
        
        Returns:
            Received data as bytes or memoryview
        
        Example:
            >>> data = bridge.send_from_powershell("mydata")
            >>> obj = PSObject.from_bytes(data)
        """
        var_name = variable.lstrip('$')
        
        # Start async PowerShell send operation
        cmd = f"""
            Send-VariableToPython -ChannelName '{self.channel_name_short}' -Variable ${var_name} -ChunkSizeMB {self.default_chunk_bytes // (1024*1024)} -TimeoutSeconds {int(timeout)} -Scope '{self._scope}'
        """
        future = self.shell.run_async(cmd.strip(), timeout=timeout)
        
        # Receive data in Python (this blocks until transfer completes)
        bytes_data = self._receive_from_powershell(timeout=timeout, return_memoryview=True)
        
        # Wait for PowerShell command to finish
        result = future.result()
        if result.err:
            raise RuntimeError(f"PowerShell send failed: {result.err}")
        
        return bytes_data if return_memoryview else bytes(bytes_data)
    
    def _receive_from_powershell(
        self,
        *,
        timeout: float = 30.0,
        return_memoryview: bool = False
    ) -> bytes | memoryview:
        """Receive data from PowerShell (zero-copy).
        
        Args:
            timeout: Timeout in seconds
            return_memoryview: If True, return memoryview (zero-copy)
        
        Returns:
            bytes or memoryview of received data
        """
        dll = self._dll
        timeout_ms = int(timeout * 1000)
        chunks = []
        
        while True:
            chunk_index = ctypes.c_uint32()
            chunk_offset = ctypes.c_uint64()
            chunk_length = ctypes.c_uint64()
            
            result = dll.VS_WaitPs2PyChunk(
                self._handle,
                ctypes.byref(chunk_index),
                ctypes.byref(chunk_offset),
                ctypes.byref(chunk_length),
                timeout_ms
            )
            
            if result == VS_TIMEOUT:
                # PowerShell might have finished immediately after the last ACK
                if dll.VS_IsPs2PyComplete(self._handle):
                    break
                raise TimeoutError("Timeout waiting for PowerShell chunk")
            elif result != VS_OK:
                raise RuntimeError(f"VS_WaitPs2PyChunk failed: {result}")
            
            # Zero-copy read from shared memory
            ptr = ctypes.c_void_p(self._mem_base_addr + chunk_offset.value)
            c_array = ctypes.cast(ptr, ctypes.POINTER(ctypes.c_ubyte * chunk_length.value)).contents
            chunk_mv = memoryview(c_array)
            
            chunks.append(bytes(chunk_mv))
            
            # Acknowledge chunk
            result = dll.VS_AckPs2PyChunk(self._handle)
            if result != VS_OK:
                raise RuntimeError(f"VS_AckPs2PyChunk failed: {result}")
            
            # Check if complete
            is_complete = dll.VS_IsPs2PyComplete(self._handle)
            if not is_complete:
                poll_deadline = time.perf_counter() + min(0.2, timeout)
                while time.perf_counter() < poll_deadline:
                    if dll.VS_IsPs2PyComplete(self._handle):
                        is_complete = 1
                        break
                    time.sleep(0.001)
            if is_complete:
                break
        
        if not chunks:
            data = b""
        else:
            data = chunks[0] if len(chunks) == 1 else b"".join(chunks)
        
        return memoryview(data) if return_memoryview else data
    
    # =========================================================================
    # PYTHON → POWERSHELL
    # =========================================================================
    
    def send(
        self,
        data: bytes,
        variable: str,
        *,
        chunk_size: Optional[int] = None,
        timeout: float = 30.0
    ) -> None:
        """Send bytes from Python to PowerShell (all-in-one).
        
        Starts PowerShell receive operation, sends data from Python, and waits for completion.
        This is a complete transfer operation in the opposite direction of receive().
        
        Args:
            data: Bytes to send
            variable: PowerShell variable name to store data (e.g., "myvar" or "$myvar")
            chunk_size: Chunk size in bytes (default: self.default_chunk_bytes)
            timeout: Timeout in seconds
        
        Example:
            >>> bridge.send(b"Hello PowerShell", "mydata")
            >>> # Now $mydata contains the bytes in PowerShell
        """
        if not isinstance(data, bytes):
            raise TypeError("data must be bytes")

        var_name = variable.lstrip('$')
        
        # Start async PowerShell receive operation
        future = self._receive_to_powershell_async(var_name, timeout=timeout)

        chunk_bytes = chunk_size or self.default_chunk_bytes
        timeout_ms = int(timeout * 1000)
        
        # Begin transfer
        dll = self._dll
        result = dll.VS_BeginPy2PsTransfer(self._handle, len(data), chunk_bytes)
        if result != VS_OK:
            raise RuntimeError(f"VS_BeginPy2PsTransfer failed: {result}")
        
        # Send chunks
        num_chunks = (len(data) + chunk_bytes - 1) // chunk_bytes
        
        for i in range(num_chunks):
            offset = i * chunk_bytes
            chunk_len = min(chunk_bytes, len(data) - offset)
            chunk_data = data[offset:offset + chunk_len]
            
            c_array = (ctypes.c_ubyte * len(chunk_data)).from_buffer_copy(chunk_data)
            
            result = dll.VS_SendPy2PsChunk(
                self._handle, i, c_array, chunk_len, timeout_ms
            )
            if result != VS_OK:
                raise RuntimeError(f"VS_SendPy2PsChunk failed at chunk {i}: {result}")
            
            result = dll.VS_WaitPy2PsAck(self._handle, timeout_ms)
            if result == VS_TIMEOUT:
                raise TimeoutError(f"Timeout waiting for PowerShell ACK at chunk {i}")
            elif result != VS_OK:
                raise RuntimeError(f"VS_WaitPy2PsAck failed at chunk {i}: {result}")
        
        # Mark complete
        result = dll.VS_FinishPy2PsTransfer(self._handle)
        if result != VS_OK:
            raise RuntimeError(f"VS_FinishPy2PsTransfer failed: {result}")
        
        # Wait for PowerShell to complete receive
        future_res = future.result(timeout=5.0)
        if future_res.err:
            raise RuntimeError(f"PowerShell receive failed: {future_res.err}")
    
    def _receive_to_powershell_async(
        self,
        variable: str,
        *,
        timeout: float = 30.0,
    ) -> Future[Any]:
        """Internal: Start PowerShell receive operation asynchronously.
        
        Args:
            variable: PowerShell variable name (without $)
            timeout: Timeout in seconds
        
        Returns:
            Future representing the receive operation
        """
        # Use run_async completion future instead of spawning jobs
        cmd = f"""
            Receive-VariableFromPython -ChannelName '{self.channel_name_short}' -VariableName '{variable}' -TimeoutSeconds {int(timeout)} -Scope '{self._scope}'
        """
        command = cmd.strip()
        future = self.shell.run_async(command, timeout=timeout)
        return self._track_future(future)

    @property
    def handle(self) -> Any:
        """Get channel handle."""
        return self._handle
    
    @property
    def memory_base(self) -> Any:
        """Get shared memory base address."""
        return self._mem_base_addr
