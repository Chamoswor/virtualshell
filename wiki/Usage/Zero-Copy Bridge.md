# Zero-Copy Bridge

The Zero-Copy Bridge provides high-performance data transfer between Python and PowerShell using shared memory. This feature is **Windows-only** and requires the native `win_pwsh.dll`.

## Overview

Traditional data exchange between Python and PowerShell involves:
1. Serializing data to text/JSON
2. Sending via stdin/stdout
3. Deserializing on the other side

This approach has significant overhead for large data. The Zero-Copy Bridge eliminates most of this overhead by:
- Using **shared memory** for direct data access
- **Chunked transfers** for large data
- **Zero-copy reads** via memoryview in Python
- **Automatic CliXml serialization** for PowerShell objects
- **PSObject deserialization** in Python with full type preservation

## When to Use

✅ **Good use cases:**
- Transferring PowerShell objects to Python (Get-Process, Get-Service, etc.)
- Large binary data (images, files, arrays)
- High-frequency data exchange
- Performance-critical applications
- Large JSON/XML datasets

❌ **Not ideal for:**
- Small strings or simple values (use regular `shell.run()`)
- One-time transfers
- Cross-platform code (use standard Shell methods)
- Systems without admin rights to install DLL

## Basic Usage

### Setup

```python
from virtualshell import Shell, ZeroCopyBridge, PSObject

# Create shell and bridge (automatically generates unique channel name)
with Shell(timeout_seconds=60) as shell:
    with ZeroCopyBridge(shell, frame_mb=64, chunk_mb=4) as bridge:
        # Use bridge here
        pass
```

### PowerShell → Python (Recommended Pattern)

Send PowerShell objects to Python with automatic deserialization:

```python
from virtualshell import Shell, ZeroCopyBridge, PSObject

with Shell() as shell:
    # Create PowerShell object
    shell.run("""
    $myObject = [PSCustomObject]@{
        Name = 'TestServer'
        Status = 'Running'
        Memory = 1024MB
        CreatedAt = Get-Date
    }
    """)
    
    with ZeroCopyBridge(shell) as bridge:
        # Serialize PowerShell object to bytes
        bridge.serialize("myObject", out_var="bytes")
        
        # Send from PowerShell to Python (all-in-one)
        data = bridge.receive("bytes")
        
        # Parse to Python object
        obj = PSObject.from_bytes(data)
        
        # Access properties with full type preservation
        print(f"Name: {obj['Name']}")           # String
        print(f"Status: {obj['Status']}")       # String
        print(f"Memory: {obj['Memory']}")       # int (bytes)
        print(f"Created: {obj['CreatedAt']}")   # datetime object
```

### Python → PowerShell

Send bytes from Python to PowerShell:

```python
# Prepare data in Python
data = b"Hello from Python" * 1000

with ZeroCopyBridge(shell) as bridge:
    # Send to PowerShell (all-in-one)
    bridge.send(data, "myData")
    
    # Now $myData is available in PowerShell
    result = shell.run("$myData.Length")
    print(f"Bytes in PowerShell: {result.out}")
```

## PSObject - PowerShell Object Serialization & Deserialization

The `PSObject` class provides bidirectional conversion between PowerShell CliXml format and Python objects with full type preservation.

### Supported Types

| PowerShell Type | Python Type | Example |
|----------------|-------------|---------|
| `[string]` | `str` | `"Hello"` |
| `[int]`, `[long]` | `int` | `42` |
| `[double]`, `[float]` | `float` | `3.14` |
| `[bool]` | `bool` | `True` |
| `[datetime]` | `datetime` | `datetime(2025, 11, 8, tzinfo=...)` |
| `[hashtable]` | `dict` | `{'key': 'value'}` |
| `[array]` | `list` | `[1, 2, 3]` |
| `[PSCustomObject]` | `PSObject` | Nested object |
| `[guid]` | `str` | `"123e4567-e89b..."` |
| `[version]` | `str` | `"1.0.0"` |
| `$null` | `None` | `None` |

### Accessing Properties

```python
# Get property with type information
prop = obj.get_property("Name")
print(f"Name: {prop.value} (type: {prop.type})")

# Shorthand access (recommended)
name = obj["Name"]
status = obj["Status"]

# Check object type
print(f"PowerShell type: {obj.type_name}")
```

### Serializing Python Objects to PowerShell

You can serialize a `PSObject` back to CliXml bytes and send it to PowerShell:

```python
# Parse PowerShell object
obj = PSObject.from_bytes(data)

# Modify properties in Python
obj.properties["Name"].value = "UpdatedName"
obj.properties["Status"].value = "Modified"

# Serialize back to CliXml bytes
clixml_bytes = obj.to_bytes()

# Send to PowerShell
with ZeroCopyBridge(shell) as bridge:
    bridge.send(clixml_bytes, "modifiedObject")
    
    # Deserialize in PowerShell (bytes → object)
    bridge.deserialize("modifiedObject")
    
    # Now $modifiedObject is a full PowerShell object
    result = shell.run("$modifiedObject.Name")
    print(result.out)  # "UpdatedName"
```

### Real-World Examples

#### Windows Services

```python
with Shell() as shell:
    shell.run("""
    $services = Get-Service | Select-Object -First 5 | ForEach-Object {
        [PSCustomObject]@{
            Name = $_.Name
            DisplayName = $_.DisplayName
            Status = $_.Status.ToString()
            StartType = $_.StartType.ToString()
        }
    }
    """)
    
    with ZeroCopyBridge(shell) as bridge:
        bridge.serialize("services", out_var="bytes")
        data = bridge.receive("bytes")

        services_obj = PSObject.from_bytes(data) # Parse to Python
        services = services_obj["Items"][0] # First service object

        print(f"Service: {services['Name']}")
        print(f"Display: {services['DisplayName']}")
        print(f"Status: {services['Status']}")
```

#### System Information

```python
with Shell() as shell:
    shell.run("""
    $os = Get-CimInstance Win32_OperatingSystem
    $systemInfo = [PSCustomObject]@{
        OSName = $os.Caption
        Version = $os.Version
        BootTime = $os.LastBootUpTime
        TotalRAM = [long]$os.TotalVisibleMemorySize * 1KB
        Processes = (Get-Process).Count
    }
    """)
    
    with ZeroCopyBridge(shell) as bridge:
        bridge.serialize("systemInfo", out_var="bytes")
        data = bridge.receive("bytes")
        info = PSObject.from_bytes(data)
        
        # Access with full type preservation
        print(f"OS: {info['OSName']}")
        print(f"Version: {info['Version']}")
        print(f"Boot Time: {info['BootTime']}")  # datetime object!
        print(f"RAM: {info['TotalRAM'] / (1024**3):.2f} GB")
        print(f"Processes: {info['Processes']}")
```

## API Reference

### ZeroCopyBridge

```python
bridge = ZeroCopyBridge(
    shell,              # Shell instance
    frame_mb=64,        # Memory size per direction (MB)
    chunk_mb=4,         # Chunk size for transfers (MB)
    scope="Local"       # "Local" or "Global" scope
)
```

### Methods

#### `serialize(variable, *, depth=1, out_var=None, timeout=30.0)`

Serialize PowerShell object to CliXml bytes in-place.

```python
# Serialize $myObject to $bytes
bridge.serialize("myObject", out_var="bytes")

# With depth control for nested objects
bridge.serialize("complexObject", out_var="bytes", depth=3)
```

**Parameters:**
- `variable`: PowerShell variable name (with or without `$`)
- `depth`: Serialization depth (default: 1)
- `out_var`: Output variable name (default: overwrites input variable)
- `timeout`: Timeout in seconds

**Returns:** `bool` - True if successful

#### `deserialize(variable, *, out_var=None, timeout=30.0)`

Deserialize CliXml bytes in PowerShell variable back to PowerShell object.

```python
# Restore PowerShell object from bytes
bridge.deserialize("myBytes")

# Or store in different variable
bridge.deserialize("myBytes", out_var="restoredObject")
```

**Parameters:**
- `variable`: PowerShell variable name containing CliXml bytes
- `out_var`: Output variable name (default: overwrites input variable)
- `timeout`: Timeout in seconds

**Returns:** `bool` - True if successful

#### `receive(variable, *, timeout=30.0, return_memoryview=False)`

Receive variable from PowerShell to Python (all-in-one operation).

```python
# Receive as bytes (default)
data = bridge.receive("myData")

# Receive as memoryview (zero-copy)
data_view = bridge.receive("myData", return_memoryview=True)
```

**Parameters:**
- `variable`: PowerShell variable name
- `timeout`: Timeout in seconds
- `return_memoryview`: If True, return memoryview (zero-copy)

**Returns:** `bytes` or `memoryview`

#### `send(data, variable, *, chunk_size=None, timeout=30.0)`

Send bytes from Python to PowerShell (all-in-one operation).

```python
# Send data to PowerShell variable
bridge.send(b"Hello PowerShell", "myData")

# Now $myData is available in PowerShell
```

**Parameters:**
- `data`: Bytes to send
- `variable`: PowerShell variable name to create
- `chunk_size`: Chunk size in bytes (default: from `chunk_mb`)
- `timeout`: Timeout in seconds

### PSObject

```python
# Parse CliXml bytes to Python object
obj = PSObject.from_bytes(data)

# Access properties
name = obj["PropertyName"]          # Shorthand
prop = obj.get_property("PropertyName")  # Full property info

# Inspect object
print(obj.type_name)                # PowerShell type name
print(obj.properties)               # Dict of all properties

# Serialize back to CliXml bytes
clixml_bytes = obj.to_bytes()

# Send to PowerShell and deserialize
bridge.send(clixml_bytes, "restoredBytes")
bridge.deserialize("restoredBytes")  # Now it's a PowerShell object again
```

## Advanced Patterns

### Python ↔ PowerShell Object Round-Trip

Complete example showing how to modify PowerShell objects in Python and send them back:

```python
from virtualshell import Shell, ZeroCopyBridge, PSObject

with Shell() as shell:
    # Create PowerShell object
    shell.run("""
    $user = [PSCustomObject]@{
        Name = 'John Doe'
        Age = 30
        Active = $true
        Roles = @('Admin', 'User')
    }
    """)
    
    with ZeroCopyBridge(shell) as bridge:
        # Send from PowerShell to Python
        bridge.serialize("user", out_var="userBytes")
        data = bridge.receive("userBytes")
        
        # Parse and modify in Python
        user = PSObject.from_bytes(data)
        user.properties["Name"].value = "Jane Smith"
        user.properties["Age"].value = 35
        user.properties["Roles"].value.append("Manager")
        
        # Serialize and send back to PowerShell
        modified_bytes = user.to_bytes()
        bridge.send(modified_bytes, "modifiedUserBytes")
        
        # Deserialize in PowerShell
        bridge.deserialize("modifiedUserBytes", out_var="modifiedUser")
        
        # Verify in PowerShell
        result = shell.run("""
        Write-Output "Name: $($modifiedUser.Name)"
        Write-Output "Age: $($modifiedUser.Age)"
        Write-Output "Roles: $($modifiedUser.Roles -join ', ')"
        """)
        print(result.out)
```

### Complete Workflow Example

```python
from virtualshell import Shell, ZeroCopyBridge, PSObject
from datetime import datetime

with Shell(timeout_seconds=60) as shell:
    # Gather data in PowerShell
    shell.run("""
    $data = Get-Process | Select-Object -First 10 | ForEach-Object {
        [PSCustomObject]@{
            Name = $_.Name
            PID = $_.Id
            Memory = $_.WorkingSet64
            StartTime = $_.StartTime
        }
    }
    """)
    
    with ZeroCopyBridge(shell, frame_mb=64, chunk_mb=4) as bridge:
        # Serialize to CliXml bytes
        bridge.serialize("data", out_var="bytes")
        
        # Transfer to Python
        data = bridge.receive("bytes")
        
        # Parse to Python object
        processes_obj = PSObject.from_bytes(data)
        processes = processes_obj["Items"]

        # Print process information
        for proc in processes:
            if isinstance(proc, PSObject):
                print(f"Process: {proc['Name']}")
                print(f"  PID: {proc['PID']}")
                mem_mb = int(proc['Memory']) / 1024 / 1024
                print(f"  Memory: {mem_mb:.1f} MB")
                start_time = proc['StartTime']
                if isinstance(start_time, datetime):
                    print(f"  Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
                else:
                    print(f"  Started: N/A (system process)")
            print()
```

### Large File Transfer

```python
from virtualshell import Shell, ZeroCopyBridge
from pathlib import Path

#  Create a large binary file in Python
data = b'0123456789ABCDEF' * 65536 * 32  # ~32 MB
Path("large_file.bin").write_bytes(data)


with Shell(timeout_seconds=120, set_UTF8=True, strip_results=True) as shell:
# Read file in Python
    file_data = Path("large_file.bin").read_bytes()

    with ZeroCopyBridge(shell) as bridge:
        # Send to PowerShell
        bridge.send(file_data, "fileData")
        
        # Save in PowerShell
        shell.run("[IO.File]::WriteAllBytes('output.bin', $fileData)")
```

### Transferring Modified PowerShell Objects

```python
from virtualshell import Shell, ZeroCopyBridge, PSObject

with Shell(timeout_seconds=60, set_UTF8=True, strip_results=True) as shell:
    # Get processes from PowerShell - explicitly select properties we want
    shell.run("""
    $procs = Get-Process | Select-Object -First 5 -Property Name, Id, WorkingSet64
    """)

    with ZeroCopyBridge(shell) as bridge:
        # Transfer to Python
        bridge.serialize("procs", out_var="procBytes")
        data = bridge.receive("procBytes")
        procs = PSObject.from_bytes(data)

        for proc in procs["Items"]:
            memory_mb = proc["WorkingSet64"] / (1024 * 1024)
            
            proc.properties["MemoryMB"] = PSObject.Property(
                name="MemoryMB",
                type=float,
                value=round(memory_mb, 2)
            )
        
        # Send modified object back to PowerShell
        modified_bytes = procs.to_bytes()
        bridge.send(modified_bytes, "modifiedProcs")
        bridge.deserialize("modifiedProcs")
        
        # Use in PowerShell
        result = shell.run("$modifiedProcs | Format-Table Name, MemoryMB")
        print(result.out)
```

### Nested Objects with Depth Control

```python
from virtualshell import Shell, ZeroCopyBridge, PSObject

with Shell(timeout_seconds=60, set_UTF8=True, strip_results=True) as shell:
    # Get processes from PowerShell - explicitly select properties we want
    shell.run("""
    $complex = [PSCustomObject]@{
        Level1 = [PSCustomObject]@{
            Level2 = [PSCustomObject]@{
                Level3 = 'Deep value'
            }
        }
    }
    """)

    with ZeroCopyBridge(shell) as bridge:
        # Serialize with depth 3 to capture all levels
        bridge.serialize("complex", out_var="bytes", depth=3)
        data = bridge.receive("bytes")
        obj = PSObject.from_bytes(data)
        print(obj)
        # Access nested properties
        level1 = obj["Level1"]
        if isinstance(level1, PSObject):
            level2 = level1["Level2"]
            if isinstance(level2, PSObject):
                value = level2["Level3"]
                print(f"Value: {value}")
```

## Performance Tips

### 1. Use Memoryview for Zero-Copy

```python
# Zero-copy (fastest) - no data duplication
data = bridge.receive("myData", return_memoryview=True)
size = len(data)

# Creates copy (slower for large data)
data = bridge.receive("myData", return_memoryview=False)
```

### 2. Adjust Chunk Size

```python
# Small chunks (1-4 MB) - better for many small transfers
bridge = ZeroCopyBridge(shell, chunk_mb=2)

# Large chunks (16-64 MB) - better for large single transfers
bridge = ZeroCopyBridge(shell, chunk_mb=32)
```

### 3. Control Serialization Depth

```python
# Shallow (depth=1) - faster, less data
bridge.serialize("obj", depth=1)  # Default

# Deep (depth=3+) - slower, captures all nested objects
bridge.serialize("complexObj", depth=3)
```

### 4. Reuse Bridge Instances

```python
# Good - create once, use many times
with ZeroCopyBridge(shell) as bridge:
    for item in items:
        bridge.send(item, "data")
        result = shell.run("Process-Data $data")

# Avoid - creating new bridges is expensive
for item in items:
    with ZeroCopyBridge(shell) as bridge:  # Slow!
        bridge.send(item, "data")
```

## Error Handling

```python
try:
    with ZeroCopyBridge(shell) as bridge:
        # Serialize - may fail for non-serializable types
        if not bridge.serialize("myObj", out_var="bytes"):
            print("Failed to serialize object")
            exit(1)
        
        # Transfer - may timeout or fail
        data = bridge.receive("bytes", timeout=30.0)
        
        # Parse - may fail for invalid CliXml
        obj = PSObject.from_bytes(data)
        
        # Modify and serialize back
        obj.properties["Status"].value = "Updated"
        modified_bytes = obj.to_bytes()
        
        # Send back to PowerShell
        bridge.send(modified_bytes, "modifiedBytes")
        
        # Deserialize in PowerShell
        if not bridge.deserialize("modifiedBytes"):
            print("Failed to deserialize in PowerShell")
            exit(1)
        
except ValueError as e:
    print(f"Serialization or parsing error: {e}")
except TimeoutError:
    print("Transfer timed out - increase timeout or reduce data size")
except RuntimeError as e:
    print(f"Bridge error: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")
```

## Limitations

- **Windows only** - Requires `win_pwsh.dll`
- **Same machine** - Cannot transfer between remote machines
- **Memory constraints** - Frame size limits maximum transfer size
- **Serialization limits** - Some PowerShell types cannot be serialized (COM objects, FileStreams, PSCredentials, etc.)
- **Execution policy** - Automatically set to Bypass (may conflict with strict policies)

## Troubleshooting

### Serialization Fails

```python
# Check if object can be serialized
if not bridge.serialize("myObj", out_var="bytes"):
    print("Object cannot be serialized - check type")
    # Try simpler object or Select-Object specific properties
```

### Timeout Errors

```python
# Increase timeout for large data
data = bridge.receive("largeData", timeout=120.0)  # 2 minutes

# Or increase chunk size
bridge = ZeroCopyBridge(shell, chunk_mb=32)
```

### "Data exceeds frame size"

```python
# Increase frame size for very large transfers
bridge = ZeroCopyBridge(shell, frame_mb=256)  # 256 MB per direction
```

### DateTime Parsing Issues

```python
# DateTime is automatically parsed to Python datetime with timezone
dt = obj["Timestamp"]
if isinstance(dt, datetime):
    print(f"Time: {dt.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Timezone: {dt.tzinfo}")
```
