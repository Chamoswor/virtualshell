# Zero-Copy Bridge - Clean Redesign

Complete redesign of the shared memory API with focus on simplicity, performance, and zero-copy transfers.

> **Platform note**
> The native zero-copy bridge currently ships with a Windows-only `win_pwsh.dll`. Python code imports cleanly on macOS/Linux, but trying to instantiate `ZeroCopyBridge` or run the performance harness will raise a friendly `RuntimeError` until non-Windows backends are implemented.

## Design Principles

1. **Python owns channel lifecycle** - PowerShell never closes channels
2. **Always-on chunking** - All transfers use configurable chunking
3. **True zero-copy** - Direct memory access via offsets and memoryview
4. **Type flexibility** - PowerShell objects serialized via C++/CLI

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Shared Memory Layout                      │
├─────────────────┬───────────────────┬───────────────────────┤
│  Header (192B)  │  PY→PS Region     │  PS→PY Region         │
│  - Metadata     │  (frame_bytes)    │  (frame_bytes)        │
│  - Chunk state  │                   │                       │
└─────────────────┴───────────────────┴───────────────────────┘

Events:
  PY→PS: py2ps:ready, py2ps:ack
  PS→PY: ps2py:ready, ps2py:ack
```

## API Overview

### Python API

```python
from virtualshell import Shell
from virtualshell.zero_copy_bridge_shell import ZeroCopyBridge

with Shell(timeout_seconds=60) as shell:
    with ZeroCopyBridge(shell, channel_name="my_channel", frame_mb=64, chunk_mb=4) as bridge:
        # Trigger PowerShell to send data asynchronously
        bridge.send_from_powershell("$myvar", use_job=False, timeout=30)

        # Receive bytes from PowerShell (zero-copy memoryview available)
        payload = bridge.receive(timeout=30.0, return_memoryview=True)

        # Send bytes back to PowerShell and store in $result
        bridge.receive_to_powershell("$result", use_job=False, timeout=30)
        bridge.send(b"hello world", timeout=30)
```

### PowerShell API

```powershell
# Import module
. .\src\virtualshell\zero_copy_bridge.ps1

# Receive from Python
Receive-VariableFromPython `
    -ChannelName "my_channel" `
    -VariableName "mydata" `
    -TimeoutSeconds 30

# Now $mydata contains the bytes

# Send variable to Python
$data = 1..1000
Send-VariableToPython `
    -ChannelName "my_channel" `
    -Variable $data `
    -ChunkSizeMB 4 `
    -TimeoutSeconds 30
```

## Features

### Chunking

All transfers are automatically chunked:
- **Configurable chunk size** (default: 4 MB)
- **Same channel reused** for all chunks (memory efficient)
- **Automatic chunk reassembly**
- **Progress tracking** via chunk index

### Zero-Copy

- **Python side**: Direct memoryview of shared memory (no copy!)
- **PowerShell side**: Marshal.Copy from shared memory (one copy only)
- **Offset-based addressing**: Works across process boundaries
- **No temporary buffers**: Data written once, read once

### Type Handling

PowerShell objects are serialized via C++/CLI:
- `byte[]` → Direct copy (fastest)
- `string` → UTF-8 encode
- Objects → CliXml serialize (preserves type info)

## File Structure

```
win_pwsh_dll/
├── include/
│   └── vs_shm.h              # C++ API header
├── src/
│   ├── vs_shm.cpp            # Core implementation
│   └── object_serializer.cpp # C++/CLI serialization
└── CMakeLists.txt            # Build config

src/virtualshell/
├── zero_copy_bridge_shell.py # Python bridge helper
└── zero_copy_bridge.ps1      # PowerShell bridge module
```

## Building

```bash
# Use new CMakeLists
cd win_pwsh_dll
cmake -B build -S . -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release

# DLL will be in: build/Release/win_pwsh.dll
```

## Testing

Run the async performance harness to validate both transfer directions:

```powershell
python test_performance_bridge.py --sizes 10,50,100 --timeout 120
```

For quick sanity checks during development:

```powershell
python test_perf_minimal.py
python test_ps_send_only.py
```

## Performance

Expected throughput:
- Small data (<1 MB): 50-100 MB/s
- Medium data (1-100 MB): 200-400 MB/s
- Large data (>100 MB): 300-500 MB/s

Chunking overhead:
- 4 MB chunks: ~5% overhead
- 16 MB chunks: ~2% overhead
- 64 MB chunks: ~1% overhead

## Migration from v1

Key differences:
- **Simpler API**: Fewer functions, clearer semantics
- **Python owns channel**: PowerShell never calls `VS_DestroyChannel`
- **Always chunked**: No special handling for large data
- **Consistent naming**: `Py2Ps` vs `Ps2Py` everywhere

## Implementation Notes

### Memory Safety

- **Atomic operations** for header fields
- **Mutex protection** for critical sections
- **Event signaling** for synchronization
- **No race conditions** on chunk boundaries

### Error Handling

Status codes:
- `VS_OK (0)`: Success
- `VS_TIMEOUT (1)`: Operation timed out
- `VS_ERR_INVALID (-1)`: Invalid arguments
- `VS_ERR_SYSTEM (-2)`: System error
- `VS_ERR_TOO_LARGE (-4)`: Data exceeds frame size

### Customization

Adjust defaults via parameters:
- `frame_mb`: Total memory per direction (default: 64 MB)
- `chunk_mb`: Chunk size (default: 4 MB)
- `timeout`: Operation timeout (default: 30 seconds)

## License

Same as parent project.
