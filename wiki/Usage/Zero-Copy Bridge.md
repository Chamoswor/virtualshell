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
- **C++/CLI serialization** for PowerShell objects

## When to Use

✅ **Good use cases:**
- Transferring large binary data (images, files, arrays)
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
from virtualshell import Shell
from virtualshell.zero_copy_bridge_shell import ZeroCopyBridge

# Create a shell instance
shell = Shell(timeout_seconds=60)
shell.start()

# Create bridge (automatically loads PowerShell module and DLL)
bridge = ZeroCopyBridge(
    shell,
    channel_name="my_data_channel",  # Unique name for this channel
    frame_mb=64,                     # Memory per direction (default: 64 MB)
    chunk_mb=4,                      # Chunk size (default: 4 MB)
    scope="Local"                    # "Local" or "Global"
)
```

### Python → PowerShell

Send bytes from Python to PowerShell:

```python
# Prepare data in Python
data = b"Hello from Python" * 1000

# Start PowerShell receive (async)
future = bridge.receive_to_powershell("$myData", timeout=30.0)

# Send data
bridge.send(data, timeout=30.0)

# Wait for PowerShell to finish receiving
result = future.result(timeout=30.0)
print(f"PowerShell received: {result.success}")

# Now $myData is available in PowerShell
output = shell.run("$myData.Length")
print(f"Bytes in PowerShell: {output.out}")
```

### PowerShell → Python

Send data from PowerShell to Python:

```python
# Create data in PowerShell
shell.run("$testData = [byte[]]::new(1048576)")  # 1 MB

# Start PowerShell send (async)
future = bridge.send_from_powershell("$testData", timeout=30.0)

# Receive in Python (zero-copy with memoryview)
data = bridge.receive(timeout=30.0, return_memoryview=True)
print(f"Received {len(data)} bytes")

# Wait for PowerShell to complete
result = future.result(timeout=30.0)

# Convert memoryview to bytes if needed
data_bytes = bytes(data)
```

## Advanced Patterns

### Context Manager

Use context managers for automatic cleanup:

```python
with Shell(timeout_seconds=60) as shell:
    with ZeroCopyBridge(shell, channel_name="temp_channel") as bridge:
        # Transfer data
        future = bridge.receive_to_powershell("$data", timeout=10.0)
        bridge.send(b"test data", timeout=10.0)
        future.result()
        
        # Channel automatically cleaned up when exiting context
```

### Large File Transfer

Transfer files efficiently:

```python
from pathlib import Path

# Read file
file_path = Path("large_file.bin")
file_data = file_path.read_bytes()

# Send to PowerShell
future = bridge.receive_to_powershell("$fileData", timeout=120.0)
bridge.send(file_data, timeout=120.0)
future.result()

# Save in PowerShell
shell.run("[IO.File]::WriteAllBytes('C:\\output.bin', $fileData)")
```

### PowerShell Objects

Send PowerShell objects (automatically serialized):

```python
# Create complex PowerShell object
shell.run("""
$myObject = [PSCustomObject]@{
    Name = 'Test'
    Values = 1..1000
    Data = @{Key='Value'}
}
""")

# Send to Python
future = bridge.send_from_powershell("$myObject", timeout=30.0)
serialized_data = bridge.receive(timeout=30.0)
result = future.result()

# Data is CliXml serialized bytes
print(f"Received {len(serialized_data)} bytes of serialized object")

# Deserialize in PowerShell if needed
future = bridge.receive_to_powershell("$deserializedData", timeout=30.0)
bridge.send(serialized_data, timeout=30.0)
future.result()
```

### Batch Processing

Process multiple items efficiently:

```python
# Process multiple datasets
datasets = [b"data1" * 1000, b"data2" * 1000, b"data3" * 1000]

for i, data in enumerate(datasets):
    # Send each dataset
    future = bridge.receive_to_powershell(f"$dataset{i}", timeout=10.0)
    bridge.send(data, timeout=10.0)
    future.result()

# Process in PowerShell
result = shell.run("""
$results = @()
for ($i = 0; $i -lt 3; $i++) {
    $varName = "dataset$i"
    $data = Get-Variable -Name $varName -ValueOnly
    $results += $data.Length
}
$results -join ','
""")
print(f"Processed sizes: {result.out}")
```

## Performance Tips

### 1. Use Memoryview for Zero-Copy

Always use `return_memoryview=True` when you don't need to modify the data:

```python
# Zero-copy (fastest)
data = bridge.receive(return_memoryview=True)
size = len(data)  # No copy!

# Creates copy (slower for large data)
data = bridge.receive(return_memoryview=False)
```

### 2. Adjust Chunk Size

Match chunk size to your data patterns:

```python
# Small chunks (1-4 MB) - better for streaming
bridge = ZeroCopyBridge(shell, chunk_mb=2)

# Large chunks (16-64 MB) - better raw throughput
bridge = ZeroCopyBridge(shell, chunk_mb=32)
```

### 3. Reuse Channels

Create one bridge and reuse it:

```python
# Good - reuse bridge
bridge = ZeroCopyBridge(shell, channel_name="main")
for i in range(100):
    bridge.send(data)
    # ... receive ...

# Avoid - creating new bridges repeatedly
for i in range(100):
    bridge = ZeroCopyBridge(shell, channel_name=f"temp_{i}")  # Slow!
    bridge.send(data)
```

### 4. Use Appropriate Timeouts

Set realistic timeouts based on data size:

```python
# Small data (< 1 MB)
bridge.send(small_data, timeout=5.0)

# Large data (> 100 MB)
bridge.send(large_data, timeout=120.0)
```

### 5. Prefer byte[] in PowerShell

For maximum performance, use byte arrays:

```python
# Fast - byte array
shell.run("$data = [byte[]]::new(1000000)")
bridge.send_from_powershell("$data")

# Slower - objects require serialization
shell.run("$data = 1..1000000")
bridge.send_from_powershell("$data")
```

## Common Patterns

### Binary File I/O

```python
# Python -> PowerShell -> File
with open("input.bin", "rb") as f:
    data = f.read()

future = bridge.receive_to_powershell("$fileData")
bridge.send(data)
future.result()

shell.run("[IO.File]::WriteAllBytes('output.bin', $fileData)")

# File -> PowerShell -> Python
shell.run("$fileData = [IO.File]::ReadAllBytes('input.bin')")
future = bridge.send_from_powershell("$fileData")
data = bridge.receive()
future.result()

with open("output.bin", "wb") as f:
    f.write(data)
```

### Data Processing Pipeline

```python
import numpy as np

# Generate data in Python
array = np.random.bytes(10_000_000)

# Send to PowerShell for processing
future = bridge.receive_to_powershell("$rawData")
bridge.send(array)
future.result()

# Process in PowerShell
shell.run("""
$processedData = [byte[]]::new($rawData.Length)
for ($i = 0; $i -lt $rawData.Length; $i++) {
    $processedData[$i] = $rawData[$i] -bxor 0xFF
}
""")

# Get result back
future = bridge.send_from_powershell("$processedData")
result = bridge.receive()
future.result()
```

## Error Handling

Always handle potential errors:

```python
try:
    future = bridge.receive_to_powershell("$data", timeout=30.0)
    bridge.send(large_data, timeout=30.0)
    result = future.result(timeout=30.0)
    
    if not result.success:
        print(f"PowerShell error: {result.err}")
        
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
- **Type preservation** - Complex PowerShell objects serialize to CliXml (not native Python types)
- **Execution policy** - Automatically set to Bypass (may conflict with strict policies)

## Troubleshooting

### "Failed to create channel"

```python
# Ensure channel name is unique
bridge = ZeroCopyBridge(shell, channel_name=f"channel_{os.getpid()}")
```

### "Timeout waiting for PowerShell chunk"

```python
# Increase timeout
data = bridge.receive(timeout=120.0)  # 2 minutes

# Or reduce data size/increase chunk size
bridge = ZeroCopyBridge(shell, chunk_mb=16)
```

### "Data exceeds frame size"

```python
# Increase frame size
bridge = ZeroCopyBridge(shell, frame_mb=256)  # 256 MB per direction
```
