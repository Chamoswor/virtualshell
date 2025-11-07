from virtualshell import Shell, FastBridge, ZeroCopyBuffer
import time, os

print("=" * 70)
print("FastBridge - Python → PowerShell Large Data Transfer")
print("=" * 70)
print()

with Shell(timeout_seconds=200, set_UTF8=True) as sh: 
    data_size_mb = 10
    frame_mb = 256  # 10% larger than data - simple solution!
    
    bridge = FastBridge(sh, channel_name="test_channel", frame_mb=frame_mb)

    data = bytes(range(256)) * 1000 * 4 * data_size_mb
    print(f"Sending {len(data):,} bytes ({len(data)/1024/1024:.1f} MB)...")
    print(f"Frame size: {frame_mb} MB")
    print()
    
    t0 = time.perf_counter()
    bridge.send(data, variable="bigdata", timeout=120, chunk_threshold_mb=9999)  # No chunking
    t1 = time.perf_counter()
    
    elapsed = (t1-t0)*1000
    throughput = (len(data)/1024/1024)/((t1-t0))
    
    print(f"✓ Sent in {elapsed:.1f} ms")
    print(f"✓ Throughput: {throughput:.1f} MB/s")
    print()
    
    # Verify in PowerShell
    res = sh.run("""
        if ($null -eq $global:bigdata) {
            Write-Host '✗ ERROR: Variable is NULL'
        } else {
            $sizeMB = [math]::Round($global:bigdata.Length/1MB, 2)
            Write-Host "✓ Variable received successfully!"
            Write-Host "  Length: $($global:bigdata.Length) bytes ($sizeMB MB)"
            Write-Host "  Type: $($global:bigdata.GetType().FullName)"
            Write-Host "  First 10 bytes: $($global:bigdata[0..9])"
            Write-Host "  Last 10 bytes: $($global:bigdata[($global:bigdata.Length-10)..($global:bigdata.Length-1)])"
        }
    """, raise_on_error=False, timeout=10)
    
    print(res.out)

    # Use different channel for receive to avoid conflicts
    bridge_recv = FastBridge(sh, channel_name="recv_channel", frame_mb=frame_mb)
    
    # Manually send from PowerShell, then receive in Python
    sh.run("""
        [FastBridge]::SendBytesNoWait('recv_channel', 576716800, $global:bigdata)
    """, raise_on_error=True, timeout=30)
    
    # Now try to receive (should already be waiting)
    get_data = bridge_recv._receive_single(timeout=30, format=None)
    with get_data as buf:
        print(f"Receiving back in Python...")
        t0 = time.perf_counter()
        length = buf.length
        t1 = time.perf_counter()
        
        elapsed = (t1-t0)*1000
        throughput = (length/1024/1024)/((t1-t0))
        
        print(f"✓ Received {length:,} bytes in {elapsed:.1f} ms")
        print(f"✓ Throughput: {throughput:.1f} MB/s")