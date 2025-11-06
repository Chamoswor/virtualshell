import os
import time
from virtualshell import Shell, SharedMemoryBridge, SharedMemoryChannel

N = 75 * 1024 * 1024
payload = os.urandom(N)

with Shell(timeout_seconds=120, set_UTF8=True) as shell:
    bridge = SharedMemoryBridge(shell, default_frame_bytes=N)
    setattr(shell, "_vshell_shared_memory_loaded", False)
    setattr(shell, "_vshell_shared_memory_bridge_loaded", False)
    bridge._ensure_ready()

    pub = bridge.publish(payload, variable="global:Buf", format="bytes")
    print("publish_seq", pub.sequence)

    name = bridge._make_channel_name()
    channel = SharedMemoryChannel(name, pub.frame_bytes)
    try:
        ps_cmd = (
            "Export-SharedMemoryVarBytes "  # <-- Spesifikk funksjon
            f"-ChannelName '{name}' "
            f"-FrameBytes {pub.frame_bytes} "
            "-VariableName 'global:Buf' "  # <-- Direkte variabel-referanse
        )
        t0 = time.perf_counter()
        res = shell.run(ps_cmd, raise_on_error=True)
        t1 = time.perf_counter()
        print("shell.run ms", (t1 - t0) * 1000, "exit", res.exit_code)
        print("res.err len", len(res.err or ""))
        print(res.err)

        type_check = shell.run(
            (
                "${tmpSB} = [ScriptBlock]::Create('$global:Buf'); "
                "$tmpResult = ${tmpSB}.InvokeReturnAsIs(); "
                "$tmpResult.GetType().FullName`n$($tmpResult.GetType().IsArray)`n$($tmpResult.Length)"
            ),
            raise_on_error=True,
        )
        print("InvokeReturnAsIs details:", type_check.out.strip())

        t2 = time.perf_counter()
        seq = bridge._wait_for_powershell(channel)
        t3 = time.perf_counter()
        length = int(channel.get_powershell_length())
        print("wait ms", (t3 - t2) * 1000, "seq", seq, "len", length)

        buf = bytearray(length)
        t4 = time.perf_counter()
        channel.read_into_powershell(seq - 1, buf)
        t5 = time.perf_counter()
        print("read ms", (t5 - t4) * 1000)
    finally:
        del channel
