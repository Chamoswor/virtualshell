import os
import json
from pathlib import Path
from virtualshell import Shell, SharedMemoryBridge

N = 75 * 1024 * 1024
payload = os.urandom(N)

with Shell(timeout_seconds=120, set_UTF8=True) as shell:
    bridge = SharedMemoryBridge(shell, default_frame_bytes=N)
    setattr(shell, "_vshell_shared_memory_loaded", False)
    setattr(shell, "_vshell_shared_memory_bridge_loaded", False)
    bridge._ensure_ready()

    pub = bridge.publish(payload, variable="global:Buf", format="bytes")
    script = Path("bench/profile_export.ps1").resolve()

    res = shell.script(
        script,
        args={
            "ChannelName": pub.channel_name,
            "FrameBytes": str(pub.frame_bytes),
            "VariableName": "global:Buf",
            "Encoding": "utf8",
            "Mode": "Export",
        },
        raise_on_error=True,
    )

    print("profile_export exit:", res.exit_code)
    print("profile_export stdout:")
    print(res.out)
    print("profile_export stderr:")
    print(res.err)
