#!/usr/bin/env python3
"""
Gather system information relevant for Snakemake profiling.
"""

import platform
import os
import subprocess
import json
from pathlib import Path
from datetime import datetime


def run_command(cmd):
    """Run a shell command and return output."""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            return result.stdout.strip()
    except Exception:
        pass
    return None


def get_cpu_info():
    """Get CPU information."""
    info = {
        "processor": platform.processor() or "Unknown",
        "architecture": platform.machine(),
        "python_cpu_count": os.cpu_count(),
    }

    # macOS specific
    if platform.system() == "Darwin":
        # Get detailed CPU info
        sysctl_info = run_command("sysctl -n machdep.cpu.brand_string")
        if sysctl_info:
            info["model"] = sysctl_info

        # Get performance cores vs efficiency cores (Apple Silicon)
        perf_cores = run_command("sysctl -n hw.perflevel0.physicalcpu")
        eff_cores = run_command("sysctl -n hw.perflevel1.physicalcpu")
        if perf_cores and eff_cores:
            info["performance_cores"] = int(perf_cores)
            info["efficiency_cores"] = int(eff_cores)

    # Linux specific
    elif platform.system() == "Linux":
        # Parse /proc/cpuinfo
        try:
            with open("/proc/cpuinfo", "r") as f:
                for line in f:
                    if line.startswith("model name"):
                        info["model"] = line.split(":")[1].strip()
                        break
        except Exception:
            pass

    return info


def get_memory_info():
    """Get memory information."""
    info = {}

    # Try using psutil if available
    try:
        import psutil

        mem = psutil.virtual_memory()
        info["total_gb"] = round(mem.total / (1024**3), 2)
        info["available_gb"] = round(mem.available / (1024**3), 2)
        info["used_percent"] = mem.percent
    except ImportError:
        # Fallback to system commands
        if platform.system() == "Darwin":
            mem_size = run_command("sysctl -n hw.memsize")
            if mem_size:
                info["total_gb"] = round(int(mem_size) / (1024**3), 2)
        elif platform.system() == "Linux":
            mem_info = run_command("grep MemTotal /proc/meminfo")
            if mem_info:
                kb = int(mem_info.split()[1])
                info["total_gb"] = round(kb / (1024**2), 2)

    return info


def get_filesystem_info():
    """Get filesystem information for current directory and common paths."""
    info = {}
    paths_to_check = [
        ("current_dir", os.getcwd()),
        ("home", Path.home()),
        ("tmp", "/tmp"),
    ]

    for name, path in paths_to_check:
        if not os.path.exists(path):
            continue

        fs_info = {"path": str(path)}

        # Get filesystem type
        if platform.system() == "Darwin":
            # Get mount point for the path
            mount_result = run_command(f"df '{path}' | tail -1")
            if mount_result:
                mount_point = mount_result.split()[-1] if mount_result else None

                if mount_point:
                    # Get filesystem type from mount command
                    mount_info = run_command(f"mount | grep '^{mount_point} '")
                    if not mount_info:
                        # Try without the caret for paths that might be symlinks
                        mount_info = run_command(f"mount | grep ' {mount_point} '")

                    if mount_info:
                        # Parse mount output: /dev/disk3s1s1 on / (apfs, local, journaled)
                        if "(" in mount_info:
                            fs_details = mount_info.split("(")[1].split(")")[0]
                            fs_parts = [p.strip() for p in fs_details.split(",")]

                            # Determine filesystem type
                            if "apfs" in fs_parts:
                                fs_info["type"] = "APFS (SSD)"
                            elif "hfs" in fs_parts:
                                fs_info["type"] = "HFS+"
                            elif "nfs" in fs_details.lower():
                                fs_info["type"] = "NFS (Network)"
                            elif "smbfs" in fs_details.lower():
                                fs_info["type"] = "SMB (Network)"
                            else:
                                fs_info["type"] = fs_parts[0] if fs_parts else "Unknown"

                            # Add mount attributes
                            if "local" in fs_parts:
                                fs_info["mount_type"] = "local"
                            elif (
                                "nfs" in fs_details.lower()
                                or "smbfs" in fs_details.lower()
                            ):
                                fs_info["mount_type"] = "network"

        elif platform.system() == "Linux":
            # Use stat command to get filesystem type
            result = run_command(f"stat -f -c %T '{path}'")
            if result:
                # Map common Linux filesystem types
                fs_map = {
                    "ext4": "ext4 (likely SSD/HDD)",
                    "xfs": "XFS",
                    "btrfs": "Btrfs",
                    "nfs": "NFS (Network)",
                    "tmpfs": "tmpfs (RAM)",
                    "zfs": "ZFS",
                }
                fs_info["type"] = fs_map.get(result, result)

        # Get disk usage
        try:
            statvfs = os.statvfs(path)
            total_gb = (statvfs.f_blocks * statvfs.f_frsize) / (1024**3)
            free_gb = (statvfs.f_available * statvfs.f_frsize) / (1024**3)  # type: ignore
            fs_info["total_gb"] = round(total_gb, 2)  # type: ignore
            fs_info["free_gb"] = round(free_gb, 2)
            fs_info["used_percent"] = round(((total_gb - free_gb) / total_gb) * 100, 1)
        except Exception:
            pass

        info[name] = fs_info

    return info


def get_io_performance():
    """Get basic I/O performance metrics."""
    info = {}

    # Simple write/read test in current directory
    test_file = "io_test_tmp.bin"
    test_size = 100 * 1024 * 1024  # 100MB

    try:
        # Write test
        import time

        start = time.time()
        with open(test_file, "wb") as f:
            f.write(os.urandom(test_size))
            f.flush()
            os.fsync(f.fileno())
        write_time = time.time() - start
        info["write_speed_mb_s"] = round(test_size / (1024 * 1024) / write_time, 2)

        # Read test
        start = time.time()
        with open(test_file, "rb") as f:
            _ = f.read()
        read_time = time.time() - start
        info["read_speed_mb_s"] = round(test_size / (1024 * 1024) / read_time, 2)

        # Cleanup
        os.remove(test_file)

    except Exception as e:
        info["error"] = str(e)

    return info


def main():
    """Gather and display system information."""
    print("=" * 60)
    print("System Information for Snakemake Profiling")
    print("=" * 60)
    print(f"Timestamp: {datetime.now().isoformat()}")
    print(f"Platform: {platform.system()} {platform.release()}")
    print(f"Python: {platform.python_version()}")
    print()

    # CPU Information
    print("CPU Information:")
    cpu_info = get_cpu_info()
    for key, value in cpu_info.items():
        print(f"  {key}: {value}")
    print()

    # Memory Information
    print("Memory Information:")
    mem_info = get_memory_info()
    for key, value in mem_info.items():
        print(f"  {key}: {value}")
    print()

    # Filesystem Information
    print("Filesystem Information:")
    fs_info = get_filesystem_info()
    for name, details in fs_info.items():
        print(f"  {name}:")
        for key, value in details.items():
            print(f"    {key}: {value}")
    print()

    # I/O Performance
    print("I/O Performance (current directory):")
    io_info = get_io_performance()
    for key, value in io_info.items():
        print(f"  {key}: {value}")
    print()

    # Save to JSON
    output_file = f"system_info_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    all_info = {
        "timestamp": datetime.now().isoformat(),
        "platform": {
            "system": platform.system(),
            "release": platform.release(),
            "python": platform.python_version(),
        },
        "cpu": cpu_info,
        "memory": mem_info,
        "filesystem": fs_info,
        "io_performance": io_info,
    }

    with open(output_file, "w") as f:
        json.dump(all_info, f, indent=2)

    print(f"Results saved to: {output_file}")
    print("=" * 60)


if __name__ == "__main__":
    main()
