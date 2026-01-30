"""
System Information - Utilities for detecting CPU and GPU resources
"""

import psutil
import subprocess
import platform

def check_mmpbsa_installed():
    """Check if gmx_MMPBSA is available in the current environment"""
    try:
        result = subprocess.run(["which", "gmx_MMPBSA"], capture_output=True, text=True)
        return result.returncode == 0
    except:
        return False

def cpu_info():
    """
    Get number of CPU cores available
    
    Returns:
        Number of logical CPU cores
    """
    try:
        return psutil.cpu_count(logical=True)
    except Exception as e:
        print(f"Error detecting CPU cores: {e}")
        return 1  # Safe default

def cpu_info_detailed():
    """
    Get detailed CPU information
    
    Returns:
        Dictionary with CPU details
    """
    try:
        return {
            'logical_cores': psutil.cpu_count(logical=True),
            'physical_cores': psutil.cpu_count(logical=False),
            'cpu_percent': psutil.cpu_percent(interval=1),
            'cpu_freq': psutil.cpu_freq().current if psutil.cpu_freq() else None,
            'platform': platform.processor()
        }
    except Exception as e:
        print(f"Error getting detailed CPU info: {e}")
        return {
            'logical_cores': 1,
            'physical_cores': 1,
            'cpu_percent': 0,
            'cpu_freq': None,
            'platform': 'Unknown'
        }

def gpu_info():
    """
    Get list of available GPUs (NVIDIA, AMD, Intel)
    
    Returns:
        List of GPU names (empty list if no GPUs found)
    """
    gpus = []
    
    # Try NVIDIA GPUs first
    try:
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0 and result.stdout.strip():
            gpus.extend([gpu.strip() for gpu in result.stdout.strip().splitlines()])
    except FileNotFoundError:
        pass  # nvidia-smi not available
    except subprocess.TimeoutExpired:
        pass
    except Exception as e:
        print(f"Error checking NVIDIA GPUs: {e}")
    
    # Try AMD GPUs
    try:
        result = subprocess.run(
            ["rocm-smi", "--showproductname"],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0 and result.stdout.strip():
            # Parse rocm-smi output
            for line in result.stdout.splitlines():
                if "GPU" in line and ":" in line:
                    gpu_name = line.split(":")[-1].strip()
                    if gpu_name:
                        gpus.append(f"AMD {gpu_name}")
    except FileNotFoundError:
        pass  # rocm-smi not available
    except subprocess.TimeoutExpired:
        pass
    except Exception as e:
        print(f"Error checking AMD GPUs: {e}")
    
    # Try Intel GPUs (on Linux)
    try:
        if platform.system() == "Linux":
            result = subprocess.run(
                ["lspci"],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0:
                for line in result.stdout.splitlines():
                    if "VGA" in line and "Intel" in line:
                        # Extract GPU name
                        parts = line.split(":")
                        if len(parts) >= 3:
                            gpu_name = parts[2].strip()
                            gpus.append(f"Intel {gpu_name}")
    except FileNotFoundError:
        pass
    except subprocess.TimeoutExpired:
        pass
    except Exception as e:
        print(f"Error checking Intel GPUs: {e}")
    
    return gpus

def gpu_info_detailed():
    """
    Get detailed GPU information (NVIDIA only for now)
    
    Returns:
        List of dictionaries with GPU details
    """
    gpus = []
    
    try:
        result = subprocess.run(
            [
                "nvidia-smi",
                "--query-gpu=name,memory.total,memory.used,memory.free,temperature.gpu,utilization.gpu",
                "--format=csv,noheader,nounits"
            ],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        if result.returncode == 0 and result.stdout.strip():
            for line in result.stdout.strip().splitlines():
                parts = [p.strip() for p in line.split(',')]
                if len(parts) >= 6:
                    gpus.append({
                        'name': parts[0],
                        'memory_total_mb': int(parts[1]),
                        'memory_used_mb': int(parts[2]),
                        'memory_free_mb': int(parts[3]),
                        'temperature_c': int(parts[4]),
                        'utilization_percent': int(parts[5])
                    })
    except FileNotFoundError:
        pass  # nvidia-smi not available
    except subprocess.TimeoutExpired:
        pass
    except Exception as e:
        print(f"Error getting detailed GPU info: {e}")
    
    return gpus

def memory_info():
    """
    Get system memory information
    
    Returns:
        Dictionary with memory details (in GB)
    """
    try:
        mem = psutil.virtual_memory()
        return {
            'total_gb': mem.total / (1024**3),
            'available_gb': mem.available / (1024**3),
            'used_gb': mem.used / (1024**3),
            'percent': mem.percent
        }
    except Exception as e:
        print(f"Error getting memory info: {e}")
        return {
            'total_gb': 0,
            'available_gb': 0,
            'used_gb': 0,
            'percent': 0
        }

def disk_info(path='/'):
    """
    Get disk space information for a given path
    
    Args:
        path: Path to check disk space (default: root)
    
    Returns:
        Dictionary with disk details (in GB)
    """
    try:
        disk = psutil.disk_usage(path)
        return {
            'total_gb': disk.total / (1024**3),
            'used_gb': disk.used / (1024**3),
            'free_gb': disk.free / (1024**3),
            'percent': disk.percent
        }
    except Exception as e:
        print(f"Error getting disk info: {e}")
        return {
            'total_gb': 0,
            'used_gb': 0,
            'free_gb': 0,
            'percent': 0
        }

def system_summary():
    """
    Get a complete system summary
    
    Returns:
        Dictionary with all system information
    """
    return {
        'cpu': cpu_info_detailed(),
        'gpu': gpu_info(),
        'memory': memory_info(),
        'disk': disk_info(),
        'platform': {
            'system': platform.system(),
            'release': platform.release(),
            'version': platform.version(),
            'machine': platform.machine(),
            'processor': platform.processor()
        }
    }

def check_gromacs_requirements():
    """
    Check if system meets minimum requirements for GROMACS
    
    Returns:
        Tuple of (meets_requirements, warnings)
    """
    warnings = []
    
    # Check CPU cores
    cores = cpu_info()
    if cores < 2:
        warnings.append(f"Low CPU count ({cores} cores). At least 4 cores recommended.")
    
    # Check memory
    mem = memory_info()
    if mem['total_gb'] < 4:
        warnings.append(f"Low memory ({mem['total_gb']:.1f} GB). At least 8 GB recommended.")
    
    # Check disk space
    disk = disk_info()
    if disk['free_gb'] < 10:
        warnings.append(f"Low disk space ({disk['free_gb']:.1f} GB free). At least 50 GB recommended.")
    
    # Check if GROMACS is installed
    try:
        result = subprocess.run(
            ["which", "gmx"],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode != 0:
            warnings.append("GROMACS (gmx) not found in PATH. Please install GROMACS.")
    except Exception:
        warnings.append("Could not check for GROMACS installation.")
    
    meets_requirements = len(warnings) == 0
    return meets_requirements, warnings

if __name__ == "__main__":
    """Test system information detection"""
    print("=== System Information ===")
    print(f"CPU cores: {cpu_info()}")
    print(f"GPUs: {gpu_info()}")
    print(f"Memory: {memory_info()['total_gb']:.1f} GB")
    print(f"Disk space: {disk_info()['free_gb']:.1f} GB free")
    print("\n=== GROMACS Requirements Check ===")
    meets_req, warnings = check_gromacs_requirements()
    if meets_req:
        print("✅ System meets GROMACS requirements")
    else:
        print("⚠️ Warnings:")
        for warning in warnings:
            print(f"  - {warning}")