"""
MDP Utilities - Helper functions for manipulating GROMACS MDP files
"""

import os
import re
import shutil
from datetime import datetime

def ns_to_nsteps(ns, timestep_fs=2):
    """
    Convert nanoseconds to number of MD steps
    
    Args:
        ns: Simulation time in nanoseconds
        timestep_fs: Timestep in femtoseconds (default: 2 fs)
    
    Returns:
        Number of steps (integer)
    
    Example:
        >>> ns_to_nsteps(10.0, 2)
        5000000
    """
    # 1 ns = 1,000,000 fs
    # steps = (ns * 1,000,000 fs) / (timestep in fs)
    return int((ns * 1_000_000) / timestep_fs)

def get_mdp_file(gromacs_dir, stage):
    """
    Get the MDP file path for the given stage
    
    Args:
        gromacs_dir: Path to GROMACS working directory
        stage: Simulation stage ("setup", "equilibration", "production")
    
    Returns:
        Full path to MDP file
    
    Raises:
        FileNotFoundError: If no suitable MDP file is found and creation fails
    """
    stage_options = {
        "setup": [
            "step4_0_minimization.mdp",
            "step4.0_minimization.mdp", 
            "minim.mdp", 
            "em.mdp"
        ],
        "equilibration": [
            "step4.1_equilibration.mdp",  # CHARMM-GUI uses period
            "step4_1_equilibration.mdp",  # Some variants use underscore
            "step4_equilibration.mdp",
            "equil.mdp",
            "nvt.mdp"
        ],
        "production": [
            "step5_production.mdp",
            "step5.0_production.mdp",
            "md.mdp",
            "prod.mdp"
        ]
    }
    
    # Get possible filenames for this stage
    possible_files = stage_options.get(stage.lower(), ["step5_production.mdp"])
    
    # Find which one exists
    for fname in possible_files:
        fpath = os.path.join(gromacs_dir, fname)
        if os.path.exists(fpath):
            return fpath
    
    # If none found, create a basic one using gromacs_runner
    # Import here to avoid circular dependency
    from gromacs_runner import create_basic_mdp
    return create_basic_mdp(gromacs_dir, stage)

def read_mdp_parameter(mdp_path, parameter_name):
    """
    Read a specific parameter value from an MDP file
    
    Args:
        mdp_path: Path to MDP file
        parameter_name: Name of parameter to read (case-insensitive)
    
    Returns:
        Parameter value as string, or None if not found
    """
    if not os.path.exists(mdp_path):
        return None
    
    try:
        with open(mdp_path, 'r') as f:
            for line in f:
                stripped = line.strip()
                
                # Skip empty lines and comments
                if not stripped or stripped.startswith(';') or stripped.startswith('#'):
                    continue
                
                # Check if this line contains the parameter
                # Match: "parameter = value" or "parameter=value"
                match = re.match(
                    rf'^\s*{re.escape(parameter_name)}\s*=\s*(.+?)(?:\s*;.*)?$',
                    line,
                    re.IGNORECASE
                )
                if match:
                    return match.group(1).strip()
        
        return None
        
    except Exception as e:
        print(f"Error reading MDP parameter '{parameter_name}': {e}")
        return None

def update_mdp_nsteps(mdp_path, ns, backup=True):
    """
    Update nsteps parameter in MDP file based on simulation time in nanoseconds
    
    Args:
        mdp_path: Path to MDP file
        ns: Simulation time in nanoseconds
        backup: Whether to create a backup of the original file (default: True)
    
    Returns:
        Number of steps (nsteps value)
    
    Raises:
        FileNotFoundError: If MDP file doesn't exist
        ValueError: If unable to update file
    """
    if not os.path.exists(mdp_path):
        raise FileNotFoundError(f"MDP file not found: {mdp_path}")
    
    # Read dt (timestep) from the MDP file if it exists
    dt_str = read_mdp_parameter(mdp_path, 'dt')
    if dt_str:
        try:
            dt_ps = float(dt_str)  # dt in picoseconds
            timestep_fs = dt_ps * 1000  # Convert to femtoseconds
        except ValueError:
            timestep_fs = 2  # Default to 2 fs if parsing fails
    else:
        timestep_fs = 2  # Default to 2 fs
    
    # Calculate nsteps
    nsteps = ns_to_nsteps(ns, timestep_fs)
    
    # Create backup if requested
    if backup:
        backup_path = f"{mdp_path}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        try:
            shutil.copy2(mdp_path, backup_path)
        except Exception as e:
            print(f"Warning: Could not create backup: {e}")
    
    # Read and update the file
    updated = False
    new_lines = []
    
    try:
        with open(mdp_path, 'r') as f:
            for line in f:
                stripped = line.strip()
                
                # Keep empty lines as-is
                if not stripped:
                    new_lines.append(line)
                    continue
                
                # Keep comment lines as-is
                if stripped.startswith(';') or stripped.startswith('#'):
                    new_lines.append(line)
                    continue
                
                # Check if this is an nsteps line (case-insensitive)
                # Match: "nsteps = 123" or "nsteps=123"
                if re.match(r'^\s*nsteps\s*=', line, re.IGNORECASE):
                    # Preserve indentation
                    indent = line[:len(line) - len(line.lstrip())]
                    
                    # Check for inline comment
                    comment_match = re.search(r';.*$', line)
                    comment = comment_match.group(0) if comment_match else ""
                    
                    # Write updated line with preserved formatting
                    new_lines.append(f"{indent}nsteps = {nsteps}{' ' + comment if comment else ''}\n")
                    updated = True
                else:
                    new_lines.append(line)
        
        # If nsteps wasn't found, add it at the end with a comment
        if not updated:
            new_lines.append(f"\n; Added by GROMACS MD Runner\n")
            new_lines.append(f"nsteps = {nsteps}\n")
        
        # Write updated file
        with open(mdp_path, "w") as f:
            f.writelines(new_lines)
        
        return nsteps
        
    except Exception as e:
        raise ValueError(f"Error updating MDP file: {str(e)}")

def update_mdp_parameter(mdp_path, parameter_name, parameter_value, backup=True):
    """
    Update or add any parameter in an MDP file
    
    Args:
        mdp_path: Path to MDP file
        parameter_name: Name of parameter to update
        parameter_value: New value for the parameter
        backup: Whether to create a backup (default: True)
    
    Returns:
        True if successful
    
    Raises:
        FileNotFoundError: If MDP file doesn't exist
        ValueError: If unable to update file
    """
    if not os.path.exists(mdp_path):
        raise FileNotFoundError(f"MDP file not found: {mdp_path}")
    
    # Create backup if requested
    if backup:
        backup_path = f"{mdp_path}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        try:
            shutil.copy2(mdp_path, backup_path)
        except Exception as e:
            print(f"Warning: Could not create backup: {e}")
    
    updated = False
    new_lines = []
    
    try:
        with open(mdp_path, 'r') as f:
            for line in f:
                stripped = line.strip()
                
                # Keep empty lines and comments as-is
                if not stripped or stripped.startswith(';') or stripped.startswith('#'):
                    new_lines.append(line)
                    continue
                
                # Check if this line contains the parameter (case-insensitive)
                if re.match(rf'^\s*{re.escape(parameter_name)}\s*=', line, re.IGNORECASE):
                    # Preserve indentation
                    indent = line[:len(line) - len(line.lstrip())]
                    
                    # Check for inline comment
                    comment_match = re.search(r';.*$', line)
                    comment = comment_match.group(0) if comment_match else ""
                    
                    # Write updated line
                    new_lines.append(
                        f"{indent}{parameter_name} = {parameter_value}"
                        f"{' ' + comment if comment else ''}\n"
                    )
                    updated = True
                else:
                    new_lines.append(line)
        
        # If parameter wasn't found, add it at the end
        if not updated:
            new_lines.append(f"\n; Added by GROMACS MD Runner\n")
            new_lines.append(f"{parameter_name} = {parameter_value}\n")
        
        # Write updated file
        with open(mdp_path, "w") as f:
            f.writelines(new_lines)
        
        return True
        
    except Exception as e:
        raise ValueError(f"Error updating MDP parameter: {str(e)}")

def validate_mdp_file(mdp_path):
    """
    Basic validation of MDP file
    
    Args:
        mdp_path: Path to MDP file
    
    Returns:
        Tuple of (is_valid, error_messages)
    """
    if not os.path.exists(mdp_path):
        return False, ["MDP file does not exist"]
    
    errors = []
    
    try:
        with open(mdp_path, 'r') as f:
            content = f.read()
        
        # Check for required parameters based on integrator
        integrator = read_mdp_parameter(mdp_path, 'integrator')
        
        if integrator and integrator.lower() != 'steep':
            # MD run - check for dt
            dt = read_mdp_parameter(mdp_path, 'dt')
            if not dt:
                errors.append("Missing required parameter: dt (timestep)")
        
        # Check for nsteps
        nsteps = read_mdp_parameter(mdp_path, 'nsteps')
        if not nsteps:
            errors.append("Missing required parameter: nsteps")
        
        # Check file is not empty
        if len(content.strip()) == 0:
            errors.append("MDP file is empty")
        
        return len(errors) == 0, errors
        
    except Exception as e:
        return False, [f"Error reading MDP file: {str(e)}"]

def get_mdp_info(mdp_path):
    """
    Extract useful information from MDP file
    
    Args:
        mdp_path: Path to MDP file
    
    Returns:
        Dictionary with MDP parameters
    """
    info = {
        'integrator': None,
        'dt': None,
        'nsteps': None,
        'temperature': None,
        'pressure_coupling': None
    }
    
    if not os.path.exists(mdp_path):
        return info
    
    info['integrator'] = read_mdp_parameter(mdp_path, 'integrator')
    info['dt'] = read_mdp_parameter(mdp_path, 'dt')
    info['nsteps'] = read_mdp_parameter(mdp_path, 'nsteps')
    info['temperature'] = read_mdp_parameter(mdp_path, 'ref_t')
    info['pressure_coupling'] = read_mdp_parameter(mdp_path, 'pcoupl')
    
    return info

# mdp_utils.py - improved version
def generate_default_mmpbsa_in(output_path, use_pb=True, n_frames_estimate=1000):
    """
    Generate a better default mmpbsa.in for CHARMM-GUI systems
    """
    content = f"""&general
sys_name = "CHARMM-GUI_Protein-Ligand",
startframe = 1,
endframe = {n_frames_estimate},          # will be adjusted later if needed
interval = 5,                            # analyze every 5th frame (adjust based on trajectory length)
verbose = 2,
# For CHARMM force fields, use charmm_radii in PB (added in gmx_MMPBSA v1.5+)
# PBRadii = 7,                           # uncomment for PB + CHARMM
/

&gb
igb = 5,                                 # igb=5 or 8 recommended for modern GB
saltcon = 0.150,                         # physiological ~150 mM
/
"""
    if use_pb:
        content += """
&pb
istrng = 0.150,
fillratio = 4.0,
radiopt = 0,                             # use radii from topology (recommended for CHARMM)
inp = 2,                                 # inp=2 often better for non-polar
/
"""
    else:
        content += """
# PB section commented out - using GB only
"""

    content += """
# Optional: Add decomposition later if wanted
# &decomp
# idecomp=2,
# dec_verbose=3,
# /
"""
    with open(output_path, 'w') as f:
        f.write(content)