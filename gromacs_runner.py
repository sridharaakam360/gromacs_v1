"""
GROMACS MD Runner - Core execution module
Handles GROMACS command execution, file management, and process monitoring
"""

import subprocess
import os
import signal
import re
import sys
import time
import multiprocessing
from datetime import datetime




def check_gmx_command():
    """Check if gmx command is available and return the command name"""
    try:
        result = subprocess.run(
            ["which", "gmx"], 
            capture_output=True, 
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            return "gmx"
        
        # Try alternative GROMACS commands
        variants = ["gmx_mpi", "gmx_d", "gromacs"]
        for variant in variants:
            result = subprocess.run(
                ["which", variant], 
                capture_output=True, 
                text=True,
                timeout=5
            )
            if result.returncode == 0:
                return variant
        
        return None
        
    except subprocess.TimeoutExpired:
        return None
    except Exception as e:
        print(f"Error checking for gmx command: {e}")
        return None

def validate_environment(gromacs_dir, stage):
    """
    Check if all required files exist for the stage
    
    Args:
        gromacs_dir: Path to GROMACS working directory
        stage: Simulation stage ("setup", "equilibration", "production")
    
    Raises:
        Exception: If required files are missing
    
    Returns:
        True if validation passes
    """
    # Basic required files for all stages
    required_files = ["topol.top"]
    
    # Stage-specific requirements
    if stage == "setup":
        required_files.append("step3_input.gro")
    
    elif stage == "equilibration":
        # Need either setup output or original input
        setup_gro = os.path.join(gromacs_dir, "setup.gro")
        original_gro = os.path.join(gromacs_dir, "step3_input.gro")
        
        if not os.path.exists(setup_gro) and not os.path.exists(original_gro):
            raise Exception(
                "Missing input structure for equilibration. "
                "Run 'setup' stage first or provide step3_input.gro"
            )
    
    elif stage == "production":
        # Need either equilibration output, setup output, or original input
        equil_gro = os.path.join(gromacs_dir, "equil.gro")
        setup_gro = os.path.join(gromacs_dir, "setup.gro")
        original_gro = os.path.join(gromacs_dir, "step3_input.gro")
        
        if not any(os.path.exists(f) for f in [equil_gro, setup_gro, original_gro]):
            raise Exception(
                "Missing input structure for production. "
                "Run 'setup' or 'equilibration' stage first, or provide step3_input.gro"
            )
    
    # Check all required files
    missing_files = []
    for file in required_files:
        if not os.path.exists(os.path.join(gromacs_dir, file)):
            missing_files.append(file)
    
    if missing_files:
        raise Exception(f"Missing required files: {', '.join(missing_files)}")
    
    return True

def find_mdp_file(gromacs_dir, stage):
    """
    Find MDP file for the given stage
    
    Args:
        gromacs_dir: Path to GROMACS working directory
        stage: Simulation stage
    
    Returns:
        Path to MDP file (creates basic one if not found)
    """
    stage_mdp_options = {
        "setup": [
            "step4_0_minimization.mdp", 
            "step4.0_minimization.mdp",
            "minim.mdp", 
            "em.mdp"
        ],
        "equilibration": [
            "step4.1_equilibration.mdp",  # Period is correct for CHARMM-GUI
            "step4_1_equilibration.mdp",  # Underscore variant
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
    
    possible_files = stage_mdp_options.get(stage, ["step5_production.mdp"])
    
    for fname in possible_files:
        fpath = os.path.join(gromacs_dir, fname)
        if os.path.exists(fpath):
            return fpath
    
    # If no MDP found, create a basic one
    print(f"Warning: No MDP file found for {stage}, creating basic template")
    return create_basic_mdp(gromacs_dir, stage)

def create_basic_mdp(gromacs_dir, stage):
    """
    Create a basic MDP file if none exists
    
    Args:
        gromacs_dir: Path to GROMACS working directory
        stage: Simulation stage
    
    Returns:
        Path to created MDP file
    """
    if stage == "setup":
        content = """; Energy Minimization
; Created automatically by GROMACS MD Runner

integrator               = steep
nsteps                   = 5000
emtol                    = 1000.0
emstep                   = 0.01

; Output control
nstenergy                = 100
nstlog                   = 100
nstxout                  = 0
nstvout                  = 0

; Neighbor searching
cutoff-scheme            = Verlet
ns_type                  = grid
nstlist                  = 10
rlist                    = 1.2

; Electrostatics
coulombtype              = PME
rcoulomb                 = 1.2

; VdW
vdwtype                  = Cut-off
vdw-modifier             = Force-switch
rvdw                     = 1.2

; Constraints
constraints              = none
"""
        fname = "minim.mdp"
    
    elif stage == "equilibration":
        content = """; NVT Equilibration
; Created automatically by GROMACS MD Runner

; Run parameters
integrator               = md
dt                       = 0.002
nsteps                   = 50000

; Output control
nstxout                  = 1000
nstvout                  = 1000
nstenergy                = 1000
nstlog                   = 1000
nstxout-compressed       = 1000
compressed-x-grps        = System

; Bond constraints
constraints              = h-bonds
constraint_algorithm     = lincs
continuation             = no

; Neighbor searching
cutoff-scheme            = Verlet
ns_type                  = grid
nstlist                  = 20
rlist                    = 1.2

; Electrostatics
coulombtype              = PME
rcoulomb                 = 1.2

; Van der Waals
vdwtype                  = Cut-off
vdw-modifier             = Force-switch
rvdw                     = 1.2

; Temperature coupling
tcoupl                   = v-rescale
tc-grps                  = System
tau_t                    = 1.0
ref_t                    = 300

; Pressure coupling
pcoupl                   = no

; Velocity generation
gen_vel                  = yes
gen_temp                 = 300
gen_seed                 = -1
"""
        fname = "equil.mdp"
    
    else:  # production
        content = """; Production MD
; Created automatically by GROMACS MD Runner

; Run parameters
integrator               = md
dt                       = 0.002
nsteps                   = 500000

; Output control
nstxout                  = 5000
nstvout                  = 5000
nstenergy                = 5000
nstlog                   = 5000
nstxout-compressed       = 5000
compressed-x-grps        = System

; Bond constraints
constraints              = h-bonds
constraint_algorithm     = lincs
continuation             = yes

; Neighbor searching
cutoff-scheme            = Verlet
ns_type                  = grid
nstlist                  = 20
rlist                    = 1.2

; Electrostatics
coulombtype              = PME
rcoulomb                 = 1.2

; Van der Waals
vdwtype                  = Cut-off
vdw-modifier             = Force-switch
rvdw                     = 1.2

; Temperature coupling
tcoupl                   = v-rescale
tc-grps                  = System
tau_t                    = 1.0
ref_t                    = 300

; Pressure coupling
pcoupl                   = Parrinello-Rahman
pcoupltype               = isotropic
tau_p                    = 5.0
ref_p                    = 1.0
compressibility          = 4.5e-5

; Velocity generation
gen_vel                  = no
"""
        fname = "md.mdp"
    
    output_path = os.path.join(gromacs_dir, fname)
    
    try:
        with open(output_path, 'w') as f:
            f.write(content)
        return output_path
    except Exception as e:
        raise Exception(f"Failed to create MDP file {fname}: {str(e)}")

def run_md(
    gromacs_dir, 
    stage, 
    threads=1, 
    use_gpu=False,
    log_callback=None, 
    progress_callback=None, 
    pid_callback=None
):
    """
    Run a GROMACS MD simulation stage
    
    Args:
        gromacs_dir: Path to GROMACS working directory
        stage: Simulation stage ("setup", "equilibration", "production")
        threads: Number of CPU threads to use
        use_gpu: Whether to use GPU acceleration
        log_callback: Function to receive log messages
        progress_callback: Function to receive progress updates (current_step, total_steps)
        pid_callback: Function to receive process ID
    
    Returns:
        Exit code (0 for success)
    
    Raises:
        Exception: On validation or execution errors
    """
    # Default callbacks if none provided
    if log_callback is None:
        log_callback = lambda msg: print(msg, end='')
    if progress_callback is None:
        progress_callback = lambda current, total: None
    if pid_callback is None:
        pid_callback = lambda pid: None
    
    try:
        # Validate environment
        validate_environment(gromacs_dir, stage)
        
        # Check GROMACS installation
        gmx_cmd = check_gmx_command()
        if not gmx_cmd:
            raise Exception("GROMACS (gmx) not found in PATH. Please install GROMACS.")
        
        # Find MDP file
        mdp_file = find_mdp_file(gromacs_dir, stage)
        
        # Get nsteps from MDP for progress tracking
        from mdp_utils import read_mdp_parameter
        nsteps_str = read_mdp_parameter(mdp_file, 'nsteps')
        total_steps = int(nsteps_str) if nsteps_str else 50000
        
        # Determine input/output files based on stage
        if stage == "setup":
            input_structure = "step3_input.gro"
            output_prefix = "setup"
        elif stage == "equilibration":
            # Try to use output from previous stage
            if os.path.exists(os.path.join(gromacs_dir, "setup.gro")):
                input_structure = "setup.gro"
            else:
                input_structure = "step3_input.gro"
            output_prefix = "equil"
        else:  # production
            # Try to use output from equilibration, then setup, then original
            if os.path.exists(os.path.join(gromacs_dir, "equil.gro")):
                input_structure = "equil.gro"
            elif os.path.exists(os.path.join(gromacs_dir, "setup.gro")):
                input_structure = "setup.gro"
            else:
                input_structure = "step3_input.gro"
            output_prefix = "md"
        
        # Log setup
        log_file = os.path.join(gromacs_dir, f"{output_prefix}.log")
        log_callback(f"\n{'=' * 70}\n")
        log_callback(f"GROMACS MD RUNNER - {stage.upper()} STAGE\n")
        log_callback(f"{'=' * 70}\n")
        log_callback(f"📁 Working directory: {gromacs_dir}\n")
        log_callback(f"📝 MDP file: {os.path.basename(mdp_file)}\n")
        log_callback(f"🔢 Total steps: {total_steps:,}\n")
        log_callback(f"🧵 CPU threads: {threads}\n")
        log_callback(f"📊 Input structure: {input_structure}\n")
        log_callback(f"📁 Output prefix: {output_prefix}\n")
        log_callback(f"{'=' * 70}\n\n")
        
        # Step 1: grompp (preprocessing)
        log_callback(f"🔧 Running grompp (preprocessing)...\n")
        
        grompp_cmd = [
            gmx_cmd, "grompp",
            "-f", os.path.basename(mdp_file),
            "-c", input_structure,
            "-p", "topol.top",
            "-o", f"{output_prefix}.tpr",
            "-maxwarn", "10"
        ]
        
        log_callback(f"Command: {' '.join(grompp_cmd)}\n")
        
        with open(log_file, "w") as f:
            f.write(f"{'=' * 70}\n")
            f.write(f"GROMPP PREPROCESSING\n")
            f.write(f"{'=' * 70}\n")
            f.write(f"Started at: {datetime.now().isoformat()}\n")
            f.write(f"Command: {' '.join(grompp_cmd)}\n")
            f.write(f"{'=' * 70}\n\n")
        
        grompp_result = subprocess.run(
            grompp_cmd,
            cwd=gromacs_dir,
            capture_output=True,
            text=True,
            timeout=300  # 5 minutes max for preprocessing
        )
        
        # Log grompp output
        with open(log_file, "a") as f:
            f.write(grompp_result.stdout)
            if grompp_result.stderr:
                f.write("\n--- STDERR ---\n")
                f.write(grompp_result.stderr)
        
        if grompp_result.returncode != 0:
            error_msg = f"❌ grompp failed with exit code {grompp_result.returncode}\n"
            error_msg += f"Check {log_file} for details\n"
            log_callback(error_msg)
            raise Exception(error_msg)
        
        log_callback(f"✅ Preprocessing completed\n\n")
        
        # Emit special marker after grompp completes
        log_callback("__SETUP_COMPLETED__")
        
        # Step 2: mdrun (actual simulation)
        mdrun_cmd = [
            gmx_cmd, "mdrun",
            "-deffnm", output_prefix,
            "-nt", str(threads)
        ]
        
        if use_gpu:
            mdrun_cmd.extend(["-nb", "gpu", "-pme", "gpu", "-bonded", "gpu"])
            log_callback(f"🎮 GPU acceleration enabled\n")
        
        log_callback(f"🚀 Starting {stage.capitalize()} MD simulation...\n")
        log_callback(f"Command: {' '.join(mdrun_cmd)}\n")
        log_callback(f"{'=' * 70}\n")
        
        with open(log_file, "a") as f:
            f.write(f"\n{'=' * 70}\n")
            f.write(f"MDRUN COMMAND\n")
            f.write(f"{'=' * 70}\n")
            f.write(f"{' '.join(mdrun_cmd)}\n")
            f.write(f"{'=' * 70}\n\n")
        
        # Start the process
        process = subprocess.Popen(
            mdrun_cmd,
            cwd=gromacs_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        pid_callback(process.pid)
        log_callback(f"📊 Process PID: {process.pid}\n\n")
        
        # Monitor output
        start_time = time.time()
        last_progress_update = 0
        last_step = 0
        
        # More specific error patterns
        error_patterns = [
            r"Fatal error",
            r"Segmentation fault",
            r"ERROR:",
            r"Error termination",
            r"gmx.*returned non-zero"
        ]
        
        for line in iter(process.stdout.readline, ''):
            if line:
                log_callback(line)
                with open(log_file, "a") as f:
                    f.write(line)
                
                # Parse progress - multiple patterns
                step = None
                current_time = time.time()
                
                # Pattern 1: Standard "Step" output from MD runs
                step_match = re.search(r"Step\s+(\d+)", line, re.IGNORECASE)
                if step_match:
                    step = int(step_match.group(1))
                
                # Pattern 2: Energy minimization or setup stage (e.g., "Step   100 ...")
                if step is None:
                    em_match = re.search(r"Step\s*=\s*(\d+)", line, re.IGNORECASE)
                    if em_match:
                        step = int(em_match.group(1))
                
                # Pattern 3: Progress percentage patterns (for setup/preprocessing)
                if step is None and "%" in line:
                    pct_match = re.search(r"(\d+(?:\.\d+)?)\s*%", line)
                    if pct_match:
                        pct = float(pct_match.group(1))
                        # Convert percentage to step approximation
                        step = int((pct / 100.0) * total_steps)
                
                # Update progress if we extracted a step
                if step is not None and current_time - last_progress_update > 0.5 and step != last_step:
                    progress_callback(step, total_steps)
                    last_progress_update = current_time
                    last_step = step
                
                # Check for errors with more specific patterns
                for pattern in error_patterns:
                    if re.search(pattern, line, re.IGNORECASE):
                        log_callback(f"⚠️ Error detected: {line.strip()}\n")
                        break
        
        process.wait()
        
        # Ensure progress reaches 100% on completion
        if process.returncode == 0:
            progress_callback(total_steps, total_steps)
        
        # Calculate runtime
        runtime = time.time() - start_time
        hours = int(runtime // 3600)
        minutes = int((runtime % 3600) // 60)
        seconds = int(runtime % 60)
        
        # Final status
        success = process.returncode == 0
        status_emoji = "✅" if success else "❌"
        
        final_msg = f"\n{status_emoji} Simulation {'completed successfully' if success else 'failed'}\n"
        final_msg += f"⏱️  Runtime: {hours:02d}:{minutes:02d}:{seconds:02d}\n"
        final_msg += f"🔢 Exit code: {process.returncode}\n"
        
        log_callback(final_msg)
        
        with open(log_file, "a") as f:
            f.write(f"\n{'=' * 70}\n")
            f.write(f"SIMULATION {'COMPLETED' if success else 'FAILED'}\n")
            f.write(f"{'=' * 70}\n")
            f.write(f"Runtime: {hours:02d}:{minutes:02d}:{seconds:02d}\n")
            f.write(f"Exit code: {process.returncode}\n")
            f.write(f"Completed at: {datetime.now().isoformat()}\n")
            f.write(f"{'=' * 70}\n")
        
        if not success:
            raise Exception(f"MD simulation failed with exit code {process.returncode}")
        
        return process.returncode
    
    except subprocess.TimeoutExpired:
        error_msg = "❌ Process timed out\n"
        log_callback(error_msg)
        raise Exception(error_msg)
    except Exception as e:
        error_msg = f"❌ Unexpected error: {str(e)}\n"
        log_callback(error_msg)
        try:
            with open(log_file, "a") as f:
                f.write(f"\n❌ ERROR: {str(e)}\n")
                f.write(f"Timestamp: {datetime.now().isoformat()}\n")
        except Exception:
            pass  # Ignore errors writing to log
        raise

def stop_md(pid, timeout=10):
    """
    Stop a running MD simulation gracefully, then forcefully if needed
    
    Args:
        pid: Process ID to stop
        timeout: Seconds to wait before force-killing
    
    Returns:
        True if process was stopped, False otherwise
    """
    if not pid:
        return False
    
    try:
        # Check if process exists first
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            # Process already dead
            return True
        
        # Try graceful termination (SIGTERM)
        os.kill(pid, signal.SIGTERM)
        
        # Wait for process to terminate
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                os.kill(pid, 0)  # Check if process still exists
                time.sleep(0.2)
            except ProcessLookupError:
                # Process terminated successfully
                return True
        
        # If still running after timeout, force kill (SIGKILL)
        try:
            os.kill(pid, signal.SIGKILL)
            time.sleep(0.5)
            
            # Verify it's dead
            try:
                os.kill(pid, 0)
                return False  # Still alive somehow
            except ProcessLookupError:
                return True  # Finally dead
                
        except ProcessLookupError:
            return True  # Died before we could kill it
            
    except ProcessLookupError:
        # Process already dead
        return True
    except PermissionError:
        print(f"Permission denied when trying to stop process {pid}")
        return False
    except Exception as e:
        print(f"Error stopping process {pid}: {e}")
        return False


def get_trajectory_frames(trajectory_path, timeout=30):
    """
    Get number of frames in a GROMACS trajectory file
    
    Args:
        trajectory_path: Path to .xtc or .trr file
        timeout: Timeout in seconds for gmx check command
    
    Returns:
        Number of frames, or None if unable to determine
    """
    try:
        result = subprocess.run(
            ["gmx", "check", "-f", trajectory_path],
            capture_output=True,
            text=True,
            timeout=timeout
        )
        
        if result.returncode == 0:
            output = result.stdout + result.stderr
            
            # Look for "Last frame X" pattern
            match = re.search(r"Last frame\s+(\d+)", output)
            if match:
                # Frames are 0-indexed, so add 1
                return int(match.group(1)) + 1
            
            # Alternative pattern: "# frames: X"
            match = re.search(r"#\s*frames:?\s*(\d+)", output, re.IGNORECASE)
            if match:
                return int(match.group(1))
        
        return None
        
    except subprocess.TimeoutExpired:
        print(f"Warning: gmx check timed out for {trajectory_path}")
        return None
    except FileNotFoundError:
        print("Warning: gmx command not found")
        return None
    except Exception as e:
        print(f"Warning: Could not determine frame count: {e}")
        return None


def parse_mmpbsa_input(input_file):
    """
    Parse mmpbsa.in file to extract settings
    
    Args:
        input_file: Path to mmpbsa.in file
    
    Returns:
        Dictionary with parsed settings
    """
    settings = {
        'startframe': 1,
        'endframe': None,
        'interval': 1
    }
    
    try:
        with open(input_file, 'r') as f:
            content = f.read()
        
        # Extract startframe
        match = re.search(r'startframe\s*=\s*(\d+)', content, re.IGNORECASE)
        if match:
            settings['startframe'] = int(match.group(1))
        
        # Extract endframe
        match = re.search(r'endframe\s*=\s*(\d+)', content, re.IGNORECASE)
        if match:
            settings['endframe'] = int(match.group(1))
        
        # Extract interval
        match = re.search(r'interval\s*=\s*(\d+)', content, re.IGNORECASE)
        if match:
            settings['interval'] = int(match.group(1))
        
    except Exception as e:
        print(f"Warning: Could not parse mmpbsa.in: {e}")
    
    return settings


def detect_index_groups(work_dir, tpr_file, log_callback=None):
    """
    Auto-detect receptor and ligand groups from index.ndx (CHARMM-GUI style)
    Returns: (receptor_group: int, ligand_group: int)
    """
    def log(msg):
        if log_callback:
            log_callback(msg)

    index_path = os.path.join(work_dir, "index.ndx")

    # Case 1: index.ndx doesn't exist
    if not os.path.exists(index_path):
        log("⚠️  index.ndx file not found in working directory.")
        log("→ Using **default fallback values**: Receptor = 1, Ligand = 13")
        return 1, 13

    receptor_group = None
    ligand_group   = None

    try:
        current_group_num = 0
        with open(index_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line.startswith('[') and line.endswith(']'):
                    current_group_num += 1
                    name_raw = line[1:-1].strip()
                    name = name_raw.lower()

                    # Receptor: prefer full protein, avoid partial like -H
                    if 'protein' in name and '-h' not in name and receptor_group is None:
                        receptor_group = current_group_num
                        log(f"✓ Auto-detected **receptor** group: {current_group_num} → '{name_raw}'")

                    # Ligand: match common names, exclude ions/water
                    ligand_keywords = ['unk', 'lig', 'mol', 'ligand', 'het', 'resname', 'drug', 'comp', 'inh', 'sub']
                    ion_keywords = ['pot', 'cla', 'na', 'cl', 'ion', 'tip', 'wat', 'sol']
                    if any(kw in name for kw in ligand_keywords) and not any(ik in name for ik in ion_keywords):
                        if ligand_group is None:
                            ligand_group = current_group_num
                            log(f"✓ Auto-detected **ligand** group: {current_group_num} → '{name_raw}'")

        # Fallbacks if nothing found
        messages = []
        if receptor_group is None:
            receptor_group = 1
            messages.append("   → Receptor set to default: 1 (Protein)")
        if ligand_group is None:
            ligand_group = 13
            messages.append("   → Ligand set to default: 13 (common for UNK in CHARMM-GUI)")

        if messages:
            log("⚠️  Partial or no auto-detection — using fallback values")
            for msg in messages:
                log(msg)
        else:
            log("✅ Successfully auto-detected both groups from index.ndx")

        log(f"→ Using groups → Receptor: {receptor_group} | Ligand: {ligand_group}")

        return receptor_group, ligand_group

    except Exception as e:
        log(f"❌ Error during group auto-detection: {str(e)}")
        log("→ Falling back to safe defaults: Receptor = 1, Ligand = 13")
        return 1, 13




def run_mmpbsa(
    work_dir, 
    tpr_file, 
    trajectory, 
    index_file, 
    input_file="mmpbsa.in",
    topology_file="topol.top",
    receptor_group=None,
    ligand_group=None,
    n_cores=None,
    log_callback=None, 
    progress_callback=None
):
    """
    Run gmx_MMPBSA calculation with proper error handling and progress tracking
    
    Args:
        work_dir: Working directory containing all files
        tpr_file: TPR file name (e.g., "md.tpr")
        trajectory: Trajectory file name (e.g., "md.xtc")
        index_file: Index file name (e.g., "index.ndx")
        input_file: MMPBSA input file (default: "mmpbsa.in")
        topology_file: Topology file (default: "topol.top")
        receptor_group: Receptor group index (auto-detect if None)
        ligand_group: Ligand group index (auto-detect if None)
        n_cores: Number of CPU cores to use (auto-detect if None)
        log_callback: Function to receive log messages
        progress_callback: Function to receive progress updates (percentage)
    
    Returns:
        Exit code (0 for success)
    
    Raises:
        Exception: On validation or execution errors
    """
    def log(msg):
        if log_callback:
            log_callback(msg)
        else:
            print(msg, end='')
    
    def update_progress(pct):
        if progress_callback:
            progress_callback(min(100, int(pct)))
    
    try:
        log(f"\n{'=' * 70}\n")
        log(f"GMXMMPBSA BINDING FREE ENERGY CALCULATION\n")
        log(f"{'=' * 70}\n")
        
        # Validate required files
        required_files = {
            'TPR': tpr_file,
            'Trajectory': trajectory,
            'Topology': topology_file,
            'Input': input_file
        }
        
        missing = []
        for name, fname in required_files.items():
            fpath = os.path.join(work_dir, fname)
            if not os.path.exists(fpath):
                missing.append(f"{name} ({fname})")
        
        if missing:
            raise Exception(f"Missing required files: {', '.join(missing)}")
        
        log(f"✅ All required files found\n")
        
        # Get trajectory frame count
        traj_path = os.path.join(work_dir, trajectory)
        n_frames = get_trajectory_frames(traj_path)
        
        if n_frames is not None:
            log(f"📊 Trajectory has {n_frames} frames\n")
        else:
            log(f"⚠️ Could not determine frame count from trajectory\n")
            n_frames = 1000  # Fallback estimate
        
        # Parse MMPBSA input settings
        input_path = os.path.join(work_dir, input_file)
        settings = parse_mmpbsa_input(input_path)
        
        # Calculate effective frames for analysis
        start = settings['startframe']
        end = settings['endframe'] if settings['endframe'] else n_frames
        interval = settings['interval']
        
        effective_frames = max(1, (end - start + 1) // interval)
        log(f"📊 Will analyze ~{effective_frames} frames (start={start}, end={end}, interval={interval})\n")
        
        # Auto-detect optimal core count
        if n_cores is None:
            max_cores = multiprocessing.cpu_count()
            # Use at most: (1) all but 1 core, (2) number of frames
            n_cores = min(max_cores - 1, effective_frames)
            n_cores = max(1, n_cores)  # At least 1
        
        # Ensure cores <= frames (gmx_MMPBSA requirement)
        if n_cores > effective_frames:
            log(f"⚠️ Reducing cores from {n_cores} to {effective_frames} (must have ≤ frames)\n")
            n_cores = effective_frames
        
        log(f"🧵 Using {n_cores} CPU core(s) for parallel calculation\n")
        
        # Auto-detect receptor and ligand groups if not provided
        if receptor_group is None or ligand_group is None:
            auto_receptor, auto_ligand = detect_index_groups(work_dir, tpr_file, log)
            if receptor_group is None:
                receptor_group = auto_receptor
            if ligand_group is None:
                ligand_group = auto_ligand
        
        log(f"🎯 Receptor group: {receptor_group}, Ligand group: {ligand_group}\n")
        
        # Check if gmx_MMPBSA is available
        try:
            result = subprocess.run(
                ["which", "gmx_MMPBSA"],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode != 0:
                raise Exception("gmx_MMPBSA not found in PATH. Please ensure it's installed and activated.")
        except subprocess.TimeoutExpired:
            raise Exception("Timeout checking for gmx_MMPBSA")
        
        log(f"{'=' * 70}\n\n")
        
        # Build command
        # Note: Use --use-hwthread-cpus if MPI slots are limited
        cmd = [
            "mpirun",
            "--use-hwthread-cpus",  # Allow oversubscription if needed
            "-np", str(n_cores),
            "gmx_MMPBSA",
            "-O",  # Overwrite existing files
            "-i", input_file,
            "-cs", tpr_file,
            "-ct", trajectory,
            "-ci", index_file,
            "-cg", str(receptor_group), str(ligand_group),
            "-cp", topology_file  # CRITICAL: topology file is required!
        ]
        
        log(f"🚀 Starting MMPBSA calculation...\n")
        log(f"Command: {' '.join(cmd)}\n")
        log(f"{'=' * 70}\n\n")
        
        # Create log file
        mmpbsa_log = os.path.join(work_dir, "gmx_MMPBSA.log")
        
        # Start process
        process = subprocess.Popen(
            cmd,
            cwd=work_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
        
        log(f"📊 Process PID: {process.pid}\n\n")
        
        # Monitor output with timeout handling
        start_time = time.time()
        timeout_seconds = 3600 * 24  # 24 hours max
        
        frame_count = 0
        last_progress = 0
        
        # Error patterns to detect
        error_patterns = [
            r"Fatal error",
            r"Error:",
            r"MMPBSA_Error",
            r"Traceback",
            r"failed",
            r"Cannot find",
            r"No such file"
        ]
        
        while True:
            # Check timeout
            if time.time() - start_time > timeout_seconds:
                process.kill()
                raise Exception(f"MMPBSA calculation timed out after {timeout_seconds}s")
            
            line = process.stdout.readline()
            
            # Check if process finished
            if not line and process.poll() is not None:
                break
            
            if line:
                line_stripped = line.strip()
                log(line)
                
                # Track progress based on various output patterns
                
                # Pattern 1: Frame processing (e.g., "Processing frame 10/100")
                if "frame" in line_stripped.lower():
                    match = re.search(r'(\d+)\s*/\s*(\d+)', line_stripped)
                    if match:
                        current = int(match.group(1))
                        total = int(match.group(2))
                        pct = (current / total) * 100
                        if pct > last_progress:
                            update_progress(pct)
                            last_progress = pct
                
                # Pattern 2: Progress bars (e.g., "50%|##########|")
                if "%" in line_stripped and "|" in line_stripped:
                    match = re.search(r'(\d+)%', line_stripped)
                    if match:
                        pct = int(match.group(1))
                        if pct > last_progress:
                            update_progress(pct)
                            last_progress = pct
                
                # Pattern 3: Stage completions
                stage_markers = [
                    "Building AMBER topologies",
                    "Preparing trajectories",
                    "Running calculations",
                    "Parsing results",
                    "completed successfully"
                ]
                
                for i, marker in enumerate(stage_markers):
                    if marker.lower() in line_stripped.lower():
                        # Each stage represents 20% progress
                        pct = ((i + 1) / len(stage_markers)) * 100
                        if pct > last_progress:
                            update_progress(pct)
                            last_progress = pct
                
                # Check for errors
                for pattern in error_patterns:
                    if re.search(pattern, line_stripped, re.IGNORECASE):
                        log(f"\n⚠️ Potential error detected: {line_stripped}\n")
        
        # Wait for process to finish
        returncode = process.wait()
        
        # Calculate runtime
        runtime = time.time() - start_time
        hours = int(runtime // 3600)
        minutes = int((runtime % 3600) // 60)
        seconds = int(runtime % 60)
        
        log(f"\n{'=' * 70}\n")
        
        if returncode == 0:
            log(f"✅ MMPBSA calculation completed successfully!\n")
            update_progress(100)
            
            # Check for output files
            result_files = [
                "FINAL_RESULTS_MMPBSA.dat",
                "FINAL_RESULTS_MMPBSA.csv"
            ]
            
            found_results = []
            for rf in result_files:
                if os.path.exists(os.path.join(work_dir, rf)):
                    found_results.append(rf)
            
            if found_results:
                log(f"📄 Results saved to: {', '.join(found_results)}\n")
            
        else:
            log(f"❌ MMPBSA calculation failed with exit code {returncode}\n")
            log(f"💡 Check {mmpbsa_log} for details\n")
            raise Exception(f"MMPBSA failed with exit code {returncode}")
        
        log(f"⏱️  Runtime: {hours:02d}:{minutes:02d}:{seconds:02d}\n")
        log(f"{'=' * 70}\n")
        
        return returncode
        
    except subprocess.TimeoutExpired:
        raise Exception("MMPBSA process timed out")
    except KeyboardInterrupt:
        if 'process' in locals():
            process.kill()
        raise Exception("MMPBSA calculation interrupted by user")
    except Exception as e:
        log(f"\n❌ Error: {str(e)}\n")
        raise