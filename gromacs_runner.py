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
import select
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
        
        # Try standard installation path before giving up
        if os.path.exists("/usr/local/gromacs/bin/gmx"):
            return "/usr/local/gromacs/bin/gmx"
        
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
        # Need either setup output (minimized structure) or original input
        # Check for various minimization output names
        setup_candidates = [
            "setup.gro",
            "step4_0_minimization.gro",
            "step4.0_minimization.gro",
            "minim.gro"
        ]
        
        has_setup = False
        for cand in setup_candidates:
            if os.path.exists(os.path.join(gromacs_dir, cand)):
                has_setup = True
                break
        
        original_gro = os.path.join(gromacs_dir, "step3_input.gro")
        
        if not has_setup and not os.path.exists(original_gro):
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
        # Try to use output from previous stage (setup/minimization)
            input_structure = "step3_input.gro"  # Default fallback
            
            # Check for various minimization output names (priority order)
            setup_candidates = [
                "setup.gro",
                "step4_0_minimization.gro",
                "step4.0_minimization.gro",
                "minim.gro"
            ]
            
            for cand in setup_candidates:
                if os.path.exists(os.path.join(gromacs_dir, cand)):
                    input_structure = cand
                    break
            
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
            "-r", input_structure,
            "-p", "topol.top"
        ]

        if os.path.exists(os.path.join(gromacs_dir, "index.ndx")):
            grompp_cmd.extend(["-n", "index.ndx"])
            
        grompp_cmd.extend([
            "-o", f"{output_prefix}.tpr",
            "-maxwarn", "10"
        ])
        
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


def create_custom_index(work_dir, tpr_file, log_callback=None):
    """
    Automates the creation of custom index groups using gmx make_ndx.
    Tries multiple common residue names if primary names don't exist.
    
    Args:
        work_dir: Working directory path
        tpr_file: Path to .tpr file
        log_callback: Optional callback for logging messages
    
    Returns:
        Tuple of (success: bool, index_file_path: str, receptor_name: str, ligand_name: str)
        The names returned are the residue names used for the groups (e.g., 'PROA', 'UNK')
    """
    def log(msg):
        if log_callback:
            log_callback(msg)
    
    try:
        log("🔧 Creating custom index groups...")
        
        # Define backup residue names to try if primaries fail
        # These are typical in GROMACS/CHARMM systems
        receptor_candidates = ["PROA", "PROT", "PRO", "Protein", "PTERA"]
        ligand_candidates = ["UNK", "LIG", "MOL", "HET", "COMP", "RES", "DRG", "INH"]
        
        # Try each combination until one succeeds
        attempted = []
        for receptor_res in receptor_candidates:
            for ligand_res in ligand_candidates:
                try:
                    # Construct the gmx make_ndx command with proper escaping
                    echo_cmd = f'echo -e "r {receptor_res}\\nr {ligand_res}\\nq"'
                    index_file = os.path.join(work_dir, "index_mmpbsa.ndx")
                    index_basename = os.path.basename(index_file)
                    tpr_basename = os.path.basename(tpr_file)
                    
                    # Build and run the command
                    cmd = f'{echo_cmd} | gmx make_ndx -f {tpr_basename} -o {index_basename} 2>&1'
                    
                    result = subprocess.run(
                        cmd,
                        shell=True,
                        cwd=work_dir,
                        capture_output=True,
                        text=True,
                        timeout=30
                    )
                    
                    # Check if index file was created successfully
                    if result.returncode == 0 and os.path.exists(index_file):
                        # Quick validation: check if file has content
                        if os.path.getsize(index_file) > 0:
                            log(f"✅ Successfully created index file using residues:")
                            log(f"   Receptor: {receptor_res} | Ligand: {ligand_res}")
                            return True, index_file, receptor_res, ligand_res
                    
                    attempted.append(f"{receptor_res}+{ligand_res}")
                
                except subprocess.TimeoutExpired:
                    attempted.append(f"{receptor_res}+{ligand_res} (timeout)")
                    continue
                except Exception:
                    attempted.append(f"{receptor_res}+{ligand_res} (error)")
                    continue
        
        # If we get here, no combination worked
        log("⚠️  Could not create index file with standard residue names")
        log(f"   Tried: {', '.join(attempted[:5])}...")  # Show first 5 attempts
        log("\n💡 TROUBLESHOOTING:")
        log("   1. Check what residues exist in your system:")
        log(f"      gmx editconf -f {tpr_basename} -pr && cat confout.gro | head -100")
        log("   2. Or check the topology file for residue names")
        log("   3. Then use those residue names with gmx make_ndx manually:")
        log('      echo -e "r YOUR_RECEPTOR\\nr YOUR_LIGAND\\nq" | gmx make_ndx -f md.tpr -o index.ndx')
        return False, None, None, None
            
    except Exception as e:
        log(f"❌ Error creating custom index: {str(e)}")
        return False, None, None, None


def detect_index_groups(work_dir, tpr_file, log_callback=None):
    """
    Auto-detect receptor and ligand groups from index.ndx (CHARMM-GUI style)
    If index.ndx doesn't exist or has invalid groups, automatically creates it using gmx make_ndx.
    Returns: (receptor_group: int, ligand_group: int)
    """
    def log(msg):
        if log_callback:
            log_callback(msg)

    index_path = os.path.join(work_dir, "index.ndx")

    # Helper function to parse index file and get available groups
    def parse_available_groups(ndx_file):
        """Parse index file and return list of (group_number, group_name) tuples"""
        groups = []
        try:
            current_group_num = -1
            with open(ndx_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line.startswith('[') and line.endswith(']'):
                        current_group_num += 1
                        name = line[1:-1].strip()
                        groups.append((current_group_num, name))
        except Exception as e:
            log(f"Warning: Could not parse {ndx_file}: {e}")
        return groups

    # Case 1: index.ndx doesn't exist - Attempt to create it automatically
    if not os.path.exists(index_path):
        log("⚠️  index.ndx file not found in working directory.")
        log("🔄 Attempting to create custom index groups automatically...")
        
        success, custom_index, receptor_name, ligand_name = create_custom_index(work_dir, tpr_file, log_callback)
        
        if success and os.path.exists(custom_index):
            # Use the newly created index file
            index_path = custom_index
            log(f"✅ Auto-created index file: {custom_index}")
        else:
            log("⚠️  Could not auto-create index.ndx.")
            log("→ Using **default fallback values**: Receptor = 1, Ligand = 13")
            return 1, 13

    receptor_group = None
    ligand_group   = None

    try:
        # Parse the index file and get all available groups
        available_groups = parse_available_groups(index_path)
        
        if not available_groups:
            log("❌ Index file is empty or has no groups.")
            log("→ Falling back to safe defaults: Receptor = 1, Ligand = 13")
            return 1, 13
        
        log(f"📋 Available groups in index file: {len(available_groups)} groups found")
        for gnum, gname in available_groups:
            log(f"   Group {gnum}: {gname}")
        
        # Now perform auto-detection
        # GROMACS index groups are zero-based:
        # [System]=0, [Protein]=1, ...
        for group_num, name_raw in available_groups:
            name = name_raw.lower()

            # Receptor: prefer full protein, avoid partial like -H
            if 'protein' in name and '-h' not in name and receptor_group is None:
                receptor_group = group_num
                log(f"✓ Auto-detected **receptor** group: {group_num} → '{name_raw}'")

            # Ligand: match common names, exclude ions/water
            ligand_keywords = ['unk', 'lig', 'mol', 'ligand', 'het', 'resname', 'drug', 'comp', 'inh', 'sub']
            ion_keywords = ['pot', 'cla', 'na', 'cl', 'ion', 'tip', 'wat', 'sol']
            if any(kw in name for kw in ligand_keywords) and not any(ik in name for ik in ion_keywords):
                if ligand_group is None:
                    ligand_group = group_num
                    log(f"✓ Auto-detected **ligand** group: {group_num} → '{name_raw}'")

        # If auto-detection failed, try intelligent fallback
        if receptor_group is None or ligand_group is None:
            log("⚠️  Standard keywords not found. Analyzing available groups...")
            
            # Look for SOLU (solute) group - common pattern in many setups
            solu_group = None
            for gnum, gname in available_groups:
                if gname.lower() == 'solu':
                    solu_group = gnum
                    log(f"   Found SOLU group at index {gnum} - this is likely protein + ligand")
                    break
            
            # Identify groups to avoid (solvent/ions)
            bad_groups = []
            for gnum, gname in available_groups:
                gname_lower = gname.lower()
                if any(kw in gname_lower for kw in ['solv', 'water', 'wat', 'ion', 'sol', 'tip']):
                    bad_groups.append(gnum)
                    log(f"   Excluding group {gnum} ({gname}) - contains solvent/ions")
            
            # Smart fallback strategy
            if solu_group is not None and receptor_group is None:
                receptor_group = solu_group
                log(f"   Using SOLU group ({solu_group}) for receptor - will create separate ligand via gmx_MMPBSA")
            
            # If still no groups, find the best non-solvent groups
            if receptor_group is None:
                for gnum, gname in available_groups:
                    if gnum not in bad_groups:
                        receptor_group = gnum
                        log(f"   Using group {gnum} ({gname}) for receptor")
                        break
            
            if ligand_group is None:
                # Try to find a second good group, or use the same as receptor
                for gnum, gname in available_groups:
                    if gnum not in bad_groups and gnum != receptor_group:
                        ligand_group = gnum
                        log(f"   Using group {gnum} ({gname}) for ligand")
                        break
                
                # If only one good group exists, use it for both (gmx_MMPBSA will handle separation)
                if ligand_group is None:
                    ligand_group = receptor_group
                    log(f"   Using same group ({receptor_group}) for both - relying on gmx_MMPBSA auto-detection")

        # Final safety fallback
        if receptor_group is None:
            receptor_group = 1
        if ligand_group is None:
            ligand_group = 2

        available_group_nums = [g[0] for g in available_groups]
        
        log(f"→ Using groups → Receptor: {receptor_group} | Ligand: {ligand_group}")
        
        # Validate and warn if using solvent groups
        solvent_keywords = ['solv', 'water', 'wat', 'ion', 'sol', 'tip']
        for gnum, gname in available_groups:
            if gnum in [receptor_group, ligand_group]:
                if any(kw in gname.lower() for kw in solvent_keywords):
                    log(f"⚠️  WARNING: Group {gnum} ({gname}) contains solvent - gmx_MMPBSA may fail!")
                    log("🔄 Attempting to create a new custom index file...")
                    
                    success, custom_index, _, _ = create_custom_index(work_dir, tpr_file, log_callback)
                    if success and os.path.exists(custom_index):
                        # Replace the old index.ndx with the new one
                        try:
                            import shutil
                            shutil.copy(custom_index, index_path)
                            log(f"✅ Replaced index.ndx with auto-created version")
                        except Exception as e:
                            log(f"Note: Could not replace index.ndx: {e}")
                        
                        # Re-parse and use new groups
                        available_groups = parse_available_groups(index_path)
                        if available_groups:
                            receptor_group = available_groups[0][0]
                            ligand_group = available_groups[1][0] if len(available_groups) > 1 else available_groups[0][0]
                            log(f"✅ Using newly created groups: Receptor={receptor_group}, Ligand={ligand_group}")
                    break

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
    receptor_group=1,
    ligand_group=13,
    n_cores=1,
    log_callback=None, 
    progress_callback=None,
    pid_callback=None
):
    """
    Optimized MMPBSA runner using specific system paths and environment prep.
    """
    def log(msg):
        if log_callback: log_callback(msg)

    # 1. Define absolute paths based on user environment
    mmpbsa_bin_path = "/home/sridhar/gromacs_v1/mmpbsa/bin/gmx_MMPBSA"
    amber_bin_path = "/home/sridhar/miniconda/envs/amber_env/bin"
    
    # 2. Update Environment Variables so gmx_MMPBSA finds sander/cpptraj
    env = os.environ.copy()
    env["PATH"] = f"{amber_bin_path}:/home/sridhar/gromacs_v1/mmpbsa/bin:{env.get('PATH', '')}"

    try:
        log(f"🚀 Initializing gmx_MMPBSA from: {mmpbsa_bin_path}\n")
        
        # 3. Build the MPI command
        # Note: Added -O to overwrite and specific path to executable
        cmd = [
            "mpirun", "--use-hwthread-cpus", 
            "-np", str(n_cores),
            mmpbsa_bin_path,
            "-O",
            "-i", input_file,
            "-cs", tpr_file,
            "-ct", trajectory,
            "-ci", index_file,
            "-cg", str(receptor_group), str(ligand_group),
            "-cp", topology_file
        ]

        log(f"📋 Command: {' '.join(cmd)}\n")

        process = subprocess.Popen(
            cmd,
            cwd=work_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=env  # Use the updated environment
        )

        # Inform caller of the started process PID so UI/monitoring can track it
        if pid_callback:
            try:
                pid_callback(process.pid)
            except Exception:
                pass

        # Read output without blocking indefinitely; use select so we can
        # detect process termination even if stdout is quiet.
        try:
            while True:
                reads, _, _ = select.select([process.stdout], [], [], 0.5)
                if reads:
                    line = process.stdout.readline()
                    if not line:
                        # EOF reached for stdout; if process ended, break.
                        if process.poll() is not None:
                            break
                        continue

                    log(line)
                    # Simple progress tracking based on frame completion
                    if "calculating" in line.lower() and "/" in line:
                        try:
                            match = re.search(r'(\d+)/(\d+)', line)
                            if match and progress_callback:
                                pct = (int(match.group(1)) / int(match.group(2))) * 100
                                progress_callback(pct)
                        except:
                            pass
                else:
                    # No new data this iteration; check if process exited.
                    if process.poll() is not None:
                        # Drain any remaining output
                        try:
                            remaining = process.stdout.read()
                            if remaining:
                                for l in remaining.splitlines(True):
                                    log(l)
                                    if "calculating" in l.lower() and "/" in l and progress_callback:
                                        try:
                                            match = re.search(r'(\d+)/(\d+)', l)
                                            if match:
                                                pct = (int(match.group(1)) / int(match.group(2))) * 100
                                                progress_callback(pct)
                                        except: pass
                        except Exception:
                            pass
                        break

        finally:
            returncode = process.wait()

        if returncode != 0:
            raise Exception(f"gmx_MMPBSA failed with exit code {returncode}")

        # Confirm expected final result file presence if available
        final_result_path = os.path.join(work_dir, 'FINAL_RESULTS_MMPBSA.dat')
        if os.path.exists(final_result_path):
            log(f"\n✅ MMPBSA calculation finished successfully! Results: {final_result_path}\n")
        else:
            log("\n✅ MMPBSA calculation finished successfully!\n")

        return 0

    except Exception as e:
        log(f"\n❌ MMPBSA Error: {str(e)}")
        raise e
