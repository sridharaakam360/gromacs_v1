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
nstxout                  = 0
nstvout                  = 0
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
    
    fpath = os.path.join(gromacs_dir, fname)
    with open(fpath, "w") as f:
        f.write(content)
    
    return fpath

def select_input_structure(gromacs_dir, stage):
    """
    Select the appropriate input structure file based on stage and available files
    
    Args:
        gromacs_dir: Path to GROMACS working directory
        stage: Simulation stage
    
    Returns:
        Filename (not full path) of the input structure
    
    Raises:
        FileNotFoundError: If no suitable input file is found
    """
    if stage == "setup":
        gro_file = "step3_input.gro"
        if not os.path.exists(os.path.join(gromacs_dir, gro_file)):
            raise FileNotFoundError(
                f"Input structure {gro_file} not found. "
                "Please provide step3_input.gro from CHARMM-GUI."
            )
    
    elif stage == "equilibration":
        # Priority: setup output > original input
        if os.path.exists(os.path.join(gromacs_dir, "setup.gro")):
            gro_file = "setup.gro"
        elif os.path.exists(os.path.join(gromacs_dir, "step3_input.gro")):
            gro_file = "step3_input.gro"
        else:
            raise FileNotFoundError(
                "No input structure found for equilibration. "
                "Run 'setup' stage first or provide step3_input.gro."
            )
    
    else:  # production
        # Priority: equilibration output > setup output > original input
        if os.path.exists(os.path.join(gromacs_dir, "equil.gro")):
            gro_file = "equil.gro"
        elif os.path.exists(os.path.join(gromacs_dir, "setup.gro")):
            gro_file = "setup.gro"
        elif os.path.exists(os.path.join(gromacs_dir, "step3_input.gro")):
            gro_file = "step3_input.gro"
        else:
            raise FileNotFoundError(
                "No input structure found for production. "
                "Run 'setup' or 'equilibration' stage first, or provide step3_input.gro."
            )
    
    return gro_file

def run_md(
    gromacs_dir,
    use_gpu,
    threads,
    total_steps,
    resume=False,
    log_callback=None,
    progress_callback=None,
    pid_callback=None,
    stage="production"
):
    """
    Run GROMACS MD simulation
    
    Args:
        gromacs_dir: Path to GROMACS working directory
        use_gpu: Whether to use GPU acceleration
        threads: Number of CPU threads
        total_steps: Total number of MD steps to run
        resume: Whether to resume from checkpoint
        log_callback: Function to call with log messages
        progress_callback: Function to call with progress updates
        pid_callback: Function to call with process PID
        stage: Simulation stage ("setup", "equilibration", "production")
    
    Returns:
        Exit code (0 for success)
    
    Raises:
        Exception: If simulation fails
    """
    
    # Default callbacks if none provided
    if log_callback is None:
        log_callback = lambda x: print(x, end="")
    if progress_callback is None:
        progress_callback = lambda step, total: None
    if pid_callback is None:
        pid_callback = lambda pid: None
    
    # Check gmx command first
    gmx_cmd = check_gmx_command()
    if gmx_cmd is None:
        error_msg = "❌ GROMACS (gmx) command not found! Please install GROMACS first.\n"
        log_callback(error_msg)
        raise Exception(error_msg)
    
    log_callback(f"✅ Using GROMACS command: {gmx_cmd}\n")
    
    # Validate environment
    try:
        validate_environment(gromacs_dir, stage)
    except Exception as e:
        log_callback(f"❌ {str(e)}\n")
        raise
    
    # Find MDP file
    mdp_file = find_mdp_file(gromacs_dir, stage)
    mdp_basename = os.path.basename(mdp_file)
    
    # Determine input structure file
    try:
        gro_file = select_input_structure(gromacs_dir, stage)
    except FileNotFoundError as e:
        log_callback(f"❌ {str(e)}\n")
        raise
    
    # Output file base name
    deffnm_map = {
        "setup": "setup",
        "equilibration": "equil",
        "production": "md"
    }
    deffnm = deffnm_map.get(stage, "md")
    
    # Create log directory
    logs_dir = os.path.join(gromacs_dir, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(logs_dir, f"{deffnm}_{timestamp}.log")
    
    # Validate total_steps
    if total_steps <= 0:
        error_msg = f"❌ Invalid total_steps: {total_steps}. Must be > 0.\n"
        log_callback(error_msg)
        raise ValueError(error_msg)
    
    try:
        # Write header to log
        with open(log_file, "w") as f:
            f.write("=" * 70 + "\n")
            f.write(f"GROMACS {stage.upper()} RUN\n")
            f.write("=" * 70 + "\n")
            f.write(f"Timestamp: {datetime.now().isoformat()}\n")
            f.write(f"Directory: {gromacs_dir}\n")
            f.write(f"Stage: {stage}\n")
            f.write(f"MDP: {mdp_basename}\n")
            f.write(f"Input: {gro_file}\n")
            f.write(f"Output: {deffnm}\n")
            f.write(f"GPU: {use_gpu}\n")
            f.write(f"Threads: {threads}\n")
            f.write(f"Total Steps: {total_steps}\n")
            f.write(f"Resume: {resume}\n")
            f.write("=" * 70 + "\n\n")
        
        log_callback(f"📁 Working directory: {gromacs_dir}\n")
        log_callback(f"📝 Log file: {log_file}\n")
        log_callback(f"🎯 Stage: {stage.capitalize()}\n")
        log_callback(f"📄 Input structure: {gro_file}\n")
        log_callback(f"⚙️  MDP file: {mdp_basename}\n")
        
        # Grompp step (preprocessing)
        tpr_file = os.path.join(gromacs_dir, f"{deffnm}.tpr")
        need_grompp = not resume or not os.path.exists(tpr_file)
        
        if need_grompp:
            grompp_cmd = [
                gmx_cmd, "grompp",
                "-f", mdp_basename,
                "-c", gro_file,
                "-r", gro_file,          # ✅ FIX: reference file for restraints
                "-p", "topol.top",
                "-o", f"{deffnm}.tpr",
                "-maxwarn", "2"
            ]

            
            # Add index file if it exists
            index_file = os.path.join(gromacs_dir, "index.ndx")
            if os.path.exists(index_file):
                grompp_cmd.extend(["-n", "index.ndx"])
            
            log_callback(f"🔧 Running grompp (preprocessing)...\n")
            with open(log_file, "a") as f:
                f.write(f"\n{'=' * 70}\n")
                f.write(f"GROMPP COMMAND\n")
                f.write(f"{'=' * 70}\n")
                f.write(f"{' '.join(grompp_cmd)}\n\n")
            
            try:
                grompp_result = subprocess.run(
                    grompp_cmd, 
                    cwd=gromacs_dir, 
                    capture_output=True, 
                    text=True,
                    timeout=600  # 10 minute timeout (increased from 5)
                )
            except subprocess.TimeoutExpired:
                error_msg = "❌ grompp timed out after 10 minutes\n"
                log_callback(error_msg)
                raise Exception(error_msg)
            
            with open(log_file, "a") as f:
                f.write("=== grompp STDOUT ===\n")
                f.write(grompp_result.stdout if grompp_result.stdout else "(empty)\n")
                f.write("\n=== grompp STDERR ===\n")
                f.write(grompp_result.stderr if grompp_result.stderr else "(empty)\n")
                f.write("=" * 70 + "\n\n")
            
            if grompp_result.returncode != 0:
                error_msg = f"❌ grompp failed with exit code {grompp_result.returncode}\n"
                if grompp_result.stderr:
                    # Show more of the error (1000 chars instead of 500)
                    error_msg += f"Error output:\n{grompp_result.stderr[:1000]}\n"
                log_callback(error_msg)
                raise Exception(f"grompp failed: {grompp_result.stderr}")
            
            log_callback(f"✅ grompp completed successfully\n")
        else:
            log_callback(f"↩️ Resuming from existing .tpr file\n")
        
        # Mdrun step (MD execution)
        mdrun_cmd = [
            gmx_cmd, "mdrun",
            "-deffnm", deffnm,
            "-nt", str(threads),
            "-nsteps", str(total_steps)  # Always specify nsteps explicitly
        ]
        
        # Add checkpoint for resume
        cpt_file = os.path.join(gromacs_dir, f"{deffnm}.cpt")
        if resume and os.path.exists(cpt_file):
            mdrun_cmd.extend(["-cpi", f"{deffnm}.cpt"])
            log_callback(f"↩️ Resuming from checkpoint: {deffnm}.cpt\n")
        
        # GPU settings
        if use_gpu:
            mdrun_cmd.extend([
                "-nb", "gpu",      # Non-bonded on GPU
                "-pme", "gpu",     # PME on GPU
                "-update", "gpu",  # Update on GPU (if supported)
                "-bonded", "cpu"   # Keep bonded on CPU for compatibility
            ])
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
    

import multiprocessing  # Add this import at the top

def run_mmpbsa(work_dir, tpr_file, trajectory, index_file, input_file="mmpbsa.in", log_callback=None, progress_callback=None):
    """
    Executes the gmx_MMPBSA command with MPI parallelization.
    """
    # Auto-detect cores (use all but 1 to avoid system lag)
    n_cores = max(1, multiprocessing.cpu_count() - 1)
    
    if log_callback:
        log_callback(f"Using {n_cores} CPU cores for parallel MMPBSA calculation")
    
    cmd = [
        "mpirun", "-np", str(n_cores),
        "gmx_MMPBSA", "-O",
        "-i", input_file,
        "-cs", tpr_file,
        "-ct", trajectory,
        "-ci", index_file,
        "-cg", "1", "13",  # Default groups – change if needed
        "-nogui"
    ]
    
    # Reuse your subprocess logic from run_md (with live streaming for logs/progress)
    process = subprocess.Popen(
        cmd,
        cwd=work_dir,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )
    
    total_frames = 1000  # Placeholder – parse from mmpbsa.in or trajectory for real progress
    frame_count = 0
    
    while True:
        line = process.stdout.readline()
        if not line and process.poll() is not None:
            break
        
        if line:
            if log_callback:
                log_callback(line.strip())
            
            # Rough progress (customize based on output lines like "Processing frame X")
            if "frame" in line.lower() or "snapshot" in line.lower():
                frame_count += 1
                pct = (frame_count / total_frames) * 100
                if progress_callback:
                    progress_callback(pct)
    
    returncode = process.wait()
    
    if returncode != 0:
        error_msg = f"MMPBSA failed with code {returncode}"
        if log_callback:
            log_callback(error_msg)
        raise Exception(error_msg)
    
    if log_callback:
        log_callback("MMPBSA completed successfully")
    if progress_callback:
        progress_callback(100)