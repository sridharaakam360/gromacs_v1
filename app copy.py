import streamlit as st
import os
import threading
import queue
import time

from system_info import cpu_info, gpu_info
from mdp_utils import update_mdp_nsteps, get_mdp_file
from gromacs_runner import run_md, stop_md

from system_info import check_mmpbsa_installed
# --------------------------------------------------
# Module-level lock for thread safety
# --------------------------------------------------
_state_lock = threading.Lock()

# --------------------------------------------------
# Constants
# --------------------------------------------------
MAX_FILE_SIZE = 100 * 1024 * 1024  # 100 MB
MAX_SIMULATION_TIME_NS = 1000  # Maximum simulation time in nanoseconds

# --------------------------------------------------
# Setup
# --------------------------------------------------
st.set_page_config(page_title="GROMACS MD Runner", layout="wide")
st.title("🧪 GROMACS MD Runner (CHARMM-GUI)")

# --------------------------------------------------
# Session state defaults (without Queue - that's created separately)
# --------------------------------------------------
defaults = {
    "logs": [],
    "running": False,
    "paused": False,
    "finished": False,
    "error": None,
    "progress": 0,
    "md_pid": None,
    "total_steps": 0,
    "show_logs": False,
    "current_stage": "setup",
    "setup_completed": False,
    "last_log_file": None,
    "md_thread": None,
    "_prev_setup_completed": False  # Track state changes
}

for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

# Initialize Queue separately for each session
if "log_queue" not in st.session_state:
    st.session_state.log_queue = queue.Queue()

# --------------------------------------------------
# Helper Functions
# --------------------------------------------------
def safe_read_file(filepath, max_size=MAX_FILE_SIZE):
    """Safely read file with size check"""
    try:
        file_size = os.path.getsize(filepath)
        if file_size > max_size:
            raise ValueError(
                f"File too large: {file_size / 1024 / 1024:.1f} MB "
                f"(max {max_size / 1024 / 1024:.1f} MB)"
            )
        
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            return f.read()
    except Exception as e:
        raise Exception(f"Error reading file: {str(e)}")

def validate_inputs(charmm_dir, ns, threads, cpus, current_stage):
    """Validate all inputs before running simulation"""
    errors = []
    
    # Check directory
    if not charmm_dir or not os.path.isdir(charmm_dir):
        errors.append("Invalid GROMACS directory")
        return errors  # Early return if directory is invalid
    
    # Check simulation time
    if ns <= 0:
        errors.append("Simulation time must be greater than 0")
    elif ns > MAX_SIMULATION_TIME_NS:
        errors.append(f"Simulation time too large (max {MAX_SIMULATION_TIME_NS} ns)")
    
    # Check threads
    if threads < 1 or threads > cpus:
        errors.append(f"Threads must be between 1 and {cpus}")
    
    # Check required files based on stage
    required_files = {
        "setup": ["topol.top", "step3_input.gro"],
        "equilibration": ["topol.top"],
        "production": ["topol.top"]
    }
    
    for file in required_files.get(current_stage, []):
        if not os.path.exists(os.path.join(charmm_dir, file)):
            errors.append(f"Missing required file: {file}")
    
    # Check MDP file exists
    try:
        mdp_path = get_mdp_file(charmm_dir, current_stage)
        if not os.path.exists(mdp_path):
            errors.append(f"MDP file not found for {current_stage} stage")
    except Exception as e:
        errors.append(f"MDP file error: {str(e)}")
    
    return errors

# --------------------------------------------------
# System info
# --------------------------------------------------
try:
    cpus = cpu_info()
    gpus = gpu_info()
except Exception as e:
    st.error(f"Error detecting system info: {e}")
    cpus = 1
    gpus = []

st.sidebar.header("🖥 System")
st.sidebar.write(f"CPU cores: {cpus}")
if gpus:
    st.sidebar.success(f"GPU detected: {gpus[0]}")
else:
    st.sidebar.info("No GPU detected")

# Debug info
with st.sidebar.expander("🔍 Debug State"):
    st.write(f"**setup_completed**: {st.session_state.setup_completed}")
    st.write(f"**running**: {st.session_state.running}")
    st.write(f"**finished**: {st.session_state.finished}")
    st.write(f"**progress**: {st.session_state.progress}%")
    st.write(f"**current_stage**: {st.session_state.current_stage}")

# --------------------------------------------------
# Inputs
# --------------------------------------------------
charmm_dir = st.text_input(
    "CHARMM-GUI GROMACS folder",
    value=os.path.expanduser("~/Downloads/charmm-gui/gromacs"),
    help="Path to the GROMACS directory from CHARMM-GUI"
)

# Auto-detect files in the directory
if charmm_dir and os.path.isdir(charmm_dir):
    st.markdown("### 📁 Input Files in Directory")
    
    required_files = [
        "step3_input.gro",
        "step3_input.pdb",
        "step4_0_minimization.mdp",
        "step4.1_equilibration.mdp",
        "step4_equilibration.mdp",
        "step5_production.mdp",
        "topol.top"
    ]
    
    found_files = {}
    for fname in required_files:
        fpath = os.path.join(charmm_dir, fname)
        if os.path.isfile(fpath):
            found_files[fname] = fpath
    
    if found_files:
        cols = st.columns([3, 1])
        with cols[0]:
            st.write("**Found Files:**")
        with cols[1]:
            st.write("**Action**")
        
        for fname, fpath in found_files.items():
            col1, col2 = st.columns([3, 1])
            with col1:
                st.write(f"✅ {fname}")
            with col2:
                if st.button("View", key=f"view_{fname}"):
                    try:
                        content = safe_read_file(fpath)
                        with st.expander(f"📄 {fname}", expanded=False):
                            st.code(content, language="text")
                    except ValueError as e:
                        st.error(str(e))
                    except Exception as e:
                        st.error(f"Error reading file: {e}")
    else:
        st.warning("⚠️ No GROMACS files found in this directory")
    
    # Display log files section
    st.markdown("### 📋 Generated Log Files")
    import glob
    
    # Look in both root and logs subdirectory
    log_patterns = [
        os.path.join(charmm_dir, "md_run_*.log"),
        os.path.join(charmm_dir, "logs", "*.log")
    ]
    
    log_files = []
    for pattern in log_patterns:
        log_files.extend(glob.glob(pattern))
    
    # Remove duplicates and sort by modification time (newest first)
    log_files = sorted(
        list(set(log_files)), 
        key=lambda x: os.path.getmtime(x) if os.path.exists(x) else 0, 
        reverse=True
    )
    
    if log_files:
        cols = st.columns([3, 1])
        with cols[0]:
            st.write("**Log Files:**")
        with cols[1]:
            st.write("**Action**")
        
        for log_file in log_files[:10]:  # Show last 10 log files
            fname = os.path.basename(log_file)
            col1, col2 = st.columns([3, 1])
            with col1:
                # Show file size and modification time
                try:
                    size_kb = os.path.getsize(log_file) / 1024
                    mtime = time.strftime('%Y-%m-%d %H:%M', time.localtime(os.path.getmtime(log_file)))
                    st.write(f"📄 {fname} ({size_kb:.1f} KB, {mtime})")
                except Exception:
                    st.write(f"📄 {fname}")
            with col2:
                if st.button("View", key=f"view_log_{fname}"):
                    try:
                        content = safe_read_file(log_file)
                        with st.expander(f"📋 {fname}", expanded=False):
                            st.text_area("Log Content", value=content, height=300, disabled=True, key=f"log_content_{fname}")
                    except ValueError as e:
                        st.error(str(e))
                    except Exception as e:
                        st.error(f"Error reading log file: {e}")
    else:
        st.info("ℹ️ No log files yet. Run a simulation to generate logs.")
    
    st.divider()

ns = st.number_input(
    "Simulation time (ns)", 
    min_value=0.1, 
    max_value=float(MAX_SIMULATION_TIME_NS),
    value=10.0,
    help=f"Simulation time in nanoseconds (max {MAX_SIMULATION_TIME_NS} ns)"
)

# Stage selection with gating
st.markdown("### 🎯 Simulation Stage")

# ✅ NEW: Auto-detect setup completion from files
try:
    if charmm_dir and os.path.isdir(charmm_dir):
        # Check if setup output file exists (setup.gro indicates setup was completed)
        setup_output = os.path.join(charmm_dir, "setup.gro")
        
        if os.path.exists(setup_output) and not st.session_state.setup_completed:
            st.session_state.setup_completed = True
            st.success("✅ Setup output detected - all stages unlocked!")
        
except Exception as e:
    pass

col1, col2 = st.columns([2, 3])

with col1:
    available_stages = ["Setup"]
    if st.session_state.setup_completed:
        available_stages = ["Setup", "Equilibration", "Production"]
        st.success("✅ All stages unlocked!")
    else:
        st.info("⚠️ Complete **Setup** first to unlock other stages")
    
    stage = st.radio(
        "Stage",
        available_stages,
        horizontal=False,
        help="Select the simulation stage to run"
    )
    # Convert to lowercase for internal use
    st.session_state.current_stage = stage.lower()

with col2:
    stage_info = {
        "setup": "🔧 Initialize system, run energy minimization",
        "equilibration": "🌡️ Warm up system, equilibrate temperature & pressure",
        "production": "🎬 Collect production MD data for analysis"
    }
    st.markdown(f"**{stage_info.get(st.session_state.current_stage, '')}**")

run_mode = st.radio(
    "Run mode",
    ["CPU only", "GPU (recommended)"] if gpus else ["CPU only"],
    help="Select GPU for faster simulations if available"
)

# ⚠️ Info about GPU limitations
if st.session_state.current_stage == "setup":
    st.info("ℹ️ **Setup stage note**: GPU will be disabled (energy minimization not supported). CPU will be used.")

threads = st.slider(
    "CPU threads", 
    1, 
    cpus, 
    min(8, cpus),
    help="Number of CPU threads to use"
)

# --------------------------------------------------
# Callbacks (thread safe)
# --------------------------------------------------
def log_callback(line):
    """Add log line to queue (thread-safe)"""
    try:
        st.session_state.log_queue.put(line)
    except Exception:
        pass  # Silently ignore queue errors

def progress_callback(step, total):
    """Update progress (thread-safe)"""
    if total > 0:
        with _state_lock:
            st.session_state.progress = int((step / total) * 100)

def pid_callback(pid):
    """Store process PID (thread-safe)"""
    with _state_lock:
        st.session_state.md_pid = pid

# --------------------------------------------------
# Background runner
# --------------------------------------------------
def run_job(gromacs_dir_param, use_gpu_param, threads_param, total_steps_param, stage_param, resume=False):
    """
    Run MD simulation in background thread
    """
    try:
        result = run_md(
            gromacs_dir=gromacs_dir_param,
            use_gpu=use_gpu_param,
            threads=threads_param,
            total_steps=total_steps_param,
            resume=resume,
            log_callback=log_callback,
            progress_callback=progress_callback,
            pid_callback=pid_callback,
            stage=stage_param
        )

        with _state_lock:
            # ✅ force completion state
            st.session_state.progress = 100
            st.session_state.running = False
            st.session_state.paused = False
            st.session_state.finished = True
            st.session_state.md_pid = None   # ✅ CRITICAL: clear stale PID

            # ✅ unlock next stages
            if stage_param == "setup" and result == 0:
                st.session_state.setup_completed = True  # ✅ SET FLAG DIRECTLY
                log_callback(f"\n🔓 SETUP UNLOCKED - setup_completed set to True\n")
                log_callback("__SETUP_COMPLETED__")

                log_callback(
                    "\n✅ Setup stage completed successfully! "
                    "You can now run Equilibration and Production.\n"
                )
            else:
                log_callback(f"\n📊 Stage: {stage_param}, Result: {result}\n")

    except Exception as e:
        error_msg = f"\n❌ Error: {str(e)}\n"
        with _state_lock:
            st.session_state.error = str(e)
            st.session_state.running = False
            st.session_state.paused = False
            st.session_state.finished = False
            st.session_state.md_pid = None
        log_callback(error_msg)

# --------------------------------------------------
# Control buttons
# --------------------------------------------------
col1, col2, col3 = st.columns(3)

run_disabled = st.session_state.running
if st.session_state.current_stage in ["equilibration", "production"] and not st.session_state.setup_completed:
    run_disabled = True

with col1:
    if run_disabled and st.session_state.current_stage in ["equilibration", "production"]:
        st.button("▶ Run MD", disabled=True, help="Complete Setup stage first")
        st.caption("⚠️ Complete Setup first")
    else:
        if st.button("▶ Run MD", disabled=run_disabled, help="Start the MD simulation"):
            # Validate inputs
            validation_errors = validate_inputs(
                charmm_dir, ns, threads, cpus, st.session_state.current_stage
            )
            
            if validation_errors:
                for error in validation_errors:
                    st.error(f"❌ {error}")
            else:
                try:
                    mdp_path = get_mdp_file(charmm_dir, st.session_state.current_stage)
                    nsteps = update_mdp_nsteps(mdp_path, ns)

                    with _state_lock:
                        st.session_state.total_steps = nsteps
                        st.session_state.running = True
                        st.session_state.finished = False
                        st.session_state.paused = False
                        st.session_state.error = None
                        st.session_state.logs.clear()
                        st.session_state.progress = 0

                    # Create and start thread (non-daemon for persistence)
                    # Pass all parameters explicitly - thread cannot access session state
                    
                    # ⚠️ CRITICAL: GPU doesn't support energy minimization (setup stage)
                    # Force CPU-only for setup, GPU for others
                    use_gpu = run_mode.startswith("GPU")
                    if st.session_state.current_stage == "setup":
                        if use_gpu:
                            st.warning("⚠️ GPU disabled for Setup stage (energy minimization not supported on GPU). Using CPU only.")
                        use_gpu = False
                    
                    thread = threading.Thread(
                        target=run_job,
                        kwargs={
                            "gromacs_dir_param": charmm_dir,
                            "use_gpu_param": use_gpu,
                            "threads_param": threads,
                            "total_steps_param": nsteps,
                            "stage_param": st.session_state.current_stage,
                            "resume": False
                        },
                        daemon=False  # Changed from True to persist across reruns
                    )
                    thread.start()
                    st.session_state.md_thread = thread
                    
                    st.rerun()
                    
                except Exception as e:
                    st.error(f"❌ Error starting simulation: {str(e)}")

with col2:
    if st.button("⏸ Pause", disabled=not st.session_state.running, help="Pause the simulation"):
        success = stop_md(st.session_state.md_pid)
        with _state_lock:
            st.session_state.running = False
            st.session_state.paused = True
        
        if success:
            st.warning("⏸ MD paused (checkpoint saved)")
        else:
            st.error("❌ Failed to pause simulation")

with col3:
    if st.button("▶ Resume", disabled=st.session_state.running or not st.session_state.paused, help="Resume the paused simulation"):
        with _state_lock:
            st.session_state.running = True
            st.session_state.error = None
            st.session_state.paused = False

        # Pass all parameters explicitly
        thread = threading.Thread(
            target=run_job,
            kwargs={
                "gromacs_dir_param": charmm_dir,
                "use_gpu_param": run_mode.startswith("GPU"),
                "threads_param": threads,
                "total_steps_param": st.session_state.total_steps,
                "stage_param": st.session_state.current_stage,
                "resume": True
            },
            daemon=False
        )
        thread.start()
        st.session_state.md_thread = thread
        
        st.rerun()

# --------------------------------------------------
# Progress bar + status
# --------------------------------------------------
st.progress(st.session_state.progress / 100.0 if st.session_state.progress <= 100 else 1.0)

status_emoji = {
    "running": "🟢",
    "paused": "⏸",
    "finished": "✅",
    "idle": "⏹"
}

if st.session_state.running:
    status = f"{status_emoji['running']} Running"
elif st.session_state.paused:
    status = f"{status_emoji['paused']} Paused"
elif st.session_state.finished:
    status = f"{status_emoji['finished']} Finished"
else:
    status = f"{status_emoji['idle']} Idle"

st.markdown(f"**Status:** {status}")

if st.session_state.setup_completed:
    st.success("✅ Setup completed - Equilibration & Production unlocked")

# Stage status info
st.info(
    f"📌 Current Stage: **{st.session_state.current_stage.capitalize()}** | "
    f"Total steps: **{st.session_state.total_steps:,}** | "
    f"Progress: **{st.session_state.progress}%**"
)

# --------------------------------------------------
# View log toggle (UNDER status bar)
# --------------------------------------------------
col1, col2, col3 = st.columns(3)
with col1:
    if st.button("📜 Toggle logs", help="Show/hide simulation logs"):
        st.session_state.show_logs = not st.session_state.show_logs

with col2:
    if st.button("🔄 Refresh", help="Refresh the page to update status"):
        st.rerun()

with col3:
    if st.session_state.error:
        if st.button("🗑️ Clear error", help="Clear the error message"):
            with _state_lock:
                st.session_state.error = None
            st.rerun()

# --------------------------------------------------
# Collect logs (main thread) and detect special tokens
# --------------------------------------------------
_logs_changed = False
while not st.session_state.log_queue.empty():
    try:
        line = st.session_state.log_queue.get_nowait()
    except Exception:
        break

    # Detect internal control token indicating setup completion
    try:
        if isinstance(line, str) and line.strip() == "__SETUP_COMPLETED__":
            with _state_lock:
                if not st.session_state.setup_completed:
                    st.session_state.setup_completed = True
                    _logs_changed = True
            # don't append the control token to visible logs
            continue
    except Exception:
        pass

    st.session_state.logs.append(line)

# Check if background thread has finished (but state still shows running)
if st.session_state.running and st.session_state.md_thread is not None:
    if not st.session_state.md_thread.is_alive():
        # Thread finished but state wasn't updated - force completion
        with _state_lock:
            st.session_state.running = False
            st.session_state.progress = 100
            st.session_state.finished = True
            _logs_changed = True

# ✅ Check if setup_completed flag changed (thread sets it, main thread detects it)
if st.session_state.setup_completed and not st.session_state._prev_setup_completed:
    st.session_state._prev_setup_completed = True
    _logs_changed = True
elif not st.session_state.setup_completed and st.session_state._prev_setup_completed:
    st.session_state._prev_setup_completed = False

# ✅ Force rerun when setup stage completes (even if flag already was True)
if st.session_state.finished and st.session_state.current_stage == "setup" and st.session_state.progress == 100:
    # Small delay to ensure thread has finished setting all flags
    time.sleep(0.1)
    _logs_changed = True

# If state changed, rerun to update UI immediately
if _logs_changed:
    st.rerun()

# --------------------------------------------------
# Terminal-style log viewer
# --------------------------------------------------
# Show logs by default if running, or if toggle is on
should_show_logs = st.session_state.show_logs or st.session_state.running

if should_show_logs and st.session_state.logs:
    st.markdown("### 🖥 MD Run Log")
    log_content = "".join(st.session_state.logs)
    st.text_area(
        "MD Run Log",
        value=log_content,
        height=400,
        label_visibility="collapsed",
        key="log_display"
    )

# --------------------------------------------------
# Errors / success
# --------------------------------------------------
if st.session_state.error:
    st.error(f"❌ Error: {st.session_state.error}")

if st.session_state.finished and not st.session_state.error:
    st.success("✅ MD simulation completed successfully!")
    st.balloons()

# --------------------------------------------------
# Auto-refresh when running
# --------------------------------------------------
if st.session_state.running and not st.session_state.finished:
    time.sleep(2)
    st.rerun()
