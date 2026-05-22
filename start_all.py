"""
start_all.py — Sequential service launcher with health-check gating + per-service log files.
Usage: python start_all.py
"""
import subprocess
import sys
import time
import signal
import os
import zmq
import json
import threading
import urllib.request
import urllib.error
import io
from datetime import datetime

import config

# ── Force UTF-8 for this process and all children ────────────────────────────
os.environ['PYTHONIOENCODING'] = 'utf-8'

# ── Reconfigure stdout/stderr to UTF-8 if on Windows ────────────────────────
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# ── ANSI colors ───────────────────────────────────────────────────────────────
COLORS = {
    "marketdata" : "\033[94m",   # blue
    "verifier"   : "\033[95m",   # magenta
    "main"       : "\033[92m",   # green
    "snapshot"   : "\033[96m",   # cyan
    "SYSTEM"     : "\033[90m",   # grey
}
RESET = "\033[0m"
BOLD  = "\033[1m"
RED   = "\033[91m"

# ── Enable ANSI + UTF-8 on Windows terminal ───────────────────────────────────
if sys.platform == "win32":
    os.system("chcp 65001 > nul")
    import ctypes
    kernel32 = ctypes.windll.kernel32
    kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)

# ── Log directory ─────────────────────────────────────────────────────────────
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs", "services")
os.makedirs(LOG_DIR, exist_ok=True)

# ── Service definitions (ORDER MATTERS) ───────────────────────────────────────
SERVICES = [
    {
        "name"         : "marketdata",
        "cmd"          : [sys.executable, "marketdata_service.py"],
        "health_url"   : None,
        "zmq_port"     : config.ZMQ_MARKETDATA_REQ_PORT,
        "startup_wait" : 20,
        "color"        : COLORS["marketdata"],
    },
    {
        "name"         : "main",
        "cmd"          : [sys.executable, "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "5000"],
        "health_url"   : "http://localhost:5000/health",
        "startup_wait" : 15,
        "color"        : COLORS["main"],
    },
    {
        "name"         : "reconciler",
        "cmd"          : [sys.executable, "order_reconciler.py"],
        "health_url"   : None,
        "zmq_port"     : config.ZMQ_RECONCILER_REQ_PORT,
        "startup_wait" : 10,
        "color"        : COLORS["verifier"],
    },
    {
        "name"         : "snapshot",
        "cmd"          : [sys.executable, "snapshot_service.py"],
        "health_url"   : "http://localhost:8003/health",
        "startup_wait" : 5,
        "color"        : COLORS["snapshot"],
    },
]

# ── All ports used by the stack (ZMQ + HTTP) ─────────────────────────────────
ALL_PORTS = [
    config.ZMQ_MARKETDATA_REQ_PORT,
    config.ZMQ_RECONCILER_REQ_PORT,
    config.ZMQ_VERIFIER_PULL_PORT,
    config.ZMQ_SNAPSHOT_PULL_PORT,
    config.ZMQ_ORDERBOOK_REQ_PORT,
    5000,   # main FastAPI
    8002,   # order_book_service
    8003,   # snapshot_service
]

processes        = []
reported_crashed = set()
log_files: dict[str, io.TextIOWrapper] = {}


# ── Helpers ───────────────────────────────────────────────────────────────────
def tag(color, label):
    return f"{color}{BOLD}[{label}]{RESET}"

def sys_print(msg):
    print(f"{tag(COLORS['SYSTEM'], 'SYSTEM')} {msg}", flush=True)

def ts():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def open_log(name) -> io.TextIOWrapper:
    date_str = datetime.now().strftime("%Y%m%d")
    log_path = os.path.join(LOG_DIR, f"{name}_{date_str}.log")
    f        = open(log_path, "a", encoding="utf-8", buffering=1)
    f.write(f"\n{'='*60}\n")
    f.write(f"  {name.upper()} — started at {ts()}\n")
    f.write(f"{'='*60}\n\n")
    f.flush()
    sys_print(
        f"Logging {COLORS[name] if name in COLORS else ''}{BOLD}{name}{RESET}"
        f" -> logs/services/{name}_{date_str}.log"
    )
    return f


# ── Port cleanup ──────────────────────────────────────────────────────────────
def kill_port(port: int):
    """Kill any process holding a given TCP port (Windows only)."""
    try:
        result = subprocess.run(
            f'netstat -ano | findstr :{port}',
            shell=True, capture_output=True, text=True
        )
        killed_pids = set()
        for line in result.stdout.strip().splitlines():
            parts = line.split()
            # netstat line format: Proto  Local  Foreign  State  PID
            # Only match lines where the local address ends with :<port>
            if len(parts) >= 5:
                local_addr = parts[1]
                pid_str    = parts[-1]
                if f':{port}' in local_addr and pid_str.isdigit():
                    pid = int(pid_str)
                    if pid > 4 and pid not in killed_pids:   # skip System (PID 4)
                        subprocess.run(
                            f'taskkill /PID {pid} /F',
                            shell=True, capture_output=True
                        )
                        killed_pids.add(pid)
                        sys_print(f"  Killed PID {pid} holding port {port}")
    except Exception as e:
        sys_print(f"  Could not clear port {port}: {e}")


def clear_all_ports():
    """Kill zombie processes on all stack ports before launching."""
    sys_print(f"{BOLD}Clearing zombie ports before launch...{RESET}")
    for port in ALL_PORTS:
        kill_port(port)
    sys_print("Port cleanup done. Waiting 0.5s for OS to release...")
    time.sleep(0.5)


# ── Log streamer ──────────────────────────────────────────────────────────────
def stream_output(proc, name, color):
    log_f = log_files.get(name)

    def _read(stream, label):
        for line in iter(stream.readline, b""):
            text = line.decode("utf-8", errors="replace").rstrip()
            print(f"{tag(color, name)} {text}", flush=True)
            if log_f:
                log_f.write(f"[{ts()}] [{label}] {text}\n")
                log_f.flush()
        stream.close()

    threading.Thread(target=_read, args=(proc.stdout, "OUT"), daemon=True).start()
    threading.Thread(target=_read, args=(proc.stderr, "ERR"), daemon=True).start()


# ── Health checks ─────────────────────────────────────────────────────────────
def check_zmq_health(port):
    context = None
    socket  = None
    try:
        context = zmq.Context()
        socket  = context.socket(zmq.REQ)
        socket.setsockopt(zmq.LINGER, 0)
        socket.connect(f"tcp://localhost:{port}")
        socket.send_json({"command": "health_check", "payload": {}})

        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)
        socks = dict(poller.poll(2000))
        if socket in socks and socks[socket] == zmq.POLLIN:
            response = socket.recv_json()
            return response.get("success", False)
    except Exception:
        return False
    finally:
        if socket:  socket.close()
        if context: context.term()
    return False


def wait_for_health(name, url, timeout, color, zmq_port=None):
    deadline = time.time() + timeout

    if zmq_port:
        sys_print(f"Waiting for {color}{BOLD}{name}{RESET} on ZMQ port {zmq_port}")
        while time.time() < deadline:
            if check_zmq_health(zmq_port):
                sys_print(f"{color}{BOLD}{name}{RESET} is UP [OK]")
                return True
            time.sleep(0.2)
        sys_print(f"{RED}ERROR: {name} ZMQ health check timed out after {timeout}s.{RESET}")
        return False

    elif url:
        sys_print(f"Waiting for {color}{BOLD}{name}{RESET} at {url}")
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(url, timeout=2) as r:
                    if r.status == 200:
                        sys_print(f"{color}{BOLD}{name}{RESET} is UP [OK]")
                        return True
            except Exception:
                pass
            time.sleep(0.2)
        sys_print(f"{RED}ERROR: {name} health check timed out after {timeout}s.{RESET}")
        return False

    else:
        sys_print(
            f"Waiting {timeout}s for {color}{BOLD}{name}{RESET}"
            f" to initialize (no health check)..."
        )
        time.sleep(timeout)
        sys_print(f"{color}{BOLD}{name}{RESET} assumed UP.")
        return True


# ── Shutdown ──────────────────────────────────────────────────────────────────
def shutdown(sig=None, frame=None):
    sys_print("Shutting down all services...")
    for p, name in reversed(processes):
        if p.poll() is None:
            sys_print(f"  stopping {name} (pid={p.pid})")
            p.terminate()
    time.sleep(2)
    for p, name in processes:
        if p.poll() is None:
            p.kill()
    for name, f in log_files.items():
        try:
            f.write(f"\n[{ts()}] [SYSTEM] Process terminated.\n")
            f.close()
        except Exception:
            pass
    sys_print("All services stopped.")
    sys.exit(0)


signal.signal(signal.SIGINT,  shutdown)
signal.signal(signal.SIGTERM, shutdown)


# ── Main launcher ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"\n{BOLD}{'=' * 70}")
    print(f"   ALGO TRADING STACK  |  SEQUENTIAL LAUNCHER")
    print(f"   Logs -> logs/services/<name>_YYYYMMDD.log")
    print(f"{'=' * 70}{RESET}\n", flush=True)

    # ── Step 0: Kill zombie ports from previous crashes ───────────────────────
    clear_all_ports()

    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"]       = "1"

    for svc in SERVICES:
        name  = svc["name"]
        color = svc["color"]

        log_files[name] = open_log(name)
        sys_print(f"Starting {color}{BOLD}{name}{RESET} ...")

        proc = subprocess.Popen(
            svc["cmd"],
            stdout = subprocess.PIPE,
            stderr = subprocess.PIPE,
            cwd    = os.path.dirname(os.path.abspath(__file__)),
            env    = env,
        )
        processes.append((proc, name))
        stream_output(proc, name, color)

        time.sleep(0.5)

        if not wait_for_health(
            name, svc.get("health_url"),
            svc["startup_wait"], color,
            zmq_port=svc.get("zmq_port")
        ):
            sys_print(f"{RED}ERROR: Health check failed for {name}. Aborting.{RESET}")
            shutdown()

        if proc.poll() is not None:
            sys_print(
                f"{RED}ERROR: {name} crashed at launch"
                f" (code={proc.returncode}). Aborting.{RESET}"
            )
            shutdown()

    print(f"\n{BOLD}{'=' * 70}")
    print(f"   ALL SERVICES LAUNCHED  |  Ctrl+C to stop all")
    print(f"   Run: python tail_all.py  to open per-service log tabs")
    print(f"{'=' * 70}{RESET}\n", flush=True)

    # ── Keep alive + crash monitor ────────────────────────────────────────────
    try:
        while True:
            time.sleep(5)
            for p, name in processes:
                if p.poll() is not None and name not in reported_crashed:
                    reported_crashed.add(name)
                    sys_print(
                        f"{RED}WARNING: {name} exited unexpectedly"
                        f" (code={p.returncode}){RESET}"
                    )
    except KeyboardInterrupt:
        shutdown()
