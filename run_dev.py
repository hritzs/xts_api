"""
run_dev.py - Development server with hot-reloading.

This script starts the uvicorn server with its built-in reloader, configured
to watch for changes in all relevant project files. This is a more standard
approach than a custom file watcher.

Usage:
1. Install uvicorn: pip install "uvicorn[standard]"
2. Run this script: python run_dev.py

NOTE: This is for DEVELOPMENT ONLY. Do not use in production.
"""
import subprocess
import sys

import socket
import time
try:
    import config
except ImportError:
    # Create a dummy config object if config.py doesn't exist,
    # allowing the script to run and show a more specific error from uvicorn.
    class DummyConfig:
        HOST = '127.0.0.1'
        PORT = 5000
        MARKET_DATA_PORT = 8001
        ORDER_SERVICE_PORT = 8002
    config = DummyConfig()


if __name__ == "__main__":
    # The main application (e.g., 'main:app')
    APP = "main:app"
    # Host and Port from config, with fallbacks
    HOST = getattr(config, 'HOST', '127.0.0.1')
    PORT = getattr(config, 'PORT', 5000)
    # Directories to watch (recursively)
    WATCH_DIRECTORIES = ["."]
    # File extensions to watch (as glob patterns)
    # NOTE: Uvicorn's --reload flag automatically watches for .py files.
    # We only need to add other patterns we want to watch.
    WATCH_PATTERNS = ["*.json", "*.ini", "*.html", "*.js", "*.css"]
    # Build the uvicorn command
    command = [
        sys.executable,
        "-m",
        "uvicorn",
        APP,
        "--host",
        str(HOST),
        "--port",
        str(PORT),
        "--reload",
    ]

    # Add directories to watch
    for directory in WATCH_DIRECTORIES:
        command.extend(["--reload-dir", directory])

    # Add file patterns to watch
    # Note: uvicorn's default watcher already includes .py
    for pattern in WATCH_PATTERNS:
        command.extend(["--reload-include", pattern])

    # NOTE: We do NOT wait for services here anymore.
    # The main application (main.py) is responsible for spawning the microservices
    # (Market Data, Order Book, Snapshot) as subprocesses during its startup (lifespan).
    # Waiting here would cause a deadlock.

    print("\n" + "="*50)
    print("🚀 Starting Uvicorn development server with hot-reloading...")
    print(f"   Running command: {' '.join(command)}")
    print("   Press Ctrl+C to stop.")
    print("="*50 + "\n")

    try:
        # Run the command. Uvicorn will take over and handle Ctrl+C.
        subprocess.run(command)
    except KeyboardInterrupt:
        # This is caught if the user presses Ctrl+C while this script is running,
        # before or after uvicorn exits.
        print("\n👋 Shutting down development server.")
    except FileNotFoundError:
        print("\n❌ Error: 'python' or 'uvicorn' command not found in your PATH.")
        print("   Please ensure Python and uvicorn are installed and accessible.")