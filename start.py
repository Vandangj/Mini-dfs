#!/usr/bin/env python3
"""
MiniDFS Launcher
Starts the multithreaded server and opens the dashboard in the web browser.
"""

import os
import sys
import webbrowser
import threading
import time

# Ensure project root is in sys.path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from minidfs.config import SERVER_HOST, SERVER_PORT
from minidfs.app import run_server

def open_browser():
    time.sleep(1.2)
    url = f"http://{SERVER_HOST}:{SERVER_PORT}"
    print(f"[MiniDFS] Launching browser to {url}...")
    webbrowser.open(url)

if __name__ == "__main__":
    threading.Thread(target=open_browser, daemon=True).start()
    run_server(SERVER_HOST, SERVER_PORT)
