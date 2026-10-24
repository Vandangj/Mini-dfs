"""
Root entrypoint for MiniDFS Server.
Runs the multithreaded server and serves the dashboard with all 10 Distributed Concepts.
"""

import sys
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from minidfs.config import SERVER_HOST, SERVER_PORT
from minidfs.app import run_server

if __name__ == "__main__":
    run_server(SERVER_HOST, SERVER_PORT)
