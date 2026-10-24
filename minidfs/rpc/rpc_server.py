"""
Concept 1: Client-Server RPC Dispatcher
Parses and routes RPC calls (JSON-RPC 2.0 format) to Master Metadata Server
and Storage Nodes.
"""

import json
from typing import Dict, Any

class RPCDispatcher:
    def __init__(self, metadata_server):
        self.metadata_server = metadata_server

    def dispatch(self, rpc_request: Dict[str, Any]) -> Dict[str, Any]:
        """
        Processes a JSON-RPC 2.0 request:
        {
            "jsonrpc": "2.0",
            "method": "lookup_namespace",
            "params": {"path": "/user/alice/distributed_intro.txt", "user": "alice"},
            "id": 1
        }
        """
        req_id = rpc_request.get("id", 1)
        method = rpc_request.get("method")
        params = rpc_request.get("params", {})

        try:
            if method == "lookup_namespace":
                path = params.get("path", "/")
                user = params.get("user", "guest")
                result = self.metadata_server.rpc_lookup_namespace(path, caller_user=user)
                return {"jsonrpc": "2.0", "result": result, "id": req_id}

            elif method == "get_file_metadata":
                path = params.get("path", "/")
                user = params.get("user", "guest")
                result = self.metadata_server.rpc_get_file_metadata(path, caller_user=user)
                return {"jsonrpc": "2.0", "result": result, "id": req_id}

            elif method == "check_permissions":
                user = params.get("user", "guest")
                path = params.get("path", "/")
                action = params.get("action", "READ")
                result = self.metadata_server.rpc_check_permissions(user, path, action)
                return {"jsonrpc": "2.0", "result": result, "id": req_id}

            elif method == "get_chunk_locations":
                path = params.get("path", "/")
                user = params.get("user", "guest")
                result = self.metadata_server.rpc_get_chunk_locations(path, caller_user=user)
                return {"jsonrpc": "2.0", "result": result, "id": req_id}

            elif method == "create_file":
                path = params.get("path")
                user = params.get("user", "alice")
                content = params.get("content", "")
                permissions = params.get("permissions", "rwxr-xr--")
                result = self.metadata_server.rpc_create_file(path, user, content, permissions)
                return {"jsonrpc": "2.0", "result": result, "id": req_id}

            elif method == "read_file":
                path = params.get("path")
                user = params.get("user", "guest")
                result = self.metadata_server.rpc_read_file(path, user)
                return {"jsonrpc": "2.0", "result": result, "id": req_id}

            elif method == "delete_file":
                path = params.get("path")
                user = params.get("user", "guest")
                result = self.metadata_server.rpc_delete_file(path, user)
                return {"jsonrpc": "2.0", "result": result, "id": req_id}

            else:
                return {
                    "jsonrpc": "2.0",
                    "error": {"code": -32601, "message": f"Method '{method}' not found"},
                    "id": req_id
                }

        except Exception as e:
            return {
                "jsonrpc": "2.0",
                "error": {"code": -32603, "message": f"Internal RPC Error: {str(e)}"},
                "id": req_id
            }
