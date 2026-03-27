# EGESS - Experimental Gear for Evaluation of Swarm Systems
# Copyright (C) 2026  Nick Ivanov and ACSUS Lab <ivanov@rowan.edu>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.


# -------------------------------------------------------------------------
# This file implements the pull protocol. The pull protocol requests
# updates from other nodes and receives these updates within the response
# to the request (i.e., within the same session).
# -------------------------------------------------------------------------


import uuid

import egess_api
import random
import copy
import time
import requests

def request_state_from(neighbor_port, config_json, node_state, state_lock, this_port, push_queue):
    """
    Request the state of a node from another node.

    Args:
        target_port (int): The port of the node to request the state from.
        config_json (dict[str, Any]): JSON object with all-nodes configuration.
    """

    url = f"http://{config_json['base_host']}:{neighbor_port}/"

    msg = {
        "type": "state_request",  
        "from": this_port,
    }

    with state_lock:
        if node_state["DESTROYED"]:
            return

    try:
        response = requests.post(url, json=msg, timeout=config_json["request_timeout"])
        if response.status_code == 200:
            neighbor_state = response.json().get("state")
            with state_lock:
                # Neighbor responded so clear it's surveying record
                node_state["neighbor_last_heartbeat"][str(neighbor_port)] = time.time()
                node_state["neighbor_states"][str(neighbor_port)] = neighbor_state
                node_state["surveying_targets"].pop(str(neighbor_port), None)
                # If no other target remains, exit SURVEYING state and return to NORMAL state.
                if not node_state["surveying_targets"]:
                    node_state["SURVEYING"] = False
                    node_state["NORMAL"] = True    

        else: 
            # Treat non-200 (e.g. 503 from destroyed node) same as a timeout
            with state_lock:
                neighbor_key = str(neighbor_port)
                attempts = node_state["surveying_targets"].get(neighbor_key, 0) + 1
                node_state["surveying_targets"][neighbor_key] = attempts
                if not node_state["SURVEYING"]:
                    node_state["SURVEYING"] = True
                    node_state["NORMAL"] = False
                    node_state["ALARMED"] = False
                    egess_api.write_state_change_data_point(this_port, node_state, "SURVEYING")
                    event_id = str(uuid.uuid4())[:8]  # short unique ID
                    # Notify neighbors to enter ALARMED
                    for n in list(node_state["known_nodes"]):
                        push_queue.put({
                                "type": "alarmed_notification", 
                                "from": this_port,
                                "forward_count": 0, 
                                "event_id": event_id,
                                "origin_time": time.time()
                            })
                if attempts >= config_json["surveying_failure_threshold"]:
                    print(f"WARNING: Node {neighbor_port} considered DESTROYED after {attempts} failed attempts")
                    node_state["neighbor_states"][neighbor_key] = {"DESTROYED": True}
                    node_state["surveying_targets"].pop(neighbor_key, None)
                if not node_state["surveying_targets"]:
                    node_state["SURVEYING"] = False
                    node_state["NORMAL"] = True    

    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            state_lock.acquire()
            neighbor_key = str(neighbor_port)
            attempts = node_state["surveying_targets"].get(neighbor_key, 0) + 1
            node_state["surveying_targets"][neighbor_key] = attempts

            # Enter SURVEYING state
            if not node_state["SURVEYING"]:
                node_state["SURVEYING"] = True
                node_state["NORMAL"] = False
                node_state["ALARMED"] = False
                egess_api.write_state_change_data_point(this_port, node_state, "SURVEYING")
                event_id = str(uuid.uuid4())[:8]  # short unique ID
                # Notify neighbors to enter ALARMED
                for n in list(node_state["known_nodes"]):
                    push_queue.put({
                        "type": "alarmed_notification", 
                        "from": this_port, 
                        "forward_count": 0, 
                        "event_id": event_id,
                        "origin_time": time.time()
                    })

            # Check if neighbor has exceeded the failure threshold
            if attempts >= config_json["surveying_failure_threshold"]:
                print(f"WARNING: Node {neighbor_port} considered DESTROYED after {attempts} failed attempts")
                node_state["neighbor_states"][neighbor_key] = {"DESTROYED": True}
                node_state["surveying_targets"].pop(neighbor_key, None)

            # Exit Surveying if not targets remain
            if not node_state["surveying_targets"]:
                node_state["SURVEYING"] = False
                node_state["NORMAL"] = True
                # Notify neighbors to clear ALARMED
                for n in list(node_state["known_nodes"]):
                    push_queue.put({"type": "clear_alarmed", "from": this_port})
            state_lock.release()


def pull_protocol(config_json, node_state, state_lock, this_port, number_of_nodes, push_queue):
    """
    Pull protocol implementation function.

    Args:
        config_json (dict[str, Any]): JSON object with all-nodes configuration.
        node_state (dict[str, Any]): The state of this current node.
        state_lock (threading.Lock): The lock object for thread-safety of the state.
        this_port (int): The port this node listens.
        number_of_nodes (int): The total number of nodes in the network (if known).
        push_queue (queue.Queue): The queue for messages to be pushed to other node(s).
    """    

    timeout = config_json["heartbeat_timeout"] # Timeout for waiting for the response to the pull request, in seconds.
    now = time.time() # Current time in seconds since the epoch

    state_lock.acquire() # Prevent state access by other threads
    known = list(node_state["known_nodes"])
    last_seen = dict(node_state["neighbor_last_heartbeat"])
    already_surveying = set(node_state["surveying_targets"].keys())
    state_lock.release()  

    # Check for newly silent neighbors 
    for neighbor in known:
        last = last_seen.get(str(neighbor))
        if last is None or (now - last) > timeout:
            if str(neighbor) not in already_surveying:
                request_state_from(neighbor, config_json, node_state, state_lock, this_port, push_queue)

    # Retry any exisiting surveying targets
    for neighbor_key in already_surveying:
        with state_lock:
            current_targets = set(node_state["surveying_targets"].keys())
        if neighbor_key in current_targets:
            request_state_from(int(neighbor_key), config_json, node_state, state_lock, this_port, push_queue)

    # Request for information/update (a.k.a. "polling" message)
    msg = {
        "op": "pull",
        "from": this_port,
        "data": {},
        "metadata": {}
    }

    with state_lock:
        other_nodes = [n for n in node_state["known_nodes"] if n != this_port]

    if not other_nodes:
        return

    node_sample = random.sample(other_nodes, 1)

    # Send the polling (pull) request to the single randomly selected node from the list of other nodes
    egess_api.send_msg(config_json, node_state, state_lock, this_port, msg, node_sample[0])
    