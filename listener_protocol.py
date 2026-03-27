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
# This file implements the listener protocol of the node. This protocol
# is triggered each time the node receives a message (in JSON format).
# -------------------------------------------------------------------------


from flask import jsonify

import egess_api # Used for invoking commonly used EGESS API functions
import time


def listener_protocol(config_json, node_state, state_lock, this_port, number_of_nodes, push_queue, msg):
    """
    Listener protocol function.

    Args:
        config_json (dict[str, Any]): JSON object with all-nodes configuration.
        node_state (dict[str, Any]): The state of this current node.
        state_lock (threading.Lock): The lock object for thread-safety of the state.
        this_port (int): The port this node listens.
        number_of_nodes (int): The total number of nodes in the network (if known).
        push_queue (queue.Queue): The queue for messages to be pushed to other node(s).
        msg (dict[str, Any]): JSON object received via POST protocol.
    """    

    with state_lock:
        if node_state["DESTROYED"]:
            return jsonify({"error": "Node is destroyed"}), 503
    


    if msg.get("type") == "heartbeat":
        sender = str(msg["from"])
        # If the message is a heartbeat, we should update the last heartbeat time for the neighbor that sent it.
        state_lock.acquire()
        node_state["neighbor_last_heartbeat"][sender] = time.time()
        state_lock.release()
        return jsonify({"status": "ok"}), 200
    
    if msg.get("type") == "alarmed_notification":
        with state_lock:
            event_id = msg.get("event_id", "unknown")
            if not node_state["DESTROYED"] and not node_state["ALARMED"] and not node_state["SURVEYING"]:
                node_state["ALARMED"] = True
                egess_api.write_state_change_data_point(this_port, node_state, "ALARMED")
                # Forward alarm outward to normal neighbors
                forward_count = msg.get("forward_count", 0) + 1
                if forward_count < config_json["max_alarm_forwards"]:
                    push_queue.put({
                        "type": "alarm_wave",
                        "from": this_port,
                        "forward_count": forward_count,
                        "event_id": event_id,
                        "origin_time": msg.get("origin_time", 0)
                    })
        return jsonify({"status": "ok"}), 200
    
    if msg.get("type") == "alarm_wave":
        with state_lock:
            event_id = msg.get("event_id", "unknown")
            already_seen = event_id in node_state["seen_alarm_events"]
            if not node_state["DESTROYED"] and not already_seen:
                node_state["seen_alarm_events"].append(event_id)
                forward_count = msg.get("forward_count", 0) + 1
                origin_time = msg.get("origin_time", 0)
                if not node_state["ALARMED"] and not node_state["SURVEYING"]:
                    delta = round(time.time() - origin_time, 4) if origin_time else -1
                    egess_api.write_data_point(this_port, "alarm_wave_received", f"{event_id}:delta={delta}s")
                    egess_api.write_data_point(this_port, "alarm_wave_received", "{};from={}".format(event_id, str(msg.get("from", "unknown"))))
                    if forward_count < config_json["max_alarm_forwards"]:
                        push_queue.put({
                            "type": "alarm_wave",
                            "from": this_port,
                            "forward_count": forward_count,
                            "event_id": event_id,
                            "origin_time": origin_time
                        })
        return jsonify({"status": "ok"}), 200


    if msg.get("type") == "clear_alarmed":
        with state_lock:
            if node_state["ALARMED"] and not node_state["DESTROYED"]:
                node_state["ALARMED"] = False
                node_state["NORMAL"] = True
                egess_api.write_state_change_data_point(this_port, node_state, "ALARMED")
        return jsonify({"status": "ok"}), 200


    if msg.get("type") == "fire_spread":
        with state_lock:
            if not node_state["DESTROYED"] and not node_state["ON_FIRE"]:
                node_state["ON_FIRE"] = True
                node_state["fire_arrival_time"] = time.time()
                egess_api.write_data_point(this_port, "fire_spread_received", str(msg.get("from", "unknown")))
        return jsonify({"status": "ok"}), 200


    if msg.get("type") == "state_request":
        state_lock.acquire()
        snapshot = {
            "from": this_port,
            "counter": node_state["heartbeat_counter"],
            "ALARMED": node_state["ALARMED"],
            "SURVEYING": node_state["SURVEYING"],
            "DESTROYED": node_state["DESTROYED"],
            "NORMAL": node_state["NORMAL"]
        }
        state_lock.release()
        return jsonify({"state": snapshot}), 200    

    if msg["op"] == "pull": # Indicates that the message is a "pull" request
        print("PULL REQUEST RECEIVED\n") # Log receiving the request
        # And add it to the data storage with "pull_request_received" type
        egess_api.write_data_point(this_port, "pull_request_received", str(msg.get("from", "unknown")))
        # Pull requests are basically requests for updates, and they are
        # to be responded immediately within the same session. This response,
        # for example, sends the state of the current node to the pull requester.
        # Each message, regardless of purpose, must be a JSON object with three keys:
        # 1) op - singifying the type of operation;
        # 2) data - signifying the payload;
        # 3) metadata - providing some transport information about he message itself.
        return {
            "op": "receipt",
            "data": {
                "success": True,
                "message": "",
                "node_state": node_state
            },
            "metadata": {}
        }
    elif msg["op"] == "push": # If the message is a "push" message
        # Safeguard prefenting the message from being forwarded forever
        if msg["metadata"]["forward_count"] < config_json["max_forwards"]:
            # We are going to access the state of the node, so we have to lock it first
            state_lock.acquire()
            # If we are here, the push message has been accepted. Increment the number
            # of accepted messages in the state.
            node_state["accepted_messages"] = node_state["accepted_messages"] + 1


            # The parameter metadata.relay stores the port (node ID) of the node that forwarded the message.
            # If this node hasn't seen this relay before, add it to the list of known nodes in the state
            # of the node. However, if the relay is 0, it means that the message is not forwarded, so we
            # should not add it to the list of known nodes.
            if msg["metadata"]["relay"] not in node_state["known_nodes"] and msg["metadata"]["relay"] != 0:
                node_state["known_nodes"].append(msg["metadata"]["relay"])

            # Add the state change data point.
            egess_api.write_state_change_data_point(this_port, node_state, "accepted_messages")

            # Add the state change data point.
            egess_api.write_state_change_data_point(this_port, node_state, "known_nodes")

            # We finished the atomic access to the state of the node; now allow other threads to access it.
            state_lock.release()

            
            # The forward count and the relay port are stored in the metadata of the message.
            # The metadata field allows not to contaminate the data with transportation-related
            # parameters. This may be important if the data is digitally signed by the original sender.
            
            # Before we push the message to the push queue for further forwarding, we have to update the
            # metadata of the message. First, we record that the message is coming from this port.
            msg["metadata"]["relay"] = this_port

            # Second, we increment the forwarding count to make sure that the message is not forwarded
            # within the network forever.
            msg["metadata"]["forward_count"] = msg["metadata"]["forward_count"] + 1

            # Add the message to the push queue. It will be received by the push thread and processed
            # by the push protocol.
            push_queue.put(msg)

            # The returned message will be sent back to the caller as a response.
            return {
                "op": "receipt",
                "data": {
                    "success": True,
                    "message": "message enqueued"
                },
                "metadata": {}
            }
        else:
            # If the maximum number of forwards has been reached, the message will no longer be
            # pushed forward
            return {
                "op": "receipt",
                "data": {
                    "success": False,
                    "message": "message is not enqueued"
                },
                "metadata": {}
            }
    else: # The listener protocol must not receive any other types of messages
        print("ERROR: listener_protocol: unknown type of message: {}\n".format(msg["op"]))
        return {
                "op": "receipt",
                "data": {
                    "success": False,
                    "message": "unknown operation"
                },
                "metadata": {}
            }
