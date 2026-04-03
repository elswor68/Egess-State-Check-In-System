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

    # ── DESTROYED CHECK ───────────────────────────────────────────────────────
    # Before doing anything else, check if this node has already been destroyed.
    # If it has, we immediately return a 503 error response and ignore the message.
    # A destroyed node should not be processing or responding to any messages.
    with state_lock:
        if node_state["DESTROYED"]:
            return jsonify({"error": "Node is destroyed"}), 503
    


    # ── HEARTBEAT HANDLER ─────────────────────────────────────────────────────
    # A heartbeat is a "I am alive" ping sent periodically by neighboring nodes
    # via the background protocol. When we receive one, we record the current time
    # as the last time we heard from that neighbor. This timestamp is used by
    # pull_protocol to detect when a neighbor has gone silent (i.e. been destroyed).
    if msg.get("type") == "heartbeat":
        sender = str(msg["from"])
        # If the message is a heartbeat, we should update the last heartbeat time for the neighbor that sent it.
        state_lock.acquire()
        node_state["neighbor_last_heartbeat"][sender] = time.time()
        state_lock.release()
        return jsonify({"status": "ok"}), 200
    

    # ── ALARMED NOTIFICATION HANDLER ─────────────────────────────────────────
    # An alarmed_notification is sent by a SURVEYING node to its neighbors when
    # it first detects that one of its neighbors has gone silent (possibly destroyed).
    # If this node is currently NORMAL (not already ALARMED, SURVEYING, or DESTROYED),
    # it transitions to ALARMED state and forwards an alarm_wave outward to its
    # own neighbors so the alert spreads further across the network.
    if msg.get("type") == "alarmed_notification":
        with state_lock:
            # Extract the unique event ID that identifies which destruction triggered
            # this alarm. This allows us to track multiple separate destruction events
            # independently without them interfering with each other.
            event_id = msg.get("event_id", "unknown")
            # Only transition to ALARMED if this node is currently in NORMAL state.
            # SURVEYING nodes already know about the failure directly so they don't
            # need to become ALARMED. DESTROYED nodes can't change state at all.
            if not node_state["DESTROYED"] and not node_state["ALARMED"] and not node_state["SURVEYING"]:
                node_state["ALARMED"] = True
                # Log the state change to data.csv so we can track when this node became ALARMED.
                egess_api.write_state_change_data_point(this_port, node_state, "ALARMED")
                # Increment the forward count to track how many hops this alarm has traveled.
                forward_count = msg.get("forward_count", 0) + 1
                # Only forward the alarm wave if we haven't hit the maximum hop limit.
                # This prevents the alarm from bouncing around the network forever.
                if forward_count < config_json["max_alarm_forwards"]:
                    # Put an alarm_wave message on the push queue.
                    # push_protocol will pick this up and send it to all known neighbors.
                    # We pass origin_time through unchanged so every node can calculate
                    # how long the wave took to reach them from the original detection point.
                    push_queue.put({
                        "type": "alarm_wave",
                        "from": this_port,
                        "forward_count": forward_count,
                        "event_id": event_id,
                        "origin_time": msg.get("origin_time", 0)
                    })
        return jsonify({"status": "ok"}), 200
    

    # ── ALARM WAVE HANDLER ────────────────────────────────────────────────────
    # An alarm_wave is the outward-propagating notification that travels beyond
    # the ALARMED ring to NORMAL nodes further from the failure.
    # Unlike alarmed_notification, receiving an alarm_wave does NOT change a node's
    # state. It just logs the timing (delta) so we can measure how far the node
    # is from the failure based on how long the wave took to arrive.
    if msg.get("type") == "alarm_wave":
        with state_lock:
            event_id = msg.get("event_id", "unknown")
            # Check if this node has already seen this specific alarm event.
            # already_seen prevents a node from logging and forwarding the same wave
            # more than once, which would cause infinite loops in the network.
            already_seen = event_id in node_state["seen_alarm_events"]
            if not node_state["DESTROYED"] and not already_seen:
                # Mark this event as seen so we don't process it again.
                node_state["seen_alarm_events"].append(event_id)
                forward_count = msg.get("forward_count", 0) + 1
                origin_time = msg.get("origin_time", 0)
                # Only log timing and forward the wave if this node is NORMAL.
                # ALARMED and SURVEYING nodes are already aware of the failure so their
                # timing data would be meaningless for distance measurement.
                # Stopping the wave here also prevents it from bouncing back toward
                # the damage zone.
                if not node_state["ALARMED"] and not node_state["SURVEYING"]:
                    # Calculate how many seconds have passed since the wave originated.
                    # This delta is the key measurement — a larger delta means this node
                    # is further away from the destroyed node.
                    delta = round(time.time() - origin_time, 4) if origin_time else -1
                    # Log the delta time to data.csv for analysis and visualization.
                    egess_api.write_data_point(this_port, "alarm_wave_received", f"{event_id}:delta={delta}s")
                    # Log which node forwarded the wave to us (the immediate sender).
                    egess_api.write_data_point(this_port, "alarm_wave_received", "{};from={}".format(event_id, str(msg.get("from", "unknown"))))
                    # Forward the wave onward to our neighbors if we haven't hit the hop limit.
                    # origin_time is passed through unchanged so future nodes still measure
                    # from the original detection time, not from when we received it.
                    if forward_count < config_json["max_alarm_forwards"]:
                        push_queue.put({
                            "type": "alarm_wave",
                            "from": this_port,
                            "forward_count": forward_count,
                            "event_id": event_id,
                            "origin_time": origin_time
                        })
        return jsonify({"status": "ok"}), 200


    # ── CLEAR ALARMED HANDLER ─────────────────────────────────────────────────
    # A clear_alarmed message is sent by a SURVEYING node when it finishes surveying
    # and determines that all its failed neighbors have been accounted for.
    # This tells neighboring ALARMED nodes that the situation has been resolved
    # and they can return to NORMAL state.  
    if msg.get("type") == "clear_alarmed":
        with state_lock:
             # Only clear ALARMED state if this node is currently ALARMED and not DESTROYED.
            if node_state["ALARMED"] and not node_state["DESTROYED"]:
                node_state["ALARMED"] = False
                node_state["NORMAL"] = True
                # Log the transition back to NORMAL in data.csv.
                egess_api.write_state_change_data_point(this_port, node_state, "ALARMED")
        return jsonify({"status": "ok"}), 200


    # ── FIRE SPREAD HANDLER ───────────────────────────────────────────────────
    # A fire_spread message is sent by a node that has just been destroyed by fire.
    # When received, this node marks itself as ON_FIRE and records the arrival time.
    # The fire_destruction_protocol thread then periodically checks if this node
    # should actually burn down based on fire_spread_probability and fire_spread_delay.
    # This creates organic spreading behavior — nodes catch fire from neighbors but
    # don't instantly burn, giving a realistic probabilistic propagation pattern.
    if msg.get("type") == "fire_spread":
        with state_lock:
            # Only catch fire if not already destroyed or already on fire.
            if not node_state["DESTROYED"] and not node_state["ON_FIRE"]:
                node_state["ON_FIRE"] = True
                # Record exactly when the fire arrived at this node.
                # fire_destruction_protocol uses this to check when fire_spread_delay
                # has elapsed before deciding whether the node burns down.
                node_state["fire_arrival_time"] = time.time()
                # Log which node the fire spread from so we can trace the fire path.
                egess_api.write_data_point(this_port, "fire_spread_received", str(msg.get("from", "unknown")))
        return jsonify({"status": "ok"}), 200


    # ── STATE REQUEST HANDLER ─────────────────────────────────────────────────
    # A state_request is sent by pull_protocol when a node wants to check if a
    # neighbor is still alive. The neighbor responds with a snapshot of its current
    # state. If pull_protocol gets this response it knows the neighbor is alive.
    # If it gets no response (timeout or connection error), it enters SURVEYING state.
    if msg.get("type") == "state_request":
        state_lock.acquire()
        # Build a lightweight snapshot of the current node's key state fields.
        # We don't send the entire node_state because it contains large structures
        # like the latency matrix that aren't needed for a simple liveness check.
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


    # ── PULL REQUEST HANDLER ──────────────────────────────────────────────────
    # A pull message is a polling request from another node asking for our state.
    # We respond immediately with our full node_state within the same HTTP session.
    # This is synchronous — the requester waits for our response before continuing.
    # This is different from push which is asynchronous (fire and forget via the queue).
    if msg["op"] == "pull": # Indicates that the message is a "pull" request
        print("PULL REQUEST RECEIVED\n") # Log receiving the request
        # And add it to the data storage with "pull_request_received" type
        egess_api.write_data_point(this_port, "pull_request_received", str(msg.get("from", "unknown")))
        # Respond immediately with our full node state.
        # The requester uses this to learn about us and update their own known state.
        return {
            "op": "receipt",
            "data": {
                "success": True,
                "message": "",
                "node_state": node_state
            },
            "metadata": {}
        }
   
    # ── PUSH MESSAGE HANDLER ──────────────────────────────────────────────────
    # A push message is a broadcast message being forwarded hop by hop through
    # the network. Each node that receives it updates its own state from the metadata,
    # then re-queues it to be forwarded to its own neighbors. The forward_count in
    # the metadata limits how many hops the message can travel to prevent it from
    # looping around the network forever.
    elif msg["op"] == "push": # If the message is a "push" message
        # Safeguard preventing the message from being forwarded forever
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
    
    # ── UNKNOWN MESSAGE HANDLER ───────────────────────────────────────────────
    # If we receive a message with an op field we don't recognize, log the error
    # and return a failure receipt. This should never happen in normal operation
    # but acts as a safety net for unexpected message types.
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
