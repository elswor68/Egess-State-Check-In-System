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
# This is the main (starter) code for a node of a swarm network. It all
# starts here.
# -------------------------------------------------------------------------

# LIBRARY IMPORTS
from logging import root
import sys  # To access command line arguments
import json # To encode and decode JSON format
from flask import Flask, request, jsonify # For sending and receiving messages
import threading # For running push, background, listener, and pull threads
import queue # For implementing the push queue
import time # For sleep, to make sure that background is not too active
import math


# LOCAL IMPORTS
import push_protocol # Contains the pull protocol of the node
import background_protocol # Contains the background protocol of the node
import listener_protocol # Contains the listener protocol of the node
import pull_protocol # Contains the pull protocol of the node
import destruction_protocol # Contains the destruction protocol of the node


def _auto_grid_size(number_of_nodes):
    root = int(math.isqrt(int(number_of_nodes)))
    if root > 0 and root * root == int(number_of_nodes):
        return int(root)
    root = int(math.ceil(math.sqrt(float(number_of_nodes))))
    if root < 2:
        root = 2
    return int(root)

def _hex_neighbors_odd_r(col, row, grid):
    if row % 2 == 0:
        candidates = [
            (col - 1, row), (col + 1, row),
            (col, row - 1), (col - 1, row - 1),
            (col, row + 1), (col - 1, row + 1),
        ]
    else:
        candidates = [
            (col - 1, row), (col + 1, row),
            (col + 1, row - 1), (col, row - 1),
            (col + 1, row + 1), (col, row + 1),
        ]
    return [(c, r) for c, r in candidates if 0 <= c < grid and 0 <= r < grid]


def pull(config_json, node_state, state_lock, this_port, number_of_nodes, push_queue):
    """
    Thread function that initiates the pull protocol of the node.

    Args:
        config_json (dict[str, Any]): JSON object with all-nodes configuration.
        node_state (dict[str, Any]): The state of this current node.
        state_lock (threading.Lock): The lock object for thread-safety of the state.
        this_port (int): The port this node listens.
        number_of_nodes (int): The total number of nodes in the network (if known).
        push_queue (queue.Queue): The queue for messages to be pushed to other node(s).
    """
    while True:
        time.sleep(config_json["pull_period"]) # Keep invoking the pull protocol with pre-determined frequency
        pull_protocol.pull_protocol(config_json, node_state, state_lock, this_port, number_of_nodes, push_queue)


def push(config_json, node_state, state_lock, this_port, number_of_nodes, push_queue):
    """
    Thread function that initiates the push protocol of the node.

    Args:
        config_json (dict[str, Any]): JSON object with all-nodes configuration.
        node_state (dict[str, Any]): The state of this current node.
        state_lock (threading.Lock): The lock object for thread-safety of the state.
        this_port (int): The port this node listens.
        number_of_nodes (int): The total number of nodes in the network (if known).
        push_queue (queue.Queue): The queue for messages to be pushed to other node(s).
    """
    while True:
        msg = push_queue.get() # Get the next message from the push queue
        # ... and immediately invoke the push protocol for this message.
        push_protocol.push_protocol(config_json, node_state, state_lock, this_port, number_of_nodes, push_queue, msg)


def listener(config_json, node_state, state_lock, this_port, number_of_nodes, push_queue):
    """
    Thread function that initiates the listener protocol of the node.

    Args:
        config_json (dict[str, Any]): JSON object with all-nodes configuration.
        node_state (dict[str, Any]): The state of this current node.
        state_lock (threading.Lock): The lock object for thread-safety of the state.
        this_port (int): The port this node listens.
        number_of_nodes (int): The total number of nodes in the network (if known).
        push_queue (queue.Queue): The queue for messages to be pushed to other node(s).
    """
    app = Flask(__name__) # Create an app

    @app.route("/", methods=['POST']) # Listen to POST request to the default endpoint
    def egess_api():
        if not request.is_json: # Make sure the message is in JSON format. TODO: Check for specific fields.
            return jsonify({"error": "OOPS: Not JSON!"}), 400
        else:
            msg = request.get_json() # The message is a legitimate JSON object
            # Invoke the listener protocol for this message
            return listener_protocol.listener_protocol(config_json, node_state, state_lock, this_port, number_of_nodes, push_queue, msg)
    
    app.run(host=config_json["base_host"], port=this_port)


def background(config_json, node_state, state_lock, this_port, number_of_nodes, push_queue):
    """
    Thread function that initiates the background protocol of the node.

    Args:
        config_json (dict[str, Any]): JSON object with all-nodes configuration.
        node_state (dict[str, Any]): The state of this current node.
        state_lock (threading.Lock): The lock object for thread-safety of the state.
        this_port (int): The port this node listens.
        number_of_nodes (int): The total number of nodes in the network (if known).
        push_queue (queue.Queue): The queue for messages to be pushed to other node(s).
    """
    while True:
        time.sleep(config_json["background_period"]) # Keep invoking the background protocol with pre-determined frequency
        background_protocol.background_protocol(config_json, node_state, state_lock, this_port, number_of_nodes, push_queue)


def destruction(config_json, node_state, state_lock, this_port):
    """
    Thread function that initiates the destruction protocol of the node.

    Args:
        config_json (dict[str, Any]): JSON object with all-nodes configuration.
        node_state (dict[str, Any]): The state of this current node.
        state_lock (threading.Lock): The lock object for thread-safety of the state.
        this_port (int): The port this node listens.
    """
    destruction_protocol.destruction_protocol(config_json, node_state, state_lock, this_port)

def fire_destruction(config_json, node_state, state_lock, this_port, push_queue):
    import fire_destruction_protocol
    fire_destruction_protocol.fire_destruction_protocol(config_json, node_state, state_lock, this_port, push_queue)

def tornado_destruction(config_json, node_state, state_lock, this_port, push_queue):
    import tornado_destruction_protocol
    tornado_destruction_protocol.tornado_destruction_protocol(config_json, node_state, state_lock, this_port, push_queue)

def main():
    # The program accepts exactly two argumens:
    # 1) port that the node will listen;
    # 2) the total number of nodes in the network.
    if len(sys.argv) != 4:
        print("ERROR Two arguments expected.")
        print("USAGE: {} <port> <number_of_nodes> <destruction_mode>".format(sys.argv[0]))
        print("Modes: random, fire, tornado")
        exit(1)
    
    config_file = "config.json" # This file has the configuration that applies to all nodes.
    node_state_init_file = "node_state_init.json" # This is the initial "seed" state that each node is initialized with.

    this_port = int(sys.argv[1]) # Read from the command line argument the port number to listen.
    number_of_nodes = int(sys.argv[2]) # Read from the command line the total number of nodes.
    destruction_mode = sys.argv[3] # Read from the command line the destruction mode (e.g., random, fire, tornado). This will determine which destruction protocol to use.

    with open(config_file) as file: # Read all-nodes configuration file
        config_json = json.load(file) # Convert into JSON object. ATTENTION: this object is not for writing (for thread safety)

    with open(node_state_init_file) as file: # Real the initial state for all nodes
        node_state = json.load(file) # Convert into JSON object. ATTENTION: this object can only be used with a state lock


    grid_size = _auto_grid_size(number_of_nodes)
    node_index = this_port - config_json["base_port"]
    this_col = node_index % grid_size
    this_row = node_index // grid_size
    for (nc, nr) in _hex_neighbors_odd_r(this_col, this_row, grid_size):
        neighbor_port = config_json["base_port"] + nr * grid_size + nc
        if neighbor_port != this_port:
            node_state["known_nodes"].append(neighbor_port)



    # Add a latency matrix to the initial state
    # Latency matrix represents latency in seconds between a pair of nodes
    # As the nodes change their configuration (e.g., physically move), the 
    # latency matrix can dynamically change to represent this.
    node_state["latency_matrix"] = []

    matrix_size = number_of_nodes
    for i in range(matrix_size):
        row = [config_json["default_latency"]] * matrix_size
        node_state["latency_matrix"].append(row)


    # Initialize neighbor_last_heartbeat so nodes aren't immediately surveyed at startup
    now = time.time()
    for neighbor in node_state["known_nodes"]:
        node_state["neighbor_last_heartbeat"][str(neighbor)] = now


    # State lock for thread safety of the node_state object.
    # ATTENTION: Never read or write from/to the node_state object before acquiring the state_lock first
    # ATTENTION: Make sure that the state_lock object is released right after accessing the node_state object
    #               ("accessing" may mean an atomic sequence of read-writes).
    state_lock = threading.Lock() 
    
    # Create a push queue object. Each time we want to propagate/push/forward a message, we should use this queue.
    # The maximal size of the queue is specified in the all-nodes configuration file.
    push_queue = queue.Queue(maxsize=config_json["push_queue_maxsize"])
    # The push thread enqueues the message from the push queue in a thread-safe manner and executes the push protocol
    # to decide which nodes to send this message to.
    push_thread = threading.Thread(target=push, args=(config_json, node_state, state_lock, this_port, number_of_nodes, push_queue))
    # The push thread must start first to make sure that if any other thread enqueues something in the push queue, the receiving
    # side of the queue is ready. The push thread will not start "doing" anything disruptive before the push queue has at
    # least one message, so it is safe to start it first.
    push_thread.start()


    # Create the background thread. The background thread changes the state of the current node on its own schedule.
    # It is not directly invoked or controlled by incoming message or any queue. Essentially, it is only controlled by
    # the progression of time or other local characteristics (such as local sensor readings). For instance, if the current
    # node is a UAV (drone), then it would be the background thread that updates the state of the node to reflect the 
    # current GPS coordinates and altitude of the drone.
    background_thread = threading.Thread(target=background, args=(config_json, node_state, state_lock, this_port, number_of_nodes, push_queue))
    # The background thread must start before the listener and pull threads to ensure the accuracy of the node's state
    background_thread.start() 

    # Create a pull thread. This thread opens up the port, listens for the incoming messages, checks if the messages
    # are in JSON format, and then passes them to the listener protocol function for further processing.
    listener_thread = threading.Thread(target=listener, args=(config_json, node_state, state_lock, this_port, number_of_nodes, push_queue))
    # Start listener thread
    listener_thread.start()

    
    if destruction_mode == "fire":
        destruction_thread = threading.Thread(target=fire_destruction, args=(config_json, node_state, state_lock, this_port, push_queue))
    elif destruction_mode == "tornado":
        destruction_thread = threading.Thread(target=tornado_destruction, args=(config_json, node_state, state_lock, this_port, push_queue))
    else:
        destruction_thread = threading.Thread(target=destruction, args=(config_json, node_state, state_lock, this_port))
    destruction_thread.start()

    # Create the pull thread. The pull thread contacts (polls) other nodes to get some information or updates from them.    
    pull_thread = threading.Thread(target=pull, args=(config_json, node_state, state_lock, this_port, number_of_nodes, push_queue))
    pull_thread.start()

    # Join the threads in the opposite (LIFO) order
    pull_thread.join()
    listener_thread.join()
    destruction_thread.join()
    background_thread.join()
    push_thread.join()

# This code prevents running the initialization code if this file is imported by another module
# More information here: https://docs.python.org/3/library/__main__.html
if __name__ == "__main__":
    main()