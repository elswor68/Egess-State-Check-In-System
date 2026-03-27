import time
import random
import egess_api

def fire_destruction_protocol(config_json, node_state, state_lock, this_port, push_queue):
    time.sleep(random.uniform(0, config_json["destruction_check_period"]))
    
    while True:
        time.sleep(1)
        with state_lock:
            if node_state["DESTROYED"]:
                return

            # Spontaneous ignition
            if not node_state["ON_FIRE"]:
                if random.random() < config_json["fire_ignition_probability"]:
                    node_state["ON_FIRE"] = True
                    node_state["fire_arrival_time"] = time.time()
                    egess_api.write_data_point(this_port, "fire_ignition", str(this_port))

            # Check if pending fire should consume this node
            if node_state["ON_FIRE"] and node_state["fire_arrival_time"]:
                elapsed = time.time() - node_state["fire_arrival_time"]
                if elapsed >= config_json["fire_spread_delay"]:
                    if random.random() < config_json["fire_spread_probability"]:
                        node_state["DESTROYED"] = True
                        node_state["NORMAL"] = False
                        node_state["ON_FIRE"] = False
                        egess_api.write_state_change_data_point(this_port, node_state, "DESTROYED")
                        # Spread to neighbors
                        push_queue.put({
                            "type": "fire_spread",
                            "from": this_port
                        })
                    else:
                        # Fire didn't take hold
                        node_state["ON_FIRE"] = False
                        node_state["fire_arrival_time"] = None
                        egess_api.write_data_point(this_port, "fire_survived", str(this_port))
                    return