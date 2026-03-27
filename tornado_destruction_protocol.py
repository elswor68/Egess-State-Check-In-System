import time
import math
import random
import egess_api

def tornado_destruction_protocol(config_json, node_state, state_lock, this_port, push_queue):
    base_port  = config_json["base_port"]
    n_nodes    = config_json["max_port"] - base_port + 1
    grid_size  = int(math.ceil(math.sqrt(n_nodes)))

    node_index = this_port - base_port
    this_col   = node_index % grid_size
    this_row   = node_index // grid_size

    # All nodes launched in the same minute share this seed
    # so they all generate identical tornado parameters
    rng = random.Random(int(time.time()) // 60)

    # 0 = left→right, 1 = right→left, 2 = top→bottom, 3 = bottom→top
    direction    = rng.randint(0, 3)
    tornado_width = 2

    if direction in (0, 1):  # horizontal sweep, tornado spans 2 rows
        start_row    = rng.randint(0, grid_size - tornado_width)
        tornado_band = set(range(start_row, start_row + tornado_width))
        if this_row not in tornado_band:
            return  # not in path, this node is safe
        step = this_col if direction == 0 else (grid_size - 1 - this_col)
        egess_api.write_data_point(this_port, "tornado_info",
            f"dir={'L→R' if direction==0 else 'R→L'} rows={start_row}-{start_row+1} step={step}")

    else:  # vertical sweep, tornado spans 2 cols
        start_col    = rng.randint(0, grid_size - tornado_width)
        tornado_band = set(range(start_col, start_col + tornado_width))
        if this_col not in tornado_band:
            return  # not in path
        step = this_row if direction == 2 else (grid_size - 1 - this_row)
        egess_api.write_data_point(this_port, "tornado_info",
            f"dir={'T→B' if direction==2 else 'B→T'} cols={start_col}-{start_col+1} step={step}")

    destroy_time = (time.time()
                    + config_json["tornado_start_delay"]
                    + step * config_json["tornado_wave_delay"])

    while True:
        time.sleep(0.5)
        with state_lock:
            if node_state["DESTROYED"]:
                return
        if time.time() >= destroy_time:
            with state_lock:
                if not node_state["DESTROYED"]:
                    node_state["DESTROYED"] = True
                    node_state["NORMAL"] = False
                    egess_api.write_state_change_data_point(
                        this_port, node_state, "DESTROYED")
            return