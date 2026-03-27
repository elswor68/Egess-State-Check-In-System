"""
EGESS Swarm Visualizer
----------------------
Reads data.csv and produces:
  1. egess_animation.gif         — animated hex grid (1s steps)
  2. egess_destruction_spread.png — destruction/fire/tornado spread pattern

Usage:
    python3 visualize.py              # reads data.csv in current directory
    python3 visualize.py mydata.csv   # reads a specific file
"""

import sys
import math
import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.animation as animation
import numpy as np

# ── Configuration ──────────────────────────────────────────────────────────────
BASE_PORT  = 9000
GRID_SIZE  = 7
DATA_FILE  = sys.argv[1] if len(sys.argv) > 1 else "data.csv"
OUT_ANIM   = "egess_animation.gif"
OUT_STATIC = "egess_destruction_spread.png"
FPS        = 6
TIME_STEP  = 1.0

STATE_PRIORITY = {
    "NORMAL":    0,
    "WAVE":      1,   # cyan flash when alarm wave passes through
    "ALARMED":   2,
    "SURVEYING": 3,
    "ON_FIRE":   4,
    "DESTROYED": 5,
}

COLOURS = {
    "NORMAL":    "#4CAF50",
    "WAVE":      "#00BCD4",   # cyan
    "ALARMED":   "#FF9800",
    "SURVEYING": "#2196F3",
    "ON_FIRE":   "#FF5722",
    "DESTROYED": "#F44336",
}
EDGE_COL = "#FFFFFF"
HEX_SIZE = 1.0

# ── Hex helpers ────────────────────────────────────────────────────────────────
def hex_center(col, row):
    x = col * HEX_SIZE * math.sqrt(3)
    if row % 2 == 1:
        x += HEX_SIZE * math.sqrt(3) / 2
    y = row * HEX_SIZE * 1.5
    return x, y

def hex_corners(cx, cy, size=HEX_SIZE):
    return [(cx + size * math.cos(math.radians(60*i - 30)),
             cy + size * math.sin(math.radians(60*i - 30))) for i in range(6)]

def port_to_cr(port):
    idx = port - BASE_PORT
    return idx % GRID_SIZE, idx // GRID_SIZE

def all_centers():
    return {BASE_PORT + i: hex_center(*port_to_cr(BASE_PORT + i))
            for i in range(GRID_SIZE * GRID_SIZE)}

# ── Parse CSV ──────────────────────────────────────────────────────────────────
def parse_csv(path):
    if not os.path.exists(path):
        print(f"ERROR: Cannot find {path}")
        print(f"       Run this script from the same folder as data.csv")
        print(f"       Current directory: {os.getcwd()}")
        sys.exit(1)
    events = []
    with open(path) as f:
        for line in f:
            parts = line.strip().split(";")
            if len(parts) < 3:
                continue
            try:
                events.append((float(parts[1]), int(parts[0]),
                                parts[2], parts[3] if len(parts) > 3 else ""))
            except ValueError:
                continue
    events.sort(key=lambda e: e[0])
    t_min = events[0][0]  if events else 0
    t_max = events[-1][0] if events else 0
    return events, t_min, t_max

# ── Classify event to state ────────────────────────────────────────────────────
def classify_event(etype, value):
    if etype == "fire_ignition":       return "ON_FIRE"
    if etype == "fire_survived":       return "NORMAL"
    if etype == "alarm_wave_received": return "WAVE"
    if etype != "state_change":        return None
    if "DESTROYED=True"   in value: return "DESTROYED"
    if "SURVEYING=True"   in value: return "SURVEYING"
    if "ALARMED=True"     in value: return "ALARMED"
    if ("NORMAL=True" in value or
        "SURVEYING=False" in value or
        "ALARMED=False"   in value): return "NORMAL"
    return None

# ── Detect run mode ────────────────────────────────────────────────────────────
def detect_mode(events):
    for _, _, etype, _ in events:
        if etype == "fire_ignition":  return "fire"
        if etype == "tornado_info":   return "tornado"
    return "random"

# ── Build snapshots ────────────────────────────────────────────────────────────
def build_snapshots(events, t_min, t_max):
    """
    1-second windows. Within each window show the highest-priority state.
    WAVE is non-sticky — only appears as a peak flash, doesn't change cur state.
    DESTROYED is sticky forever.
    """
    n    = GRID_SIZE * GRID_SIZE
    cur  = {BASE_PORT + i: "NORMAL" for i in range(n)}
    frames = []
    times  = np.arange(t_min, t_max + TIME_STEP, TIME_STEP)
    ei = 0

    for ft in times:
        wend = ft + TIME_STEP
        peak = {}

        while ei < len(events) and events[ei][0] < wend:
            ts, port, etype, value = events[ei]
            new_s = classify_event(etype, value)
            if new_s and port in cur:
                if cur[port] == "DESTROYED":
                    ei += 1
                    continue
                if new_s == "WAVE":
                    # Non-sticky — only update peak, not cur state
                    prev = peak.get(port, "NORMAL")
                    if STATE_PRIORITY.get("WAVE", 0) > STATE_PRIORITY.get(prev, 0):
                        peak[port] = "WAVE"
                else:
                    cur[port] = new_s
                    prev = peak.get(port, "NORMAL")
                    if STATE_PRIORITY.get(new_s, 0) > STATE_PRIORITY.get(prev, 0):
                        peak[port] = new_s
            ei += 1

        snapshot = {}
        for port in cur:
            display = cur[port]
            pk = peak.get(port)
            if pk and STATE_PRIORITY.get(pk, 0) > STATE_PRIORITY.get(display, 0):
                display = pk
            snapshot[port] = display
        frames.append((ft, snapshot))

    return frames

# ── Draw helpers ───────────────────────────────────────────────────────────────
def draw_frame(ax, snapshot, title, centers):
    ax.clear()
    ax.set_aspect("equal"); ax.axis("off")
    ax.set_facecolor("#1a1a2e")
    ax.set_title(title, fontsize=9, color="white", pad=4)
    for port, state in snapshot.items():
        cx, cy = centers[port]
        poly = plt.Polygon(hex_corners(cx, cy), closed=True,
                           facecolor=COLOURS.get(state, COLOURS["NORMAL"]),
                           edgecolor=EDGE_COL, linewidth=0.8)
        ax.add_patch(poly)
        ax.text(cx, cy, str(port - BASE_PORT), ha="center", va="center",
                fontsize=5.5, color="white", fontweight="bold")
    xs = [v[0] for v in centers.values()]
    ys = [v[1] for v in centers.values()]
    m = HEX_SIZE * 1.2
    ax.set_xlim(min(xs)-m, max(xs)+m)
    ax.set_ylim(min(ys)-m, max(ys)+m)

def make_legend():
    return [mpatches.Patch(facecolor=v, label=k, edgecolor="grey")
            for k, v in COLOURS.items()]

# ══════════════════════════════════════════════════════════════════════════════
# 1. ANIMATION
# ══════════════════════════════════════════════════════════════════════════════
def make_animation(frames, t_min, mode):
    print(f"Building animation ({len(frames)} frames) — mode: {mode} …")
    centers = all_centers()
    fig, ax = plt.subplots(figsize=(7, 7))
    fig.patch.set_facecolor("#1a1a2e")

    def update(i):
        ft, snapshot = frames[i]
        draw_frame(ax, snapshot,
                   f"EGESS Swarm [{mode}]  |  t = +{ft - t_min:.0f}s", centers)
        ax.legend(handles=make_legend(), loc="upper right",
                  fontsize=6, framealpha=0.7)

    ani = animation.FuncAnimation(fig, update, frames=len(frames),
                                  interval=1000 // FPS, repeat=True)
    ani.save(OUT_ANIM, writer="pillow", fps=FPS, dpi=110)
    plt.close(fig)
    print(f"  → saved {OUT_ANIM}")

# ══════════════════════════════════════════════════════════════════════════════
# 2. DESTRUCTION SPREAD
# ══════════════════════════════════════════════════════════════════════════════
def make_destruction_spread(events, t_min, mode):
    print(f"Building destruction spread image — mode: {mode} …")
    centers = all_centers()

    destroyed_at  = {}
    ignition_at   = {}
    tornado_band  = set()
    spread_edges  = []
    wave_received = {}   # port -> first delta string
    tornado_label = ""

    for ts, port, etype, value in events:
        elapsed = ts - t_min
        if etype == "state_change" and "DESTROYED=True" in value:
            if port not in destroyed_at:
                destroyed_at[port] = elapsed
        elif etype == "fire_ignition":
            if port not in ignition_at:
                ignition_at[port] = elapsed
        elif etype == "fire_spread_received":
            try:
                spread_edges.append((int(value), port))
            except ValueError:
                pass
        elif etype == "tornado_info":
            tornado_band.add(port)
            if not tornado_label:
                tornado_label = value
        elif etype == "alarm_wave_received" and "delta=" in value:
            if port not in wave_received:
                wave_received[port] = value   # e.g. "abc123:delta=0.53s"

    if not destroyed_at and not ignition_at and not tornado_band:
        print("  No destruction events found — skipping."); return

    all_t = list(destroyed_at.values()) + list(ignition_at.values())
    max_t = max(all_t) if all_t else 1
    order = {p: i+1 for i, (p, _) in
             enumerate(sorted(destroyed_at.items(), key=lambda x: x[1]))}

    fig, ax = plt.subplots(figsize=(9, 9))
    fig.patch.set_facecolor("#1a1a2e")
    ax.set_aspect("equal"); ax.axis("off")
    ax.set_facecolor("#1a1a2e")

    if mode == "tornado":
        title = (f"Tornado Destruction Pattern\n"
                 f"{tornado_label}  |  purple = tornado path  |  colour = time")
    elif mode == "fire":
        title = ("Fire Destruction Spread\n"
                 "★ = ignition  |  arrows = spread  |  colour = time")
    else:
        title = "Destruction Pattern\n(colour = time, number = order)"

    ax.set_title(title, fontsize=10, color="white", pad=8)
    cmap = plt.cm.YlOrRd

    # Fire spread arrows
    if mode == "fire":
        for src, dst in spread_edges:
            if src in centers and dst in centers:
                sx, sy = centers[src]
                dx, dy = centers[dst]
                ax.annotate("", xy=(dx, dy), xytext=(sx, sy),
                            arrowprops=dict(arrowstyle="->,head_width=0.15",
                                            color="#FF572266", lw=1.0))

    # Draw hexes
    for i in range(GRID_SIZE * GRID_SIZE):
        port   = BASE_PORT + i
        cx, cy = centers[port]

        if port in destroyed_at:
            colour = cmap(destroyed_at[port] / max_t)
            label  = str(order[port])
        elif mode == "tornado" and port in tornado_band:
            colour = "#9C27B0"
            label  = str(i)
        elif mode == "fire" and port in ignition_at:
            colour = "#FF572244"
            label  = str(i)
        else:
            colour = COLOURS["NORMAL"]
            label  = str(i)

        poly = plt.Polygon(hex_corners(cx, cy), closed=True,
                           facecolor=colour, edgecolor="#ffffff", linewidth=0.7)
        ax.add_patch(poly)
        ax.text(cx, cy, label, ha="center", va="center",
                fontsize=6, color="white", fontweight="bold")

        # Fire ignition star
        if mode == "fire" and port in ignition_at:
            ax.text(cx, cy + HEX_SIZE * 0.55, "★",
                    ha="center", va="center", fontsize=9, color="yellow")

        # Alarm wave delta label — show on surviving normal nodes
        if port in wave_received and port not in destroyed_at:
            raw = wave_received[port]
            # extract just the delta number e.g. "0.53" from "abc:delta=0.53s"
            try:
                delta_str = raw.split("delta=")[1].replace("s", "")
                delta_val = float(delta_str)
                ax.text(cx, cy - HEX_SIZE * 0.45,
                        f"{delta_val:.2f}s",
                        ha="center", va="center",
                        fontsize=4.5, color="#00BCD4")
            except (IndexError, ValueError):
                pass

    xs = [v[0] for v in centers.values()]
    ys = [v[1] for v in centers.values()]
    m  = HEX_SIZE * 1.5
    ax.set_xlim(min(xs)-m, max(xs)+m)
    ax.set_ylim(min(ys)-m, max(ys)+m)

    # Colorbar
    sm = plt.cm.ScalarMappable(cmap=cmap,
                                norm=plt.Normalize(vmin=0, vmax=max_t))
    sm.set_array([])
    cbar = fig.colorbar(sm, ax=ax, fraction=0.03, pad=0.02)
    cbar.set_label("Time since start (s)", color="white", fontsize=8)
    cbar.ax.yaxis.set_tick_params(color="white")
    plt.setp(cbar.ax.yaxis.get_ticklabels(), color="white")

    n_d = len(destroyed_at)
    n_t = GRID_SIZE * GRID_SIZE
    extra = (f"  |  Ignition points: {len(ignition_at)}" if mode == "fire"
             else f"  |  Tornado path: {len(tornado_band)} nodes" if mode == "tornado"
             else "")
    ax.text(0.02, 0.02,
            f"Destroyed: {n_d}/{n_t}  |  Survived: {n_t-n_d}/{n_t}{extra}"
            f"  |  cyan = alarm wave delta",
            transform=ax.transAxes, fontsize=8, color="white",
            verticalalignment="bottom")

    plt.tight_layout()
    plt.savefig(OUT_STATIC, dpi=130, facecolor=fig.get_facecolor())
    plt.close(fig)
    print(f"  → saved {OUT_STATIC}")

# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print(f"Reading {DATA_FILE} …")
    print(f"Looking in: {os.path.abspath(DATA_FILE)}")
    events, t_min, t_max = parse_csv(DATA_FILE)
    print(f"  {len(events)} events  |  duration: {t_max - t_min:.1f}s")

    mode = detect_mode(events)
    print(f"  Detected mode: {mode}")

    frames = build_snapshots(events, t_min, t_max)
    make_animation(frames, t_min, mode)
    make_destruction_spread(events, t_min, mode)
    print("Done.")
    print(f"Output files written to: {os.getcwd()}")