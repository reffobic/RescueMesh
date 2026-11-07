import re, sys, pathlib, urllib.request, time, os, subprocess, threading
import tkinter as tk

# Either a folder (demo_logs) or three URLs: http://ip1:8000,http://ip2:8000,http://ip3:8000
SRC = sys.argv[1] if len(sys.argv) > 1 else "demo_logs"
REPO = pathlib.Path(__file__).resolve().parent.parent  # NEW: repo root, where cargo runs
DEFAULT_NODES = "1@127.0.0.1:7001,2@127.0.0.1:7002,3@127.0.0.1:7003"  # NEW

def parse_line(node, line):
    line = line.strip()
    if line.startswith("FAILINJECT_SKIP,"):
        _, rnd, who = line.split(",")[:3]
        return {"node": node, "type": "skip", "round": rnd, "who": who}
    if line.startswith("FAILINJECT,"):
        _, rnd, victim, secs = line.split(",")[:4]
        return {"node": node, "type": "inject", "round": rnd,
                "victim": int(victim), "secs": int(secs)}
    m = re.match(r"\[node (\d+)\] MUTED for (\d+)\s*s", line)
    if m:
        return {"node": node, "type": "muted", "secs": int(m.group(2))}
    if "REJOINED" in line:
        return {"node": node, "type": "rejoined"}
    if line.startswith("SYNC,"):
        return {"node": node, "type": "sync"}
    return None

def get_text(n):
    try:
        if SRC.startswith("http"):
            base = SRC.split(",")[n - 1].strip().rstrip("/")
            url = f"{base}/node{n}.log"
            return urllib.request.urlopen(url, timeout=2).read().decode(errors="ignore")
        f = pathlib.Path(SRC) / f"node{n}.log"
        return f.read_text(errors="ignore") if f.exists() else ""
    except Exception:
        return ""

def read_events():
    events = []
    for n in (1, 2, 3):
        for line in get_text(n).splitlines():
            e = parse_line(n, line)
            if e:
                events.append(e)
    return events

def describe(e):
    n, t = e["node"], e["type"]
    if t == "inject":
        return f"[node {n}] round {e['round']}: node {e['victim']} chosen, mute {e['secs']}s"
    if t == "skip":
        return f"[node {n}] round {e['round']}: injection skipped (peers not all alive)"
    if t == "muted":
        return f"[node {n}] MUTED for {e['secs']}s"
    if t == "sync":
        return f"[node {n}] syncing state from a peer"
    if t == "rejoined":
        return f"[node {n}] REJOINED"
    return str(e)

# NEW: turn the CLIENT_METRICS line into a short readable summary
def summarize(line):
    def g(k):
        m = re.search(rf"{k}=([\d.]+)", line)
        return m.group(1) if m else "?"
    return (f"submitted {g('submitted')}, succeeded {g('succeeded')}, failed {g('failed')}, "
            f"timed out {g('timed_out')} | mean {g('mean_ms')} ms, p95 {g('p95_ms')} ms, "
            f"{g('throughput_rps')} req/s")

COLORS = {"UP": "#2e7d32", "MUTED": "#c62828", "REJOINING": "#ef6c00"}

class App:
    def __init__(self, root):
        root.title("RescueMesh demo")
        top = tk.Frame(root)
        top.pack(padx=10, pady=10)
        self.state = {1: "UP", 2: "UP", 3: "UP"}
        self.until = {1: 0, 2: 0, 3: 0}
        self.cards = {}
        for n in (1, 2, 3):
            lbl = tk.Label(top, width=14, height=4, fg="white", font=("Arial", 16, "bold"))
            lbl.pack(side="left", padx=6)
            self.cards[n] = lbl
        self.round_label = tk.Label(root, text="waiting for the first injection round...",
                                    font=("Arial", 12))
        self.round_label.pack()

        # NEW: client control panel
        panel = tk.LabelFrame(root, text="Clients", padx=8, pady=8)
        panel.pack(fill="x", padx=10)
        self.fields = {}
        for i, (label, key, default) in enumerate([
            ("Users", "users", "10"),
            ("Incidents per user", "per_user", "10"),
            ("Delay between requests (ms)", "rate", "0"),
        ]):
            tk.Label(panel, text=label).grid(row=0, column=2 * i, padx=4)
            ent = tk.Entry(panel, width=6)
            ent.insert(0, default)
            ent.grid(row=0, column=2 * i + 1, padx=4)
            self.fields[key] = ent
        tk.Label(panel, text="Nodes").grid(row=1, column=0, padx=4, pady=4)
        self.nodes_entry = tk.Entry(panel, width=70)
        self.nodes_entry.insert(0, DEFAULT_NODES)
        self.nodes_entry.grid(row=1, column=1, columnspan=5, sticky="w", padx=4)
        self.run_btn = tk.Button(panel, text="Run clients", command=self.run_clients)
        self.run_btn.grid(row=0, column=6, padx=10)
        self.result = tk.Label(root, text="", font=("Arial", 11), wraplength=700, justify="left")
        self.result.pack(padx=10, pady=4)
        self.result_text = ""      # written by the worker thread, shown by tick()

        self.feed = tk.Listbox(root, width=80, height=14, font=("Consolas", 10))
        self.feed.pack(padx=10, pady=10)
        self.seen = {1: 0, 2: 0, 3: 0}
        self.loaded = False
        self.refresh(root)
        self.tick(root)

    # NEW: start the Rust client with the values typed in the boxes
    def run_clients(self):
        try:
            users = int(self.fields["users"].get())
            per_user = int(self.fields["per_user"].get())
            rate = int(self.fields["rate"].get())
        except ValueError:
            self.result_text = "Users, incidents and delay must be whole numbers."
            return
        env = dict(os.environ)
        env["RM_NODES"] = self.nodes_entry.get().strip()
        env["RM_CLIENT_USERS"] = str(users)
        env["RM_CLIENT_REQUESTS_PER_USER"] = str(per_user)
        env["RM_CLIENT_RATE_MS"] = str(rate)
        self.run_btn.config(state="disabled")
        self.result_text = f"Running {users} users x {per_user} incidents = {users * per_user} requests..."
        threading.Thread(target=self._worker, args=(env,), daemon=True).start()

    def _worker(self, env):
        try:
            p = subprocess.run(["cargo", "run", "--release", "--quiet", "--", "client"],
                               cwd=REPO, env=env, capture_output=True, text=True)
            line = next((l for l in p.stdout.splitlines() if l.startswith("CLIENT_METRICS")), "")
            self.result_text = summarize(line) if line else "Client failed: " + p.stderr[-300:]
        except Exception as ex:
            self.result_text = f"Could not start client: {ex}"
        self.finished = True

    def apply(self, e, live):
        n, t = e["node"], e["type"]
        if t == "inject":
            self.round_label.config(
                text=f"Round {e['round']}: node {e['victim']} chosen, mute {e['secs']}s")
        elif not live:
            return
        elif t == "muted":
            self.state[n] = "MUTED"
            self.until[n] = time.time() + e["secs"]
        elif t == "sync":
            self.state[n] = "REJOINING"
        elif t == "rejoined":
            self.state[n] = "UP"

    def refresh(self, root):
        events = read_events()
        for n in (1, 2, 3):
            mine = [e for e in events if e["node"] == n]
            for e in mine[self.seen[n]:]:
                self.feed.insert(tk.END, describe(e))
                self.feed.yview_moveto(1.0)
                self.apply(e, self.loaded)
            self.seen[n] = len(mine)
        self.loaded = True
        root.after(1000, self.refresh, root)

    finished = False

    def tick(self, root):
        now = time.time()
        for n in (1, 2, 3):
            s = self.state[n]
            text = f"Node {n}\n{s}"
            if s == "MUTED":
                text += f" {max(0, self.until[n] - now):.0f}s"
            self.cards[n].config(text=text, bg=COLORS[s])
        self.result.config(text=self.result_text)      # NEW
        if self.finished:                              # NEW
            self.finished = False
            self.run_btn.config(state="normal")
        root.after(200, self.tick, root)

if __name__ == "__main__":
    root = tk.Tk()
    App(root)
    root.mainloop()
