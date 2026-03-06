import argparse
import json
import os
import re
import webbrowser
from typing import Any, Dict, List, Optional

from pyvis.network import Network

CLAIMS_PATH = "data/processed/claims.jsonl"
CURRENT_STATE_PATH = "data/processed/current_state.jsonl"

OUT_DIR_DEFAULT = "data/processed/graphs"  # will contain issue_153101.html etc.
INDEX_HTML_DEFAULT = "index.html"


def load_jsonl(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        return []
    rows: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def issue_num_from_entity_id(entity_id: str) -> Optional[int]:
    # "github:rust-lang/rust:issue#153101" -> 153101
    m = re.search(r"#(\d+)$", entity_id)
    if not m:
        return None
    return int(m.group(1))


def entity_id_from_issue_num(current_state_rows: List[Dict[str, Any]], issue_num: int) -> Optional[str]:
    for row in current_state_rows:
        eid = row.get("entity_id", "")
        n = issue_num_from_entity_id(eid)
        if n == issue_num:
            return eid
    return None


def build_pyvis_graph(claims: List[Dict[str, Any]], issue_entity_id: str) -> Network:
    net = Network(
        height="820px",
        width="100%",
        directed=True,
        notebook=False,
        bgcolor="#ffffff",
        font_color="#111111",
    )

    # Better looking + more spacious defaults (ForceAtlas2)
    net.set_options(r"""
    var options = {
      "nodes": {
        "shape": "dot",
        "borderWidth": 1,
        "font": {"size": 18, "face": "arial"},
        "scaling": {"min": 12, "max": 52}
      },
      "edges": {
        "arrows": {"to": {"enabled": true, "scaleFactor": 0.6}},
        "smooth": {"type": "dynamic"},
        "color": {"inherit": false, "color": "rgba(120,160,190,0.22)"},
        "width": 1
      },
      "physics": {
        "enabled": true,
        "solver": "forceAtlas2Based",
        "forceAtlas2Based": {
          "gravitationalConstant": -80,
          "centralGravity": 0.005,
          "springLength": 260,
          "springConstant": 0.01,
          "avoidOverlap": 1.0
        },
        "stabilization": {"enabled": true, "iterations": 2000, "updateInterval": 50}
      },
      "interaction": {
        "hover": true,
        "tooltipDelay": 80,
        "navigationButtons": true,
        "keyboard": true
      }
    }
    """)

    # Collect nodes
    nodes = set()
    for c in claims:
        s = c["subject"]["id"]
        o = c["object"]["value"]
        nodes.add(s)
        nodes.add(o)

    # Add nodes
    for n in nodes:
        if "issue#" in n:
            color = "#9ecae1"  # blue
            label = n.split(":")[-1]
            title = n
            size = 50 if n == issue_entity_id else 36
        elif isinstance(n, str) and n.startswith(("A-", "E-", "T-", "C-")):
            color = "#fdae6b"  # orange (labels)
            label = n
            title = n
            size = 30
        else:
            color = "#a1d99b"  # green (people etc.)
            label = str(n)
            title = str(n)
            size = 30

        net.add_node(n, label=label, title=title, color=color, size=size)

    # Add edges (no edge labels; hover shows details)
    for c in claims:
        s = c["subject"]["id"]
        o = c["object"]["value"]
        pred = c.get("predicate", "")
        ev = c.get("event_time", "")
        conf = c.get("confidence", None)

        edge_title = pred
        if ev:
            edge_title += f"\n{ev}"
        if conf is not None:
            edge_title += f"\nconf={conf}"

        net.add_edge(s, o, title=edge_title)

    return net


def write_issue_html(out_path: str, net: Network) -> None:
    # IMPORTANT: avoid pyvis "template None" notebook bug by using write_html with notebook=False
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    net.write_html(out_path, notebook=False, open_browser=False)


def build_index_html(out_dir: str, issue_nums: List[int], index_path: str) -> None:
    issue_nums_sorted = sorted(issue_nums)
    options = "\n".join([f'<option value="{n}">{n}</option>' for n in issue_nums_sorted])

    html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Memory Graph Explorer</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; }}
    .row {{ display:flex; gap:12px; align-items:center; flex-wrap:wrap; }}
    select, input, button {{ font-size:16px; padding:8px; }}
    .hint {{ margin-top:10px; color:#555; }}
    .grid {{ margin-top:18px; display:grid; grid-template-columns: repeat(auto-fill, minmax(140px, 1fr)); gap:10px; }}
    a.card {{ display:block; padding:10px; border:1px solid #ddd; border-radius:10px; text-decoration:none; color:#111; }}
    a.card:hover {{ border-color:#999; }}
  </style>
</head>
<body>
  <h1>Memory Graph Explorer</h1>

  <div class="row">
    <label for="issueSelect"><b>Select issue:</b></label>
    <select id="issueSelect">
      {options}
    </select>

    <input id="searchBox" placeholder="Type issue number (e.g. 153101)" />
    <button onclick="go()">Open</button>
  </div>

  <div class="hint">
    Tip: you can type an issue number and press Enter. Each issue opens its own clean graph (no hairball).
  </div>

  <h2 style="margin-top:22px;">Quick links</h2>
  <div class="grid">
    {"".join([f'<a class="card" href="issue_{n}.html">Issue {n}</a>' for n in issue_nums_sorted[:120]])}
  </div>

  <script>
    const sel = document.getElementById("issueSelect");
    const box = document.getElementById("searchBox");

    function go() {{
      const v = (box.value || sel.value || "").trim();
      if (!v) return;
      window.location.href = "issue_" + v + ".html";
    }}

    box.addEventListener("keydown", (e) => {{
      if (e.key === "Enter") go();
    }});
  </script>
</body>
</html>
"""
    os.makedirs(out_dir, exist_ok=True)
    with open(index_path, "w", encoding="utf-8") as f:
        f.write(html)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("issue_number", nargs="?", type=int, help="Render a single issue graph (e.g. 153101)")
    ap.add_argument("--outdir", default=OUT_DIR_DEFAULT, help="Output dir for HTML graphs")
    ap.add_argument("--index-name", default=INDEX_HTML_DEFAULT, help="Index HTML filename inside outdir")
    ap.add_argument("--build-all", action="store_true", help="Build one HTML per issue + an index.html to select issues")
    ap.add_argument("--open", action="store_true", help="Open the generated HTML in your browser")
    args = ap.parse_args()

    current_state = load_jsonl(CURRENT_STATE_PATH)
    if not current_state:
        raise RuntimeError(f"Missing {CURRENT_STATE_PATH}. Run build_current_state.py first.")

    claims = load_jsonl(CLAIMS_PATH)
    if not claims:
        raise RuntimeError(f"Missing {CLAIMS_PATH}. Run extract first.")

    issue_nums = []
    for row in current_state:
        eid = row.get("entity_id", "")
        n = issue_num_from_entity_id(eid)
        if n is not None:
            issue_nums.append(n)

    outdir = args.outdir
    index_path = os.path.join(outdir, args.index_name)

    # Mode 1: build-all (recommended for selection UX)
    if args.build_all:
        os.makedirs(outdir, exist_ok=True)

        # Build each issue html
        for n in sorted(issue_nums):
            issue_eid = entity_id_from_issue_num(current_state, n)
            if not issue_eid:
                continue
            issue_claims = [c for c in claims if c["subject"]["id"] == issue_eid]
            if not issue_claims:
                continue

            net = build_pyvis_graph(issue_claims, issue_eid)
            out_path = os.path.join(outdir, f"issue_{n}.html")
            write_issue_html(out_path, net)

        # Build index with selector
        build_index_html(outdir, issue_nums, index_path)

        print(f"Wrote: {index_path}")
        if args.open:
            webbrowser.open(f"file://{os.path.abspath(index_path)}")
        return

    # Mode 2: single issue (CLI)
    if args.issue_number is None:
        # If user runs without args, generate index anyway (nice default)
        build_index_html(outdir, issue_nums, index_path)
        print(f"No issue provided. Wrote selector page: {index_path}")
        if args.open:
            webbrowser.open(f"file://{os.path.abspath(index_path)}")
        return

    issue_eid = entity_id_from_issue_num(current_state, args.issue_number)
    if not issue_eid:
        raise RuntimeError(f"Issue #{args.issue_number} not found in current_state.jsonl")

    issue_claims = [c for c in claims if c["subject"]["id"] == issue_eid]
    if not issue_claims:
        raise RuntimeError(f"No claims for issue #{args.issue_number}")

    net = build_pyvis_graph(issue_claims, issue_eid)
    out_path = os.path.join(outdir, f"issue_{args.issue_number}.html")
    write_issue_html(out_path, net)

    print(f"Wrote: {out_path}")
    if args.open:
        webbrowser.open(f"file://{os.path.abspath(out_path)}")


if __name__ == "__main__":
    main()