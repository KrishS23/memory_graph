import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import streamlit as st
from pyvis.network import Network


# -----------------------------
# Paths (match your repo layout)
# -----------------------------
CLAIMS_PATH = "data/processed/claims.jsonl"
EVIDENCE_PATH = "data/processed/evidence.jsonl"
CURRENT_STATE_PATH = "data/processed/current_state.jsonl"
DUP_REPORT_PATH = "data/processed/dedup_report.json"
DUP_EDGES_PATH = "data/processed/duplicate_edges.jsonl"


# -----------------------------
# Helpers
# -----------------------------
def load_jsonl(path: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not os.path.exists(path):
        return rows
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def parse_ts(ts: str) -> Optional[datetime]:
    """Parse ISO time like '2026-02-27T16:05:20Z' -> datetime(UTC)."""
    if not ts:
        return None
    try:
        # 'Z' => UTC
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def issue_number_from_entity(entity_id: str) -> str:
    # "github:rust-lang/rust:issue#153101" -> "153101"
    if "#" in entity_id:
        return entity_id.split("#")[-1]
    return entity_id


def is_issue_entity_id(entity_id: str) -> bool:
    return ":issue#" in entity_id


@st.cache_data(show_spinner=False)
def load_all_data():
    claims = load_jsonl(CLAIMS_PATH)
    evidence = load_jsonl(EVIDENCE_PATH)
    current_state = load_jsonl(CURRENT_STATE_PATH)
    evidence_by_id = {e["evidence_id"]: e for e in evidence if "evidence_id" in e}
    return claims, evidence_by_id, current_state


def claim_matches_filters(
    c: Dict[str, Any],
    min_conf: float,
    allowed_types: List[str],
    time_range: Optional[Tuple[datetime, datetime]],
) -> bool:
    if c.get("claim_type") not in allowed_types:
        return False
    if float(c.get("confidence", 0.0)) < float(min_conf):
        return False
    if time_range is None:
        return True
    t = parse_ts(c.get("event_time", ""))
    if t is None:
        return False
    return time_range[0] <= t <= time_range[1]


def build_pyvis_graph(issue_claims: List[Dict[str, Any]]) -> Network:
    """
    Create a spacious PyVis graph:
      Issue node -> Object nodes (label/person/status etc)
      Edge hover shows predicate + time + confidence
    """
    net = Network(height="740px", width="100%", directed=True, notebook=False)

    # Make it more spacious
    net.toggle_physics(True)
    # Barnes-Hut works well for medium graphs
    net.barnes_hut(
        gravity=-26000,
        central_gravity=0.1,
        spring_length=280,
        spring_strength=0.006,
        damping=0.12,
        overlap=0.5,
    )

    nodes: Dict[str, Dict[str, Any]] = {}

    def add_node(node_id: str):
        if node_id in nodes:
            return

        # Color/label logic
        if is_issue_entity_id(node_id):
            color = "#9ecae1"  # light blue
            label = node_id.split(":")[-1]  # issue#153101
            title = node_id
            size = 26
        elif node_id.startswith(("A-", "E-", "T-", "C-")):
            color = "#fdae6b"  # orange-ish
            label = node_id
            title = node_id
            size = 18
        elif node_id in ("open", "closed"):
            color = "#a1d99b"  # green-ish
            label = node_id
            title = node_id
            size = 18
        else:
            # assignee or other entity
            color = "#a1d99b"
            label = node_id
            title = node_id
            size = 18

        nodes[node_id] = dict(label=label, title=title, color=color, size=size)

    # Add nodes + edges
    for c in issue_claims:
        s = c["subject"]["id"]
        o = c["object"]["value"]

        add_node(s)
        add_node(o)

    for node_id, props in nodes.items():
        net.add_node(node_id, **props)

    for c in issue_claims:
        s = c["subject"]["id"]
        o = c["object"]["value"]
        pred = c.get("predicate", "")
        t = c.get("event_time", "")
        conf = c.get("confidence", 0.0)

        # show predicate on hover + extra metadata
        hover = f"{pred}<br>{t}<br>conf={conf}"
        net.add_edge(s, o, title=hover, label="")  # keep edge label empty => cleaner

    return net


def safe_time_slider(
    label: str,
    times: List[datetime],
    default_full_range: bool = True,
):
    """
    Returns None if no usable range, else (start,end).
    Fixes the "min must be less than max" Streamlit crash.
    """
    if len(times) < 2:
        st.sidebar.info("Not enough distinct timestamps for time-range slider.")
        return None

    tmin, tmax = min(times), max(times)
    if tmin == tmax:
        st.sidebar.info("All events share the same timestamp; time slider disabled.")
        return None

    if default_full_range:
        return st.sidebar.slider(label, min_value=tmin, max_value=tmax, value=(tmin, tmax))
    else:
        return st.sidebar.slider(label, min_value=tmin, max_value=tmax, value=(tmin, tmax))


# -----------------------------
# App
# -----------------------------
def main():
    st.set_page_config(page_title="Memory Graph Explorer", layout="wide")
    st.title("Memory Graph Explorer")
    st.caption("Navigate issues → see claims graph → click a claim → view supporting evidence + duplicates/merges.")

    claims, evidence_by_id, current_state = load_all_data()

    # Basic checks
    if not os.path.exists(CLAIMS_PATH):
        st.error(f"Missing {CLAIMS_PATH}. Run extraction first.")
        return
    if not os.path.exists(CURRENT_STATE_PATH):
        st.error(f"Missing {CURRENT_STATE_PATH}. Run build_current_state.py first.")
        return

    # Build all issues list from current_state
    issue_ids = [row.get("entity_id") for row in current_state if row.get("entity_id")]
    issue_ids = [i for i in issue_ids if is_issue_entity_id(i)]
    if not issue_ids:
        st.error("No issues found in current_state.jsonl")
        return

    # Sidebar filters
    st.sidebar.header("Navigation + Filters")

    all_claim_types = sorted({c.get("claim_type") for c in claims if c.get("claim_type")})
    selected_types = st.sidebar.multiselect("Claim types", all_claim_types, default=all_claim_types)

    min_conf = st.sidebar.slider("Min confidence", 0.0, 1.0, 0.0, 0.01)

    # Global time filter (affects which issues are "active" and shown in dropdown)
    st.sidebar.subheader("Time filter")
    enable_time = st.sidebar.checkbox("Enable time range filter", value=False)

    chosen_global_range = None
    if enable_time:
        all_times = [parse_ts(c.get("event_time", "")) for c in claims]
        all_times = [t for t in all_times if t is not None]
        chosen_global_range = safe_time_slider("Event time range (UTC)", all_times)

    # Compute "active issues" in time range/type/conf
    if chosen_global_range is None:
        active_issue_ids = issue_ids[:]  # show all
    else:
        active_set = set()
        for c in claims:
            sid = c.get("subject", {}).get("id")
            if not sid:
                continue
            if sid not in issue_ids:
                continue
            if claim_matches_filters(c, min_conf, selected_types, chosen_global_range):
                active_set.add(sid)

        active_issue_ids = sorted(active_set, key=lambda x: int(issue_number_from_entity(x)))

        if not active_issue_ids:
            st.sidebar.warning("No issues have claims in the selected time/type/conf filters.")
            st.stop()

    active_issue_nums = [issue_number_from_entity(eid) for eid in active_issue_ids]

    selected_issue_num = st.sidebar.selectbox(
        "Select issue number",
        active_issue_nums,
        index=0,
    )

    # Resolve entity_id from issue number
    selected_issue_id = None
    for eid in active_issue_ids:
        if eid.endswith(f"#{selected_issue_num}"):
            selected_issue_id = eid
            break
    if selected_issue_id is None:
        # fallback: search all known issues
        for eid in issue_ids:
            if eid.endswith(f"#{selected_issue_num}"):
                selected_issue_id = eid
                break

    if selected_issue_id is None:
        st.error("Could not resolve selected issue entity_id.")
        return

    # Filter claims for this issue
    issue_claims_all = [c for c in claims if c.get("subject", {}).get("id") == selected_issue_id]
    issue_claims = [
        c for c in issue_claims_all
        if claim_matches_filters(c, min_conf, selected_types, chosen_global_range)
    ]

    # Layout: Graph + Right panel
    col1, col2 = st.columns([2.3, 1], gap="large")

    with col1:
        st.subheader(f"Graph: issue #{selected_issue_num}")
        if not issue_claims:
            st.warning("No claims for this issue match current filters.")
        else:
            net = build_pyvis_graph(issue_claims)
            html = net.generate_html(notebook=False)
            st.components.v1.html(html, height=760, scrolling=True)

        # Optional: show current state summary right under the graph
        cs = next((x for x in current_state if x.get("entity_id") == selected_issue_id), None)
        if cs:
            st.markdown("### Current state (computed)")
            st.json(cs)

    with col2:
        st.subheader("Claims → Evidence panel")

        if not issue_claims:
            st.info("Nothing to show.")
            st.stop()

        # Sort newest first
        sorted_claims = sorted(issue_claims, key=lambda x: x.get("event_time", ""), reverse=True)

        claim_labels = []
        for c in sorted_claims:
            claim_labels.append(
                f'{c.get("event_time","")} | {c.get("predicate","")} | {c.get("object",{}).get("value","")} | conf={c.get("confidence",0)}'
            )

        selected_idx = st.selectbox(
            "Select a claim",
            list(range(len(sorted_claims))),
            format_func=lambda i: claim_labels[i],
        )

        selected_claim = sorted_claims[selected_idx]
        st.markdown("#### Selected claim (raw)")
        st.code(json.dumps(selected_claim, indent=2), language="json")

        st.markdown("#### Evidence (source metadata + excerpt)")
        ev_ids = selected_claim.get("evidence_ids", [])
        if not ev_ids:
            st.info("No evidence attached to this claim.")
        else:
            for ev_id in ev_ids:
                ev = evidence_by_id.get(ev_id)
                if not ev:
                    st.warning(f"Missing evidence_id: {ev_id}")
                    continue

                st.write(f"**evidence_id:** `{ev.get('evidence_id','')}`")
                st.write(f"**artifact_id:** `{ev.get('artifact_id','')}`")
                st.write(f"**timestamp:** `{ev.get('timestamp','')}`")
                st.write(f"**url:** {ev.get('url','')}")
                quote = ev.get("quote", "")
                if quote:
                    st.code(quote, language="text")
                else:
                    st.caption("(No excerpt text stored for this evidence.)")
                st.divider()

        st.markdown("### Duplicates / merges")
        # Show dedup report summary if present
        if os.path.exists(DUP_REPORT_PATH):
            with open(DUP_REPORT_PATH, "r", encoding="utf-8") as f:
                rep = json.load(f)
            st.markdown("**dedup_report.json**")
            st.json(rep)
        else:
            st.caption("No dedup_report.json found (optional).")

        # Show relevant duplicate edges
        if os.path.exists(DUP_EDGES_PATH):
            dup_edges = load_jsonl(DUP_EDGES_PATH)
            # naive filter: anything that mentions this issue id string
            needle = f"#{selected_issue_num}"
            related = [d for d in dup_edges if needle in json.dumps(d)]
            st.write(f"duplicate_edges related to issue #{selected_issue_num}: **{len(related)}**")
            if related:
                st.json(related[:30])
        else:
            st.caption("No duplicate_edges.jsonl found (optional).")


if __name__ == "__main__":
    main()