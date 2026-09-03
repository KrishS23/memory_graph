import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import streamlit as st
from pyvis.network import Network

from src.retrieval.query import retrieve
from src.retrieval.generate import generate_answer

CLAIMS_PATH = "data/processed/claims.jsonl"
EVIDENCE_PATH = "data/processed/evidence.jsonl"
CURRENT_STATE_PATH = "data/processed/current_state.jsonl"
DUP_REPORT_PATH = "data/processed/dedup_report.json"
DUP_EDGES_PATH = "data/processed/duplicate_edges.jsonl"
FAISS_INDEX_PATH = "data/processed/faiss.index"


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
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def issue_number_from_entity(entity_id: str) -> str:
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
    net = Network(height="740px", width="100%", directed=True, notebook=False)
    net.toggle_physics(True)
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
        if is_issue_entity_id(node_id):
            color = "#9ecae1"
            label = node_id.split(":")[-1]
            title = node_id
            size = 26
        elif node_id.startswith(("A-", "E-", "T-", "C-")):
            color = "#fdae6b"
            label = node_id
            title = node_id
            size = 18
        elif node_id in ("open", "closed"):
            color = "#a1d99b"
            label = node_id
            title = node_id
            size = 18
        else:
            color = "#a1d99b"
            label = node_id
            title = node_id
            size = 18

        nodes[node_id] = {
            "label": label,
            "title": title,
            "color": color,
            "size": size,
        }

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
        hover = f"{pred}<br>{t}<br>conf={conf}"
        net.add_edge(s, o, title=hover, label="")

    return net


def safe_time_slider(label: str, times: List[datetime]) -> Optional[Tuple[datetime, datetime]]:
    if len(times) < 2:
        st.sidebar.info("Not enough distinct timestamps for time-range slider.")
        return None

    tmin, tmax = min(times), max(times)
    if tmin == tmax:
        st.sidebar.info("All events share the same timestamp; time slider disabled.")
        return None

    return st.sidebar.slider(
        label,
        min_value=tmin,
        max_value=tmax,
        value=(tmin, tmax),
    )


def render_graph_explorer():
    claims, evidence_by_id, current_state = load_all_data()

    if not os.path.exists(CLAIMS_PATH):
        st.error(f"Missing {CLAIMS_PATH}. Run extraction first.")
        return
    if not os.path.exists(CURRENT_STATE_PATH):
        st.error(f"Missing {CURRENT_STATE_PATH}. Run build_current_state.py first.")
        return

    issue_ids = [row.get("entity_id") for row in current_state if row.get("entity_id")]
    issue_ids = [i for i in issue_ids if is_issue_entity_id(i)]
    if not issue_ids:
        st.error("No issues found in current_state.jsonl")
        return

    st.sidebar.header("Navigation + Filters")

    all_claim_types = sorted({c.get("claim_type") for c in claims if c.get("claim_type")})
    selected_types = st.sidebar.multiselect("Claim types", all_claim_types, default=all_claim_types)

    min_conf = st.sidebar.slider("Min confidence", 0.0, 1.0, 0.0, 0.01)

    st.sidebar.subheader("Time filter")
    enable_time = st.sidebar.checkbox("Enable time range filter", value=False)

    chosen_global_range = None
    if enable_time:
        all_times = [parse_ts(c.get("event_time", "")) for c in claims]
        all_times = [t for t in all_times if t is not None]
        chosen_global_range = safe_time_slider("Event time range (UTC)", all_times)

    if chosen_global_range is None:
        active_set = set()
        for c in claims:
            sid = c.get("subject", {}).get("id")
            if not sid or sid not in issue_ids:
                continue
            if claim_matches_filters(c, min_conf, selected_types, None):
                active_set.add(sid)
        active_issue_ids = sorted(active_set, key=lambda x: int(issue_number_from_entity(x)))
        if not active_issue_ids:
            active_issue_ids = issue_ids[:]
    else:
        active_set = set()
        for c in claims:
            sid = c.get("subject", {}).get("id")
            if not sid or sid not in issue_ids:
                continue
            if claim_matches_filters(c, min_conf, selected_types, chosen_global_range):
                active_set.add(sid)

        active_issue_ids = sorted(active_set, key=lambda x: int(issue_number_from_entity(x)))

        if not active_issue_ids:
            st.sidebar.warning("No issues have claims in the selected time/type/conf filters.")
            st.stop()

    active_issue_nums = [issue_number_from_entity(eid) for eid in active_issue_ids]

    st.sidebar.markdown("### Active issues in current filters")
    st.sidebar.write(f"**Count:** {len(active_issue_nums)}")

    SHOW_N = 50
    preview = active_issue_nums[:SHOW_N]

    st.sidebar.caption(
        f"Showing first {min(SHOW_N, len(active_issue_nums))} issues. Use the dropdown below to inspect one."
    )

    search_q = st.sidebar.text_input("Search active issue number", value="").strip()
    if search_q:
        preview = [x for x in active_issue_nums if search_q in x][:SHOW_N]

    st.sidebar.code(", ".join(preview) if preview else "(no matches)", language="text")

    st.sidebar.download_button(
        "Download active issues list",
        data="\n".join(active_issue_nums),
        file_name="active_issues.txt",
        mime="text/plain",
    )

    selected_issue_num = st.sidebar.selectbox(
        "Select issue number",
        active_issue_nums,
        index=0 if active_issue_nums else None,
    )

    selected_issue_id = None
    for eid in active_issue_ids:
        if eid.endswith(f"#{selected_issue_num}"):
            selected_issue_id = eid
            break
    if selected_issue_id is None:
        for eid in issue_ids:
            if eid.endswith(f"#{selected_issue_num}"):
                selected_issue_id = eid
                break

    if selected_issue_id is None:
        st.error("Could not resolve selected issue entity_id.")
        return

    issue_claims_all = [c for c in claims if c.get("subject", {}).get("id") == selected_issue_id]
    issue_claims = [
        c for c in issue_claims_all
        if claim_matches_filters(c, min_conf, selected_types, chosen_global_range)
    ]

    col1, col2 = st.columns([2.3, 1], gap="large")

    with col1:
        st.subheader(f"Graph: issue #{selected_issue_num}")
        if not issue_claims:
            st.warning("No claims for this issue match current filters.")
        else:
            net = build_pyvis_graph(issue_claims)
            html = net.generate_html(notebook=False)
            st.components.v1.html(html, height=760, scrolling=True)

        cs = next((x for x in current_state if x.get("entity_id") == selected_issue_id), None)
        if cs:
            st.markdown("### Current state (computed)")
            st.json(cs)

    with col2:
        st.subheader("Claims → Evidence panel")

        if not issue_claims:
            st.info("Nothing to show.")
            st.stop()

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
        if os.path.exists(DUP_REPORT_PATH):
            with open(DUP_REPORT_PATH, "r", encoding="utf-8") as f:
                rep = json.load(f)
            st.markdown("**dedup_report.json**")
            st.json(rep)
        else:
            st.caption("No dedup_report.json found (optional).")

        if os.path.exists(DUP_EDGES_PATH):
            dup_edges = load_jsonl(DUP_EDGES_PATH)
            needle = f"#{selected_issue_num}"
            related = [d for d in dup_edges if needle in json.dumps(d)]
            st.write(f"duplicate_edges related to issue #{selected_issue_num}: **{len(related)}**")
            if related:
                st.json(related[:30])
        else:
            st.caption("No duplicate_edges.jsonl found (optional).")


def render_search_tab():
    st.subheader("🔍 Ask the Memory Graph")
    st.caption("Retrieval runs over embedded claims. Generation grounds answers strictly in retrieved evidence.")

    if not os.path.exists(FAISS_INDEX_PATH):
        st.warning(
            "No FAISS index found. Run `embed_claims.py` and `build_index.py` first "
            "before using search here."
        )
        return

    query = st.text_input("Enter a question about issue history:")
    k = st.slider("Number of results (k)", min_value=1, max_value=20, value=5)

    if not query:
        return

    with st.spinner("Retrieving relevant claims..."):
        results = retrieve(query, k=k)

    if not results:
        st.info("No relevant claims found.")
        return

    st.markdown(f"### Top {len(results)} retrieved claims")
    for r in results:
        claim = r["claim"]
        ev = r["evidence"]
        st.markdown(
            f"**{claim['subject']['id']} {claim['predicate']} {claim['object']['value']}**  "
            f"— score: `{r['score']:.3f}`"
        )
        if ev:
            st.caption(f"\"{ev['quote']}\"")
            st.caption(f"source: {ev['url']}")
        st.divider()

    if st.button("Generate grounded answer"):
        with st.spinner("Asking Gemini..."):
            answer = generate_answer(query, k=k)
        st.markdown("### Answer")
        st.write(answer)


def main():
    st.set_page_config(page_title="Memory Graph Explorer", layout="wide")
    st.title("Memory Graph Explorer")
    st.caption("Navigate issues → see claims graph → click a claim → view supporting evidence + duplicates/merges.")

    tab1, tab2 = st.tabs(["📊 Graph Explorer", "🔍 Ask the Memory Graph"])

    with tab1:
        render_graph_explorer()

    with tab2:
        render_search_tab()


if __name__ == "__main__":
    main()