import streamlit as st
import pandas as pd


def sidebar_filters(region_options, query_default="Which regions should VF prioritize for emergency care investment?"):
    st.sidebar.header("Planner Controls")
    selected_region = st.sidebar.selectbox("Region filter", [""] + sorted(region_options), index=0)
    query = st.sidebar.text_area("Ask a planning question", value=query_default, height=120)
    return {
        "query": query,
        "region": selected_region or None,
    }


def render_badges(query_type: str, confidence: float, retrieval_path: str) -> None:
    cols = st.columns(3)
    cols[0].metric("Query Type", query_type or "unknown")
    cols[1].metric("Confidence", f"{confidence:.2f}")
    cols[2].metric("Retrieval Path", retrieval_path or "none")


def render_summary_card(summary: str, risk_concern: str | None = None, suggested_action: str | None = None) -> None:
    st.subheader("Answer Summary")
    st.write(summary)
    if risk_concern:
        st.caption(f"Risk / concern: {risk_concern}")
    if suggested_action:
        st.caption(f"Suggested action: {suggested_action}")


def render_evidence_table(evidence_rows):
    st.subheader("Evidence")
    if not evidence_rows:
        st.info("No evidence rows were returned for this query.")
        return
    st.dataframe(pd.DataFrame(evidence_rows), use_container_width=True)


def render_anomaly_cards(anomaly_rows):
    if not anomaly_rows:
        return
    st.subheader("Anomaly Cards")
    for row in anomaly_rows:
        with st.expander(f"{row.get('name', 'Unknown')} | score {row.get('anomaly_score', 'n/a')}"):
            for key, value in row.items():
                st.write(f"**{key}**: {value}")


def render_planning_view(planning_rows):
    if not planning_rows:
        return
    st.subheader("Planning View")
    st.dataframe(pd.DataFrame(planning_rows), use_container_width=True)