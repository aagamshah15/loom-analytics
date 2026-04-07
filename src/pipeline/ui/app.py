from __future__ import annotations

from pathlib import Path
import sys

try:
    import streamlit as st
    import streamlit.components.v1 as components
except ImportError as exc:  # pragma: no cover - import guard for non-UI environments
    raise RuntimeError(
        "Streamlit is not installed. Run `pip install -r requirements.txt` to use the UI."
    ) from exc

SRC_ROOT = Path(__file__).resolve().parents[2]
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from pipeline.business.router import (
    build_dashboard,
    build_insight_candidates,
    default_dashboard_title,
    detect_business_context,
    section_options,
    workflow_overview,
)
from pipeline.common.reporting import build_markdown_summary, build_report_dict
from pipeline.run import run_pipeline
from pipeline.ui.helpers import create_ephemeral_workspace, persist_uploaded_file


def main() -> None:
    st.set_page_config(
        page_title="Loom",
        page_icon="CSV",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    _inject_styles()

    st.title("Loom")
    st.caption("Upload a CSV, review non-obvious insights, and only then turn the approved story into a business dashboard.")

    with st.sidebar:
        st.subheader("Run Controls")
        csv_file = st.file_uploader("CSV file", type=["csv"])
        validate_only = st.checkbox("Validate only", value=False)
        run_button = st.button("Analyze CSV", type="primary", use_container_width=True)

        st.markdown("---")
        st.markdown(
            "The app runs the full validation and cleaning pipeline in the background, then lets you approve individual insights before building the dashboard."
        )

    _render_intro()

    if run_button:
        if csv_file is None:
            st.error("Upload a CSV before running the pipeline.")
        else:
            _run_from_upload(csv_file=csv_file, validate_only=validate_only)

    result = st.session_state.get("last_run")
    if result:
        _render_result(result)


def _run_from_upload(csv_file, validate_only: bool) -> None:
    with create_ephemeral_workspace() as workspace:
        workspace_path = Path(workspace)
        input_path = persist_uploaded_file(workspace_path, csv_file.name, csv_file.getvalue())

        with st.spinner("Running the analytics pipeline..."):
            context = run_pipeline(
                input_path=input_path,
                output_dir=None,
                config_path=None,
                validate_only=validate_only,
                persist_outputs=False,
                include_visualizations=False,
            )

    report = build_report_dict(context)
    summary_markdown = build_markdown_summary(context)
    business_context = detect_business_context(context)

    st.session_state["last_run"] = {
        "input_name": csv_file.name,
        "report": report,
        "summary_markdown": summary_markdown,
        "business_context": business_context,
        "built_dashboard": None,
    }
    if context.errors:
        st.warning("The pipeline finished with handled errors. Review the report details below.")
    else:
        st.success("Analysis complete.")


def _render_intro() -> None:
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown(
            """
            <div class="metric-card">
              <div class="metric-title">Upload</div>
              <div class="metric-copy">Bring in a raw CSV and optionally attach a YAML config.</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with col2:
        st.markdown(
            """
            <div class="metric-card">
              <div class="metric-title">Review</div>
              <div class="metric-copy">Approve or reject rare insights one by one, and steer the analysis with a text prompt.</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with col3:
        st.markdown(
            """
            <div class="metric-card">
              <div class="metric-title">Build</div>
              <div class="metric-copy">Generate an editable dashboard from the approved insight set and download it as HTML.</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def _render_result(result: dict) -> None:
    report = result["report"]
    quality_report = report.get("quality_report", {})
    run_id = report["run_id"]

    st.markdown("---")
    st.subheader("Run Summary")
    metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
    metric_col1.metric("Quality Score", quality_report.get("score", "n/a"))
    metric_col2.metric("Rows", report.get("row_count", 0))
    metric_col3.metric("Columns", report.get("column_count", 0))
    metric_col4.metric("Warnings", len(report.get("warnings", [])))

    meta_col1, meta_col2 = st.columns([2, 1])
    with meta_col1:
        st.markdown(f"**Input file:** `{result['input_name']}`")
        st.markdown("**Mode:** `session-only business workflow`")
    with meta_col2:
        if report.get("warnings"):
            st.warning("\n".join(report["warnings"]))
        if report.get("errors"):
            st.error("\n".join(error["message"] for error in report["errors"]))

    business_context = result.get("business_context")
    if business_context is not None:
        _render_business_workflow(result, run_id)
    else:
        st.info("No specialized template matched this dataset yet.")


def _render_business_workflow(result: dict, run_id: str) -> None:
    business_context = result["business_context"]
    kind = business_context["kind"]
    analysis = business_context["analysis"]
    prompt_key = f"{run_id}_analysis_prompt"
    if prompt_key not in st.session_state:
        st.session_state[prompt_key] = ""

    overview = workflow_overview(kind, analysis)
    dataset = analysis["dataset"]

    st.subheader(overview["title"])
    col1, col2, col3, col4 = st.columns(4)
    for col, metric in zip([col1, col2, col3, col4], overview["metrics"]):
        col.metric(metric[0], metric[1])

    st.markdown(
        """
        <div class="workflow-card">
          <div class="workflow-title">What this looks like at first glance</div>
          <div class="workflow-copy">
            {blurb}
          </div>
        </div>
        """.format(blurb=overview["blurb"]),
        unsafe_allow_html=True,
    )

    st.subheader("Hidden Insight Review")
    st.text_area(
        "Additional instructions",
        key=prompt_key,
        height=110,
        placeholder="Example: focus more on volatility and drawdowns, de-emphasize dividends, make the dashboard useful for executives.",
    )

    review_bundle = build_insight_candidates(kind, analysis, st.session_state[prompt_key])
    insights = review_bundle["insights"]
    focus_tags = review_bundle["focus_tags"]

    if focus_tags:
        st.markdown("**Recognized focus areas:** " + ", ".join(f"`{tag}`" for tag in focus_tags))

    bulk_col1, bulk_col2, bulk_col3 = st.columns([1, 1, 2])
    if bulk_col1.button("Approve All", key=f"{run_id}_approve_all"):
        for insight in insights:
            st.session_state[_insight_key(run_id, insight["id"])] = True
    if bulk_col2.button("Reject All", key=f"{run_id}_reject_all"):
        for insight in insights:
            st.session_state[_insight_key(run_id, insight["id"])] = False
    bulk_col3.markdown("Approve the insights you want preserved as the dashboard blueprint.")

    approved_count = 0
    for insight in insights:
        widget_key = _insight_key(run_id, insight["id"])
        if widget_key not in st.session_state:
            st.session_state[widget_key] = bool(insight.get("recommended", False))
        approved = st.checkbox(
            f"Use in dashboard: {insight['title']}",
            key=widget_key,
        )
        if approved:
            approved_count += 1
        st.markdown(
            f"""
            <div class="insight-card">
              <div class="insight-meta">
                <span class="insight-badge">{insight['category']}</span>
                <span class="insight-severity">{insight['severity'].upper()}</span>
              </div>
              <div class="insight-message">{insight['summary']}</div>
              <div class="insight-action">{insight['detail']}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.subheader("Build Dashboard")
    st.markdown("The build action is always available. It uses only the approved insights and your dashboard settings below.")

    title_key = f"{run_id}_dash_title"
    subtitle_key = f"{run_id}_dash_subtitle"
    sections_key = f"{run_id}_dash_sections"
    metric_count_key = f"{run_id}_dash_metric_count"
    show_notes_key = f"{run_id}_dash_show_notes"

    domain_section_options = section_options(kind)
    default_sections = list(domain_section_options.keys())
    if title_key not in st.session_state:
        st.session_state[title_key] = default_dashboard_title(kind)
    if subtitle_key not in st.session_state:
        st.session_state[subtitle_key] = f"Approved insights from {dataset['start_year']} to {dataset['end_year']}"
    if sections_key not in st.session_state:
        st.session_state[sections_key] = default_sections
    if metric_count_key not in st.session_state:
        st.session_state[metric_count_key] = 4
    if show_notes_key not in st.session_state:
        st.session_state[show_notes_key] = True

    settings_col1, settings_col2 = st.columns([2, 1])
    with settings_col1:
        st.text_input("Dashboard title", key=title_key)
        st.text_input("Dashboard subtitle", key=subtitle_key)
        st.multiselect(
            "Sections to include",
            options=list(domain_section_options.keys()),
            default=st.session_state[sections_key],
            format_func=lambda value: domain_section_options[value],
            key=sections_key,
        )
    with settings_col2:
        st.slider("Metric cards", min_value=2, max_value=6, key=metric_count_key)
        st.checkbox("Show approved insight notes", key=show_notes_key)
        st.metric("Approved Insights", approved_count)

    build_col1, build_col2 = st.columns([1, 3])
    build_clicked = build_col1.button("Build Dashboard", type="primary", use_container_width=True)
    if approved_count == 0:
        build_col2.warning("Approve at least one insight to build the dashboard.")
    else:
        build_col2.info("Once built, the dashboard can be re-generated any time with a different approval set or layout settings.")

    if build_clicked and approved_count > 0:
        approved_ids = [insight["id"] for insight in insights if st.session_state[_insight_key(run_id, insight["id"])]]
        dashboard = build_dashboard(
            kind=kind,
            analysis=analysis,
            approved_insight_ids=approved_ids,
            user_prompt=st.session_state[prompt_key],
            settings={
                "title": st.session_state[title_key],
                "subtitle": st.session_state[subtitle_key],
                "included_sections": st.session_state[sections_key],
                "metric_count": st.session_state[metric_count_key],
                "show_notes": st.session_state[show_notes_key],
            },
        )
        result["built_dashboard"] = dashboard
        st.session_state["last_run"] = result
        st.success("Dashboard built from the approved insight set.")

    built_dashboard = result.get("built_dashboard")
    if built_dashboard is not None:
        st.subheader("Dashboard Preview")
        components.html(built_dashboard["html"], height=built_dashboard["height"], scrolling=True)
        st.download_button(
            "Download Static HTML",
            data=built_dashboard["html"],
            file_name=built_dashboard["download_name"],
            mime="text/html",
            use_container_width=True,
        )


def _insight_key(run_id: str, insight_id: str) -> str:
    return f"{run_id}_approve_{insight_id}"


def _inject_styles() -> None:
    st.markdown(
        """
        <style>
        .stApp,
        .stApp [data-testid="stAppViewContainer"],
        .stApp [data-testid="stHeader"] {
            background:
                radial-gradient(circle at top left, rgba(218, 233, 244, 0.85), transparent 30%),
                linear-gradient(180deg, #f7fafc 0%, #eef4f7 100%);
        }
        .stApp,
        .stApp p,
        .stApp label,
        .stApp span,
        .stApp li,
        .stApp h1,
        .stApp h2,
        .stApp h3,
        .stApp h4,
        .stApp h5,
        .stApp h6,
        .stApp div {
            color: #132737;
        }
        .stApp a {
            color: #1f5f8b;
        }
        .stApp .stCaptionContainer,
        .stApp [data-testid="stMarkdownContainer"] p,
        .stApp [data-testid="stMarkdownContainer"] li {
            color: #2a3a46;
        }
        .stApp [data-testid="stSidebar"] {
            background: rgba(244, 248, 251, 0.92);
            border-right: 1px solid rgba(22, 50, 79, 0.08);
        }
        .stApp [data-testid="stSidebar"] p,
        .stApp [data-testid="stSidebar"] label,
        .stApp [data-testid="stSidebar"] span,
        .stApp [data-testid="stSidebar"] div,
        .stApp [data-testid="stSidebar"] small,
        .stApp [data-testid="stSidebar"] h1,
        .stApp [data-testid="stSidebar"] h2,
        .stApp [data-testid="stSidebar"] h3,
        .stApp [data-testid="stSidebar"] h4,
        .stApp [data-testid="stSidebar"] h5,
        .stApp [data-testid="stSidebar"] h6 {
            color: #16324f;
        }
        .stApp [data-testid="stFileUploaderDropzone"] {
            background: rgba(255, 255, 255, 0.88);
            border: 1px dashed rgba(22, 50, 79, 0.25);
        }
        .stApp [data-testid="stSidebar"] [data-testid="stFileUploaderDropzone"] * {
            color: #16324f;
        }
        .stApp [data-testid="stSidebar"] section[data-testid="stFileUploader"] button,
        .stApp [data-testid="stSidebar"] section[data-testid="stFileUploader"] button * {
            color: #16324f;
        }
        .stApp [data-testid="stSidebar"] button[kind="secondary"] {
            background: rgba(255, 255, 255, 0.92);
            color: #16324f;
            border-color: rgba(22, 50, 79, 0.2);
        }
        .stApp [data-testid="stSidebar"] button[kind="secondary"] * {
            color: #16324f;
        }
        .stApp [data-testid="stSidebar"] [data-testid="stCheckbox"] label,
        .stApp [data-testid="stSidebar"] [data-testid="stFileUploaderInstructions"] span,
        .stApp [data-testid="stSidebar"] [data-testid="stFileUploaderInstructions"] small {
            color: #16324f;
        }
        .stApp [data-testid="stMetric"] {
            background: rgba(255, 255, 255, 0.72);
            border: 1px solid rgba(22, 50, 79, 0.08);
            border-radius: 16px;
            padding: 1rem;
        }
        .stApp [data-testid="stMetricLabel"] *,
        .stApp [data-testid="stMetricValue"] *,
        .stApp [data-testid="stMetricDelta"] * {
            color: #16324f;
        }
        .stApp button[kind="primary"] {
            background: linear-gradient(135deg, #16324f 0%, #1f5f8b 100%);
            color: #f8fbfd;
            border: none;
        }
        .stApp button[kind="secondary"] {
            color: #16324f;
            border-color: rgba(22, 50, 79, 0.2);
        }
        .stApp [data-baseweb="tab-list"] {
            gap: 0.5rem;
        }
        .stApp button[data-baseweb="tab"] {
            background: rgba(255, 255, 255, 0.72);
            border-radius: 999px;
            padding: 0.4rem 0.95rem;
            border: 1px solid rgba(22, 50, 79, 0.08);
        }
        .stApp button[data-baseweb="tab"] p {
            color: #49647d;
            font-weight: 600;
        }
        .stApp button[data-baseweb="tab"][aria-selected="true"] {
            background: #16324f;
            border-color: #16324f;
        }
        .stApp button[data-baseweb="tab"][aria-selected="true"] p {
            color: #f8fbfd;
        }
        .stApp [data-testid="stAlert"] {
            border-radius: 16px;
        }
        .stApp [data-testid="stAlert"] * {
            color: inherit;
        }
        .stApp [data-testid="stSuccess"] {
            background: rgba(199, 242, 216, 0.92);
            color: #155b2f;
        }
        .stApp [data-testid="stWarning"] {
            background: rgba(255, 243, 191, 0.96);
            color: #7a5600;
        }
        .stApp [data-testid="stError"] {
            background: rgba(255, 219, 219, 0.96);
            color: #7d1f1f;
        }
        .stApp [data-testid="stInfo"] {
            background: rgba(214, 235, 255, 0.96);
            color: #1c4d80;
        }
        .stApp code {
            background: #102230;
            color: #7df0aa;
            border-radius: 8px;
            padding: 0.2rem 0.45rem;
        }
        .metric-card, .insight-card, .workflow-card {
            border: 1px solid rgba(22, 50, 79, 0.12);
            border-radius: 18px;
            background: rgba(255, 255, 255, 0.86);
            padding: 1rem 1.1rem;
            box-shadow: 0 10px 30px rgba(22, 50, 79, 0.05);
        }
        .metric-card {
            min-height: 124px;
        }
        .metric-title, .insight-severity, .workflow-title {
            font-size: 0.8rem;
            letter-spacing: 0.08em;
            color: #49647d;
            font-weight: 700;
            margin-bottom: 0.5rem;
            text-transform: uppercase;
        }
        .metric-copy, .insight-action, .workflow-copy {
            color: #2a3a46;
            line-height: 1.45;
        }
        .insight-meta {
            display: flex;
            gap: 0.6rem;
            margin-bottom: 0.45rem;
            align-items: center;
        }
        .insight-badge {
            background: #e8f1f7;
            border-radius: 999px;
            padding: 0.25rem 0.55rem;
            font-size: 0.72rem;
            color: #1f5f8b;
            font-weight: 700;
            text-transform: uppercase;
        }
        .insight-message {
            font-size: 1rem;
            color: #132737;
            margin-bottom: 0.5rem;
            font-weight: 600;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
