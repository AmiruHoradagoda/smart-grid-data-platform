"""Run with: uv run streamlit run dashboard/app.py"""

from collections import defaultdict
from datetime import date, datetime

import plotly.graph_objects as go
import streamlit as st

import api_client

st.set_page_config(page_title="Smart Grid Monitoring Dashboard", page_icon="⚡", layout="wide")

ENERGY = {
    "total_consumption_kwh": ("Consumption", "#2563eb"),
    "total_solar_generation_kwh": ("Solar generation", "#10b981"),
    "total_grid_import_kwh": ("Grid import", "#f59e0b"),
}


@st.cache_data(ttl=15, show_spinner=False)
def fetch(resource, *args):
    return getattr(api_client, resource)(*args)


def load(resource, *args):
    try:
        return fetch(resource, *args)
    except api_client.APIError as error:
        st.warning(str(error))
        return None


def chart(figure, title, y_title, key):
    figure.update_layout(
        title=title, template="plotly_white", height=350,
        margin=dict(l=20, r=20, t=60, b=40),
        yaxis_title=y_title, legend=dict(orientation="h", y=-0.2),
        font=dict(family="Arial", size=13),
    )
    st.plotly_chart(figure, width="stretch", key=key)


st.title("Smart Grid Energy Monitoring & Billing Dashboard")
st.caption("Real-time energy monitoring, renewable contribution, grid dependency, and household billing insights.")
status, refresh = st.columns([4, 1])
with refresh:
    if st.button("Refresh Dashboard", type="primary", width="stretch"):
        fetch.clear()
        st.rerun()
with status:
    try:
        health = fetch("get_health")
        if health.get("status") == "healthy" and health.get("database") == "connected":
            st.success("System status: HEALTHY · API and database connected")
        else:
            st.warning("System status: DEGRADED · check API and database")
    except api_client.APIError as error:
        st.error(str(error))
        st.info("Start the API with: uv run uvicorn src.api.main:app --reload")
        st.stop()

st.caption(f"API: {api_client.API_BASE_URL} · Cache: 15 seconds · Loaded at {datetime.now():%H:%M:%S} local time. Refresh manually for new results.")
zones = load("get_latest_zones")
alert_result = load("get_renewable_alerts")

st.subheader("Grid at a glance")
if zones:
    totals = {field: sum(float(row[field]) for row in zones) for field in ENERGY}
    renewable = (100 * totals["total_solar_generation_kwh"] / totals["total_consumption_kwh"]
                 if totals["total_consumption_kwh"] else 0)
    cards = st.columns(5)
    for card, (field, (label, _)) in zip(cards, ENERGY.items()):
        card.metric(label, f"{totals[field]:,.2f} kWh")
    cards[3].metric("Renewable contribution", f"{renewable:.1f}%")
    cards[4].metric("Active renewable alerts", len(alert_result["alerts"]) if alert_result is not None else "Unavailable")
    times = sorted({row["window_end"] for row in zones})
    st.caption(f"Latest stored simulated window: {times[-1]}. Values describe stored windows, not wall-clock freshness. Renewable contribution is total solar / total consumption.")
    if len(times) > 1:
        st.warning("Zones have different latest window times; KPI totals combine their latest available readings.")
else:
    st.info("Latest zone metrics are unavailable." if zones is None else "No zone metrics stored yet.")

st.subheader("Energy flow over time")
history = load("get_zone_history", 300)
if history:
    windows = defaultdict(lambda: {field: 0.0 for field in ENERGY})
    coverage = defaultdict(set)
    for row in history:
        for field in ENERGY:
            windows[row["window_end"]][field] += float(row[field])
        coverage[row["window_end"]].add(row["grid_zone"])
    times = sorted(windows)[-100:]
    figure = go.Figure()
    for field, (label, color) in ENERGY.items():
        figure.add_trace(go.Scatter(x=times, y=[windows[t][field] for t in times],
                                   name=label, mode="lines", line=dict(color=color, width=2.5)))
    figure.update_layout(xaxis_title="Simulated window end", hovermode="x unified")
    chart(figure, "Energy Flow Over Time", "Energy (kWh)", "history")
    st.caption("Grid import represents the remaining demand after local solar generation. Simulated time; sums of returned zones per window, up to 100 windows from 300 recent rows.")
    expected_zones = {row["grid_zone"] for row in zones} if zones else set.union(*coverage.values())
    if any(coverage[t] != expected_zones for t in times):
        st.warning("Some history windows contain fewer zones. Their totals are partial, including any window cut by the row limit.")
else:
    st.info("Zone history is unavailable." if history is None else "No historical zone readings yet.")

if zones:
    left, right = st.columns(2)
    names = [row["grid_zone"] for row in zones]
    with left:
        figure = go.Figure()
        for field, (label, color) in ENERGY.items():
            figure.add_trace(go.Bar(x=names, y=[row[field] for row in zones], name=label, marker_color=color))
        figure.update_layout(barmode="group")
        chart(figure, "Current Energy Balance by Grid Zone", "Energy (kWh)", "balance")
    with right:
        figure = go.Figure(go.Bar(x=names, y=[row["renewable_contribution_pct"] for row in zones], marker_color="#10b981"))
        if alert_result is not None:
            figure.add_hline(y=alert_result["threshold_pct"], line_dash="dash", line_color="#ef4444",
                             annotation_text="Low Renewable Threshold", annotation_position="top left")
        chart(figure, "Renewable Contribution by Grid Zone", "Generation / consumption (%)", "renewable")
    st.caption("Generation relative to consumption can exceed 100%. The reference line alone does not indicate an alert; the API also checks the active period.")
    st.subheader("Key operational insights")
    columns = st.columns(4)
    facts = [("Highest consumption", "total_consumption_kwh", max, "kWh"),
             ("Highest grid import", "total_grid_import_kwh", max, "kWh"),
             ("Lowest renewable contribution", "renewable_contribution_pct", min, "%"),
             ("Highest renewable contribution", "renewable_contribution_pct", max, "%")]
    for column, (label, field, select, unit) in zip(columns, facts):
        row = select(zones, key=lambda r: r[field])
        with column:
            st.markdown(f"**{label}**")
            st.write(f"{row['grid_zone']} · {row[field]:,.2f} {unit}")

st.subheader("Renewable alerts")
if alert_result is None:
    st.info("Alert status is unavailable; this does not mean there are no alerts.")
elif not alert_result["alerts"]:
    st.success("No low-renewable alerts for the current active period in the latest stored data.")
else:
    for alert in alert_result["alerts"]:
        st.warning(f"{alert['grid_zone']}: renewable contribution is {alert['renewable_contribution_pct']:.1f}%, below the {alert_result['threshold_pct']:g}% threshold. Simulated window: {alert['window_end']}.")
st.caption("Alert decisions come from FastAPI using its configured daylight period and stored simulated window times. Nighttime low solar is ignored.")

st.divider()
st.subheader("Household billing analysis")
st.caption("Simulated estimated household bills in LKR, not actual utility revenue. Tariff-tier averages also reflect differences in grid usage.")
billing_date = st.date_input("Billing date", value=date(2026, 1, 1))
bills = load("get_daily_billing", billing_date)
if bills:
    total = sum(float(row["estimated_bill_lkr"]) for row in bills)
    cards = st.columns(4)
    cards[0].metric("Total estimated billing", f"LKR {total:,.2f}")
    cards[1].metric("Average household bill", f"LKR {total / len(bills):,.2f}")
    cards[2].metric("Daily grid import", f"{sum(float(r['total_grid_import_kwh']) for r in bills):,.2f} kWh")
    cards[3].metric("Households", len(bills))
    ordered = sorted(bills, key=lambda r: float(r["estimated_bill_lkr"]), reverse=True)
    tiers = defaultdict(list)
    for row in bills:
        tiers[row["billing_tier"]].append(float(row["estimated_bill_lkr"]))
    left, right = st.columns([3, 2])
    with left:
        figure = go.Figure(go.Bar(x=[r["household_id"] for r in ordered], y=[r["estimated_bill_lkr"] for r in ordered], marker_color="#2563eb"))
        chart(figure, "Estimated Household Bills", "Estimated bill (LKR)", "bills")
    with right:
        figure = go.Figure(go.Bar(x=list(tiers), y=[sum(values) / len(values) for values in tiers.values()], marker_color="#64748b"))
        chart(figure, "Average Household Bill by Tariff Tier", "Average bill (LKR)", "tiers")
    labels = {"household_id": "Household", "grid_zone": "Zone",
              "total_consumption_kwh": "Consumption (kWh)", "total_solar_generation_kwh": "Solar generation (kWh)",
              "total_grid_import_kwh": "Grid import (kWh)", "billing_tier": "Tariff tier",
              "tariff_rate": "Rate (LKR/kWh)", "estimated_bill_lkr": "Estimated bill (LKR)"}
    table = [{label: round(float(row[field]), 2) if field in ENERGY or field in ("tariff_rate", "estimated_bill_lkr")
              else row[field] for field, label in labels.items()} for row in bills]
    st.dataframe(table, hide_index=True, width="stretch")
else:
    st.info("Billing data is unavailable." if bills is None else f"No billing data found for {billing_date}. Choose a date with finalized energy and tariff data.")
