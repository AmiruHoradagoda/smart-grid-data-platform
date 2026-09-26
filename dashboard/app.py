"""Business dashboard. Run: uv run streamlit run dashboard/app.py"""
from collections import defaultdict
from datetime import datetime, date

import plotly.graph_objects as go
import streamlit as st
import api_client

st.set_page_config(page_title="Smart Grid | Energy & Billing", page_icon="?", layout="wide")
st.markdown("""<style>
.block-container {max-width:1450px;padding-top:2rem;padding-bottom:2rem;}
div[data-testid="stMetric"] {border:1px solid #dce4ec;border-radius:12px;padding:18px;}
h1 {letter-spacing:-1px;}
</style>""", unsafe_allow_html=True)

FIELDS = {"total_consumption_kwh": ("Consumption", "#2563eb"),
          "total_solar_generation_kwh": ("Solar generation", "#10b981"),
          "total_grid_import_kwh": ("Grid import", "#f59e0b")}

@st.cache_data(ttl=15, show_spinner=False)
def fetch(resource, *args):
    return getattr(api_client, resource)(*args)

def load(resource, *args):
    try:
        return fetch(resource, *args)
    except api_client.APIError:
        st.warning("Data is temporarily unavailable. Please refresh or try again shortly.")
        return None

def timestamp(value):
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))

def percent(n, d):
    return 100 * n / d if d else 0.0

def style(fig, y):
    fig.update_layout(height=340, margin=dict(l=10,r=10,t=20,b=30),
                      yaxis_title=y, legend=dict(orientation="h", y=-0.2),
                      font=dict(size=13), template="plotly_white")
    return fig

latest = load("get_latest_zones")
latest_time = max((timestamp(r["window_end"]) for r in latest), default=None) if latest else None
heading, clock = st.columns([3, 1])
with heading:
    st.title("Energy & Billing")
    st.caption("SMART GRID OPERATIONS")
with clock:
    st.markdown("**Latest simulated reading**")
    st.markdown(f"### {latest_time:%d %b %Y}" if latest_time else "### Unavailable")
    st.caption(f"{latest_time:%H:%M} ? simulated time" if latest_time else "")

controls = st.columns([2, 3, 1])
with controls[0]:
    selected_date = st.date_input("Date", value=latest_time.date() if latest_time else date(2026,1,1))
with controls[2]:
    if st.button("Refresh", width="stretch"):
        fetch.clear()
        st.rerun()
history = load("get_zone_history", 1000, selected_date)
energy_tab, billing_tab = st.tabs(["Energy performance", "Daily billing"])
with energy_tab:
    if history:
        grouped = defaultdict(list)
        for row in history:
            grouped[timestamp(row["window_end"])].append(row)
        expected = {r["grid_zone"] for r in latest} if latest else {r["grid_zone"] for r in history}
        windows = {t: rows for t, rows in grouped.items() if {r["grid_zone"] for r in rows} == expected}
        times = sorted(windows)
        if len(history) == 1000:
            st.info("Showing the most recent 1,000 readings for this date.")
        if len(windows) != len(grouped):
            st.caption("Incomplete zone intervals are excluded from comparisons.")
        if not times:
            st.info("No complete energy intervals are available for this date.")
        else:
            with controls[1]:
                if len(times) > 1:
                    start, end = st.select_slider("Time range", options=times,
                        value=(times[0], times[-1]), format_func=lambda t: t.strftime("%H:%M"),
                        key=f"range-{selected_date}")
                else:
                    start = end = times[0]
            metric_area = st.container()
            st.subheader("Energy over time")
            fig = go.Figure()
            for field, (label, color) in FIELDS.items():
                fig.add_trace(go.Scatter(x=times, y=[sum(float(r[field]) for r in windows[t]) for t in times],
                    mode="lines+markers", marker=dict(size=3), name=label, line=dict(color=color,width=2)))
            fig.update_layout(dragmode="select", selectdirection="h", hovermode="x unified")
            fig.update_xaxes(title="Simulated time", tickformat="%H:%M")
            selection = st.plotly_chart(style(fig,"Energy per interval (kWh)"), width="stretch",
                key=f"trend-{selected_date}-{start}-{end}", on_select="rerun", selection_mode="box",
                config={"displaylogo":False,"modeBarButtonsToRemove":["lasso2d"]})
            points = selection.selection.points
            if points:
                chosen = [timestamp(p["x"]) for p in points]
                start, end = max(start,min(chosen)), min(end,max(chosen))
            selected_times = [t for t in times if start <= t <= end]
            if not selected_times:
                st.info("The chart selection is outside the chosen time range. Clear it or adjust the range.")
            else:
                rows = [r for t in selected_times for r in windows[t]]
                totals = {f:sum(float(r[f]) for r in rows) for f in FIELDS}
                with metric_area:
                    st.caption(f"{selected_date:%d %b %Y} | {selected_times[0]:%H:%M}?{selected_times[-1]:%H:%M} | {len(selected_times)} intervals")
                    cards=st.columns(5)
                    for card,(field,(label,_)) in zip(cards,FIELDS.items()):
                        card.metric(label,f"{totals[field]:,.2f} kWh")
                    cards[3].metric("Renewable contribution",f"{percent(totals['total_solar_generation_kwh'],totals['total_consumption_kwh']):.1f}%")
                    cards[4].metric("Grid dependency",f"{percent(totals['total_grid_import_kwh'],totals['total_consumption_kwh']):.1f}%")
                st.caption("Drag across the chart to inspect a period. Double-click to clear the selection. Values use interval end times.")
                by_zone=defaultdict(lambda: {f:0.0 for f in FIELDS})
                for row in rows:
                    for f in FIELDS: by_zone[row['grid_zone']][f]+=float(row[f])
                left,right=st.columns(2)
                with left:
                    st.subheader("Energy balance by zone")
                    fig=go.Figure()
                    for f,(label,color) in FIELDS.items():
                        fig.add_trace(go.Bar(x=list(by_zone),y=[v[f] for v in by_zone.values()],name=label,marker_color=color))
                    fig.update_layout(barmode="group")
                    st.plotly_chart(style(fig,"Energy (kWh)"),width="stretch")
                with right:
                    st.subheader("Grid dependency by zone")
                    fig=go.Figure(go.Bar(x=list(by_zone),y=[percent(v['total_grid_import_kwh'],v['total_consumption_kwh']) for v in by_zone.values()],marker_color="#64748b"))
                    st.plotly_chart(style(fig,"Grid import / consumption (%)"),width="stretch")
    elif history is not None:
        st.info("No energy readings are available for this date.")
    alerts=load("get_renewable_alerts")
    if alerts and alerts['alerts']:
        with st.expander(f"Latest renewable alerts ? {len(alerts['alerts'])}"):
            for alert in alerts['alerts']:
                st.warning(f"{alert['grid_zone']} ? {alert['renewable_contribution_pct']:.1f}% renewable contribution ? {alert['window_end']}")
            st.caption("Latest recorded alerts, independent of the historical selection.")

with billing_tab:
    st.subheader(f"Estimated billing ? {selected_date:%d %b %Y}")
    st.caption("Full-day household bills ? LKR ? Simulated tariffs. The energy chart selection does not change daily bills.")
    bills=load("get_daily_billing",selected_date)
    if bills:
        total=sum(float(r['estimated_bill_lkr']) for r in bills)
        cards=st.columns(3)
        cards[0].metric("Total estimated billing",f"LKR {total:,.2f}")
        cards[1].metric("Average household bill",f"LKR {total/len(bills):,.2f}")
        cards[2].metric("Households billed",len(bills))
        left,right=st.columns([3,2])
        with left:
            st.subheader("Household bills")
            ordered=sorted(bills,key=lambda r:float(r['estimated_bill_lkr']),reverse=True)
            fig=go.Figure(go.Bar(x=[r['household_id'] for r in ordered],y=[r['estimated_bill_lkr'] for r in ordered],marker_color="#2563eb"))
            st.plotly_chart(style(fig,"Estimated bill (LKR)"),width="stretch")
        with right:
            st.subheader("Average bill by tariff tier")
            tiers=defaultdict(list)
            for r in bills: tiers[r['billing_tier']].append(float(r['estimated_bill_lkr']))
            fig=go.Figure(go.Bar(x=list(tiers),y=[sum(v)/len(v) for v in tiers.values()],marker_color="#10b981"))
            st.plotly_chart(style(fig,"Average bill (LKR)"),width="stretch")
        with st.expander("Household details"):
            labels={'household_id':'Household','grid_zone':'Zone','total_consumption_kwh':'Consumption (kWh)',
                    'total_solar_generation_kwh':'Solar (kWh)','total_grid_import_kwh':'Grid import (kWh)',
                    'billing_tier':'Tariff tier','tariff_rate':'Rate (LKR/kWh)','estimated_bill_lkr':'Estimated bill (LKR)'}
            st.dataframe([{label:round(float(r[f]),2) if f in FIELDS or f in ('tariff_rate','estimated_bill_lkr') else r[f]
                           for f,label in labels.items()} for r in bills],hide_index=True,width="stretch")
    elif bills is not None:
        st.info("No finalized bills are available for this date.")
