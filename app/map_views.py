import pandas as pd
import plotly.express as px


REGION_COORDS = {
    "Greater Accra": {"lat": 5.6037, "lon": -0.1870},
    "Ashanti": {"lat": 6.6885, "lon": -1.6244},
    "Western": {"lat": 4.8961, "lon": -1.7831},
    "Central": {"lat": 5.1053, "lon": -1.2466},
    "Eastern": {"lat": 6.0941, "lon": -0.2591},
    "Volta": {"lat": 6.6000, "lon": 0.4700},
    "Northern": {"lat": 9.4008, "lon": -0.8393},
    "Upper East": {"lat": 10.7856, "lon": -0.8514},
    "Upper West": {"lat": 10.0601, "lon": -2.5099},
    "Bono": {"lat": 7.6500, "lon": -2.5000},
    "Bono East": {"lat": 7.7500, "lon": -1.0500},
    "Ahafo": {"lat": 7.0000, "lon": -2.5000},
    "Oti": {"lat": 7.9000, "lon": 0.3000},
    "North East": {"lat": 10.5167, "lon": -0.3667},
    "Savannah": {"lat": 9.0833, "lon": -1.8167},
    "Western North": {"lat": 6.3000, "lon": -2.8000},
}


def build_region_metric_map(region_rows, metric="facility_count"):
    if not region_rows:
        return None

    frame = pd.DataFrame(region_rows)
    if "region" not in frame.columns or metric not in frame.columns:
        return None

    frame["lat"] = frame["region"].map(lambda r: REGION_COORDS.get(r, {}).get("lat"))
    frame["lon"] = frame["region"].map(lambda r: REGION_COORDS.get(r, {}).get("lon"))
    frame = frame.dropna(subset=["lat", "lon"])

    if frame.empty:
        return None

    fig = px.scatter_geo(
        frame,
        lat="lat",
        lon="lon",
        color=metric,
        size=metric,
        hover_name="region",
        projection="natural earth",
        title=f"Region metric: {metric}",
    )
    fig.update_layout(height=500, margin={"l": 0, "r": 0, "t": 40, "b": 0})
    return fig