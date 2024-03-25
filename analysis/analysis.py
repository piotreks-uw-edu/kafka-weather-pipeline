from azure_sql_database.params import AZURE_DATABASE_URL
from azure_sql_database.model import Database, Correlation, HighPollution, CapitalPollution, Distribution
import base64
from io import BytesIO
from matplotlib.figure import Figure
import folium
from folium.plugins import HeatMap
from flask import render_template_string


database = Database(AZURE_DATABASE_URL)


def get_correlations():
    with database.session() as session:
        correlations = session.query(Correlation).all()

    # Create mappings for weatherFactor and pollutionFactor to numerical values
    weather_factors = {factor: index for index, factor in enumerate(sorted(set(c.weatherFactor for c in correlations)))}
    pollution_factors = {factor: index for index, factor in enumerate(sorted(set(c.pollutionFactor for c in correlations)))}
    
    # Prepare data for plotting
    x_values = [weather_factors[c.weatherFactor] for c in correlations]
    y_values = [pollution_factors[c.pollutionFactor] for c in correlations]
    sizes = [abs(c.pearson) * 1000 for c in correlations]  # scale Pearson values for bubble size
    
    fig = Figure(figsize=(10, 6))
    ax = fig.subplots()
    scatter = ax.scatter(x_values, y_values, s=sizes, alpha=0.5)
    
    ax.set_title("Correlation Bubble Plot")
    ax.set_xlabel("Weather Factor")
    ax.set_ylabel("Pollution Factor")
    ax.grid(True)

    # Set the tick labels for better readability
    ax.set_xticks(range(len(weather_factors)))
    ax.set_xticklabels(list(weather_factors.keys()), rotation=45, ha="right")
    ax.set_yticks(range(len(pollution_factors)))
    ax.set_yticklabels(list(pollution_factors.keys()))

    buf = BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")

    data = base64.b64encode(buf.getbuffer()).decode("ascii")
    
    return f"<img src='data:image/png;base64,{data}'/>"


def get_high_pollution():
    with database.session() as session:
        high_pollution_data = session.query(HighPollution).all()
    
    countries = [data.country for data in high_pollution_data]
    pm10_levels = [data.PM10 for data in high_pollution_data]
    
    fig = Figure(figsize=(10, 6))
    ax = fig.subplots()
    ax.bar(countries, pm10_levels, color='blue')
    
    ax.set_title("PM10 Pollution Levels by Country")
    ax.set_xlabel("Country")
    ax.set_ylabel("PM10 Level")
    ax.set_xticklabels(countries, rotation=45, ha="right")
    
    for index, value in enumerate(pm10_levels):
        ax.text(index, value, str(value), ha='center', va='bottom')
    
    buf = BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    data = base64.b64encode(buf.getbuffer()).decode("ascii")
    
    return f"<img src='data:image/png;base64,{data}'/>"  


def get_capital_pollution():
    with database.session() as session:
        pollution_data = session.query(CapitalPollution).order_by(
            CapitalPollution.NO2.desc()).all()

    capitals = [data.capital for data in pollution_data]
    no2_levels = [data.NO2 for data in pollution_data]

    fig = Figure(figsize=(10, 8))
    ax = fig.subplots()
    ax.bar(capitals, no2_levels, color='purple')

    ax.set_title("NO2 Pollution Levels by Capital")
    ax.set_xlabel("Capital")
    ax.set_ylabel("NO2 Level")
    ax.set_xticklabels(capitals, rotation=45, ha="right")

    for index, value in enumerate(no2_levels):
        ax.text(index, value, f'{value:.2f}', ha='center', va='bottom')

    buf = BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    data = base64.b64encode(buf.getbuffer()).decode("ascii")

    return f"<img src='data:image/png;base64,{data}'/>"


def get_distribution():
    with database.session() as session:
        distributions = session.query(Distribution).all()

    data = [(d.lat_int, d.lon_int, d.avg_PM10) for d in distributions]

    base_map = folium.Map(location=[0, 0], zoom_start=2)

    folium.plugins.HeatMap(data).add_to(base_map)

    map_html = base_map._repr_html_()

    html_template = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Distribution Map</title>
        <meta charset="utf-8" />
    </head>
    <body>
        <h1>Distribution Heatmap</h1>
        {{map_html | safe}}
    </body>
    </html>
    """
    return render_template_string(html_template, map_html=map_html)
