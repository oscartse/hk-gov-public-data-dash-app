import pandas as pd
import plotly.graph_objects as go
from geopy.geocoders import Nominatim
import os
pd.set_option('display.width', 500)
pd.set_option('display.max_columns', 15)
geolocator = Nominatim(user_agent="plotly")


# read temperature csv
df_temperature = pd.read_csv(
    r'C:\Users\oscartmc\PycharmProjects\hkgov-public-data-pipeline\data\02_intermediate'
    r'\wth_current_weather_report_temperature\temperature.csv')
df_temperature['updateTime'] = pd.to_datetime(df_temperature['updateTime'], format="%Y%m%d%H%M")


'''Do it first time for obtaining mapping info'''
location = [
    {"place": x, "location": geolocator.geocode(x + ", Hong Kong")}
    if x not in ('Tsuen Wan Ho Koon', 'Tsuen Wan Shing Mun Valley')
    else {"place": x, "location": geolocator.geocode(x)}
    for x in df_temperature['place'].unique()
]
for i in location:
    i.update({"address": i['location'].address, "lat": i['location'].latitude, "long": i['location'].longitude})


# joining temperature df and mapping information(lat, long)
df_temperature = pd.merge(
    df_temperature, pd.DataFrame(location),
    left_on="place", right_on="place", how="left"
).drop(columns=['location', 'unit']).astype({"updateTime": "str"})


# obtain ONLY latest data
df_temperature_latest = df_temperature[df_temperature.updateTime == max(df_temperature.updateTime)]


# temperature trace
weather_trace = [go.Scattermapbox(
    lat=df_temperature_latest['lat'],
    lon=df_temperature_latest['long'],
    mode='markers',
    marker=go.scattermapbox.Marker(
        size=3,
        color=df_temperature_latest['value'],
        colorscale='YlOrRd',
        # reversescale=True,
        showscale=True,
    ),
    text=df_temperature_latest['place'] + df_temperature_latest['value'].apply(lambda x: f", {x}*C").astype("str"),
    hoverinfo='text',
)]


# traffic mapping
gov_mapping_path = r'C:\Users\oscartmc\PycharmProjects\hkgov-public-data-pipeline\geolocation_mapping.csv'
mapping_df = pd.read_csv(gov_mapping_path).astype({"node": "str"}).drop(columns=['easting', 'northing'])


# find latest traffic CSV in path
storage_path = r'C:\Users\oscartmc\PycharmProjects\public-data-scripts\data\csv\traffic'
traffic_df = pd.read_csv(
    os.path.join(storage_path, max([x for x in os.listdir(storage_path) if not x.endswith("pkl")]))
)
traffic_df[['starting_node', 'ending_node']] = traffic_df['LINK_ID'].str.split("-", expand=True)


# merge mapping(lat, long, location_eng, location_chin) to latest traffic data
merged_df = pd.merge(
    pd.merge(
        traffic_df,
        mapping_df.rename(columns=dict(zip(mapping_df.columns, [x + "_start" for x in mapping_df.columns]))),
        left_on="starting_node", right_on='node_start', how='left'
    ),
    mapping_df.rename(columns=dict(zip(mapping_df.columns, [x + "_end" for x in mapping_df.columns]))),
    how='left', left_on="ending_node", right_on='node_end'
).drop(columns=['ending_node', 'starting_node'])


# remove ugly "straight line" data in graph
merged_df = merged_df.query("LINK_ID != '6111-6112'")


# adding new columns for better hovertext
merged_df['non_col'] = None
merged_df['linking_word'] = r"to"
merged_df['full_loc_eng_start'] = merged_df['location_lv1_eng_start']+merged_df['location_lv2_eng_start']\
                                  +merged_df['location_lv3_eng_start']+merged_df['location_lv4_eng_start']\
                                  +merged_df['location_lv5_eng_start']
merged_df['full_loc_eng_end'] = merged_df['location_lv1_eng_end']+merged_df['location_lv2_eng_end']\
                                +merged_df['location_lv3_eng_end']+merged_df['location_lv4_eng_end']\
                                +merged_df['location_lv5_eng_end']
merged_df['full_loc_chin_start'] = merged_df['location_lv5_chin_start']+merged_df['location_lv4_chin_start']\
                                   +merged_df['location_lv3_chin_start']+merged_df['location_lv2_chin_start']\
                                   +merged_df['location_lv1_chin_start']
merged_df['full_loc_chin_end'] = merged_df['location_lv5_chin_end']+merged_df['location_lv4_chin_end']\
                                 +merged_df['location_lv3_chin_end']+merged_df['location_lv2_chin_end']\
                                 +merged_df['location_lv1_chin_end']
merged_df['route_lat'] = merged_df[['wgsLat_start', 'wgsLat_end']].values.tolist()
merged_df['route_long'] = merged_df[['wgsLong_start', 'wgsLong_end']].values.tolist()


# init for figure
fig = go.Figure()


# traffic trace
gb = merged_df.groupby(['ROAD_SATURATION_LEVEL', 'REGION'])

traffic_trace = []
for x in gb.groups:
    mini_df = gb.get_group(x)
    lat_mini = mini_df[['wgsLat_start', 'wgsLat_end', 'non_col']].values.tolist()
    long_mini = mini_df[['wgsLong_start', 'wgsLong_end', 'non_col']].values.tolist()

    if "GOOD" in mini_df['ROAD_SATURATION_LEVEL'].unique()[0]:
        color = "green"
    if "AVERAGE" in mini_df['ROAD_SATURATION_LEVEL'].unique()[0]:
        color = "orange"
    if "BAD" in mini_df['ROAD_SATURATION_LEVEL'].unique()[0]:
        color = "red"
    else:
        color = "black"

    traffic_trace.append(
        go.Scattermapbox(
            mode="lines",
            lat=[y for x in lat_mini for y in x],
            lon=[y for x in long_mini for y in x],
            text=mini_df['location_lv3_chin_start'] + mini_df['location_lv2_chin_start'] + mini_df[
                'location_lv1_chin_start'],
            hoverinfo='text',
            marker={
                'size': 1,
                'color': color
            }
        )
    )


# add trace at once
for i in traffic_trace + weather_trace:
    fig.add_trace(i)


# figure layout
fig.update_layout(
    showlegend=False,
    height=700,
    width=1000,
    mapbox={
        'center': {'lat': 22.344044, 'lon': 114.100998},
        'style': "light",
        'zoom': 9.8,
        'accesstoken': "pk.eyJ1Ijoib3NjYXJ0bWMiLCJhIjoiY2tmYzdtc29nMDJjdDJzbm9wdzV4Nzg3aSJ9.zrp0XoINM_jtQqz5j0f-cA"
    }
)

fig.show()
