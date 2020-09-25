import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.graph_objects as go
import pandas as pd
import os


from geopy.geocoders import Nominatim
geolocator = Nominatim(user_agent="plotly")

route_traffic_mapping_path = r'C:\Users\oscartmc\PycharmProjects\dash-app\mapping\ultimate_geolocation_mapping_traffic.csv'
route_traffic_mapping = pd.read_csv(route_traffic_mapping_path).drop(columns=['REGION', 'ROAD_TYPE'])
mapping_path_weather_temperature = r'C:\Users\oscartmc\PycharmProjects\dash-app\mapping\geolocation_mapping_temperature.csv'
mapping_df_weather_temperature = pd.read_csv(mapping_path_weather_temperature)


def get_temperature_latest_data() -> pd.DataFrame:
    df_temperature = pd.read_csv(
        r'C:\Users\oscartmc\PycharmProjects\hkgov-public-data-pipeline\data\02_intermediate'
        r'\wth_current_weather_report_temperature\temperature.csv')
    df_temperature['updateTime'] = pd.to_datetime(df_temperature['updateTime'], format="%Y%m%d%H%M")
    df_temperature_latest = df_temperature[df_temperature.updateTime == max(df_temperature.updateTime)]

    df_temperature_latest = pd.merge(
        df_temperature_latest, mapping_df_weather_temperature,
        left_on="place", right_on="place", how="left"
    ).drop(columns=['location', 'unit']).astype({"updateTime": "str"})

    return df_temperature_latest


def get_traffic_latest_data() -> pd.DataFrame:

    # find latest traffic CSV in path
    storage_path = r'C:\Users\oscartmc\PycharmProjects\public-data-scripts\data\csv\traffic'
    traffic_df = pd.read_csv(
        os.path.join(storage_path, max([x for x in os.listdir(storage_path) if not x.endswith("pkl")])))
    traffic_df[['starting_node', 'ending_node']] = traffic_df['LINK_ID'].str.split("-", expand=True)

    ultimate_traffic_df = pd.merge(traffic_df, route_traffic_mapping, on="LINK_ID", how="left").query("LINK_ID!='6111-6112'")
    ultimate_traffic_df['linking_word'] = r'to'

    return ultimate_traffic_df


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(
    __name__,
    external_stylesheets=external_stylesheets,
)

app.layout = html.Div(
    html.Div([
        html.H4('HK weather and traffic live update'),
        html.Div(id='live-update-text'),
        dcc.Graph(id='live-update-graph'),
        dcc.Interval(
            id='interval-component',
            interval=60*1000,  # sec that update the graph
            n_intervals=0
        )
    ])
)


@app.callback(Output('live-update-text', 'children'), [Input('interval-component', 'n_intervals')])
def update_metrics(n):

    df_weather = get_temperature_latest_data()

    mean_weather = round(df_weather.value.mean(), 3)
    update_time = df_weather.updateTime.unique()[0]

    max_temp = {
        "place": df_weather[df_weather.value == df_weather.value.max()].place.iloc[0],
        "temp": df_weather[df_weather.value == df_weather.value.max()].value.iloc[0]
    }
    min_temp = {
        "place": df_weather[df_weather.value == df_weather.value.min()].place.iloc[0],
        "temp": df_weather[df_weather.value == df_weather.value.min()].value.iloc[0]
    }

    style = {'padding': '5px', 'fontSize': '16px'}
    return [
        html.Span(f'Update Time: {update_time}', style=style),
        html.Span(f'Mean Temperature: {mean_weather}', style=style),
        html.Span(f'Highest Temperature: {max_temp["place"]} {max_temp["temp"]}*C', style=style),
        html.Span(f'Lowest Temperature: {min_temp["place"]} {min_temp["temp"]}*C', style=style),
    ]


# Multiple components can update everytime interval gets fired.
@app.callback(Output('live-update-graph', 'figure'), [Input('interval-component', 'n_intervals')])
def update_graph_live(n):

    df_weather = get_temperature_latest_data()
    df_traffic = get_traffic_latest_data()

    fig = go.Figure()

    # traffic trace
    traffic_trace = []
    gb = df_traffic.groupby(['ROAD_SATURATION_LEVEL', 'REGION'])
    for x in gb.groups:
        mini_df = gb.get_group(x)

        if "GOOD" in mini_df['ROAD_SATURATION_LEVEL'].unique()[0]:
            color = "green"
        elif "AVERAGE" in mini_df['ROAD_SATURATION_LEVEL'].unique()[0]:
            color = "orange"
        elif "BAD" in mini_df['ROAD_SATURATION_LEVEL'].unique()[0]:
            color = "red"
        else:
            color = "black"

        traffic_trace.append(
            go.Scattermapbox(
                mode="markers+lines",
                lat=eval(
                    "[" + ", None ,".join(
                        [x.replace("[", "").replace("]", "") for x in mini_df['detailed_route_lat'].map(str).to_list()]
                    ) + "]"
                ),
                lon=eval(
                    "[" + ", None ,".join(
                        [x.replace("[", "").replace("]", "") for x in mini_df['detailed_route_long'].map(str).to_list()]
                    ) + "]"
                ),
                text=mini_df['full_location_chin_start'] + mini_df['linking_word'] + mini_df['full_location_chin_end'],
                hoverinfo='text',
                marker={
                    'size': 1,
                    'color': color
                }
            )
        )

    # weather trace
    weather_trace = [go.Scattermapbox(
        lat=df_weather['lat'],
        lon=df_weather['long'],
        mode='markers',
        marker=go.scattermapbox.Marker(
            size=10,
            color=df_weather['value'],
            colorscale='RdYlGn',
            cmin=10,
            cmid=25,
            cmax=40,
            reversescale=True,
            showscale=True,
        ),
        text=df_weather['place'] + df_weather['value'].apply(lambda x: f", {x}*C").astype("str"),
        hoverinfo='text',
    )]

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

    return fig


if __name__ == '__main__':
    app.run_server(debug=True)
