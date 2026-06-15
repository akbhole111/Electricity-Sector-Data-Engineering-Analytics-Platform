"""
visualization_task4.py

A real-time dashboard built with Dash and Plotly, connected to an MQTT broker,vto visualize electricity facility data, including total power, emissions,
and market price/demand, with interactive filtering capabilities.

"""

import pandas as pd
import plotly.graph_objects as go
import json
import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output
import paho.mqtt.client as mqtt
from datetime import datetime
import sys
import warnings
warnings.filterwarnings("ignore")

# Fix Windows console encoding
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')

# --- CONFIGURATION ---
MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883
MQTT_TOPIC = "comp5339/electricity/data" # NOTE: The topic should ideally match the publisher's topic: comp5339/electricity/data
MQTT_CLIENT_ID = "task4_dash"

# --- MAPPING TABLES ---
# Map fuel technology names to user-friendly names
ENERGY_TYPE_MAP = {
    "coal_brown": "Brown Coal",
    "coal_black": "Black Coal",
    "gas_ccgt": "Gas (Combined Cycle Gas Turbine)",
    "gas_ocgt": "Gas (Open-Cycle Gas Turbine)",
    "gas_steam": "Steam Gas",
    "hydro": "Hydroelectric",
    "wind": "Wind",
    "solar_utility": "Solar",
    "battery": "Battery Storage",
    "battery_discharging": "Battery Discharge",
    "hydro, pumps": "Pumped Hydro",
    "distillate": "Distillate Fuel",
}

# Map network region codes to full names for display
NETWORK_REGION_MAP = {
    "NSW1": "New South Wales (NSW)",
    "VIC1": "Victoria (VIC)",
    "QLD1": "Queensland (QLD)",
    "SA1": "South Australia (SA)",
    "TAS1": "Tasmania (TAS)",
}

# --- GLOBAL DATA STORE ---
facilities_data = {}
message_count = 0
last_update_time = None 
market_price = None
market_demand = None

# --- MQTT CALLBACKS ---
def on_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        print(f"Connected to MQTT broker at {datetime.now().strftime('%H:%M:%S')}")
        client.subscribe(MQTT_TOPIC, qos=1)
        print(f"Subscribed to topic: {MQTT_TOPIC}")
    else:
        print(f"Connection failed with code {reason_code}")

def on_message(client, userdata, msg):
    global facilities_data, message_count, last_update_time, market_price, market_demand
    try:
        data = json.loads(msg.payload.decode('utf-8'))
        facility_code = data.get('facility_code')
        
        # Capture Market Price and Demand (System-wide KPIs)
        if 'price' in data and data['price'] is not None:
            market_price = float(data['price'])
        if 'demand' in data and data['demand'] is not None:
            market_demand = float(data['demand'])
        
        # Apply mapping here for immediate data processing/storage
        raw_energy_type = data.get('energy_type', 'other')
        raw_network_region = data.get('network_region', 'Unknown')
        
        facilities_data[facility_code] = {
            'facility_code': facility_code,
            'name': data.get('facility_name', 'Unknown'),
            'network_region_raw': raw_network_region,
            'network_region': NETWORK_REGION_MAP.get(raw_network_region, raw_network_region), # Mapped value
            'energy_type_raw': raw_energy_type,
            'energy_type': ENERGY_TYPE_MAP.get(raw_energy_type, raw_energy_type.replace('_', ' ').title()), # Mapped value
            'power': float(data.get('power', 0.0) or 0.0), # Ensure power is float, handling None or empty string
            'emissions': float(data.get('emissions', 0.0) or 0.0), # Ensure emissions is float
            'latitude': data.get('latitude'),
            'longitude': data.get('longitude'),
            # STORES THE SIMULATED TIMESTAMP
            'timestamp': data.get('timestamp'), 
            # STORES THE REAL TIME (to show pipeline really works)
            'last_updated_real': datetime.now() 
        }
        message_count += 1
        # Update GLOBAL real time tracker for the KPI box
        last_update_time = datetime.now()
        
    except Exception as e:
        print(f"Processing MQTT message: {e}")

def on_disconnect(client, userdata, disconnect_flags, reason_code, properties=None):
    print(f"Disconnected from MQTT broker")

# --- MQTT CLIENT SETUP ---
def start_mqtt_client():
    client = mqtt.Client(client_id=MQTT_CLIENT_ID, callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
        client.loop_start()
        print(f"MQTT client started in background thread")
        return client
    except Exception as e:
        print(f"Failed to connect to MQTT broker: {e}")
        return None

mqtt_client = start_mqtt_client()

# --- DASH APP ---
app = dash.Dash(__name__, title="Real-Time Electricity Dashboard")

# --- CUSTOM STYLES ---

# Style for the KPI boxes
KPI_BOX_STYLE = {
    'textAlign': 'center',
    'padding': '10px 0',
    'backgroundColor': 'white',
    'borderRadius': '12px',
    'boxShadow': '0 4px 6px rgba(0,0,0,0.05), 0 1px 3px rgba(0,0,0,0.02)',
    'height': '100%',
    'display': 'flex',
    'flexDirection': 'column',
    'justifyContent': 'center',
    'minWidth': '0',
}

KPI_TITLE_STYLE = {'fontSize': '14px', 'color': '#555', 'marginBottom': '5px', 'fontWeight': 'normal'}
KPI_VALUE_STYLE = {'fontSize': '24px', 'fontWeight': 'bold'}

# Style for the new centered filter row container
FILTER_ROW_CONTAINER_STYLE = {
    'display': 'flex',
    'justifyContent': 'center',
    'marginBottom': '20px',
    'marginTop': '100px', 
    'maxWidth': '1500px', 
    'marginRight': 'auto',
    'marginLeft': 'auto',
    'padding': '0 20px',
}

# --- LAYOUT ---
app.layout = html.Div(style={'backgroundColor': '#f0f2f5', 'padding': '20px', 'fontFamily': 'Arial, sans-serif'}, children=[
    # TITLE
    html.H1("⚡ Real-Time Australian Electricity Grid Monitor ⚡", style={'textAlign': 'center', 'marginBottom': '20px'}),
    html.P("MQTT Data Stream - (6th October, 2025 - 13th October, 2025) COMP5339 Assignment 2", style={'textAlign': 'center', 'color': '#666'}),
    
    # --- 1. KPI ROW (Top Row) ---
    html.Div([
        # MQTT Status
        html.Div([html.H4("MQTT Status", style=KPI_TITLE_STYLE),
                  html.P(id='mqtt-status', children="🔴 Disconnected", style=KPI_VALUE_STYLE)], 
                 style=KPI_BOX_STYLE),

        # Active Facilities
        html.Div([html.H4("Active Facilities", style=KPI_TITLE_STYLE),
                  html.P(id='facility-count', children="0", style=KPI_VALUE_STYLE)], 
                 style=KPI_BOX_STYLE),

        # Messages Received
        html.Div([html.H4("Messages Received", style=KPI_TITLE_STYLE),
                  html.P(id='message-count', children="0", style=KPI_VALUE_STYLE)], 
                 style=KPI_BOX_STYLE),
                 
        # Market Price
        html.Div([html.H4("Market Price ($/MWh)", style=KPI_TITLE_STYLE),
                  html.P(id='market-price', children="N/A", style=KPI_VALUE_STYLE | {'color': '#2ca02c'})], 
                 style=KPI_BOX_STYLE),

        # Market Demand
        html.Div([html.H4("Market Demand (MW)", style=KPI_TITLE_STYLE),
                  html.P(id='market-demand', children="N/A", style=KPI_VALUE_STYLE | {'color': '#ff7f0e'})], 
                 style=KPI_BOX_STYLE),
                 
        # Total Power (MW)
        html.Div([html.H4("Total Power (MW)", style=KPI_TITLE_STYLE),
                  # Output is now visible here
                  html.P(id='total-power-kpi', children="0.00", style=KPI_VALUE_STYLE | {'color': '#1f77b4'})], 
                 style=KPI_BOX_STYLE),
                 
        # Total Emissions (t CO₂)
        html.Div([html.H4("Total Emissions (t CO₂)", style=KPI_TITLE_STYLE),
                  # Output is now visible here
                  html.P(id='total-emissions-kpi', children="0.00", style=KPI_VALUE_STYLE | {'color': '#d62728'})], 
                 style=KPI_BOX_STYLE),
                 
        # Last Update (Real-Time)
        html.Div([html.H4("Last Update (Real Time)", style=KPI_TITLE_STYLE),
                  html.P(id='last-update', children="N/A", style=KPI_VALUE_STYLE)], 
                 style=KPI_BOX_STYLE),

    ], style={
        'display': 'grid',
        'gridTemplateColumns': 'repeat(8, 1fr)', # 8 equal columns
        'gap': '10px',
        'height': '70px'
    }, className='kpi-row'),
    
    # --- 2. CENTERED FILTER ROW ---
    html.Div(style=FILTER_ROW_CONTAINER_STYLE, children=[
        # FILTERS ROW CONTENT
        html.Div(style={'display': 'flex', 'alignItems': 'center', 'width': '100%', 'padding': '10px', 'backgroundColor': 'white', 'borderRadius': '8px', 'boxShadow': '0 2px 4px rgba(0,0,0,0.05)', 'flexWrap': 'wrap', 'gap': '10px'}, children=[
            
            # Filter By Station Name (Input)
            html.Div(style={'flex': '1 1 150px'}, children=[
                dcc.Input(
                    id='station-search',
                    type='text',
                    placeholder='Filter By Station Name...',
                    style={'width': '85%', 'padding': '8px', 'border': '1px solid #ccc', 'borderRadius': '4px'}
                )
            ]),
            
            # Technology (Energy Type) 
            html.Div(style={'flex': '0 1 150px', 'minWidth': '300px'}, children=[
                dcc.Dropdown(
                    id='type-filter', 
                    multi=True, 
                    placeholder="Technology",
                    style={'minWidth': '300px', 'zIndex': 9999},
                    persistence=True
                )
            ]),
            
            # Region Dropdown 
            html.Div(style={'flex': '0 1 120px', 'minWidth': '200px'}, children=[
                dcc.Dropdown(
                    id='region-filter', 
                    multi=True, 
                    placeholder="Region",
                    style={'minWidth': '200px', 'zIndex': 9998},
                    persistence=True
                )
            ]),
            
            # Power/Emissions 
            html.Div(style={'flex': '0 1 150px', 'minWidth': '150px'}, children=[
                dcc.RadioItems(
                    id='display-mode',
                    options=[
                        {'label': ' Power (MW)', 'value': 'power'},
                        {'label': ' Emissions (t CO₂)', 'value': 'emissions'}
                    ],
                    value='power',
                    inline=False,
                    style={'fontSize': '12px', 'padding': '5px 0'}
                )
            ]),
            
        ])
    ]),


    # --- 3. MAIN CONTENT ROW ---
    html.Div([
        # LEFT COLUMN (Data Table)
        html.Div(style={'flex': '4', 'display': 'flex', 'flexDirection': 'column'}, children=[
            # Facility Data Table (Main Content)
            html.Div(id='facility-table-container', style={'flexGrow': '1', 'overflowY': 'auto', 'backgroundColor': 'white', 'padding': '10px', 'borderRadius': '8px', 'boxShadow': '0 2px 4px rgba(0,0,0,0.05)'}),
            
            # KPI FOOTER 
            html.Div(style={'display': 'flex', 'justifyContent': 'space-between', 'alignItems': 'center', 'marginTop': '10px', 'padding': '10px', 'backgroundColor': '#2c3e50', 'color': 'white', 'borderRadius': '4px'}, children=[
                html.P(id='total-facilities-text', children='Facilities: 0', style={'margin': '0', 'fontSize': '14px'}),
                html.P(id='total-power-text-footer', children='Total Power: 0.00 MW', style={'margin': '0', 'fontSize': '14px'}),
                html.P(id='total-emissions-text-footer', children='Total Emissions: 0.00 t CO₂', style={'margin': '0', 'fontSize': '14px'}),
            ])
        ], className='left-column-container'),

        # RIGHT COLUMN (Map)
        html.Div(style={'flex': '6', 'marginLeft': '20px'}, children=[ # Added marginLeft to create space between table and map
            # Ensure map fills its container
            html.Div(style={'height': '100%', 'backgroundColor': 'white', 'borderRadius': '8px', 'boxShadow': '0 2px 4px rgba(0,0,0,0.05)'}, children=[
                dcc.Graph(id='facility-map', style={'height': '100%'})
            ])
        ])
    ], style={'display': 'flex', 'height': '750px'}, className='main-content-row'), 
    
    # Hidden components for required callback outputs
    html.Div(style={'display': 'none'}, children=[
        html.P(id='total-power'),
        html.P(id='total-emissions')
    ]),
    
    dcc.Interval(id='interval-component', interval=2*1000, n_intervals=0)
])

# --- HELPER FUNCTION ---
def format_simulated_timestamp(iso_str):
    """Converts ISO timestamp string from data payload to DD-MM-YYYY and HH:MM:SS formats, 
    returning them as a tuple (date_str, time_str).
    """
    try:
        dt = datetime.fromisoformat(iso_str.replace('Z', '+00:00'))
        date_str = dt.strftime('%d-%m-%Y') # DD-MM-YYYY
        time_str = dt.strftime('%H:%M:%S') # HH:MM:SS
        return date_str, time_str
    except Exception:
        # Return two 'N/A' values if parsing fails
        return "N/A", "N/A"

# --- CALLBACK ---
@app.callback(
    [Output('mqtt-status', 'children'),
     Output('facility-count', 'children'),
     Output('message-count', 'children'),
     Output('market-price', 'children'),
     Output('market-demand', 'children'),
     # Updated KPI outputs to the visible IDs
     Output('total-power-kpi', 'children'), 
     Output('total-emissions-kpi', 'children'),
     Output('last-update', 'children'),
     
     # Footer outputs
     Output('total-facilities-text', 'children'),
     Output('total-power-text-footer', 'children'),
     Output('total-emissions-text-footer', 'children'),
     
     Output('type-filter', 'options'),
     Output('region-filter', 'options'),
     Output('facility-map', 'figure'),
     Output('facility-table-container', 'children')],
    [Input('interval-component', 'n_intervals'),
     Input('type-filter', 'value'),
     Input('region-filter', 'value'),
     Input('display-mode', 'value'),
     Input('station-search', 'value')]
)
def update_dashboard(n, selected_types, selected_regions, display_mode, search_term):
    global last_update_time
    
    # --- Status/KPI Updates ---
    mqtt_status = "🟢 Connected" if mqtt_client and mqtt_client.is_connected() else "🔴 Disconnected"
    facility_count = f"{len(facilities_data):,}"
    msg_count = f"{message_count:,}"
    last_update = last_update_time.strftime("%H:%M:%S") if last_update_time else "N/A"
    
    # Format Market Price and Demand
    market_price_str = f"${market_price:,.2f}" if market_price is not None else "N/A"
    market_demand_str = f"{market_demand:,.0f}" if market_demand is not None else "N/A"

    # Calculate Totals (Fixed: now guaranteed to be calculated even if 0)
    total_power = sum(f.get('power', 0.0) for f in facilities_data.values())
    total_emissions = sum(f.get('emissions', 0.0) for f in facilities_data.values())
    
    # KPI text for the top row
    total_power_kpi_str = f"{total_power:,.2f}"
    total_emissions_kpi_str = f"{total_emissions:,.2f}"
    
    # Format Footer text
    total_facilities_text = f"Facilities: {facility_count}"
    total_power_text_footer = f"Total Power: {total_power_kpi_str} MW"
    total_emissions_text_footer = f"Total Emissions: {total_emissions_kpi_str} t CO₂"

    if facilities_data:
        all_types = sorted(set(f['energy_type'] for f in facilities_data.values()))
        all_regions = sorted(set(f['network_region'] for f in facilities_data.values()))
    else:
        all_types, all_regions = [], []

    type_options = [{'label': t, 'value': t} for t in all_types]
    region_options = [{'label': r, 'value': r} for r in all_regions]

    # --- Filtering Logic ---
    filtered_data = facilities_data.copy()
    
    # 1. Filter by Technology/Type
    if selected_types:
        filtered_data = {k: v for k, v in filtered_data.items() if v['energy_type'] in selected_types}
    
    # 2. Filter by Region
    if selected_regions:
        filtered_data = {k: v for k, v in filtered_data.items() if v['network_region'] in selected_regions}
        
    # 3. Filter by Search Term (Station Name)
    if search_term:
        search_term_lower = search_term.lower()
        filtered_data = {
            k: v for k, v in filtered_data.items() 
            if search_term_lower in v['name'].lower()
        }

    # Remove facilities without lat/lon for map/table data frame creation
    filtered_data_plottable = {k: v for k, v in filtered_data.items() if v['latitude'] and v['longitude']}


    # --- Map and Table Generation ---
    if filtered_data_plottable:
        df_map = pd.DataFrame(filtered_data_plottable.values())
        value_col = display_mode
        value_label = "Power (MW)" if value_col == 'power' else "Emissions (t CO₂)"
        if value_col == 'power':
            color_scale = [
                [0.0, "#EED9B7"],  # From light beige (less power) to dark brown colour (more power)
                [0.25, "#D2B48C"],
                [0.5, "#A67C52"],
                [0.75, "#704214"],
                [1.0, "#3E1F00"]
            ]
        else:
            color_scale = [
                [0.0, "#FFC4C4"],  # From light pink (less emissions) to dark red colour (more emissions)
                [0.25, "#FF8A8A"],
                [0.5, "#E34234"],
                [0.75, "#B22222"],
                [1.0, "#660000"]
            ]


        # Apply timestamp formatting
        df_map[['simulated_date', 'simulated_time']] = df_map['timestamp'].apply(
            lambda x: pd.Series(format_simulated_timestamp(x))
        )
        
        # Hover Text
        df_map['hover_text'] = df_map.apply(
            lambda r: f"<b>{r['name']}</b><br>"
                      f"Code: {r['facility_code']}<br>"
                      f"Type: {r['energy_type']}<br>" 
                      f"Region: {r['network_region']}<br>" 
                      f"Power: {r['power']:.2f} MW<br>"
                      f"Emissions: {r['emissions']:.2f} t CO₂<br>"
                      f"Date: {r['simulated_date']}<br>"
                      f"Time: {r['simulated_time']}", 
            axis=1
        )

        # Map Figure
        fig = go.Figure(go.Scattermapbox(
            lat=df_map['latitude'],
            lon=df_map['longitude'],
            mode='markers', # Removed text label for a cleaner look on the map
            marker=go.scattermapbox.Marker(
                size=12,
                color=df_map[value_col],
                colorscale=color_scale,
                showscale=True,
                colorbar=dict(title=value_label)
            ),
            hovertext=df_map['hover_text'],
            hoverinfo='text'
        ))

        fig.update_layout(
            mapbox=dict(
                style="carto-positron",
                # Set NEM region focus
                center=dict(lat=-30, lon=142.0),
                zoom=3.7 # Adjusted zoom slightly higher than 3
            ),
            margin={"r":0,"t":0,"l":0,"b":0},
            uirevision=display_mode
        )

        # Data Table
        df_table = df_map[['name', 'network_region', 'energy_type', 'power', 'emissions', 'simulated_date', 'simulated_time']]
        df_table['power'] = df_table['power'].apply(lambda x: f"{x:,.2f}")
        df_table['emissions'] = df_table['emissions'].apply(lambda x: f"{x:,.2f}")
        df_table.columns = ['Name', 'Region', 'Type', 'Power (MW)', 'Emissions (t CO₂)', 'Date', 'Time']
        
        # Dash DataTable
        table = dash_table.DataTable(
            id='facility-dash-table',
            columns=[{"name": i, "id": i} for i in df_table.columns],
            data=df_table.to_dict('records'),
            style_header={
                'backgroundColor': '#2c3e50',
                'color': 'white',
                'fontWeight': 'bold',
                'textAlign': 'left',
                'fontSize': '14px',
                'border': '1px solid #1c2833'
            },
            style_data={
                'backgroundColor': 'white',
                'color': '#333',
                'border': '1px solid #f0f2f5',
                'textAlign': 'left'
            },
            style_cell={
                'padding': '8px',
                'whiteSpace': 'normal',
                'height': 'auto',
                'minWidth': '70px',
                'width': '100px',
                'maxWidth': '150px'
            },
            style_data_conditional=[
                {
                    'if': {'row_index': 'odd'},
                    'backgroundColor': '#f9f9f9',
                }
            ],
            style_table={'overflowY': 'auto', 'height': '100%', 'borderRadius': '4px'}
        )
    else:
        fig = go.Figure(go.Scattermapbox())
        # Set map center to Australia when no data is present
        fig.update_layout(mapbox=dict(style="carto-positron", center=dict(lat=-25.27, lon=133.77), zoom=3.5))
        table = html.P("No facilities match the current filters.", style={'textAlign': 'center', 'padding': '20px'})

    # Returns 13 Outputs
    return (
        mqtt_status, facility_count, msg_count, market_price_str, market_demand_str, 
        total_power_kpi_str, total_emissions_kpi_str, last_update, 
        total_facilities_text, total_power_text_footer, total_emissions_text_footer,
        type_options, region_options, fig, table
    )

# --- RUN APP ---
if __name__ == '__main__':
    print("="*80)
    print("[START] Real-Time Electricity Dashboard")
    print("="*80)
    print(f"MQTT Broker: {MQTT_BROKER}:{MQTT_PORT}")
    print(f"MQTT Topic: {MQTT_TOPIC}")
    print(f"Dashboard URL: http://127.0.0.1:8050/")
    print("="*80)
    app.run(debug=False, port=8050)

