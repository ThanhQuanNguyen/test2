from dash import Dash, html, dcc, Input, Output
import plotly.express as px
import dash_ag_grid as dag
import dash_bootstrap_components as dbc   
import pandas as pd     
import matplotlib
matplotlib.use('agg')
import matplotlib.pyplot as plt
import base64
from io import BytesIO
import numpy as np

df = pd.read_json(r"http://127.0.0.1:5000/dashboard_data")

app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.layout = dbc.Container([
    html.H1("Hotel Data Management", className='mb-2', style={'textAlign':'center'}),
    
    dbc.Row([
        dbc.Col([
            dcc.Dropdown(
                id='category',
                value='type_of_meal_plan',
                clearable=False,
                options=['type_of_meal_plan', 'required_car_parking_space', 'room_type_reserved'])
        ], width=4)
    ]),
    
    dbc.Row([
        # dbc.Col([
        #     html.Img(id='scatter-graph-matplotlib')
        # ], width=8),
         dbc.Col([
        dag.AgGrid(
            id='grid',
            rowData=df.to_dict("records"),
            columnDefs=[{"field": i} for i in df.columns],
            columnSize="sizeToFit",
        )
    ], width=6),  
    
        dbc.Col([
        html.Img(id='bar-graph-matplotlib')
    ], width=4)  
    ], className='mt-4')
        

])

@app.callback(
    Output(component_id='bar-graph-matplotlib', component_property='src'),
    Input('category', 'value')
)
def plot_scatter(selected_y_axis):
    print(f'selected_y_axis: {selected_y_axis}') 
    
    value_count = df[f'{selected_y_axis}'].value_counts()
    value_counts_df = value_count.reset_index(drop=False)
    value_counts_df.columns = [f'{selected_y_axis}', 'count']
    new_df = value_counts_df.sort_values(f'{selected_y_axis}')
     # Create a figure and an axes
    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.bar(new_df[f'{selected_y_axis}'], new_df['count'], width= 0.15)
    ax.bar_label(bars, labels=new_df['count'], label_type='edge') 
    # Save to a BytesIO object
    buf = BytesIO()
    fig.savefig(buf, format="png")
    buf.seek(0) 

    # Convert to base64
    fig_data = base64.b64encode(buf.getbuffer()).decode("ascii")
    fig_bar_matplotlib = f'data:image/png;base64,{fig_data}'    
    
    return fig_bar_matplotlib

if __name__ == '__main__':
    app.run_server(debug=False, port=8002)