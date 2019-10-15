import dash
import dash_table
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import psycopg2 
from mySecret import db_host, db, db_user, db_password, AWS_ACCESS_KEY, AWS_SECRET_KEY
import pandas as pd
app = dash.Dash(__name__)

colors = {
    'background': '#111111',
    'text': '#7FDBFF',
    'button': '#FFFFFF'
}


targets = ['T0950', 'T0953s2', 'T0957s1', 'T0957s2']

app.layout = html.Div(style={'backgroundColor': colors['background']}, children=[
    html.H1(
        children='ProteinMapper',
        style={
            'textAlign': 'center',
            'color': colors['text']
        }
    ),

    html.Div(children='A structure inspector for simulated protein models', style={
        'textAlign': 'center',
        'color': colors['text']
    }),
    
    html.Div(children=[
        html.Div(
            html.Img(
                src='assets/gitphy.gif',
                #src='https://www.google.com/imgres?imgurl=https%3A%2F%2Flookaside.fbsbx.com%2Flookaside%2Fcrawler%2Fmedia%2F%3Fmedia_id%3D947623412052831&imgrefurl=https%3A%2F%2Fwww.facebook.com%2Frandomimagesbr%2F&docid=b7WQfmnxTxbYLM&tbnid=6ZBEU1PJVnOpeM%3A&vet=10ahUKEwji__fn2J7lAhWLOcAKHfPpCqsQMwhRKAEwAQ..i&w=960&h=949&bih=698&biw=1156&q=random%20pictures&ved=0ahUKEwji__fn2J7lAhWLOcAKHfPpCqsQMwhRKAEwAQ&iact=mrc&uact=8',
                style={
                    "height": '20%',
                    "width": '20%'
                }
            ),
        style={'textAlign': 'center'}
        )],
    ),
    
    html.Div(
        html.Label('Target Protein ID'),
        style={'display': 'flex', 'align-items': 'center', 'justify-content': 'center', 'color': colors['button']},
    ),

    html.Div(children=[
        dcc.Dropdown(
            id = 'dropdown',
            options=[{'label': i, 'value': i} for i in targets],
            value='T0950',
        ),
    ], style={'margin-left':'25%', 'display': 'inline-block', 'align-items': 'center', 'justify-content': 'center', 'width': '50%', 'textAlign':'center'}),


    html.Div(children=[
        html.Label('Return simulated structure models: ', style={'color': colors['button']}),
        dcc.RadioItems(
        id = 'radio',
        options=[
            {'label': 'Excellent(4)', 'value': 4},
            {'label': 'Good(3)', 'value': 3},
            {'label': 'Not Good(2)', 'value': 2},
            {'label': 'Bad(1)', 'value': 1}
        ],
        value=4
        ),
    ], style={'width': '100%', 'display': 'flex','‘align-items': 'center',\
    'justify-content': 'center', 'color': colors['button']}),

    html.Div(children=[
        html.Button('Submit', id='button'),
        #html.Button('Download', id='download'),
    ],style={'width': '100%', 'display': 'flex','‘align-items': 'center',\
    'justify-content': 'center', 'color': colors['button']}),
    html.Div(id='display-value'),
])

@app.callback(dash.dependencies.Output('display-value', 'children'),
              [dash.dependencies.Input('dropdown', 'value'),
              dash.dependencies.Input('radio', 'value'),
              dash.dependencies.Input('button', 'n_clicks')])

def display_value(dropdown, radio, n):
    if n is None:
        return None
    if dropdown is None or radio is None:
        return "Please select target"
    query = "select * from prediction where candidate_id ILIKE '{}%' and prediction = {}".format(dropdown, radio)
    
    conn = psycopg2.connect(host=db_host, database=db,user=db_user,password=db_password)
    df = pd.read_sql_query(query, con = conn)
    conn.close()
    return [
        dash_table.DataTable(
            id = 'table',
            columns=[{"name": i, "id": i} for i in df.columns],
            data = df.to_dict('records'),
            editable=True,
            style_table={'maxWidth': '1500px'},
            #row_selectable="multi",
            #selected_rows=[0],
            style_cell = {"fontFamily": "Arial", "size": 10, 'textAlign': 'center'}
        )
    ]



if __name__ == '__main__':
    #app.run_server(debug=True, port=9000)
    app.run_server(host = '0.0.0.0',port=80)