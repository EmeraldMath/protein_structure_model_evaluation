import dash
import dash_table
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import psycopg2 
from mySecret import db_host, db, db_user, db_password, AWS_ACCESS_KEY, AWS_SECRET_KEY
import pandas as pd
app = dash.Dash()

colors = {
    'background': '#111111',
    'text': '#7FDBFF'
}

markdown_text = '''
### Dash and Markdown
A lot of text
'''
targets = ['T0466', 'T0467', 'T0467', 'T0468']

app.layout = html.Div([
    html.Label('Target Protein ID'),
    dcc.Dropdown(
        id = 'dropdown',
        options=[{'label': i, 'value': i} for i in targets],
        value=['T0466'],
    ),
    html.Label('Return simulated structure models:'),
    dcc.RadioItems(
        id = 'radio',
        options=[
            {'label': 'Excellent(4)', 'value': 'excel'},
            {'label': 'Good(3)', 'value': 'good'},
            {'label': 'Not Good(2)', 'value': 'ng'},
            {'label': 'Bad(1)', 'value': 'bad'}
        ],
        value='excel'
    ),
    html.Button('Submit', id='button'),
    html.Div(id='display-value'),
])

@app.callback(dash.dependencies.Output('display-value', 'children'),
              [dash.dependencies.Input('dropdown', 'value'),
              dash.dependencies.Input('radio', 'value'),
              dash.dependencies.Input('button', 'n_clicks')])

def display_value(value, label, n):
    if n is None:
        return None
    
    query = "select * from prediction_bk where candidate_id ILIKE '{}%' and prediction = {}".format(value, label)
    
    conn = psycopg2.connect(host=db_host, database=db,user=db_user,password=db_password)
    df = pd.read_sql_query(query, con = conn)
    conn.close()
    
    return [
        dash_table.DataTable(
            id = 'table',
            columns=[{"name": i, "id": i} for i in df.columns],
            data = df.to_dict('records'),
        )
    ]



if __name__ == '__main__':
    app.run_server(debug=True, port=9000)
    #app.run_server(host = '0,0,0,0',port=80)

