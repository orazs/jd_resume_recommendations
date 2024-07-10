import dash, time, os, joblib, base64, io
import dash_bootstrap_components as dbc
from dash import dcc, html, Input, Output, State
import dash_ag_grid as dag
from dash import Dash, DiskcacheManager, CeleryManager, Input, Output, html
import pandas as pd
import plotly.express as px
import plotly.io as pio
import plotly.graph_objects as go
import prestodb, datetime
import numpy as np

import psycopg2
def make_link(x,y):
    return "[{0}]({1})".format(x,y)


skilling_template = go.layout.Template(
    # LAYOUT
    layout={
        'plot_bgcolor': "white",
        'title':
            {'font': {'family': 'HelveticaNeue-CondensedBold, Helvetica, Sans-serif',
                      'size': 30,
                      'color': '#333'}
             },
        'font': {'family': 'Roboto',
                 'size': 16,
                 'color': '#333'},
        # Colorways
        'colorway': ['#bdd7e7', '#6baed6', '#3182bd', '#31a354'],
        # Keep adding others as needed below
        'hovermode': 'x unified'
    },

)

pio.templates.default = skilling_template


import re

jd = pd.read_csv("data/jd_processed.csv")
resume = pd.read_csv("data/resume_processed.csv")



if 'REDIS_URL' in os.environ:
    # Use Redis & Celery if REDIS_URL set as an env variable
    from celery import Celery

    celery_app = Celery(__name__, broker=os.environ['REDIS_URL'], backend=os.environ['REDIS_URL'])
    background_callback_manager = CeleryManager(celery_app)

else:
    # Diskcache for non-production apps when developing locally
    import diskcache

    cache = diskcache.Cache("./cache")
    background_callback_manager = DiskcacheManager(cache)

app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
    background_callback_manager=background_callback_manager
)
app.title = "demo"

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

green_button_style = {'background-color': 'green',
                      'color': 'white',
                      'height': '50px',
                      'width': '200px'}

red_button_style = {'background-color': 'red',
                    'color': 'white',
                    'height': '50px',
                    'width': '200px'}



@app.callback(Output(component_id='chart2', component_property='figure'),
              Input(component_id="chart1", component_property='clickData')
              )
def graph_update(clickData):
    # bar chart
    print(clickData)
    field = clickData['points'][0]['x']
    print(field)
    df = (
        jd
        .loc[lambda x: x['field']==field]
        .loc[lambda x: x['experience'].astype(int) > 0]
        .assign(salary_log=lambda x: np.log(x['salary']),
                exp_log=lambda x: np.log(x['experience'].astype(int)))
        [["salary_log", "exp_log","available_hc","type","url","title"]]
    )

    line_fig = px.scatter(df, x="salary_log", y="exp_log",size="available_hc",color="type", hover_data=df[["title","url"]])
    line_fig.update_layout(
        title=dict(text="<b>Вакансии сектора \"{0}\" </b>".format(field), font=dict(size=16),x=0.1)
    )
    line_fig.update_layout(legend_title=None, xaxis_title=None, legend=dict(
        orientation="h",
        yanchor="bottom",
        y=1.02,
        xanchor="right",
        x=1
    ))

    return line_fig








tabs = dbc.Tabs(
    [
        dbc.Tab(html.Div(id="table"), label="")
        # dbc.Tab(html.Div([
        #     dbc.Row([
        #         dbc.Col([dcc.Graph(id='cumulative-chart')], width=6, lg=6, md=6, xs=12),
        #         dbc.Col([dcc.Graph(id='thresholds-chart')  ], width=6, lg=6, md=6, xs=12)
        #     ])
        # ]), label="Last week results")
    ]
)





@app.callback(Output(component_id="table", component_property="children"),
              Input('chart2', 'clickData'),
              )
def update_table(clickData):
    jd_url = clickData['points'][0]['customdata'][1]
    df = get_recommendations(jd_url)
    df=df.assign(Резюме=lambda x: x[['Резюме','url']].apply(lambda x: make_link(*x),axis=1))
    df = df.drop("url",axis=1)


    return dag.AgGrid(
        id="grid-page-size",
        columnDefs=[{"field": i,"cellRenderer": "markdown"} for i in df.columns.tolist()],
        rowData=df.to_dict("records"),
        columnSize="sizeToFit",
        defaultColDef={"filter": True},
        dashGridOptions={"pagination": True, "paginationPageSizeSelector": False, "animateRows": False},
    )


@app.callback(Output("chart1", "figure"),
              [Input("trades", "data")])
def graph_update(json_data):
    # bar chart
    df = jd.groupby(['field', 'type'], as_index=False).agg(value=("salary", "mean")).sort_values("value",
                                                                                               ascending=False).head(10)
    bar_fig = px.bar(df,
                     x="field", y="value", color="type", text_auto=True, color_discrete_map={
            "техническое и профессиональное": "#b3cde3",
            "высшее": "#8856a7",
            "послевузовское":"#8c96c6"

        }
                     )
    bar_fig.update_traces(texttemplate="%{y:.2s}")
    bar_fig.update_layout(xaxis=dict(tickfont=dict(size=8)),title=dict(text="<b>Уровень з.п по секторам </b>", font=dict(size=16),x=0.1))
    bar_fig.update_layout(title_x=0,yaxis_title=None)

    bar_fig.update_layout(legend_title=None, xaxis_title=None, legend=dict(
        orientation="h",
        yanchor="bottom",
        y=1.02,
        xanchor="right",
        x=1
    ))
    return bar_fig




def get_recommendations(jd):
    print(jd)
    connection = psycopg2.connect(
        host="185.234.114.28",
        database="enbek",
        user="airflow",
        password="airflow",
    )
    connection.autocommit = True
    cursor = connection.cursor()
    cursor.execute(
        "SELECT * FROM public.recommendations where url_jd='{0}'".format(jd))
    rows = cursor.fetchall()
    rows = pd.DataFrame(rows,columns=["url_resume","url_jd","title_sim", "skills_sim", "duties_sim", "education_sim", "total_sim"])
    rows = pd.merge(rows, resume, how="inner",left_on="url_resume",right_on="url")[["title","category","sex","age","salary","skills","url_resume","total_sim"]]
    return rows.rename(columns={"title":"Резюме","category":"Сектор","sex":"Пол","age":"Возраст","salary":"ЗП","skills":"Навыки","url_resume":"url","total_sim":"Скоринг (из 100)"})

@app.callback(
    Output("chart_title", "children"),
    Input("chart2", "clickData"),
)
def update_chart_title(clickData):
    print(clickData)
    url =  clickData['points'][0]['customdata'][1]
    title = clickData['points'][0]['customdata'][0]

    return ["Рекомендованные резюме для вакансии ", html.A(title,href=url,target="_blank")]




    return ["Рекомендованные резюме для вакансии ", html.A(title,href=url,target="_blank")]

def description_card():
    """

    :return: A Div containing dashboard title & descriptions.
    """
    return html.Div(

        id="description-card",
        children=[
            html.Base([], target="_blank"),
            html.H3("Job board analysis", style={"color": "#3182bd"}),
            html.Div(
                id="intro",
                children=[html.P("job posting-resume recommendations")

                          ],
            ),
        ],
    )


app.layout = dbc.Container([
dbc.Row(children=[
        dbc.Col([description_card()
                 ], width=2, lg=2, md=10, xs=12),
    dbc.Col([dcc.Graph(id="chart1",clickData={'points': [{'field': 'https://enbek.kz/ru/vacancy/metodist-metodist~3994355'}]})], width=5, lg=5, md=5,xs=12),
        dbc.Col([dcc.Graph(id="chart2",clickData={'points': [{'url': 'https://enbek.kz/ru/vacancy/metodist-metodist~3994355'}]})], width=5, lg=5, md=5, xs=12),
    dbc.Row([


        dbc.Col([], width=2),
        dbc.Col([html.Div(id="chart_title"), html.Br(),
                 tabs], width=10, lg=10, md=5, xs=12),

                 dcc.Store(id="trades")

                 ])

    ])
    ,

    # dcc.Store stores the intermediate value
    dcc.Store(id='intermediate-value')
], fluid=True, style={"margin": 10, "padding": 10}

)

if __name__ == '__main__':
    app.run_server(debug=False, port=7777, host='0.0.0.0')
