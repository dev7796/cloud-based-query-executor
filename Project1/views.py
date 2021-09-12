from flask import Flask, request, render_template
from utils import connect_mysql, connect_redshift

app = Flask(__name__)


@app.route('/mysql', methods=['GET'])
def query_mysql():
    query = request.args.get('query', 'show tables;')
    connection = connect_mysql(host='cs527mysql2.chmrmo5grph7.us-east-1.rds.amazonaws.com', user='Maaaartian',
                               password='mysqls250+38qqcn', db='test')
    col_name, content, query_time = connection.run_query(query)
    result = {'col_name': col_name, 'result': content, 'query_time': query_time}
    connection.disconnect()
    return result


@app.route('/redshift', methods=['GET'])
def query_redshift():
    query = request.args.get('query', 'show tables;')
    connection = connect_redshift(host='cs527redshift.clyua6krsyew.us-east-1.redshift.amazonaws.com', user='group3',
                                  password='Redshift123',
                                  database='dev')
    col_name, content, query_time = connection.run_query(query)
    result = {'col_name': col_name, 'result': content, 'query_time': query_time}
    connection.disconnect()
    return result


@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')
