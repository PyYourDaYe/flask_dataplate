from flask import Flask, url_for, redirect, render_template, request
from datetime import timedelta
# from pymongo import MongoClient


app = Flask(__name__)


app.config['UPLOAD_FOLDER'] = './uploads'
app.config['SECRET_KEY'] = '123456'
app.config['permanent_session_lifetime'] = timedelta(days=1)  # 设置session的过期时间
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = timedelta(seconds=1)
# app.config.companylist = ''
# app.config.filename = ''
# app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 限制文件大小
# mongo_conn = MongoClient('192.168.1.171', 17017)


'---------------------------------------以下只是用来测试-----------------------------------------------'
@app.route('/index')
def index_page():
    return '<h1>index_page</h1>'


@app.route('/index/<username>')
def hello_world(username):
    return 'Hello World! Waiting for the perfect.'+'\n'+'you delivered %s' % username

movies = [
    {'title': 'My Neighbor Totoro', 'year': '1988'},
    {'title': 'Dead Poets Society', 'year': '1989'},
    {'title': 'A Perfect World', 'year': '1993'},
    {'title': 'Leon', 'year': '1994'}
]


@app.route('/ceshi',methods=['GET', 'POST'])
def ceshi_page():
    if request.method == 'POST':
        username = request.form['input_text']
        if username == "user":
            return render_template('ceshi.html', name=1, movies=movies)
        else:
            message = "Failed Login"
            return render_template('ceshi.html', name=0)
    return render_template('ceshi.html', name=0)


@app.route('/url/demo1')
def demo1():
    """
        url_for是构建url，返回合成的url；
        redirect是重定向；
    """
    return redirect(url_for('hello_world', username='hehe'), 404)


'-----------------------------------------以上只是用来测试----------------------------------------------'


@app.route('/')
@app.route('/<name>')
def dataplate_index(name=None):
    return render_template('flask首页.html', name=name)


@app.route('/science')
def data_science_index():
    return render_template('数据科学.html')


# 注册蓝图
from down import download
app.register_blueprint(download)
from up import upload
app.register_blueprint(upload)

if __name__ == '__main__':
    # handler = logging.FileHandler('flask.log', encoding='UTF-8')
    # handler.setLevel(logging.DEBUG)
    # logging_format = logging.Formatter(
    #     '%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(lineno)s - %(message)s')
    # handler.setFormatter(logging_format)
    # app.logger.addHandler(handler)

    app.run(host="0.0.0.0", port=5000, debug=True)
