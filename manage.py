from flask import Flask, url_for, redirect, render_template, g
from datetime import timedelta
from pymongo import MongoClient

UPLOAD_FOLDER = './uploads'


app = Flask(__name__)


app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['SECRET_KEY'] = '123456'
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = timedelta(seconds=1)
# app.config.companylist = ''
# app.config.filename = ''
# app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 限制文件大小
# mongo_conn = MongoClient('192.168.1.110', 17017)
mongo_conn = MongoClient('192.168.1.110', 17017)


@app.route('/')
def index_page():
    return '<h1>index_page</h1>'


@app.route('/<username>')
def hello_world(username):
    return 'Hello World! Waiting for the perfect.'+'\n'+'you delivered %s' % username


@app.route('/url/demo1')
def demo1():
    """
        url_for是构建url，返回合成的url；
        redirect是重定向；
    """
    return redirect(url_for('hello_world', username='hehe'), 404)


'----------------------------------------------------------------------------'


@app.route('/page/')
@app.route('/page/<name>')
def demo_page1(name=None):
    return render_template('demo_page1.html', name=name)


from download import download
app.register_blueprint(download)
from upload import upload
app.register_blueprint(upload)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=True)
