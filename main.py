from flask import Flask, jsonify, render_template, request, g, url_for, make_response
from sqlalchemy import create_engine
import schedule
import time
app = Flask(__name__)


@app.route('/')
def index():
    return render_template('home.html')


@app.route('/socialmedia')
def socialmedia():
    return render_template("socialmedia.html")


@app.route('/newpapers')
def newpapers():
    return render_template("newspapers.html")


@app.route('/transfer')
def transfer():
    return render_template("Exports.html")


def checkjob():
    print("hello tobyman")


if __name__ == "__main__":
    app.run(debug=True)
    schedule.every(4).seconds.do(checkjob)
    while 1:
        schedule.run_pending()
        time.sleep(1)
    # app.run(debug=True)
