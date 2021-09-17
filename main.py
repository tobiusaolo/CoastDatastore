from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, jsonify, render_template, request, g, url_for, make_response
# from flask_apscheduler import APScheduler

from sqlalchemy import create_engine
from scrapper.newscraper import galaxyradio_scrapper, simba_scrapper, gambuuze_scrapper, ssegwanga_scrapper, dembe_scrapper
from scrapper.scrapper import socialmedia


def sensor():
    galaxyradio_scrapper()
    simba_scrapper()
    gambuuze_scrapper()
    ssegwanga_scrapper()
    dembe_scrapper()
    socialmedia()


sched = BackgroundScheduler(daemon=True)
sched.add_job(sensor, 'interval', hours=10)
sched.start()


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
    # app.run(debug=True)
