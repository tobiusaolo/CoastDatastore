from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, jsonify, render_template, request, g, url_for, make_response
# from flask_apscheduler import APScheduler

from sqlalchemy import create_engine
# from scrapper.newscraper import galaxyradio_scrapper, simba_scrapper, gambuuze_scrapper, ssegwanga_scrapper, dembe_scrapper
# from scrapper.scrapper import socialmedia


# def sensor():
# galaxyradio_scrapper()
# simba_scrapper()
# gambuuze_scrapper()
# ssegwanga_scrapper()
# dembe_scrapper()
# socialmedia()


# sched = BackgroundScheduler(daemon=True)
# sched.add_job(sensor, 'interval', hours=10)
# sched.start()


app = Flask(__name__)
db_connect = create_engine(
    'postgresql://awjzgmwqiatzjg:e4424ae3d375e2057bcc9cde832672940d44ea2c05260e28ccb04dc1575ec52d@ec2-34-204-22-76.compute-1.amazonaws.com:5432/dabbhqt4pegslv')
conn = db_connect.connect()


@app.route('/')
def index():
    # count corpus size
    sum = 0
    socialsum = 0
    newssum = 0
    general_corpus = conn.execute('''SELECT corpus FROM socialmedia''')
    nets = list(general_corpus.fetchall())
    for csize in nets:
        for x in csize:
            sum = sum+int(x)
    # count social media data
    name = "sociamedia"
    social_data = conn.execute(
        '''SELECT corpus FROM socialmedia WHERE source_type=('{name}')'''.format(name=name))
    mediaz = list(social_data.fetchall())
    for socialval in mediaz:
        for sc in socialval:
            socialsum = socialsum+int(sc)
    # count newpapers data
    stype = 'news'
    new_data = conn.execute(
        '''SELECT corpus FROM socialmedia WHERE source_type=('{name}')'''.format(name=stype))
    newslist = list(new_data.fetchall())
    for newsval in newslist:
        for nc in newsval:
            newssum = newssum+int(nc)
    # Display data in the dataset
    query = conn.execute("select * from socialmedia")
    data = query.cursor.fetchall()
    return render_template('home.html', data=data, corpusz=sum, socialcorpus=socialsum, newscorpus=newssum)


@app.route('/socialmedia')
def socialmedia():
    # Display data in the dataset
    query = conn.execute(
        '''SELECT * FROM socialmedia WHERE source_type=('{name}')'''.format(name='sociamedia'))
    data = query.cursor.fetchall()
    return render_template("socialmedia.html", data=data)


@app.route('/newpapers')
def newpapers():
    # Display data in the dataset
    query = conn.execute(
        '''SELECT * FROM socialmedia WHERE source_type=('{name}')'''.format(name='news'))
    data = query.cursor.fetchall()
    return render_template("newspapers.html", data=data)


@app.route('/transfer')
def transfer():
    return render_template("Exports.html")


def checkjob():
    print("hello tobyman")


if __name__ == "__main__":
    app.run(debug=True)
    # app.run(debug=True)
