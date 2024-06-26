import os
import facade as f
from flask import Flask
from collections import deque
from datetime import datetime
from zoneinfo import ZoneInfo
import random

app = Flask(__name__)

fifo_queue = deque(maxlen=10)

def scheduled_task():
    step = random.randrange(500, 1500)/700
    log_info = f.send_to_kafka(step)
    fifo_queue.append(log_info)

f.schedule(scheduled_task)

start_time = datetime.now(ZoneInfo("Europe/Warsaw"))
start_time_string = start_time.strftime('%Y-%m-%d %H:%M:%S')

fifo_queue.append(f'Started at {start_time_string}')   


@app.route('/')
def home():
    content = """
    <h1>Processing Data on Air Pollution in Europe on a Modern Data Platform</h1>
    <p><a href="/high_pollution">Countries With the Highest Levels of PM10 pollution</a>
    <p><a href="/correlations">Correlation Between Weather Conditions and Air Pollution Levels</a>
    <p><a href="/capital_pollution">Capital Cities With the Highest Levels of Traffic Polution</a>
    <p><a href="/distribution">Distribution of Specific Pollutants Across Europe</a>
    <p><a href="/kafka">Kafka processing log</a>
    """
    return content


@app.route('/correlations')
def correlations():
    return f.get_correlations()


@app.route('/high_pollution')
def high_pollution():
    return f.get_high_pollution()


@app.route('/capital_pollution')
def capital_pollution():
    return f.get_capital_pollution()


@app.route('/distribution')
def distribution():
    return f.get_distribution()


@app.route('/kafka')
def kafka():
 

    return "<p>".join(list(fifo_queue))

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
    

