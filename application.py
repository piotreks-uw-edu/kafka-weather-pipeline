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
    step = random.randrange(900, 1100)/1000
    log_info = f.send_to_kafka(step)
    fifo_queue.append(log_info)

f.schedule(scheduled_task)

start_time = datetime.now(ZoneInfo("Europe/Warsaw"))
start_time_string = start_time.strftime('%Y-%m-%d %H:%M:%S')

fifo_queue.append(f'Started at {start_time_string}')


@app.route('/')
def home():
    return "<p>".join(list(fifo_queue))

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)

