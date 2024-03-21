from apscheduler.schedulers.background import BackgroundScheduler


def schedule(scheduled_task):
    scheduler = BackgroundScheduler(daemon=True)
    scheduler.add_job(scheduled_task, 'interval', minutes=1)
    scheduler.start()    