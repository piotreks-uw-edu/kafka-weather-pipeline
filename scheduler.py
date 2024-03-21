from apscheduler.schedulers.background import BackgroundScheduler


def schedule(scheduled_task):
    scheduler = BackgroundScheduler(daemon=True)
    scheduler.add_job(scheduled_task, 'interval', hours=6)
    scheduler.start()    