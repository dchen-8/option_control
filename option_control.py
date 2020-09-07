import tradier_api_util
import influxdb_util

import schedule
import time
import threading

from absl import app

class ContinuousScheduler(schedule.Scheduler):
    def run_continuously(self, interval=1):
        """Continuously run, while executing pending jobs at each elapsed
        time interval.
        @return cease_continuous_run: threading.Event which can be set to
        cease continuous run.
        Please note that it is *intended behavior that run_continuously()
        does not run missed jobs*. For example, if you've registered a job
        that should run every minute and you set a continuous run interval
        of one hour then your job won't be run 60 times at each interval but
        only once.
        """
        cease_continuous_run = threading.Event()

        class ScheduleThread(threading.Thread):
            @classmethod
            def run(cls):
                while not cease_continuous_run.is_set():
                    self.run_pending()
                    time.sleep(interval)

        continuous_thread = ScheduleThread()
        continuous_thread.start()
        return cease_continuous_run


class OptionControl(object):

    def __init__(self):
        self.scheduler = ContinuousScheduler()
        self.tradier = tradier_api_util.Tradier()
        self.influx_client = influxdb_util.OptionControlInfluxDB(database='options')

        # Turn on Continuous Scheduler
        self.scheduler.run_continuously()
    
    def test_job(self):
        self.scheduler.every(10).seconds.do(self.save_stock_data)

    def save_stock_data(self):
        results = self.collect_stock_data()

        self.influx_client.write(results, 'stocks')
        return schedule.CancelJob

    def collect_stock_data(self):
        stock_list = 'aapl,goog,tsla,bac,dis'
        return self.tradier.get_symbol(stock_list, return_for_influx=True)


def main(argv):
    del argv  # Unused.
#     tradier = tradier_api_util.Tradier()
# #   results = tradier.get_symbol('aapl', return_for_influx=True)
#     results = tradier.get_calendar('10', '2020')
#     print(results)

    option_control = OptionControl()
    option_control.test_job()

    # client = influxdb_util.OptionControlInfluxDB(database='calendar')
    # client._client.create_database('calendar')
    # client.write(results)


if __name__ == '__main__':
  app.run(main)