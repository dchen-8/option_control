import tradier_api_util
import influxdb_util

import datetime
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
        print('OptionControl started!')

    def schedule_calendar_check(self):
        self.scheduler.every().day.at('06:00').do(self.check_market_open)
        print('Scheduled job: Calendar Check -', datetime.datetime.now())

    def check_market_open(self):
        clock_data = self.tradier.get_clock()

        # If the next state is open; schedule job to query once a min.
        if clock_data.get('next_state') == 'open':
            print('Scheduled job: Stocks Run -', datetime.datetime.now())
            self.schedule_stocks_run()

    def schedule_stocks_run(self):
        stock_market_open = datetime.datetime.strptime('06:30', '%H:%M')
        stock_market_close = datetime.datetime.strptime('13:00', '%H:%M')

        total_trading_minutes = int((stock_market_close - stock_market_open).seconds/60)

        for each_min in range(total_trading_minutes + 1):
            schedule_run = stock_market_open + datetime.timedelta(minutes=each_min)
            schedule_run_str = schedule_run.strftime('%H:%M')
            self.scheduler.every().day.at(schedule_run_str).do(self.save_stock_data).tag('stock_runs')
        print('Scheduled {} jobs!'.format(self.scheduler.jobs))

    def save_stock_data(self):
        results = self.collect_stock_data()

        self.influx_client.write(results, 'stocks')
        return schedule.CancelJob

    def collect_stock_data(self):
        stock_list = 'aapl,goog,tsla,bac,dis'
        return self.tradier.get_symbol(stock_list, return_for_influx=True)


def main(argv):
    del argv  # Unused.

    print('Main thread started!')
    option_control = OptionControl()
    option_control.schedule_calendar_check()
    print('Main thread exiting!!!')


if __name__ == '__main__':
  app.run(main)