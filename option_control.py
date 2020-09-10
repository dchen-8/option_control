import tradier_api_util
import influxdb_util

import datetime
import schedule
import time
import threading
import pytz

from absl import app

PACIFIC_TZ = pytz.timezone('US/Pacific')

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
		# TODO: Remove hard coded list of stocks
		self.stock_list = 'aapl,goog,tsla,bac,dis,amzn,fb,nflx,apha'

		# Turn on Continuous Scheduler
		self.scheduler.run_continuously()
		print('OptionControl started!')

	def schedule_calendar_check(self):
		# 11:30AM should be 4:30AM PST time; Market should open around 1AM
		self.scheduler.every().day.at('11:30').do(self.schedule_stock_data_minute)
		# Schedule job to return the current running jobs.
		self.scheduler.every().hour.at(':00').do(self.jobs_check)
		print(self.scheduler.jobs)
		print('Scheduled job: Calendar Check -', datetime.datetime.now())

	def jobs_check(self):
		print(self.scheduler.jobs)

	def schedule_stock_data_minute(self):

		today = datetime.datetime.now(tz=PACIFIC_TZ)

		stock_market_open = today.replace(hour=1, minute=0, second=0, microsecond=0)
		stock_market_close = today.replace(hour=17, minute=0, second=0, microsecond=0)

		if stock_market_open < today < stock_market_close:
			self.scheduler.every(1).minute.do(self.save_stock_data, stock_market_close).tag('stock_runs')
			print('Scheduled Stock Jobs!')

	def save_stock_data(self, end):
		results = self.collect_stock_data()

		if not results:
			self.influx_client.write(results, 'stocks')
		else:
			print("Tradier API returned no stock data!")
		
		if self.scheduler.next_run.astimezone(PACIFIC_TZ) > end:
			self.scheduler.clear(tag='stock_runs')
		# print(self.scheduler.jobs)
		# return schedule.CancelJob

	def collect_stock_data(self):
		return self.tradier.get_symbol(self.stock_list, return_for_influx=True)

	def save_historical_stock_data(self):
		# This is used if there is a need to backfil some data, all the previous data is not overriden
		# There could be a case for modifying the time stamps; but possibly be done during ETL
		results = self.collect_historical_stock_data()
		if results:
			self.influx_client.write(results, 'historical_stocks')

	def collect_historical_stock_data(self):
		results = []
		for symbol in self.stock_list.split(','):
			results += self.tradier.get_three_months_historical_stocks(symbol)
		return results


def main(argv):
	del argv  # Unused.

	print('Main thread started!')
	option_control = OptionControl()
	option_control.schedule_calendar_check()
	# option_control.schedule_stock_data_minute()
	print('Main thread exiting!!!')


if __name__ == '__main__':
  app.run(main)