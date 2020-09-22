import tradier_api_util
import influxdb_util
import mongo_client

import datetime
import schedule
import time
import threading
import pytz
import pprint

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
		self.mongo_client = mongo_client.Connect.get_connection()
		# TODO: Remove hard coded list of stocks
		self.stock_list = 'aapl,goog,tsla,bac,dis,amzn,fb,nflx,apha'

		# Turn on Continuous Scheduler
		self.scheduler.run_continuously()
		print('OptionControl started!')

	def schedule_calendar_check(self):
		# On Docker start; Check if Market is open and schedule Stock Runs
		self.schedule_stock_data_minute()
		# 11:30AM should be 4:30AM PST time; Market should open around 1AM
		self.scheduler.every().day.at('11:30').do(self.schedule_stock_data_minute)
		# Schedule job to return the current running jobs.
		self.scheduler.every().hour.at(':00').do(self.jobs_check)
		# Schedule daily job to get option expirations. Refresh around midnight
		self.scheduler.every().day.at('07:30').do(self.save_option_expirations).tag('daily_option_expiration')
		pprint.pprint(self.scheduler.jobs)
		print('Scheduled job: Calendar Check -', datetime.datetime.now())

	def jobs_check(self):
		print('Printing Job Check!')
		current_jobs = self.scheduler.jobs
		pprint.pprint(current_jobs)
		# Check if job.tags currently has stock_runs schedule, if not schedule.
		current_tags = [job.tags for job in current_jobs]
		if 'stock_runs' not in current_tags:
			self.schedule_stock_data_minute()

	def query_influx_options_expiration(self):
		results = self.influx_client.get_option_expirations()
		return results

	def save_option_expirations(self):
		results = []
		for symbol in self.stock_list.split(','):
			results += self.tradier.get_option_expiration(symbol)
		
		if results:
			self.influx_client.write(results, 'options')

	def get_today_market_status(self, today):
		today_str = today.strftime('%Y-%m-%d')
		today_year = today.strftime('%Y')
		today_month = today.strftime('%m')

		# Check calendar if the market is open today
		calendar_results = self.tradier.get_calendar(year=today_year, month=today_month)
		calendar_dates = calendar_results['calendar']['days']['day']
		today_status = [x for x in calendar_dates if x['date'] == today_str][0]
		return today_status

	def schedule_stock_data_minute(self):

		today = datetime.datetime.now(tz=PACIFIC_TZ)
		stock_market_open = today.replace(hour=1, minute=0, second=0, microsecond=0)
		stock_market_close = today.replace(hour=17, minute=0, second=0, microsecond=0)

		today_market_status = self.get_today_market_status(today)

		if stock_market_open < today < stock_market_close and today_market_status['status'] == 'open':
			self.scheduler.every(1).minute.do(self.save_stock_data, stock_market_close).tag('stock_runs')
			print('Scheduled Stock Jobs!')

	def save_stock_data(self, end):
		results = self.collect_stock_data()

		if results:
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

	def write_historical_to_mongo(self):
		results = self.collect_historical_stock_data()
		stocks_db = self.mongo_client.stocks
		# Write to historical stocks table
		stocks_db.historical_stocks.insert_many(results)

	def collect_historical_stock_data(self):
		results = []
		for symbol in self.stock_list.split(','):
			result = self.tradier.get_three_months_historical_stocks(symbol)
			for each_day in result:
				each_day['symbol'] = symbol
				results.append(each_day)
		return results


def main(argv):
	del argv  # Unused.

	print('Main thread started!')
	option_control = OptionControl()
	# option_control.query_influx_options_expiration()
	option_control.schedule_calendar_check()
	# option_control.schedule_stock_data_minute()
	print('Main thread exiting!!!')


if __name__ == '__main__':
  app.run(main)