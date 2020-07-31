import sys
sys.path.append('../')
sys.path.append('../../commonfiles/python')
import data_collector_plugin as my_plugin
import os
import logging.config
import traceback
from datetime import datetime
import time
from math import isnan
import json
import numpy as np
import pandas as pd

class folly_camera_stats(my_plugin.data_collector_plugin):

  def __init__(self):
    my_plugin.data_collector_plugin.__init__(self)
    self.output_queue = None

  def initialize_plugin(self, **kwargs):
    try:
      plugin_details = kwargs['details']
      self._data_files = plugin_details.get("Settings", "data_files").split(',')
      self._log_conf = plugin_details.get("Settings", "logfile")
      self._output_file = plugin_details.get("Settings", "output_file")
      self._resample_interval = plugin_details.getint("Settings", "resample_interval")
      return True
    except Exception as e:
      self.logger.exception(e)
    return False

  def run(self):
    try:
        start_time = time.time()
        logger = None
        logging.config.fileConfig(self._log_conf)
        logger = logging.getLogger(__name__)
        logger.info("Log file opened.")

        camera_sites_stats = {
            'north': {},
            'south': {}
        }

        weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        for data_file in self._data_files:
            # Restamp times in file to have 0 padding and add weekday column.
            with open(data_file, "r") as cam_file:
                path, filename = os.path.split(self._data_file)
                filename, ext = os.path.splitext(filename)
                camera_site = 'south'
                if 'north' in filename:
                    camera_site = 'north'
                restamped = os.path.join(path, "%s_restamped.csv" % (filename))
                with open(restamped, "w") as restamped_file:
                    for ndx,row in enumerate(cam_file):
                        if ndx > 0:
                            date_col, count_col = row.split(',')
                            date_part, time_part = date_col.split(' ')
                            year,month,day = date_part.split('-')
                            hour,minute=time_part.split(':')
                            date_time = datetime(int(year), int(month), int(day), int(hour), int(minute))
                            weekday = date_time.weekday()
                            restamped_file.write("%s,%s,%s" % (date_time.strftime('%Y-%m-%d %H:%M'),weekday,count_col))
                        else:
                            restamped_file.write("m_date,weekday,object_count\n")

            dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M')
            df = pd.read_csv(restamped,
                             delimiter=',',
                             header=0,
                             delim_whitespace=False,
                             parse_dates=['m_date'],
                             date_parser=dateparse,
                             names=['m_date', 'weekday', 'object_count'])


            #df['m_date'] = pd.to_datetime(df['m_date'], format('%Y-%m-%d %H:%M'))
            df = df.set_index(pd.DatetimeIndex(df['m_date']))
            #DO weekday averages.
            start_date = df.index.min()
            end_date = df.index.max()
            weekday_df = df.groupby('weekday')
            weekday_df.index = weekday_df['weekday']
            weekday_avg = weekday_df.mean()
            weekdays_avg = []
            for row in weekday_avg.itertuples(index=True, name='Pandas'):
                weekday = getattr(row, "Index")
                avg = getattr(row, "object_count")
                val = 0
                if not isnan(avg):
                    val = int(avg+0.5)
                weekdays_avg.append({weekdays[weekday]: val})

            #for name,group in weekday_avg:
            #    print name
            #    print group

            #Get last 7 days.
            #cutoff_date = df["m_date"].max() - pd.Timedelta(days=7)

            #WE want to resample the data to a 3hr window, so create a new dataframe without the
            #weekday column.
            resampled_df = df.drop('weekday', axis=1)
            resample_hours = "%dh" % (self._resample_interval)
            resampled_hrs = resampled_df.resample(resample_hours).mean()
            resampled_hrs['m_date'] = pd.to_datetime(resampled_hrs.index.values)

            resampled_gb = resampled_hrs.groupby(resampled_hrs['m_date'].dt.hour)
    #        for name,group in tmp:
    #            print name
    #            print group
            avg_3_hour = resampled_gb.mean()
            grouped_3hrs = []
            for row in avg_3_hour.itertuples(index=True, name='Pandas'):
                hr = getattr(row, "Index")
                avg = getattr(row, "object_count")
                start_dt = "%02d:00" % (hr)
                end_dt = "%02d:00" % (hr+self._resample_interval)
                hour_range = "%s-%s" % (start_dt, end_dt)
                val = 0
                if not isnan(avg):
                    val = int(avg + 0.5)
                grouped_3hrs.append({hour_range: val})


            output_json = {
                'start_date': start_date.strftime("%Y-%d-%m %H:%M"),
                'end_date': end_date.strftime("%Y-%d-%m %H:%M"),
                'weekday_averages': weekdays_avg,
                'hourly_averages': grouped_3hrs
            }
            camera_sites_stats[camera_site] = output_json

        with open(self._output_file, "w") as json_output_file:
            json.dump(output_json, json_output_file, indent=2)

    except Exception as e:
        logger.exception(e)