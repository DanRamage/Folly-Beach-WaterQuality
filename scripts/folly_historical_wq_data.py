import sys
sys.path.append('../commonfiles/python')


import os
import logging.config
import optparse
import ConfigParser
import csv
from datetime import datetime, timedelta
import time
from pytz import timezone
from shapely.geometry import Polygon
import netCDF4 as nc
import numpy as np
from bisect import bisect_left,bisect_right
import csv
from collections import OrderedDict
import math

from NOAATideData import noaaTideData
from wqHistoricalData import tide_data_file
from wqHistoricalData import tide_data_file_ex,station_geometry,sampling_sites, wq_defines, geometry_list
from wq_output_results import wq_sample_data,wq_samples_collection,wq_advisories_file,wq_station_advisories_file
from wq_sites import wq_sample_sites

from wqHistoricalData import wq_data
from wq_output_results import wq_sample_data

from xeniaSQLiteAlchemy import xeniaAlchemy as sl_xeniaAlchemy, multi_obs as sl_multi_obs, func as sl_func
from sqlalchemy import or_
from xenia import qaqcTestFlags
from stats import calcAvgSpeedAndDir
from astronomicalCalcs import moon
#import matplotlib
#matplotlib.use('Agg')
#import matplotlib.pyplot as plt

from wqDatabase import wqDB

from date_time_utils import *


def find_le(a, x):
  'Find rightmost ndx less than or equal to x'
  i = bisect_right(a, x)
  if i:
    return i - 1
  raise ValueError


def find_ge(a, x):
  'Find leftmost ndx greater than or equal to x'
  i = bisect_left(a, x)
  if i != len(a):
    return i
  raise ValueError


class folly_historical_wq_data(wq_data):
  def __init__(self, **kwargs):
    wq_data.__init__(self, **kwargs)
    try:
      config_file = ConfigParser.RawConfigParser()
      config_file.read(kwargs['config_file'])
      xenia_database_name = config_file.get('database', 'name')

      tide_file_name = config_file.get('tide_station', 'tide_file')

    except (ConfigParser.Error, Exception) as e:
      self.logger.exception(e)
      raise
    else:
      self.site = None
      #The main station we retrieve the values from.
      self.tide_station =  None
      #These are the settings to correct the tide for the subordinate station.
      self.tide_offset_settings = None
      self.tide_data_obj = None


      try:
        self.tide_data_obj = tide_data_file_ex()
        self.tide_data_obj.open(tide_file_name)
      except (IOError, Exception) as e:
        self.logger.exception(e)
        raise
      if self.logger:
        self.logger.debug("Connection to xenia db: %s" % (xenia_database_name))
      self.nexrad_db = wqDB(xenia_database_name, type(self).__name__)
      try:
        #Connect to the xenia database we use for observations aggregation.
        self.xenia_obs_db = sl_xeniaAlchemy()
        if self.xenia_obs_db.connectDB('sqlite', None, None, xenia_database_name, None, False):
          self.logger.info("Succesfully connect to DB: %s" %(xenia_database_name))
        else:
          self.logger.error("Unable to connect to DB: %s." %(xenia_database_name))
      except Exception as e:
        self.logger.exception(e)
        raise

  def reset(self, **kwargs):
    if self.site is None or self.site != kwargs['site']:
      self.site = kwargs['site']
      #The main station we retrieve the values from.
      self.tide_station = kwargs['tide_station']
      #These are the settings to correct the tide for the subordinate station.
      self.tide_offset_settings = kwargs['tide_offset_params']

      #self.tide_data_obj = None
      #if 'tide_data_obj' in kwargs and kwargs['tide_data_obj'] is not None:
      #  self.tide_data_obj = kwargs['tide_data_obj']

      self.platforms_info = kwargs['platform_info']

    start_date = kwargs['start_date']

  """
  Function: initialize_return_data
  Purpose: INitialize our ordered dict with the data variables and assign a NO_DATA
    initial value.
  Parameters:
    wq_tests_data - An OrderedDict that is initialized.
  Return:
    None
  """
  def initialize_return_data(self, wq_tests_data):
    if self.logger:
      self.logger.debug("Creating and initializing data dict.")
    #Build variables for the base tide station.
    var_name = 'tide_range_%s' % (self.tide_station)
    wq_tests_data[var_name] = wq_defines.NO_DATA
    var_name = 'tide_hi_%s' % (self.tide_station)
    wq_tests_data[var_name] = wq_defines.NO_DATA
    var_name = 'tide_lo_%s' % (self.tide_station)
    wq_tests_data[var_name] = wq_defines.NO_DATA
    var_name = 'tide_stage_%s' % (self.tide_station)
    wq_tests_data[var_name] = wq_defines.NO_DATA

    #Build variables for the subordinate tide station.
    var_name = 'tide_range_%s' % (self.tide_offset_settings['tide_station'])
    wq_tests_data[var_name] = wq_defines.NO_DATA
    var_name = 'tide_hi_%s' % (self.tide_offset_settings['tide_station'])
    wq_tests_data[var_name] = wq_defines.NO_DATA
    var_name = 'tide_lo_%s' % (self.tide_offset_settings['tide_station'])
    wq_tests_data[var_name] = wq_defines.NO_DATA

    wq_tests_data['moon_percent_illumination'] = wq_defines.NO_DATA
    wq_tests_data['moon_phase_angle'] = wq_defines.NO_DATA

    for platform_nfo in self.platforms_info:
      handle = platform_nfo['platform_handle']
      for obs_nfo in platform_nfo['observations']:
        var_name = '%s_avg_%s' % (handle.replace('.', '_'), obs_nfo['observation'])
        wq_tests_data[var_name] = wq_defines.NO_DATA

    for boundary in self.site.contained_by:
      if len(boundary.name):
        for prev_hours in range(24, 216, 24):
          clean_var_boundary_name = boundary.name.lower().replace(' ', '_')
          var_name = '%s_nexrad_summary_%d' % (clean_var_boundary_name, prev_hours)
          wq_tests_data[var_name] = wq_defines.NO_DATA

        var_name = '%s_nexrad_dry_days_count' % (clean_var_boundary_name)
        wq_tests_data[var_name] = wq_defines.NO_DATA

        var_name = '%s_nexrad_rainfall_intensity' % (clean_var_boundary_name)
        wq_tests_data[var_name] = wq_defines.NO_DATA

        var_name = '%s_nexrad_total_1_day_delay' % (clean_var_boundary_name)
        wq_tests_data[var_name] = wq_defines.NO_DATA
        var_name = '%s_nexrad_total_2_day_delay' % (clean_var_boundary_name)
        wq_tests_data[var_name] = wq_defines.NO_DATA
        var_name = '%s_nexrad_total_3_day_delay' % (clean_var_boundary_name)
        wq_tests_data[var_name] = wq_defines.NO_DATA


    if self.logger:
      self.logger.debug("Finished creating and initializing data dict.")

    return

  def query_data(self, start_date, end_date, wq_tests_data):
    if self.logger:
      self.logger.debug("Site: %s start query data for datetime: %s" % (self.site.name, start_date))

    self.initialize_return_data(wq_tests_data)

    for platform in self.platforms_info:
      for obs_nfo in platform['observations']:
        self.get_platform_data(platform['platform_handle'],
                               obs_nfo['observation'], obs_nfo['uom'],
                               start_date,
                               wq_tests_data)

    self.get_nexrad_data(start_date, wq_tests_data)
    self.get_tide_data(start_date, wq_tests_data)
    self.get_moon_data(start_date, wq_tests_data)

    if self.logger:
      self.logger.debug("Site: %s Finished query data for datetime: %s" % (self.site.name, start_date))


  def get_moon_data(self, start_date, wq_tests_data):
    try:
      utc_tz = timezone('UTC')
      utc_time = start_date.astimezone(utc_tz)
      moonCalcs = moon(utc_time.strftime("%Y-%m-%d 00:00:00"))
      moonCalcs.doCalcs()
      wq_tests_data['moon_percent_illumination'] = moonCalcs.percentIllumination
      wq_tests_data['moon_phase_angle'] = moonCalcs.moonPhaseAngle
    except Exception as e:
      self.logger.exception(e)
    return

  def get_nexrad_data(self, start_date, wq_tests_data):
    start_time = time.time()
    if self.logger:
      self.logger.debug("Start retrieving nexrad data datetime: %s" % (start_date.strftime('%Y-%m-%d %H:%M:%S')))

    # Collect the radar data for the boundaries.
    for boundary in self.site.contained_by:
      clean_var_bndry_name = boundary.name.lower().replace(' ', '_')

      platform_handle = 'nws.%s.radarcoverage' % (boundary.name)
      if self.logger:
        self.logger.debug("Start retrieving nexrad platfrom: %s" % (platform_handle))
      # Get the radar data for previous 8 days in 24 hour intervals
      for prev_hours in range(24, 216, 24):
        var_name = '%s_nexrad_summary_%d' % (clean_var_bndry_name, prev_hours)
        radar_val = self.nexrad_db.getLastNHoursSummaryFromRadarPrecip(platform_handle,
                                                                       start_date,
                                                                       prev_hours,
                                                                       'precipitation_radar_weighted_average',
                                                                       'mm')
        if radar_val != None:
          # Convert mm to inches
          wq_tests_data[var_name] = radar_val * 0.0393701
        else:
          if self.logger:
            self.logger.error("No data available for boundary: %s Date: %s. Error: %s" % (
            var_name, start_date, self.nexrad_db.getErrorInfo()))

      # calculate the X day delay totals
      if wq_tests_data['%s_nexrad_summary_48' % (clean_var_bndry_name)] != wq_defines.NO_DATA and \
          wq_tests_data['%s_nexrad_summary_24' % (clean_var_bndry_name)] != wq_defines.NO_DATA:
        wq_tests_data['%s_nexrad_total_1_day_delay' % (clean_var_bndry_name)] = wq_tests_data['%s_nexrad_summary_48' % (
        clean_var_bndry_name)] - wq_tests_data['%s_nexrad_summary_24' % (clean_var_bndry_name)]

      if wq_tests_data['%s_nexrad_summary_72' % (clean_var_bndry_name)] != wq_defines.NO_DATA and \
          wq_tests_data['%s_nexrad_summary_48' % (clean_var_bndry_name)] != wq_defines.NO_DATA:
        wq_tests_data['%s_nexrad_total_2_day_delay' % (clean_var_bndry_name)] = wq_tests_data['%s_nexrad_summary_72' % (
        clean_var_bndry_name)] - wq_tests_data['%s_nexrad_summary_48' % (clean_var_bndry_name)]

      if wq_tests_data['%s_nexrad_summary_96' % (clean_var_bndry_name)] != wq_defines.NO_DATA and \
          wq_tests_data['%s_nexrad_summary_72' % (clean_var_bndry_name)] != wq_defines.NO_DATA:
        wq_tests_data['%s_nexrad_total_3_day_delay' % (clean_var_bndry_name)] = wq_tests_data['%s_nexrad_summary_96' % (
        clean_var_bndry_name)] - wq_tests_data['%s_nexrad_summary_72' % (clean_var_bndry_name)]

      prev_dry_days = self.nexrad_db.getPrecedingRadarDryDaysCount(platform_handle,
                                                                   start_date,
                                                                   'precipitation_radar_weighted_average',
                                                                   'mm')
      if prev_dry_days is not None:
        var_name = '%s_nexrad_dry_days_count' % (clean_var_bndry_name)
        wq_tests_data[var_name] = prev_dry_days

      rainfall_intensity = self.nexrad_db.calcRadarRainfallIntensity(platform_handle,
                                                                     start_date,
                                                                     60,
                                                                     'precipitation_radar_weighted_average',
                                                                     'mm')
      if rainfall_intensity is not None:
        var_name = '%s_nexrad_rainfall_intensity' % (clean_var_bndry_name)
        wq_tests_data[var_name] = rainfall_intensity

      if self.logger:
        self.logger.debug("Finished retrieving nexrad platfrom: %s" % (platform_handle))

    if self.logger:
      self.logger.debug("Finished retrieving nexrad data datetime: %s in %f seconds" % (start_date.strftime('%Y-%m-%d %H:%M:%S'),
                                                                                        time.time() - start_time))

  def get_tide_data(self, start_date, wq_tests_data):
    if self.logger:
      self.logger.debug("Start retrieving tide data for station: %s date: %s" % (self.tide_station, start_date))

    use_web_service = True
    if self.tide_data_obj is not None:
      use_web_service = False
      date_key = start_date.strftime('%Y-%m-%dT%H:%M:%S')
      if date_key in self.tide_data_obj:
        tide_rec = self.tide_data_obj[date_key]
        if tide_rec['range'] is not None:
          wq_tests_data['tide_range_%s' % (self.tide_station)] = tide_rec['range']

        if tide_rec['hh'] is not None:
          wq_tests_data['tide_hi_%s' % (self.tide_station)] = tide_rec['hh']

        if tide_rec['ll'] is not None:
          wq_tests_data['tide_lo_%s' % (self.tide_station)] = tide_rec['ll']

        if tide_rec['tide_stage'] is not None:
          wq_tests_data['tide_stage_%s' % (self.tide_station)] = tide_rec['tide_stage']

    #Save subordinate station values
    if wq_tests_data['tide_hi_%s'%(self.tide_station)] != wq_defines.NO_DATA:
      offset_hi = wq_tests_data['tide_hi_%s'%(self.tide_station)] * self.tide_offset_settings['hi_tide_height_offset']
      offset_lo = wq_tests_data['tide_lo_%s'%(self.tide_station)] * self.tide_offset_settings['lo_tide_height_offset']

      tide_station = self.tide_offset_settings['tide_station']
      wq_tests_data['tide_range_%s' % (tide_station)] = offset_hi - offset_lo
      wq_tests_data['tide_hi_%s' % (tide_station)] = offset_hi
      wq_tests_data['tide_lo_%s' % (tide_station)] = offset_lo

    if self.logger:
      self.logger.debug("Finished retrieving tide data for station: %s date: %s" % (self.tide_station, start_date))

    return

  def get_platform_data(self, platform_handle, variable, uom, start_date, wq_tests_data):
    start_time = time.time()
    try:
      self.logger.debug("Platform: %s Obs: %s(%s) Date: %s query" % (platform_handle, variable, uom, start_date))

      station = platform_handle.replace('.', '_')
      var_name = '%s_avg_%s' % (station, variable)
      end_date = start_date
      begin_date = start_date - timedelta(hours=24)
      dir_id = None
      sensor_id = self.xenia_obs_db.sensorExists(variable, uom, platform_handle, 1)
      if variable == 'wind_speed':
        dir_id = self.xenia_obs_db.sensorExists('wind_from_direction', 'degrees_true', platform_handle, 1)

      if sensor_id is not -1 and sensor_id is not None:
        recs = self.xenia_obs_db.session.query(sl_multi_obs) \
          .filter(sl_multi_obs.m_date >= begin_date.strftime('%Y-%m-%dT%H:%M:%S')) \
          .filter(sl_multi_obs.m_date < end_date.strftime('%Y-%m-%dT%H:%M:%S')) \
          .filter(sl_multi_obs.sensor_id == sensor_id) \
          .filter(or_(sl_multi_obs.qc_level == qaqcTestFlags.DATA_QUAL_GOOD, sl_multi_obs.qc_level == None)) \
          .order_by(sl_multi_obs.m_date).all()
        if dir_id is not None:
          dir_recs = self.xenia_obs_db.session.query(sl_multi_obs) \
            .filter(sl_multi_obs.m_date >= begin_date.strftime('%Y-%m-%dT%H:%M:%S')) \
            .filter(sl_multi_obs.m_date < end_date.strftime('%Y-%m-%dT%H:%M:%S')) \
            .filter(sl_multi_obs.sensor_id == dir_id) \
            .filter(or_(sl_multi_obs.qc_level == qaqcTestFlags.DATA_QUAL_GOOD, sl_multi_obs.qc_level == None)) \
            .order_by(sl_multi_obs.m_date).all()

        if len(recs):
          if variable == 'wind_speed':
            if sensor_id is not None and dir_id is not None:
              wind_dir_tuples = []
              direction_tuples = []
              scalar_speed_avg = None
              speed_count = 0
              for wind_speed_row in recs:
                for wind_dir_row in dir_recs:
                  if wind_speed_row.m_date == wind_dir_row.m_date:
                    # self.logger.debug("Building tuple for Speed(%s): %f Dir(%s): %f" % (
                    # wind_speed_row.m_date, wind_speed_row.m_value, wind_dir_row.m_date, wind_dir_row.m_value))
                    if scalar_speed_avg is None:
                      scalar_speed_avg = 0
                    scalar_speed_avg += wind_speed_row.m_value
                    speed_count += 1
                    # Vector using both speed and direction.
                    wind_dir_tuples.append((wind_speed_row.m_value, wind_dir_row.m_value))
                    # Vector with speed as constant(1), and direction.
                    direction_tuples.append((1, wind_dir_row.m_value))
                    break

              if len(wind_dir_tuples):
                avg_speed_dir_components = calcAvgSpeedAndDir(wind_dir_tuples)
                self.logger.debug("Platform: %s Avg Wind Speed: %f(m_s-1) %f(mph) Direction: %f" % (platform_handle,
                                                                                                    avg_speed_dir_components[
                                                                                                      0],
                                                                                                    avg_speed_dir_components[
                                                                                                      0],
                                                                                                    avg_speed_dir_components[
                                                                                                      1]))

                # Unity components, just direction with speeds all 1.
                avg_dir_components = calcAvgSpeedAndDir(direction_tuples)
                scalar_speed_avg = scalar_speed_avg / speed_count
                wq_tests_data[var_name] = scalar_speed_avg
                wind_dir_var_name = '%s_avg_%s' % (station, 'wind_from_direction')
                wq_tests_data[wind_dir_var_name] = avg_dir_components[1]
                self.logger.debug(
                  "Platform: %s Avg Scalar Wind Speed: %f(m_s-1) %f(mph) Direction: %f" % (platform_handle,
                                                                                           scalar_speed_avg,
                                                                                           scalar_speed_avg,
                                                                                           avg_dir_components[1]))
          #Calculate vector direction.
          elif variable == 'sea_surface_wave_to_direction':
            direction_tuples = []
            for dir_row in recs:
              # Vector with speed as constant(1), and direction.
              direction_tuples.append((1, dir_row.m_value))

            if len(direction_tuples):
              # Unity components, just direction with speeds all 1.
              avg_dir_components = calcAvgSpeedAndDir(direction_tuples)
              wq_tests_data[var_name] = avg_dir_components[1]
              self.logger.debug(
                "Platform: %s Avg Scalar Direction: %f" % (platform_handle,
                                                           avg_dir_components[1]))

          else:
            wq_tests_data[var_name] = sum(rec.m_value for rec in recs) / len(recs)
            self.logger.debug("Platform: %s Avg %s: %f Records used: %d" % (
              platform_handle, variable, wq_tests_data[var_name], len(recs)))

            if variable == 'water_conductivity':
              water_con = wq_tests_data[var_name]
              #if uom == 'uS_cm-1':
              water_con = water_con / 1000.0
              salinity_var = '%s_avg_%s' % (station, 'salinity')
              wq_tests_data[salinity_var] = 0.47413 / (math.pow((1 / water_con), 1.07) - 0.7464 * math.pow(10, -3))
              self.logger.debug("Platform: %s Avg %s: %f Records used: %d" % (
                platform_handle, 'salinity', wq_tests_data[salinity_var], len(recs)))
        else:
          self.logger.error(
            "Platform: %s sensor: %s(%s) Date: %s had no data" % (platform_handle, variable, uom, start_date))
      else:
        self.logger.error("Platform: %s sensor: %s(%s) does not exist" % (platform_handle, variable, uom))
      self.logger.debug("Platform: %s query finished in %f seconds" % (platform_handle, time.time()-start_time))
    except Exception as e:
      self.logger.exception(e)
      return False

    return True

def parse_file(**kwargs):
  start_time = time.time()
  #est_tz = timezone('US/Eastern')
  utc_tz = timezone('UTC')
  logger = logging.getLogger(__name__)
  logger.debug("Starting parse_file")
  dates = []
  start_date = kwargs.get('start_date', None)
  sample_collection = kwargs['samples_collection']
  sample_data_file = kwargs['data_file']

  logger.debug("Getting data from: %s" % (sample_data_file))
  header = ['Station','Date','Value']
  with open(sample_data_file, "r") as data_file:
    csv_reader = csv.DictReader(data_file, header)
    for row_ndx,row in enumerate(csv_reader):
      if row_ndx > 0:
        add_rec = False
        sample_data = wq_sample_data()
        date_obj = (utc_tz.localize(datetime.strptime(row['Date'], '%Y-%m-%dT%H:%M:%SZ')))
        if start_date is not None:
          if date_obj >= start_date:
            add_rec = True
        else:
          add_rec = False
        if add_rec:
          sample_data.date_time = date_obj
          sample_data.station = row['Station']
          sample_data.value = row['Value']
          sample_collection.append(sample_data)

  logger.debug("Finished processing file in %f seconds." % (time.time()-start_time))
  return

def main():
  parser = optparse.OptionParser()
  parser.add_option("--ConfigFile", dest="config_file", default=None,
                    help="INI Configuration file." )
  parser.add_option("--OutputDirectory", dest="output_dir", default=None,
                    help="Directory to save the historical data site files." )
  (options, args) = parser.parse_args()


  try:
    config_file = ConfigParser.RawConfigParser()
    config_file.read(options.config_file)

    logConfFile = config_file.get('logging', 'config_file')

    logging.config.fileConfig(logConfFile)
    logger = logging.getLogger('build_historical_logger')
    logger.info("Log file opened.")


    boundaries_location_file = config_file.get('boundaries_settings', 'boundaries_file')
    sites_location_file = config_file.get('boundaries_settings', 'sample_sites')
    wq_historical_db = config_file.get('database', 'name')

  except ConfigParser.Error as e:
    import traceback
    traceback.print_exc(e)
    sys.exit(-1)

  else:
    #Load the sample site information. Has name, location and the boundaries that contain the site.
    wq_sites = wq_sample_sites()
    wq_sites.load_sites(file_name=sites_location_file, boundary_file=boundaries_location_file)

    wq_historical_data = folly_historical_wq_data(config_file=options.config_file)

    sample_data_directory = '/Users/danramage/Documents/workspace/WaterQuality/FollyBeach-WaterQuality/data/sample_data'
    historical_sample_files = os.listdir(sample_data_directory)
    #start_date = timezone('UTC').localize(datetime.strptime('2005-01-01 00:00:00', '%Y-%m-%d %H:%M:%S'))
    utc_tz = timezone('UTC')
    est_tz = timezone('US/Eastern')
    data_start_date = utc_tz.localize(datetime.strptime('2005-01-01 00:00:00', '%Y-%m-%d %H:%M:%S'))
    for site in wq_sites:
      out_file = os.path.join(options.output_dir, "%s_historical_data.csv" % (site.name))
      write_header = True
      with open(out_file, 'w') as site_data_file:
        try:
          # Get the station specific tide stations
          tide_station = config_file.get(site.description, 'tide_station')
          offset_tide_station = config_file.get(site.description, 'offset_tide_station')
          offset_key = "%s_tide_data" % (offset_tide_station)
          tide_offset_settings = {
            'tide_station': config_file.get(offset_key, 'station_id'),
            'hi_tide_time_offset': config_file.getint(offset_key, 'hi_tide_time_offset'),
            'lo_tide_time_offset': config_file.getint(offset_key, 'lo_tide_time_offset'),
            'hi_tide_height_offset': config_file.getfloat(offset_key, 'hi_tide_height_offset'),
            'lo_tide_height_offset': config_file.getfloat(offset_key, 'lo_tide_height_offset')
          }
          #Get the platforms the site will use
          platforms = config_file.get(site.description, 'platforms').split(',')
          platform_nfo = []
          for platform in platforms:
            obs_uoms = config_file.get(platform,'observation').split(';')
            obs_uom_nfo = []
            for nfo in obs_uoms:
              obs,uom = nfo.split(',')
              obs_uom_nfo.append({'observation': obs,
                                  'uom': uom})
            platform_nfo.append({'platform_handle': config_file.get(platform,'handle'),
                                 'observations': obs_uom_nfo})

        except ConfigParser.Error as e:
          if logger:
            logger.exception(e)

        file_name = site.name
        for file in historical_sample_files:
          if file.find(file_name) != -1:
            samples_collection = wq_samples_collection()
            full_path = os.path.join(sample_data_directory, file)
            parse_file(data_file=full_path,
                       samples_collection=samples_collection,
                       start_date=data_start_date)

            try:
              sample_recs = samples_collection[site.name]
            except (KeyError, Exception) as e:
              logger.exception(e)
            else:
              sample_recs.sort(key=lambda x: x.date_time, reverse=False)
              auto_num = 1
              for sample_data in sample_recs:
                start_date = sample_data.date_time
                try:
                  wq_date_time_local = sample_data.date_time.astimezone(est_tz)
                  site_data = OrderedDict([
                    ('autonumber', auto_num),
                    ('station_name', site.name),
                    ('station_desc', site.description),
                    ('sample_datetime', wq_date_time_local),
                    ('sample_datetime_utc', sample_data.date_time),
                    ('enterococcus_value', sample_data.value),
                  ])
                  wq_historical_data.reset(site=site,
                                           tide_station=tide_station,
                                           tide_offset_params=tide_offset_settings,
                                           start_date=sample_data.date_time,
                                           platform_info=platform_nfo)
                  wq_historical_data.query_data(sample_data.date_time, sample_data.date_time, site_data)

                  header_buf = []
                  data = []
                  for key in site_data:
                    if write_header:
                      header_buf.append(key)
                    if site_data[key] != wq_defines.NO_DATA:
                      data.append(str(site_data[key]))
                    else:
                      data.append("")
                  if write_header:
                    site_data_file.write(",".join(header_buf))
                    site_data_file.write('\n')
                    header_buf[:]
                    write_header = False

                  site_data_file.write(",".join(data))
                  site_data_file.write('\n')
                  site_data_file.flush()
                  data[:]

                  auto_num += 1
                except Exception as e:
                  if logger:
                    logger.exception(e)
                  sys.exit(-1)
    """
    site_data = OrderedDict([('autonumber', 1),
                             ('station_name', row['SPLocation']),
                             ('sample_datetime', wq_date.strftime("%Y-%m-%d %H:%M:%S")),
                             ('sample_datetime_utc', wq_utc_date.strftime("%Y-%m-%d %H:%M:%S")),
                             ('County', row['County']),
                             ('enterococcus_value', row['enterococcus']),
                             ('enterococcus_code', row['enterococcus_code'])])
    """
  return

if __name__ == "__main__":
  main()