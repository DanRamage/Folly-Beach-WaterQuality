import sys
sys.path.append('../commonfiles/python')

import math
from datetime import datetime, timedelta
from pytz import timezone
from shapely.geometry import Polygon
import logging.config

import time
from wqHistoricalData import wq_data
from wqXMRGProcessing import wqDB
from wqHistoricalData import station_geometry,sampling_sites, wq_defines, geometry_list
from date_time_utils import get_utc_epoch
from NOAATideData import noaaTideDataExt
from xeniaSQLAlchemy import xeniaAlchemy, multi_obs, func
from xeniaSQLiteAlchemy import xeniaAlchemy as sl_xeniaAlchemy, multi_obs as sl_multi_obs, func as sl_func
from sqlalchemy import or_
from stats import calcAvgSpeedAndDir
from romsTools import closestCellFromPtInPolygon
from xenia import qaqcTestFlags
from unitsConversion import uomconversionFunctions
from astronomicalCalcs import moon


class follybeach_wq_data(wq_data):
  """
  Function: __init__
  Purpose: Initializes the class.
  Parameters:
    boundaries - The boundaries for the NEXRAD data the site falls within, this is required.
    xenia_database_name - The full file path to the xenia database that houses the NEXRAD and other
      data we use in the models. This is required.
  """

  def __init__(self, **kwargs):
    wq_data.__init__(self, **kwargs)

    self.site = None
    # The main station we retrieve the values from.
    self.tide_station = None
    # These are the settings to correct the tide for the subordinate station.
    self.tide_station_settings = None
    self.tide_data_obj = None

    self.logger.debug("Connection to xenia nexrad db: %s" % (kwargs['xenia_nexrad_db_name']))
    self.nexrad_db = wqDB(kwargs['xenia_nexrad_db_name'], type(self).__name__)
    try:
      #Connect to the xenia database we use for observations aggregation.
      self.xenia_obs_db = xeniaAlchemy()
      if self.xenia_obs_db.connectDB(kwargs['xenia_obs_db_type'], kwargs['xenia_obs_db_user'], kwargs['xenia_obs_db_password'], kwargs['xenia_obs_db_host'], kwargs['xenia_obs_db_name'], False):
        self.logger.info("Succesfully connect to DB: %s at %s" %(kwargs['xenia_obs_db_name'],kwargs['xenia_obs_db_host']))
      else:
        self.logger.error("Unable to connect to DB: %s at %s." %(kwargs['xenia_obs_db_name'],kwargs['xenia_obs_db_host']))


    except Exception,e:
      self.logger.exception(e)
      raise

    self.units_conversion = uomconversionFunctions(kwargs['units_file'])


  def reset(self, **kwargs):
    if self.site is None or self.site != kwargs['site']:
      self.site = kwargs['site']
      #The main station we retrieve the values from.
      self.tide_station_settings = kwargs['tide_station_settings']
      self.tide_station = self.tide_station_settings['tide_station']

      #self.tide_data_obj = None
      #if 'tide_data_obj' in kwargs and kwargs['tide_data_obj'] is not None:
      #  self.tide_data_obj = kwargs['tide_data_obj']

      self.platforms_info = kwargs['platform_info']

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

    '''    
    #Build variables for the base tide station.
    var_name = 'tide_range_%s' % (self.tide_station)
    wq_tests_data[var_name] = wq_defines.NO_DATA
    var_name = 'tide_hi_%s' % (self.tide_station)
    wq_tests_data[var_name] = wq_defines.NO_DATA
    var_name = 'tide_lo_%s' % (self.tide_station)
    wq_tests_data[var_name] = wq_defines.NO_DATA
    var_name = 'tide_stage_%s' % (self.tide_station)
    wq_tests_data[var_name] = wq_defines.NO_DATA
    '''
    #Build variables for the subordinate tide station.
    var_name = 'tide_range_%s' % (self.tide_station_settings['offset_tide_station'])
    wq_tests_data[var_name] = wq_defines.NO_DATA
    var_name = 'tide_hi_%s' % (self.tide_station_settings['offset_tide_station'])
    wq_tests_data[var_name] = wq_defines.NO_DATA
    var_name = 'tide_lo_%s' % (self.tide_station_settings['offset_tide_station'])
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

  """
  Function: query_data
  Purpose: Retrieves all the data used in the modelling project.
  Parameters:
    start_data - Datetime object representing the starting date to query data for.
    end_date - Datetime object representing the ending date to query data for.
    wq_tests_data - A OrderedDict object where the retrieved data is store.
  Return:
    None
  """

  def query_data(self, start_date, end_date, wq_tests_data, reset_site_specific_data_only):
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

  def get_tide_data(self, start_date, wq_tests_data):
    start_time = time.time()
    primary_tide_station = self.tide_station
    primary_station_range = 'tide_range_%s' % (primary_tide_station)
    #If we don't have data for this tide station, retrieve it. The self.tide_station_settings
    #list has the primary and subordinate stations, and the primary station can occur in multiple
    #subrdinates, so we only need to query the data once for the primary.
    #if wq_tests_data[primary_station_range] == wq_defines.NO_DATA:
    if self.logger:
      self.logger.debug("Start retrieving tide data for station: %s date: %s" % (primary_tide_station, start_date))

    tide = noaaTideDataExt(use_raw=True, logger=self.logger)

    tide_start_time = (start_date - timedelta(hours=24))
    tide_end_time = start_date

    #Try and query the NOAA soap service. We give ourselves 5 tries.
    for x in range(0, 5):
      if self.logger:
        self.logger.debug("Attempt: %d retrieving tide data for station." % (x + 1))
        tide_data = tide.calcTideRangePeakDetect(beginDate=tide_start_time,
                                                   endDate=tide_end_time,
                                                   station=primary_tide_station,
                                                   datum='MLLW',
                                                   units='feet',
                                                   timezone='GMT',
                                                   smoothData=False,
                                                   write_tide_data=False)
      if tide_data is not None:
        break
    if tide_data is not None:
      tide_range = tide_data['HH']['value'] - tide_data['LL']['value']
      primary_tide = {}
      primary_tide['tide_range_%s' % (primary_tide_station)] = tide_range
      primary_tide['tide_hi_%s' % (primary_tide_station)] = float(tide_data['HH']['value'])
      primary_tide['tide_lo_%s' % (primary_tide_station)] = float(tide_data['LL']['value'])
      primary_tide['tide_stage_%s' % (primary_tide_station)] = tide_data['tide_stage']
      '''
      wq_tests_data['tide_range_%s' % (primary_tide_station)] = tide_range
      wq_tests_data['tide_hi_%s' % (primary_tide_station)] = float(tide_data['HH']['value'])
      wq_tests_data['tide_lo_%s' % (primary_tide_station)] = float(tide_data['LL']['value'])
      wq_tests_data['tide_stage_%s' % (primary_tide_station)] = tide_data['tide_stage']
      '''
    # Save subordinate station values

    subordinate_station_var_name ='tide_range_%s' % (self.tide_station_settings['offset_tide_station'])
    if wq_tests_data[subordinate_station_var_name] == wq_defines.NO_DATA:
      if primary_tide[primary_station_range] != wq_defines.NO_DATA:
        #The subordinate calcualtions are made from the primary station. NOAA provides
        #the offset constants to apply against the primary results.
        offset_hi = primary_tide['tide_hi_%s' % (primary_tide_station)] * self.tide_station_settings['hi_tide_height_offset']
        offset_lo = primary_tide['tide_lo_%s' % (primary_tide_station)] * self.tide_station_settings['lo_tide_height_offset']

        wq_tests_data[subordinate_station_var_name] = offset_hi - offset_lo
        wq_tests_data['tide_hi_%s' % (self.tide_station_settings['offset_tide_station'])] = offset_hi
        wq_tests_data['tide_lo_%s' % (self.tide_station_settings['offset_tide_station'])] = offset_lo

    if self.logger:
      self.logger.debug("Finished retrieving tide data for station: %s date: %s in %f seconds" % (self.tide_station, start_date, time.time()-start_time))

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
