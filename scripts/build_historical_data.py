import sys
sys.path.append('../commonfiles/python')
import os
import optparse
import ConfigParser
import logging.config
from pytz import timezone
import csv
from datetime import datetime, timedelta
import time
import requests
import json
from pysqlite2 import dbapi2 as sqlite3
from pyoos.collectors.usgs.usgs_rest import UsgsRest
from pyoos.collectors.coops.coops_sos import CoopsSos
from pyoos.collectors.ndbc.ndbc_sos import NdbcSos
from wqDatabase import wqDB
from unitsConversion import uomconversionFunctions
from build_tide_file import create_tide_data_file_mp

ndbc_sites = ['FBIS1']
'''
{
'sos_obs_query': 'air_temperature',
'sites': ['FBIS1'],
'xenia_obs': [
  {
    "sos_obs_name": "air_temperature",
    "units": "celsius",
    "xenia_name": "air_temperature",
    "xenia_units": "celsius"
  }
]
},
'''
ndbc_obs = [
        {
            'sos_obs_query': 'air_pressure_at_sea_level',
            'sites': ['FBIS1'],
            'xenia_obs': [
                {
                    "sos_obs_name": "air_pressure_at_sea_level",
                    "units": "hPa",
                    "xenia_name": "air_pressure",
                    "xenia_units": "mb"
                }
            ]
        },

        {
            'sos_obs_query': 'air_temperature',
            'sites': ['FBIS1'],
            'xenia_obs': [
              {
                "sos_obs_name": "air_temperature",
                "units": "celsius",
                "xenia_name": "air_temperature",
                "xenia_units": "celsius"
              }
            ]
        },
        {
            'sos_obs_query': 'winds',
            'sites': ['FBIS1'],
            'xenia_obs': [
                {
                    "sos_obs_name": "wind_speed",
                    "units": "m_s-1",
                    "xenia_name": "wind_speed",
                    "xenia_units": "m_s-1"
                },
                {
                    "sos_obs_name": "wind_from_direction",
                    "units": "degree",
                    "xenia_name": "wind_from_direction",
                    "xenia_units": "degree"
                }

            ]
        }

    ]
carocoops_sites = ['CAP2', 'FRP2']
carocoops_obs = {
        "sea_water_practical_salinity": {
            "units": "psu",
            "xenia_name": "salinity",
            "xenia_units": "psu"
        },
        "sea_water_temperature": {
            "units": "celsius",
            "xenia_name": "water_temperature",
            "xenia_units": "celsius"
        }
}
'''
,
        "sea_water_temperature": {
            "units": "celsius",
            "xenia_name": "water_temperature",
            "xenia_units": "celsius"
        }
'''

def flatten_element(p):
  rd = {'time': p.time}
  for m in p.members:
    rd[m['standard']] = { 'value': m['value'], 'units': m['unit'] }

  return rd

def get_sample_dates(bacteria_files, start_date):
  start_time = time.time()
  est_tz = timezone('US/Eastern')
  utc_tz = timezone('UTC')
  logger = logging.getLogger(__name__)
  logger.debug("Starting get_dates")
  dates = []

  for file in bacteria_files:
    with open(file, "r") as data_file_obj:
        row_num = 0
        for row in data_file_obj:
            if row_num > 0:
                columns = row.split(',')
                date_obj = (utc_tz.localize(datetime.strptime(columns[1], '%Y-%m-%dT%H:%M:%SZ'))).astimezone(est_tz)
                if start_date is not None:
                    if date_obj >= start_date:
                      date_in_list = [date for date in dates if date_obj == date]
                      if len(date_in_list) == 0:
                        logger.debug("Adding date: %s to list" % (date_obj))
                        dates.append(date_obj)
                else:
                    date_in_list = [date for date in dates if date_obj == date]
                    if len(date_in_list) == 0:
                        logger.debug("Adding date: %s to list" % (date_obj))
                        dates.append(date_obj)
            row_num += 1
  dates.sort()
  logger.debug("Finished get_dates in %f seconds." % (time.time()-start_time))
  return dates

def process_ndbc_data(**kwargs):
    out_directory = kwargs['output_directory']
    all_dates = kwargs['all_dates']
    db_obj = kwargs['db_obj']
    units_converter = kwargs['units_converter']

    for site in ndbc_sites:
        get_ndbc_data(site, all_dates, units_converter, db_obj)
    return

def get_ndbc_data(site, dates, units_coverter, db_obj):
  start_time = time.time()
  logger = logging.getLogger(__name__)
  logger.debug("Starting get_ndbc_data")

  row_entry_date = datetime.now()
  utc_tz = timezone('UTC')
  eastern_tz= timezone('US/Eastern')

  platform_handle = 'ndbc.%s.met' % (site)
  if db_obj.platformExists(platform_handle) == -1:
    obs_list = []
    for obs_setup in ndbc_obs:
      if site in obs_setup['sites']:
        for xenia_obs in obs_setup['xenia_obs']:
          obs_list.append({'obs_name': xenia_obs['xenia_name'],
                           'uom_name': xenia_obs['xenia_units'],
                           's_order': 1})
    db_obj.buildMinimalPlatform(platform_handle, obs_list)

  sos_query = NdbcSos()
  #dates.sort(reverse=True)
  dates.sort(reverse=True)
  for rec_date in dates:
    logger.debug("Query site: %s for date: %s" % (site, rec_date))
    sos_query.clear()
    utc_end_date = rec_date.astimezone(utc_tz) + timedelta(hours=24)
    start_date = rec_date.astimezone(utc_tz) - timedelta(hours=24)

    for obs_setup in ndbc_obs:
      if site in obs_setup['sites']:
        date_ndx = None
        value_ndx = None
        lat_ndx = None
        lon_ndx = None
        depth_ndx = None

        sos_query.filter(features=[site], start=start_date, end=utc_end_date, variables=[obs_setup['sos_obs_query']])
        try:
          #results = nos_query.collect()
          response = sos_query.raw(responseFormat="text/csv")
        except Exception as e:
          logger.exception(e)
        else:
          csv_reader = csv.reader(response.split('\n'), delimiter=',')
          line_cnt = 0

          for row in csv_reader:
            for xenia_obs_setup in obs_setup['xenia_obs']:
              obs_type = xenia_obs_setup['xenia_name']
              uom_type = xenia_obs_setup['xenia_units']
              s_order = 1

              if line_cnt > 0 and len(row):
                obs_date = datetime.strptime(row[date_ndx], '%Y-%m-%dT%H:%M:%SZ')
                try:
                  obs_val = float(row[value_ndx])
                except ValueError as e:
                  logger.exception(e)
                  obs_val = 0.0
                logger.debug("Adding obs: %s(%s) Date: %s Value: %s S_Order: %d" %\
                             (obs_type, uom_type, obs_date, obs_val, s_order))
                #ADd sensor if it doesn't exist
                #self, platform_handle, sensor_name, uom_name, s_order=1
                db_obj.add_sensor_to_platform(platform_handle, obs_type, uom_type, 1)
                depth = 0
                if depth_ndx is not None:
                  depth = float(row[depth_ndx])
                try:
                    if not db_obj.addMeasurement(obs_type,
                                            uom_type,
                                            platform_handle,
                                            obs_date.strftime('%Y-%m-%dT%H:%M:%S'),
                                            float(row[lat_ndx]),
                                            float(row[lon_ndx]),
                                            depth,
                                            [obs_val],
                                            sOrder=s_order,
                                            autoCommit=True,
                                            rowEntryDate=row_entry_date ):
                      logger.error(db_obj.lastErrorMsg)
                except (Exception, sqlite3.IntegrityError) as e:
                    logger.exception(e)
              else:
                if value_ndx is None:
                  for ndx,val in enumerate(row):
                    if val.lower().find(xenia_obs_setup['sos_obs_name']) != -1:
                      value_ndx = ndx
                    if val.lower().find('date_time') != -1:
                      date_ndx = ndx
                    if val.lower().find('latitude') != -1:
                      lat_ndx = ndx
                    if val.lower().find('longitude') != -1:
                      lon_ndx = ndx
                    if val.lower().find('depth') != -1:
                      depth_ndx = ndx
            line_cnt += 1

  logger.debug("Finished get_ndbc_data in %f seconds" % (time.time() - start_time))

def process_carocoops_data(**kwargs):
    all_dates = kwargs['all_dates']
    db_obj = kwargs['db_obj']
    units_converter = kwargs['units_converter']
    for site in carocoops_sites:
        platform_handle = 'carocoops.%s.buoy' % (site)
        get_carocoops_data(platform_handle,
                           carocoops_obs,
                           units_converter,
                           db_obj,
                            all_dates)

def get_carocoops_data(platform_handle,
                           carocoops_observations,
                           units_converter,
                           xenia_db,
                           unique_dates):

  logger = logging.getLogger(__name__)
  utc_tz = timezone('UTC')
  eastern_tz= timezone('US/Eastern')
  url = "http://services.cormp.org/data.php"
  row_entry_date = datetime.now()


  #if xenia_db.platformExists(platform_handle) == -1:
  s_order = 1
  obs_list = []
  for obs_key in carocoops_observations:
    obs_info = carocoops_observations[obs_key]
    obs_list.append({'obs_name': obs_info['xenia_name'],
                     'uom_name': obs_info['xenia_units'],
                     's_order': s_order})
  xenia_db.buildMinimalPlatform(platform_handle, obs_list)

  platform_name_parts = platform_handle.split('.')
  for start_date in unique_dates:
    #utc_start_date = (eastern_tz.localize(datetime.strptime(start_date, '%Y-%m-%d'))).astimezone(utc_tz)
    utc_start_date = start_date.astimezone(utc_tz)
    start_date = utc_start_date - timedelta(hours=24)

    logger.debug("Platform: %s Begin Date: %s End Date: %s" % (platform_handle, start_date, utc_start_date))

    data_time = '%s/%s' % (start_date.strftime('%Y-%m-%dT%H:%M:%S'), utc_start_date.strftime('%Y-%m-%dT%H:%M:%S'))
    params = {
      'format': 'json',
      'platform': platform_name_parts[1].lower(),
      'time': data_time
    }
    try:
      result = requests.get(url, params=params)
      logger.debug("URL Request: %s" % (result.url))
    except Exception as e:
      logger.exception(e)
    else:
      if result.status_code == 200:
        try:
          json_data = json.loads(result.text)
        except Exception as e:
          logger.exception(e)
        else:
          coords = json_data['geometry']['coordinates']
          parameters = json_data['properties']['parameters']
          for param in parameters:
            obs_type = None
            uom_type = None
            s_order = 1

            if param['id'] == 'sea_water_temperature':
              obs_type = carocoops_observations[param['id']]['xenia_name']
              uom_type = 'celsius'

            elif param['id'] == 'sea_water_practical_salinity':
                obs_type = carocoops_observations[param['id']]['xenia_name']
                uom_type = 'psu'

            if obs_type is not None:
              try:
                observations = param['observations']
                for ndx, obs_val in enumerate(observations['values']):
                  try:
                    obs_val = float(obs_val)
                  except ValueError as e:
                    logger.exception(e)
                  else:
                    quality = int(observations['quality_levels'][ndx])
                    if quality == 0 or quality == 3:
                        logger.debug("Adding obs: %s(%s) Date: %s Value: %s S_Order: %d" %\
                                     (obs_type, uom_type, observations['times'][ndx], obs_val, s_order))
                        xenia_db.addMeasurement(obs_type,
                                                uom_type,
                                                platform_handle,
                                                observations['times'][ndx],
                                                float(coords[1]),
                                                float(coords[0]),
                                                0,
                                                [obs_val],
                                                sOrder=s_order,
                                                autoCommit=True,
                                                rowEntryDate=row_entry_date )
                    else:
                        logger.error("Quality(%d) fails Obs: %s(%s) Date: %s Value: %s S_Order: %d" %\
                                     (quality, obs_type, uom_type, observations['times'][ndx], obs_val, s_order))

              except Exception as e:
                logger.exception(e)

      else:
        logger.ERROR("Request failed with code: %d" % (result.status_code))

tide_stations = ['8665530']
def process_tide_data(**kwargs):
  start_time = time.time()
  logger = logging.getLogger('chs_historical_logger')
  logger.debug("Starting process_tide_data")

  out_directory = kwargs['output_directory']
  all_dates = kwargs['all_dates']
  db_obj = kwargs['db_obj']
  units_converter = kwargs['units_converter']
  log_conf_file = kwargs['log_config_file']
  eastern_tz = timezone('US/Eastern')
  tide_dates = []
  for tide_station in tide_stations:
    for date_rec in all_dates:
      #Add 24 hours since we want to make sure we have +/- 24 hours around our date. This
      #way we can have enough data to use if we want the sample times starting at midnight
      #or we want to use the actual sample time. Instead of getting the data for each
      #time for a given sample day, just do a more coarse approach.
      #tide_date = date_rec + timedelta(hours=24)
      #tide_date = tide_date.replace(hour=0, minute=0, second=0)
      tide_dates.append(date_rec)

  tide_output_file = os.path.join(out_directory, "%s.csv" % (tide_station))
  create_tide_data_file_mp(tide_station,
                           tide_dates,
                           tide_output_file,
                           4,
                           log_conf_file,
                           True)

  logger.debug("Finished process_tide_data in %f seconds" % (time.time()-start_time))

def main():
    parser = optparse.OptionParser()
    parser.add_option("--ConfigFile", dest="config_file",
                      help="INI Configuration file.")
    parser.add_option("--GetNDBCData", dest="get_ndbc_data", action="store_true",
                      help="")
    parser.add_option("--GetCarocoopsData", dest="get_carocoops_data", action="store_true",
                      help="")
    parser.add_option("--GetTideData", dest="get_tide_data", action="store_true",
                      help="")

    parser.add_option("--DataOutputDirectory", dest="data_directory",
                      help="")
    parser.add_option("--BacteriaFiles", dest="bacteria_files",
                      help="")
    parser.add_option("--LogConfigFile", dest="log_config_file",
                      help="INI Configuration file.")

    (options, args) = parser.parse_args()

    config_file = ConfigParser.RawConfigParser()
    config_file.read(options.config_file)

    logging.config.fileConfig(options.log_config_file)
    logger = logging.getLogger('wq_processing_logger')
    logger.info("Log file opened.")

    historical_db_name = config_file.get('database', 'name')
    units_file = config_file.get('units_conversion', 'config_file')

    dates = get_sample_dates(options.bacteria_files.split(','), None)

    units_conversion = uomconversionFunctions(units_file)
    historic_db = wqDB(historical_db_name, __name__)
    if options.get_carocoops_data:
        process_carocoops_data(units_converter=units_conversion,
                               db_obj=historic_db,
                               all_dates=dates)

    if options.get_ndbc_data:
        process_ndbc_data(output_directory=options.data_directory,
                          all_dates=dates,
                          db_obj=historic_db,
                          units_converter=units_conversion)

    if options.get_tide_data:
        # For tides, we want to use the samples date and time.
        tide_dates = []
        last_date = None
        est_tz = timezone('US/Eastern')

        for sample_date in dates:
            if (last_date is None or last_date != sample_date):
                last_date = sample_date
                if last_date not in tide_dates:
                    tide_dates.append(last_date)


        process_tide_data(output_directory=options.data_directory,
                          query_tide=True,
                          all_dates=tide_dates,
                          db_obj=historic_db,
                          units_converter=units_conversion,
                          log_config_file=options.log_config_file)

    logger.info("Log file closed.")

    return

if __name__ == "__main__":
     main()