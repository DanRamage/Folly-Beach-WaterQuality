import os
import sys
sys.path.append('../commonfiles/python')

import logging.config
from datetime import datetime
import optparse
import time
from pytz import timezone
if sys.version_info[0] < 3:
  import ConfigParser
else:
  import configparser as ConfigParser
from collections import OrderedDict
import logging
from yapsy.PluginManager import PluginManager
from multiprocessing import Queue
import numpy as np

from xgboost_model import xgb_model, xgb_ensemble
from wq_sites import wq_sample_sites
from data_collector_plugin import data_collector_plugin
from wq_prediction_engine import wq_prediction_engine, data_result_types
from stats import stats
from wqHistoricalData import wq_defines
from follybeach_wq_data import follybeach_wq_data

class follybeach_prediction_engine(wq_prediction_engine):
  def __init__(self):
    self.logger = logging.getLogger(type(self).__name__)

  def build_test_objects(self, **kwargs):
    config_file = kwargs['config_file']
    site_name = kwargs['site_name']

    model_list = []
    #Get the sites test configuration ini, then build the test objects.
    try:
      test_config_file = config_file.get(site_name, 'prediction_config')
      entero_lo_limit = config_file.getint('entero_limits', 'limit_lo')
      entero_hi_limit = config_file.getint('entero_limits', 'limit_hi')
    except ConfigParser.Error as e:
        self.logger.exception(e)
    else:
      if len(test_config_file):
        self.logger.debug("Site: %s Model Config File: %s" % (site_name, test_config_file))

        model_config_file = ConfigParser.RawConfigParser()
        model_config_file.read(test_config_file)
        #Get the number of prediction models we use for the site.
        model_count = model_config_file.getint("xgboost_models", "model_count")
        self.logger.debug("Site: %s XGB Model count: %d" % (site_name, model_count))

        for cnt in range(model_count):
          section_name = "xgboost_model_%d" % (cnt+1)
          model_name = model_config_file.get(section_name, "name")
          classifier_type =  model_config_file.get(section_name, "classifier_type")
          model_pickle_file =  model_config_file.get(section_name, "pickle_file")
          self.logger.debug("Site: %s Model name: %s equation: %s" % (site_name, model_name, model_pickle_file))

          platforms = model_config_file.get(section_name, 'platforms').split(',')
          platform_nfo = []
          for platform in platforms:
            model_platform_section = '%s_%s' % (section_name, platform)
            obs_uoms = model_config_file.get(model_platform_section, 'observation').split(';')
            obs_uom_nfo = []
            for nfo in obs_uoms:
              obs, uom = nfo.split(',')
              obs_uom_nfo.append({'observation': obs,
                                  'uom': uom})
            platform_nfo.append({'platform_handle': config_file.get(platform, 'handle'),
                                 'observations': obs_uom_nfo})

            #Get the station specific tide stations
            offset_tide_station = "%s_tide_data" % (config_file.get(site_name, 'offset_tide_station'))
            #We use the virtual tide sites as there no stations near the sites.
            tide_station_settings = {
              'tide_station': config_file.get(site_name, 'tide_station'),
              'offset_tide_station': config_file.get(offset_tide_station, 'station_id'),
              'hi_tide_time_offset': config_file.getint(offset_tide_station, 'hi_tide_time_offset'),
              'lo_tide_time_offset': config_file.getint(offset_tide_station, 'lo_tide_time_offset'),
              'hi_tide_height_offset': config_file.getfloat(offset_tide_station, 'hi_tide_height_offset'),
              'lo_tide_height_offset': config_file.getfloat(offset_tide_station, 'lo_tide_height_offset')
            }

          test_obj = xgb_model(name=model_name,
                               classifier_type=classifier_type,
                               low_limit=entero_lo_limit,
                               high_limit=entero_hi_limit,
                               model_file=model_pickle_file,
                               model_file_type='pickle',
                               platforms=platform_nfo,
                               tide_station_nfo=tide_station_settings
                               )
          model_list.append(test_obj)

    return model_list

  def collect_data(self, **kwargs):
    self.logger.info("Begin collect_data")
    try:
      simplePluginManager = PluginManager()

      yapsy_log = logging.getLogger('yapsy')
      yapsy_log.setLevel(logging.DEBUG)
      yapsy_log.disabled = False

      simplePluginManager.setCategoriesFilter({
         "DataCollector": data_collector_plugin
         })

      # Tell it the default place(s) where to find plugins
      self.logger.debug("Plugin directories: %s" % (kwargs['data_collector_plugin_directories']))
      simplePluginManager.setPluginPlaces(kwargs['data_collector_plugin_directories'])

      simplePluginManager.collectPlugins()

      output_queue = Queue()
      plugin_cnt = 0
      plugin_start_time = time.time()
      for plugin in simplePluginManager.getAllPlugins():
        self.logger.info("Starting plugin: %s" % (plugin.name))
        if plugin.plugin_object.initialize_plugin(details=plugin.details,
                                                  queue=output_queue):
          plugin.plugin_object.start()
        else:
          self.logger.error("Failed to initialize plugin: %s" % (plugin.name))
        plugin_cnt += 1

      #Wait for the plugings to finish up.
      self.logger.info("Waiting for %d plugins to complete." % (plugin_cnt))
      for plugin in simplePluginManager.getAllPlugins():
        plugin.plugin_object.join()
        plugin.plugin_object.finalize()
      while not output_queue.empty():
        results = output_queue.get()
        if results[0] == data_result_types.MODEL_DATA_TYPE:
          self.site_data = results[1]

      self.logger.info("%d Plugins completed in %f seconds" % (plugin_cnt, time.time() - plugin_start_time))
    except Exception as e:
      self.logger.exception(e)

  def run_wq_models(self, **kwargs):

    prediction_testrun_date = datetime.now()
    try:
      config_file = ConfigParser.RawConfigParser()
      config_file.read(kwargs['config_file_name'])


      data_collector_plugin_directories=config_file.get('data_collector_plugins', 'plugin_directories')
      enable_data_collector_plugins  = config_file.getboolean('data_collector_plugins', 'enable_plugins')
      if enable_data_collector_plugins and len(data_collector_plugin_directories):
        data_collector_plugin_directories = data_collector_plugin_directories.split(',')
        self.collect_data(data_collector_plugin_directories=data_collector_plugin_directories)



      boundaries_location_file = config_file.get('boundaries_settings', 'boundaries_file')
      sites_location_file = config_file.get('boundaries_settings', 'sample_sites')
      units_file = config_file.get('units_conversion', 'config_file')
      output_plugin_dirs=config_file.get('output_plugins', 'plugin_directories').split(',')
      enable_output_plugins = config_file.getboolean('output_plugins', 'enable_plugins')

      xenia_nexrad_db_file = config_file.get('database', 'name')

      #MOve xenia obs db settings into standalone ini. We can then
      #check the main ini file into source control without exposing login info.
      db_settings_ini = config_file.get('password_protected_configs', 'settings_ini')
      xenia_obs_db_config_file = ConfigParser.RawConfigParser()
      xenia_obs_db_config_file.read(db_settings_ini)

      xenia_obs_db_host = xenia_obs_db_config_file.get('xenia_observation_database', 'host')
      xenia_obs_db_user = xenia_obs_db_config_file.get('xenia_observation_database', 'user')
      xenia_obs_db_password = xenia_obs_db_config_file.get('xenia_observation_database', 'password')
      xenia_obs_db_name = xenia_obs_db_config_file.get('xenia_observation_database', 'database')


    except (ConfigParser.Error, Exception) as e:
      self.logger.exception(e)

    else:

      #Load the sample site information. Has name, location and the boundaries that contain the site.
      wq_sites = wq_sample_sites()
      wq_sites.load_sites(file_name=sites_location_file, boundary_file=boundaries_location_file)

      #First pass we want to get all the data, after that we only need to query
      #the site specific pieces.
      reset_site_specific_data_only = False
      site_data = OrderedDict()
      total_time = 0
      site_model_ensemble = []
      for site in wq_sites:
        try:
          #Get all the models used for the particular sample site.
          model_list = self.build_test_objects(config_file=config_file, site_name=site.name)
          if len(model_list) == 0:
            self.logger.error("No models found for site: %s" % (site.name))

        except (ConfigParser.Error,Exception) as e:
          self.logger.exception(e)
        else:
          try:
            if len(model_list):
              site_models = xgb_ensemble(site, model_list)
              for model in model_list:
                self.logger.debug("Site: %s Model: %s starting prediction" % (site.name, model.name))
                site_data = OrderedDict()
                model_data_dir = config_file.get(site.name, 'data_directory')

                wq_data = follybeach_wq_data(xenia_nexrad_db_name=xenia_nexrad_db_file,
                                             xenia_obs_db_type='postgres',
                                             xenia_obs_db_host=xenia_obs_db_host,
                                             xenia_obs_db_user=xenia_obs_db_user,
                                             xenia_obs_db_password=xenia_obs_db_password,
                                             xenia_obs_db_name=xenia_obs_db_name,
                                             units_file=units_file)

                wq_data.reset(site=site,
                              tide_station_settings=model.tide_settings,
                               platform_info=model.platform_settings)

                #site_data['station_name'] = site.name
                wq_data.query_data(kwargs['begin_date'],
                                      kwargs['begin_date'],
                                      site_data,
                                      reset_site_specific_data_only)

                #Save data to csv file.
                data_file = self.save_model_data(site.name, model.name, model_data_dir, site_data, kwargs['begin_date'])
                model.runTest(site_data)

              total_test_time = sum(xgb_model.test_time for xgb_model in model_list)
              self.logger.debug("Site: %s total time to execute models: %f ms" % (site.name, total_test_time * 1000))
              total_time += total_test_time

              site_models.overall_prediction()
              site_model_ensemble.append({'metadata': site,
                                          'models': site_models,
                                          'statistics': None,
                                          'entero_value': None})

              '''
              #Calculate some statistics on the entero results. This is making an assumption
              #that all the tests we are running are calculating the same value, the entero
              #amount.
              entero_stats = None
              if len(site_equations.tests):
                entero_stats = stats()
                for test in site_equations.tests:
                  if test.mlrResult is not None:
                    entero_stats.addValue(test.mlrResult)
                entero_stats.doCalculations()

              '''
          except Exception as e:
            self.logger.exception(e)

      self.logger.debug("Total time to execute all sites models: %f ms" % (total_time * 1000))

      try:
        if enable_output_plugins:
          self.output_results(output_plugin_directories=output_plugin_dirs,
                              site_model_ensemble=site_model_ensemble,
                              prediction_date=kwargs['begin_date'],
                              prediction_run_date=prediction_testrun_date)
      except Exception as e:
        self.logger.exception(e)

    return
  def save_model_data(self, site_name, model_name, data_directory, site_data, test_time):
    out_file = os.path.join(data_directory, "%s-%s-%s-data.csv" % (site_name, model_name.replace(' ', '_'), test_time.strftime("%Y-%m-%d_%H_%M_%S")))
    write_header = True
    with open(out_file, 'w') as site_data_file:
      header_buf = []
      data = []
      write_header = True
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

    return out_file

  def output_results(self, **kwargs):

    self.logger.info("Begin run_output_plugins")

    simplePluginManager = PluginManager()
    logging.getLogger('yapsy').setLevel(logging.DEBUG)
    simplePluginManager.setCategoriesFilter({
       "OutputResults": data_collector_plugin
       })

    # Tell it the default place(s) where to find plugins
    self.logger.debug("Plugin directories: %s" % (kwargs['output_plugin_directories']))
    simplePluginManager.setPluginPlaces(kwargs['output_plugin_directories'])
    yapsy_logger = logging.getLogger('yapsy')
    yapsy_logger.setLevel(logging.DEBUG)
    # yapsy_logger.parent.level = logging.DEBUG
    yapsy_logger.disabled = False

    simplePluginManager.collectPlugins()

    plugin_cnt = 0
    plugin_start_time = time.time()
    for plugin in simplePluginManager.getAllPlugins():
      try:
        self.logger.info("Starting plugin: %s" % (plugin.name))
        if plugin.plugin_object.initialize_plugin(details=plugin.details,
                                                  prediction_date=kwargs['prediction_date'].astimezone(timezone("US/Eastern")).strftime("%Y-%m-%d %H:%M:%S"),
                                                  execution_date=kwargs['prediction_run_date'].strftime("%Y-%m-%d %H:%M:%S"),
                                                  ensemble_tests=kwargs['site_model_ensemble']
                                                  ):
          plugin.plugin_object.start()
          plugin_cnt += 1
        else:
          self.logger.error("Failed to initialize plugin: %s" % (plugin.details))
      except  Exception as e:
        self.logger.exception(e)
    #Wait for the plugings to finish up.
    self.logger.info("Waiting for %d plugins to complete." % (plugin_cnt))
    for plugin in simplePluginManager.getAllPlugins():
      plugin.plugin_object.join()
      plugin.plugin_object.finalize()

    self.logger.debug("%d output plugins run in %f seconds" % (plugin_cnt, time.time() - plugin_start_time))
    self.logger.info("Finished output_results")

def main():
  parser = optparse.OptionParser()
  parser.add_option("-c", "--ConfigFile", dest="config_file",
                    help="INI Configuration file." )
  parser.add_option("-s", "--StartDateTime", dest="start_date_time",
                    help="A date to re-run the predictions for, if not provided, the default is the current day. Format is YYYY-MM-DD HH:MM:SS." )

  (options, args) = parser.parse_args()

  if(options.config_file is None):
    parser.print_help()
    sys.exit(-1)

  try:
    config_file = ConfigParser.RawConfigParser()
    config_file.read(options.config_file)

    logger = None
    use_logging = False
    logConfFile = config_file.get('logging', 'prediction_engine')
    if logConfFile:
      logging.config.fileConfig(logConfFile)
      logger = logging.getLogger(__name__)
      logger.info("Log file opened.")
      use_logging = True

  except (ConfigParser.Error,Exception) as e:
    import traceback
    traceback.print_exc(e)
    sys.exit(-1)
  else:
    dates_to_process = []
    if options.start_date_time is not None:
      #Can be multiple dates, so let's split on ','
      collection_date_list = options.start_date_time.split(',')
      #We are going to process the previous day, so we get the current date, set the time to midnight, then convert
      #to UTC.
      eastern = timezone('US/Eastern')
      try:
        for collection_date in collection_date_list:
          est = eastern.localize(datetime.strptime(collection_date, "%Y-%m-%dT%H:%M:%S"))
          #Convert to UTC
          begin_date = est.astimezone(timezone('UTC'))
          dates_to_process.append(begin_date)
      except Exception as e:
        if logger:
          logger.exception(e)
    else:
      #We are going to process the previous day, so we get the current date, set the time to midnight, then convert
      #to UTC.
      est = datetime.now(timezone('US/Eastern'))
      est = est.replace(hour=0, minute=0, second=0,microsecond=0)
      #Convert to UTC
      begin_date = est.astimezone(timezone('UTC'))
      dates_to_process.append(begin_date)

    try:
      for process_date in dates_to_process:
        prediction_engine = follybeach_prediction_engine()
        prediction_engine.run_wq_models(begin_date=process_date,
                        config_file_name=options.config_file)
        #run_wq_models(begin_date=process_date,
        #              config_file_name=options.config_file)
    except Exception as e:
      logger.exception(e)

  if logger:
    logger.info("Log file closed.")

  return

if __name__ == "__main__":
  main()
