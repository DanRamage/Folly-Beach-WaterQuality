import sys
sys.path.append('../../commonfiles/python')
import logging.config
from data_collector_plugin import data_collector_plugin
import ConfigParser
import traceback
import geojson

from dhecBeachAdvisoryReader import waterQualityAdvisory
from folly_wq_sites import folly_wq_sites


class dhec_sample_data_collector_plugin(data_collector_plugin):

  def initialize_plugin(self, **kwargs):
    data_collector_plugin.initialize_plugin(self, **kwargs)
    try:
      logger = logging.getLogger(self.__class__.__name__)
      self._plugin_details = kwargs['details']
      return True
    except Exception as e:
      logger.exception(e)
    return False

  def run(self):
    try:
      configFile = ConfigParser.RawConfigParser()
      configFile.read(self.ini_file)

      self.logging_client_cfg['disable_existing_loggers'] = True
      logging.config.dictConfig(self.logging_client_cfg)
      logger = logging.getLogger(self.__class__.__name__)
      logger.debug("run started.")

    except ConfigParser.Error, e:
      print("No log configuration file given, logging disabled.")
    except Exception,e:
      import traceback
      traceback.print_exc(e)
      sys.exit(-1)
    try:
      logger.debug("Getting config params.")
      #Base URL to the page that house an individual stations results.
      baseUrl = self._plugin_details.get('websettings', 'baseAdvisoryPageUrl')

      #output Filename for the JSON data.
      jsonFilepath = self._plugin_details.get('output', 'outputDirectory')

      #Filepath to the geoJSON file that contains the station data for all the stations.
      stationGeoJsonFile = self._plugin_details.get('stationData', 'stationGeoJsonFile')

      #The past WQ results.
      stationWQHistoryFile = self._plugin_details.get('stationData', 'stationWQHistoryFile')

      dhec_rest_url = self._plugin_details.get('websettings', 'dhec_rest_url')

      boundaries_location_file = self._plugin_details.get('boundaries_settings', 'boundaries_file')
      sites_location_file = self._plugin_details.get('boundaries_settings', 'sample_sites')

      logger.debug("Finished getting config params.")
    except ConfigParser.Error, e:
      if(logger):
        logger.exception(e)

    else:
      try:
        wq_sites = folly_wq_sites()
        wq_sites.load_sites(file_name=sites_location_file, boundary_file=boundaries_location_file)

        logger.debug("Creating dhec sample query object.")
        advisoryObj = waterQualityAdvisory(baseUrl, True)
        #See if we have a historical WQ file, if so let's use that as well.
        logger.debug("Opening historical json file: %s." % (stationWQHistoryFile))
        historyWQFile = open(stationWQHistoryFile, "r")
        logger.debug("Loading historical json file: %s." % (stationWQHistoryFile))
        historyWQAll = geojson.load(historyWQFile)
        #Now cleanup and only have historical data from sites we do predictions on.
        historyWQ = {}
        for site in wq_sites:
          historyWQ[site.name] = historyWQAll[site.name]

        logger.debug("Beginning SOAP query.")
        advisoryObj.processData(
                                geo_json_file = stationGeoJsonFile,
                                json_file_path = jsonFilepath,
                                historical_wq = historyWQ,
                                dhec_url = dhec_rest_url,
                                post_data_url = None,
                                sampling_stations=mb_sites)
        logger.debug("Finished SOAP query.")
      except (IOError,Exception) as e:
        logger.exception(e)

    return
