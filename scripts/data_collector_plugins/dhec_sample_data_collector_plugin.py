from data_collector_plugin.data_collector_plugin as data_collector_plugin
import sys
sys.path.append('../../commonfiles/python')
import os
import logging.config
import geojson
from datetime import datetime
import time
from pytz import timezone
import json
from shutil import copyfile

from dhecBeachAdvisoryReader import waterQualityAdvisory
from wq_output_results import wq_sample_data,wq_samples_collection,wq_advisories_file,wq_station_advisories_file
#from folly_sample_data import folly_wq_sample_data

from folly_wq_sites import folly_wq_sites

class dhec_sample_data_collector_plugin(data_collector_plugin):

  def initialize_plugin(self, **kwargs):
    try:
      self.logger.debug("dhec_sample_data_collector_plugin initialize_plugin started.")

      self._plugin_details = kwargs['details']
      self.logger.debug("dhec_sample_data_collector_plugin initialize_plugin finished.")
      return True
    except Exception as e:
      self.logger.exception(e)
    return False

  def run(self):
    try:
      logger_conf = self._plugin_details.get('logging', 'scraperConfigFile')
      logging.config.fileConfig(logger_conf)
      logger = logging.getLogger()
      logger.debug("run started.")

      logger.debug("Getting config params.")
      #output Filename for the JSON data.
      json_output_path = self._plugin_details.get('output', 'outputDirectory')

      #Filepath to the geoJSON file that contains the station data for all the stations.
      stationGeoJsonFile = self._plugin_details.get('stationData', 'stationGeoJsonFile')

      #Charleston sites to include. We have a list of the charleston site name then ";" then the Folly SIte name to use.
      charleston_sites = self._plugin_details.get('stationData', 'sitesFromCharlestonToInclude').split(',')

      charleston_directory = self._plugin_details.get('stationData', 'charlestonSampleDataDirectory')
      charleston_predictions = self._plugin_details.get('stationData', 'charlestonPredictionFile')

      #The past WQ results.
      stationWQHistoryFile = self._plugin_details.get('stationData', 'stationWQHistoryFile')

      dhec_rest_url = self._plugin_details.get('websettings', 'dhec_rest_url')
      dhec_soap_url = self._plugin_details.get('websettings', 'dhec_soap_url')
      boundaries_location_file = self._plugin_details.get('boundaries_settings', 'boundaries_file')
      sites_location_file = self._plugin_details.get('boundaries_settings', 'sample_sites')

      logger.debug("Finished getting config params.")

      wq_sites = folly_wq_sites()
      wq_sites.load_sites(file_name=sites_location_file, boundary_file=boundaries_location_file)

      logger.debug("Creating dhec sample query object.")
      #See if we have a historical WQ file, if so let's use that as well.
      logger.debug("Opening historical json file: %s." % (stationWQHistoryFile))
      try:
        historyWQFile = open(stationWQHistoryFile, "r")
        logger.debug("Loading historical json file: %s." % (stationWQHistoryFile))
        historyWQAll = geojson.load(historyWQFile)
        # Now cleanup and only have historical data from sites we do predictions on.
        historyWQ = {}
        for site in wq_sites:
          historyWQ[site.name] = historyWQAll[site.name]
      except IOError as e:
        logger.exception(e)
        historyWQ = None

      station_list = []
      for site in wq_sites:
          station_list.append(site.name)

      logger.debug("Creating dhec sample query object.")
      advisoryObj = waterQualityAdvisory(dhec_soap_url, True)

      today = datetime.now()
      year = today.year
      year_list = []
      for year in range(year-1, year+1, 1):
        year_list.append(year)
      soap_query_start = time.time()
      logger.debug("Beginning SOAP query.")
      data = advisoryObj.get_sample_data(station_list, year_list)
      logger.debug("Finished SOAP query in %f seconds" % (time.time()-soap_query_start))
      wq_data_collection = wq_samples_collection()

      est_tz = timezone('US/Eastern')
      utc_tz = timezone('UTC')

      for station in station_list:
        if station in data:
          station_data = data[station]
          for rec in station_data['results']:
            wq_sample = wq_sample_data()
            wq_sample.station = station
            date_obj = (utc_tz.localize(datetime.strptime(rec['date'], '%Y-%m-%dT%H:%M:%SZ'))).astimezone(est_tz)
            wq_sample.date_time = date_obj
            wq_sample.value = rec['value']
            wq_data_collection.append(wq_sample)

      #Now get the charleston wq project sites we want to include.
      for chs_site in charleston_sites:
        #Get the sample data.
        try:
          chs_site_name, folly_site_name = chs_site.split(';')
          #First we copy the water quality sample data over to the FOlly BEach directory.
          full_path = os.path.join(charleston_directory, "%s.json" % (chs_site_name))
          to_path = os.path.join(json_output_path, "%s.json" % (folly_site_name))
          copyfile(full_path, to_path)
          #Now get the most recent sample date.
          with open(to_path, "r") as chs_site_file:
            json_data = json.load(chs_site_file)
            recent = json_data['properties']['test']['beachadvisories'][-1]
            wq_sample = wq_sample_data()
            wq_sample.station = folly_site_name
            date_obj = (utc_tz.localize(datetime.strptime(recent['date'], '%Y-%m-%d %H:%M:%S'))).astimezone(est_tz)
            wq_sample.date_time = date_obj
            wq_sample.value = recent['value']
            wq_data_collection.append(wq_sample)
        except (IOError, Exception) as e:
          logger.exception(e)


      json_results_file = os.path.join(json_output_path, 'beach_advisories.json')
      logger.debug("Creating beach advisories file: %s" % (json_results_file))
      try:
        current_advisories = wq_advisories_file(wq_sites)
        current_advisories.create_file(json_results_file, wq_data_collection)
      except Exception as e:
        logger.exception(e)
      try:
        for site in wq_sites:
          if site.run_model:
            logger.debug("Creating site: %s advisories file" % (site.name))
            site_advisories = wq_station_advisories_file(site)
            site_advisories.create_file(json_output_path, wq_data_collection)
      except Exception as e:
        logger.exception(e)

    except (Exception) as e:
      if logger is not None:
        logger.exception(e)
      else:
        import traceback
        traceback.print_exc(e)

    return

  def finalize(self):
    return
