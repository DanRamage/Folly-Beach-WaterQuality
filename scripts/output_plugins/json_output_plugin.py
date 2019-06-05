import sys
sys.path.append('../../commonfiles/python')
import logging.config
import json
import time
from data_collector_plugin import data_collector_plugin

class json_output_plugin(data_collector_plugin):
  def initialize_plugin(self, **kwargs):
    try:
      data_collector_plugin.initialize_plugin(self, **kwargs)

      self._plugin_details = kwargs['details']

      self.json_outfile = self._plugin_details.get("Settings", "json_outfile")

      self.ensemble_data = kwargs.get('ensemble_tests', None)
      self.execution_date= kwargs['execution_date'],
      self.prediction_date= kwargs['prediction_date'],

      return True
    except Exception as e:
      self.logger.exception(e)
    return False

  def run(self, **kwargs):
    start_time = time.time()
    try:
      logger_conf = self._plugin_details.get('Settings', 'logging')
      #logging.config.dictConfig(self.logging_client_cfg)
      logging.config.fileConfig(logger_conf)
      logger = logging.getLogger()
      logger.debug("json output run started.")

      # Charleston sites to include. We have a list of the charleston site name then ";" then the Folly SIte name to use.
      charleston_sites = self._plugin_details.get('charleston_sites', 'overlap_sites').split(',')
      folly_names = self._plugin_details.get('charleston_sites', 'folly_names').split(',')
      charleston_predictions = self._plugin_details.get('charleston_sites', 'predictions_file')
      logger.debug("Opening JSON output file: %s" %(self.json_outfile))
      with open(self.json_outfile, 'w') as json_output_file:
        station_data = {'features' : [],
                        'type': 'FeatureCollection'}
        features = []
        if self.ensemble_data is not None:
          for rec in self.ensemble_data:
            site_metadata = rec['metadata']
            test_results = rec['models']
            if 'statistics' in rec:
              stats = rec['statistics']
            test_data = []
            for test in test_results.tests:
              test_data.append({
                'name': test.model_name,
                'p_level': test.predictionLevel.__str__(),
                'p_value': test.mlrResult,
                'data': test.data_used
              })
            features.append({
              'type': 'Feature',
              'geometry': {
                'type': 'Point',
                'coordinates': [site_metadata.object_geometry.x, site_metadata.object_geometry.y]
              },
              'properties': {
                'desc': site_metadata.name,
                'ensemble': str(test_results.ensemblePrediction),
                'station': site_metadata.name,
                'tests': test_data
              }
            })
        #Open the Charleston predictions and pull out the sites we want on Folly.
        try:
          logger.debug("Opening CHS predictions file: %s" % (charleston_predictions))
          with open(charleston_predictions, "r") as chs_pred_file:
            chs_json = json.load(chs_pred_file)
            chs_features = chs_json['contents']['stationData']['features']
            for chs_feature in chs_features:
              for ndx,chs_site in enumerate(charleston_sites):
                logger.debug("Searching for site: %s(%s)" % (chs_site,folly_names[ndx]))
                if chs_feature['properties']['station'] == chs_site:
                  #Rename the station to what we want it for the Folly WQ project.
                  chs_feature['properties']['station'] = folly_names[ndx]
                  features.append(chs_feature)
        except IOError as e:
          logger.exception(e)
        station_data['features'] = features
        json_data = {
          'status': {'http_code': 200},
          'contents': {
            'run_date': self.execution_date,
            'testDate': self.prediction_date,
            'stationData': station_data
          }
        }
        try:
          json_output_file.write(json.dumps(json_data, sort_keys=True))
        except Exception,e:
          if self.logger:
            self.logger.exception(e)
      self.logger.debug("Finished json output in %f seconds." % (time.time()-start_time))
    except IOError,e:
      self.logger.exception(e)
    return
