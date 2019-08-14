import sys
sys.path.append('../')
sys.path.append('../../commonfiles/python')
import os
import logging.config
from data_collector_plugin import data_collector_plugin
import traceback
from datetime import datetime
import time
import requests
import json

class shellfish_closings(data_collector_plugin):

  def __init__(self):
    data_collector_plugin.__init__(self)

    self.output_queue = None

  def initialize_plugin(self, **kwargs):
    try:
        data_collector_plugin.initialize_plugin(self, **kwargs)
        self.logging_client_cfg['disable_existing_loggers'] = True
        plugin_details = kwargs['details']
        self._log_conf = plugin_details.get("Settings", "logfile")
        self._output_file = plugin_details.get("Settings", "output_file")
        self._rest_request = plugin_details.get("Settings", "rest_request")

        return True
    except Exception as e:
      self.logger.exception(e)
    return False

  def run(self):
    try:
        start_time = time.time()
        logging.config.fileConfig(self._log_conf)
        logger = logging.getLogger()
        logger.debug("shellfish_closings run started.")

        requests_log = logging.getLogger("requests.packages.urllib3")
        requests_log.setLevel(logging.DEBUG)
        requests_log.propagate = True
        #NOTE: OS X has an issue with the proxy lookup. Sometimes the .get() would just crash out of the run() function with no
        #exception thrown
        session = requests.Session()
        session.trust_env = False  # Don't read proxy settings from OS
        #url = 'https://services2.arcgis.com/XZg2efAbaieYAXmu/arcgis/rest/services/Shellfish_Closures/FeatureServer/0/query?f=json&where=1%3D1&spatialRel=esriSpatialRelIntersects&geometry=%7B%22xmin%22%3A-9356861.736375863%2C%22ymin%22%3A3716399.5873283646%2C%22xmax%22%3A-8372964.308289124%2C%22ymax%22%3A4054557.000461967%2C%22spatialReference%22%3A%7B%22wkid%22%3A102100%7D%7D&geometryType=esriGeometryEnvelope&inSR=102100&outFields=OBJECTID%2CSF_AREA%2CStorm_Closure&orderByFields=OBJECTID%20DESC&outSR=4326'
        logger.debug("Querying DHEC REST: %s" % (self._rest_request))
        rest_start = time.time()
        req = session.get(self._rest_request)
        if req.status_code == 200:
            logger.debug("DHEC REST request sucess in %f seconds" % (time.time()-rest_start))
            shellfish_areas = {}
            shellish_json = req.json()
            if 'features' in shellish_json:
                for feature in shellish_json['features']:
                    attr = feature['attributes']
                    obj_id = attr['OBJECTID']
                    if 'SF_AREA' in attr:
                        area = attr['SF_AREA']
                        if area not in shellfish_areas and area is not None:
                            logger.debug("ID: %d Area: %s" % (obj_id, area))
                            shellfish_areas[area] = {'SF_AREA' : area,
                                                       'OBJECTID': obj_id,
                                                       'Storm_Closure': attr['Storm_Closure']}

            with open(self._output_file, 'w') as output_file:
                output_file.write(json.dumps(shellfish_areas))
        else:
            logger.error("DHEC REST query failed, code: %d" % (req.status_code))
        logger.debug("dhec_shellfish_closures run finished in %f seconds." % (time.time()-start_time))
    except Exception as e:
        logger.exception(e)