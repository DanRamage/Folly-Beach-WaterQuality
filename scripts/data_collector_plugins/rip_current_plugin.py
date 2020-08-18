import sys
sys.path.append('../')
sys.path.append('../../commonfiles/python')
import os
import logging.config
from data_collector_plugin import data_collector_plugin
import traceback
from datetime import datetime
import time
import json
import urlparse
import dateutil
from rip_current_scraper import RipCurrentScraper

class rip_current_predictions(data_collector_plugin):

  def __init__(self):
    data_collector_plugin.__init__(self)

    self.output_queue = None

  def initialize_plugin(self, **kwargs):
    try:
        #self.logging_client_cfg['disable_existing_loggers'] = True
        plugin_details = kwargs['details']
        self._log_conf = plugin_details.get("Settings", "logfile")
        self._output_directory = plugin_details.get("Settings", "output_directory")
        self._files_to_process = plugin_details.get("Settings", "files_to_process").split(',')
        self._rip_current_file_url = plugin_details.get("Settings", "url")
        self._stations = plugin_details.get("Settings", "stations_ids").split(',')

        return True
    except Exception as e:
      self.logger.exception(e)
    return False

  def run(self):
    try:
        start_time = time.time()
        last_check = datetime.now()
        logging.config.fileConfig(self._log_conf)
        logger = logging.getLogger()
        logger.debug("rip_current_predictions run started.")

        requests_log = logging.getLogger("requests.packages.urllib3")
        requests_log.setLevel(logging.DEBUG)
        requests_log.propagate = True

        logger.debug("Querying url: %s" % (self._rip_current_file_url))
        rip_scraper = RipCurrentScraper()
        try:
            files = rip_scraper.directory_listing(self._rip_current_file_url)
            for file_name in self._files_to_process:
                #Search for the file in the directory listing to determine it's last update time
                last_modded_file = next((item for item in files if item["file_url"].find(file_name) != -1), None)
                #Open up, if available, the last data we had for station.
                try:
                    file_parts = os.path.splitext(file_name)
                    local_file = os.path.join(self._output_directory, '%s.csv' % (file_parts[0]))
                    logger.debug("Checking last file downloaded: %s" % (local_file))
                    local_file_date = None
                    with open(local_file, 'r') as local_data_file:
                        #First row is header.
                        row = local_data_file.readline()
                        #Read a line to get the date.
                        row = local_data_file.readline().split(',')
                        local_file_date = dateutil.parser.parse(row[0])
                except Exception as e:
                    logger.exception(e)
                download_latest = False
                if local_file_date is not None:
                    if last_modded_file['last_modified'] > local_file_date:
                        logger.debug("Remote date: %s newer than our last date: %s" % (last_modded_file['last_modified'], local_file_date))
                        download_latest = True
                else:
                    download_latest = True
                if download_latest:
                    file_url = urlparse.urljoin(self._rip_current_file_url, file_name)
                    logger.debug("Downloading file url: %s" % (file_url))
                    file_data = rip_scraper.download_file(file_url)
                    try:
                        logger.debug("Writing new local file: %s" % (local_file))
                        with open(local_file, 'w') as local_data_file:
                            header = 'Date,ID,Latitude,Longitude,Station Description,NWS Area,Flag,Level'
                            local_data_file.write(header)
                            local_data_file.write('\n')
                            for row in file_data.split('\n'):
                                if(len(row)):
                                    cols = row.replace('"', '').replace(',', '').split('|')
                                    #Is the station one we want?
                                    if cols[0] in self._stations:
                                        cols.insert(0, last_modded_file['last_modified'].strftime('%Y-%m-%d %H:%M:%S'))
                                        out_row = ','.join(cols)
                                        local_data_file.write(out_row)
                                        local_data_file.write('\n')


                    except Exception as e:
                        logger.exception(e)



        except Exception as e:
            logger.exception(e)

        logger.debug("rip_current_predictions run finished in %f seconds." % (time.time()-start_time))
    except Exception as e:
        logger.exception(e)

  def finalize(self):
    return
