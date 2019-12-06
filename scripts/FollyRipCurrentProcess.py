import sys
sys.path.append('../commonfiles/python')
import os
import logging.config
import time
import json
import urlparse
import optparse
import ConfigParser
from dateutil import parser as du_parser
from geojson import Point, FeatureCollection, Feature
from rip_current_scraper import RipCurrentScraper





class RipCurrentProcessor:
    def __init__(self):
        self.logger = logging.getLogger()

    def get(self, **kwargs):

        url = kwargs['url']
        rip_files_to_process = kwargs['rip_current_files']
        output_directory = kwargs['output_directory']
        stations = kwargs['stations']

        self.logger.debug("Querying url: %s" % (url))
        rip_scraper = RipCurrentScraper()
        try:
            files = rip_scraper.directory_listing(url)
            for file_name in rip_files_to_process:
                #Search for the file in the directory listing to determine it's last update time
                last_modded_file = next((item for item in files if item["file_url"].find(file_name) != -1), None)
                #Open up, if available, the last data we had for station.
                try:
                    file_parts = os.path.splitext(file_name)
                    local_file = os.path.join(output_directory, '%s.json' % (file_parts[0]))
                    self.logger.debug("Checking last file downloaded: %s" % (local_file))
                    local_file_date = None
                    with open(local_file, 'r') as local_data_file:
                        json_data = json.load(local_data_file)
                        #All the features will have the same date, so let's just get the first one.
                        feature = json_data['features'][0]
                        local_file_date = du_parser.parse(feature['properties']['date'])
                except Exception as e:
                    self.logger.exception(e)
                download_latest = False
                if local_file_date is not None:
                    if last_modded_file['last_modified'] > local_file_date:
                        self.logger.debug("Remote date: %s newer than our last date: %s" % (last_modded_file['last_modified'], local_file_date))
                        download_latest = True
                else:
                    download_latest = True
                if download_latest:
                    file_url = urlparse.urljoin(url, file_name)
                    self.logger.debug("Downloading file url: %s" % (file_url))
                    file_data = rip_scraper.download_file(file_url)
                    try:
                        feat_collection = None
                        features = []
                        self.logger.debug("Writing new local file: %s" % (local_file))
                        for row in file_data.split('\n'):
                            if(len(row)):
                                cols = row.replace('"', '').replace(',', '').split('|')
                                #Is the station one we want?
                                if cols[0] in stations:
                                    features.append(Feature(geometry=Point((float(cols[1]), float(cols[2]))), properties={
                                        'id': cols[0],
                                        'description': cols[3],
                                        'nws_area': cols[4],
                                        'flag': cols[5],
                                        'level': cols[6],
                                        'date': last_modded_file['last_modified'].strftime('%Y-%m-%d %H:%M:%S')
                                    }))
                        feat_collection = FeatureCollection(features)
                        with open(local_file, 'w') as local_data_file:
                            json.dump(feat_collection, local_data_file)

                    except Exception as e:
                        self.logger.exception(e)

        except Exception as e:
            self.logger.exception(e)


def main():
    parser = optparse.OptionParser()
    parser.add_option("-c", "--ConfigFile", dest="config_file",
                      help="INI Configuration file.")
    (options, args) = parser.parse_args()

    if (options.config_file is None):
        parser.print_help()
        sys.exit(-1)

    try:
        config_file = ConfigParser.RawConfigParser()
        config_file.read(options.config_file)

        log_conf = config_file.get('Settings', 'logfile')
        logging.config.fileConfig(log_conf)
        logger = logging.getLogger(__name__)
        logger.info("Log file opened.")
    except Exception as e:
        import traceback
        traceback.print_exc(e)
        sys.exit(-1)
    else:
        try:
            output_directory = config_file.get("Settings", "output_directory")
            files_to_process = config_file.get("Settings", "files_to_process").split(',')
            rip_current_file_url = config_file.get("Settings", "url")
            stations = config_file.get("Settings", "stations_ids").split(',')

            rip_current = RipCurrentProcessor()
            rip_current.get(url=rip_current_file_url,
                                              rip_current_files=files_to_process,
                                              output_directory=output_directory,
                                              stations=stations)


        except Exception as e:
            logger.exception(e)
        logger.info("Log file closed.")

    return
if __name__ == "__main__":
    main()