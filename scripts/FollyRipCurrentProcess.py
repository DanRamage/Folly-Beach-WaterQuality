import sys
sys.path.append('../commonfiles/python')
import os
import logging.config
import time
from datetime import datetime
import json
import urlparse
import optparse
import ConfigParser
import pickle
from dateutil import parser as du_parser
from geojson import Point, FeatureCollection, Feature
from rip_current_scraper import RipCurrentScraper
from AlertStateMachine import SiteStates, NoAlertState, LowAlertState, MedAlertState, HiAlertState
from mako.template import Template
from mako import exceptions as makoExceptions
from smtp_utils import smtpClass




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


def send_report(out_filename, site_states, template, prediction_date, mailhost, user, password, to_list, from_addr):
    logger = logging.getLogger(__name__)

    try:
        mytemplate = Template(filename=template)

        with open(out_filename, 'w') as report_out_file:
            results_report = mytemplate.render(prediction_date=prediction_date,
                                               rip_current_sites=site_states)
            report_out_file.write(results_report)
    except TypeError, e:
        logger.exception(makoExceptions.text_error_template().render())
    except (IOError, AttributeError, Exception) as e:
        logger.exception(e)
    else:
        try:
            subject = "Rip Current Alerts for %s" % (prediction_date)
            # Now send the email.
            smtp = smtpClass(host=mailhost, user=user, password=password)
            smtp.rcpt_to(to_list)
            smtp.from_addr(from_addr)
            smtp.subject(subject)
            smtp.message(results_report)
            smtp.send(content_type="html")
        except Exception as e:
            logger.exception(e)
    logger.debug("Finished emit for email output.")


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
            run_date = datetime.now()
            output_directory = config_file.get("Settings", "output_directory")
            files_to_process = config_file.get("Settings", "files_to_process").split(',')
            rip_current_file_url = config_file.get("Settings", "url")
            stations = config_file.get("Settings", "stations_ids").split(',')
            email_ini_file = config_file.get("riptide_report", "email_settings_ini")
            report_output_directory = config_file.get("riptide_report", "directory")
            report_template = config_file.get("riptide_report", "report_template")

            email_config = ConfigParser.RawConfigParser()
            email_config.read(email_ini_file)
            mailhost = email_config.get("rip_current_email_report", "mailhost")
            user = email_config.get("rip_current_email_report", "user")
            password = email_config.get("rip_current_email_report", "password")
            to_list = email_config.get("rip_current_email_report", "toaddrs").split(',')
            from_addr = email_config.get("rip_current_email_report", "fromaddr")

            rip_current = RipCurrentProcessor()
            rip_current.get(url=rip_current_file_url,
                                              rip_current_files=files_to_process,
                                              output_directory=output_directory,
                                              stations=stations)

            for file_name in files_to_process:
                file_parts = os.path.splitext(file_name)

                # Load the pickle states, if we have on.
                pickle_outfile = os.path.join(output_directory, '%s.pickle' % (file_parts[0]))
                if os.path.exists(pickle_outfile):
                    logger.debug("Existing state file: %s loading." % (pickle_outfile))
                    try:
                        with open(pickle_outfile, "rb") as pickle_file:
                            site_states = pickle.load(pickle_file)
                            logger.debug("Existing state file: %s loaded." % (pickle_outfile))
                    except Exception as e:
                        logger.exception(e)
                        site_states = SiteStates(None)
                else:
                    logger.debug("No existing state file: %s." % (pickle_outfile))
                    site_states = SiteStates(None)

                local_file = os.path.join(output_directory, '%s.json' % (file_parts[0]))
                site_report_data = []
                prediction_date = None
                with open(local_file, 'r') as local_data_file:
                    json_data = json.load(local_data_file)
                    for feature in json_data['features']:
                        props = feature['properties']
                        site_id = props['id']
                        prev_state = site_states.get_state(site_id)
                        state = NoAlertState(site_id)
                        if props['level'].lower() == 'high':
                            state = HiAlertState(site_id)
                        elif props['level'].lower() == 'moderate':
                            state = MedAlertState(site_id)
                        elif  props['level'].lower() == 'low':
                            state = LowAlertState(site_id)
                        if site_states.update_state(site_id, state):
                            cur_state = site_states.get_state(site_id)
                            logger.debug("Site: %s state change to: %s from %s" % (site_id, str(cur_state), str(prev_state)))
                            site_report_data.append({
                                'date': props['date'],
                                'site_description': props['description'],
                                'level': props['level'],
                                'location': "%f, %f" % (feature['geometry']['coordinates'][0],feature['geometry']['coordinates'][1])
                            })
                        if prediction_date is None:
                            prediction_date = props['date']
                #Pickle state
                with open(pickle_outfile, "wb") as pickle_file:
                    pickle.dump(site_states, pickle_file)
                if len(site_report_data):
                    report_file_name = os.path.join(report_output_directory, "RipCurrentReport_%s.html" % (run_date.strftime('%Y_%m_%d-%H_%M_%S')))
                    send_report(report_file_name, site_report_data, report_template, prediction_date, mailhost, user, password, to_list, from_addr)
                else:
                    logger.debug("No alerts to send.")

        except Exception as e:
            logger.exception(e)
        logger.info("Log file closed.")

    return
if __name__ == "__main__":
    main()