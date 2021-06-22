import sys
sys.path.append('../commonfiles/python')
import os
import logging.config
import time
from datetime import datetime
import json
if sys.version_info[0] < 3:
    import urlparse
else:
    import urllib.parse as urlparse
import optparse
if sys.version_info[0] < 3:
    import ConfigParser
else:
    import configparser as ConfigParser
import requests
import pickle
from dateutil import parser as du_parser
from geojson import Point, FeatureCollection, Feature
from AlertStateMachine import SiteStates, NoAlertState, LowAlertState, MedAlertState, HiAlertState
from mako.template import Template
from mako import exceptions as makoExceptions




class RipCurrentProcessor:
    def __init__(self):
        self.logger = logging.getLogger()

    def get(self, **kwargs):

        url = kwargs['url']
        output_directory = kwargs['output_directory']
        last_check = datetime.now()
        station_settings = kwargs['station_settings']
        self.logger.debug("Querying url: %s" % (url))
        try:
            req = requests.get(url)
            output_filename = os.path.join(output_directory, 'forecasts.json')
            with open(output_filename, "w") as json_file:
                json_data = req.json()
                for key in json_data:
                    json_data[key]['date'] = last_check.strftime('%Y-%m-%d %H:%M')
                    if key in station_settings:
                        json_data[key]['wfo_url'] = station_settings[key]['wfo_url']
                        json_data[key]['guidance_url'] = station_settings[key]['guidance_url']

                json.dump(json_data, json_file, indent=4)
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
    except TypeError as e:
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
            station_settings = {}
            run_date = datetime.now()
            output_directory = config_file.get("Settings", "output_directory")
            rip_current_file_url = config_file.get("Settings", "url")
            stations = config_file.get("Settings", "stations_ids").split(',')
            email_ini_file = config_file.get("riptide_report", "email_settings_ini")
            report_output_directory = config_file.get("riptide_report", "directory")
            report_template = config_file.get("riptide_report", "report_template")

            #We need to have some station centric info, such as the link to the wfo.
            for station in stations:
                station_settings[station] = {}
                url = config_file.get(station, "wfo_url")
                station_settings[station]['wfo_url'] = url
                url = config_file.get(station, "guidance_url")
                station_settings[station]['guidance_url'] = url

            email_config = ConfigParser.RawConfigParser()
            email_config.read(email_ini_file)
            mailhost = email_config.get("rip_current_email_report", "mailhost")
            user = email_config.get("rip_current_email_report", "user")
            password = email_config.get("rip_current_email_report", "password")
            to_list = email_config.get("rip_current_email_report", "toaddrs").split(',')
            from_addr = email_config.get("rip_current_email_report", "fromaddr")

            rip_current = RipCurrentProcessor()
            rip_current.get(url=rip_current_file_url,
                              output_directory=output_directory,
                              station_settings=station_settings)
        except Exception as e:
            logger.exception(e)
        logger.info("Log file closed.")

    return
if __name__ == "__main__":
    main()