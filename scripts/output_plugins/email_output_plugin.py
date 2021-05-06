import sys
sys.path.append('../../commonfiles/python')

from mako.template import Template
from mako import exceptions as makoExceptions
from smtp_utils import smtpClass
import os
if sys.version_info[0] < 3:
  import ConfigParser
else:
  import configparser as ConfigParser

import logging.config
import time
from data_collector_plugin import data_collector_plugin

class email_output_plugin(data_collector_plugin):

  def initialize_plugin(self, **kwargs):
    try:

      #data_collector_plugin.initialize_plugin(self, **kwargs)

      self.logger.debug("Email plugin intializing started.")
      self._plugin_details = kwargs['details']

      self.prediction_date = kwargs['prediction_date']
      self.execution_date = kwargs['execution_date']
      self.ensemble_tests = kwargs['ensemble_tests']
      self.password_ini_file = self._plugin_details.get('Settings', 'password_ini_file')

      self.result_outfile = self._plugin_details.get("Settings", "results_outfile")
      self.results_template = self._plugin_details.get("Settings", "results_template")
      self.report_url = self._plugin_details.get("Settings", "report_url")

      self._run_logger_conf = self._plugin_details.get('Settings', 'logging')


      self.logger.debug("Email plugin intializing finished.")
      return True
    except Exception as e:
      self.logger.exception(e)
    return False

  def run(self):
    logger = None
    start_time = time.time()
    try:
      logging.config.fileConfig(self._run_logger_conf)
      logger = logging.getLogger()
      logger.debug("run started.")
    except Exception as e:
      import traceback
      traceback.print_exc(e)
      if logger is not None:
        logger.exception(e)
    else:
      try:
        #Get email server settings.
        password_config_file = ConfigParser.RawConfigParser()
        password_config_file.read(self.password_ini_file)


        self.mailhost = password_config_file.get("email_report_output_plugin", "mailhost")
        self.mailport = None
        self.fromaddr = password_config_file.get("email_report_output_plugin", "fromaddr")
        self.toaddrs = password_config_file.get("email_report_output_plugin", "toaddrs").split(',')
        self.subject = password_config_file.get("email_report_output_plugin", "subject")
        self.user = password_config_file.get("email_report_output_plugin", "user")
        self.password = password_config_file.get("email_report_output_plugin", "password")

        mytemplate = Template(filename=self.results_template)
        file_ext = os.path.splitext(self.result_outfile)
        file_parts = os.path.split(file_ext[0])
        #Add the prediction date into the filename
        file_name = "%s-%s%s" % (file_parts[1], self.prediction_date.replace(':', '_').replace(' ', '-'), file_ext[1])
        out_filename = os.path.join(file_parts[0], file_name)
        logger.debug("Opening output file: %s" % (out_filename))
        with open(out_filename, 'w') as report_out_file:
          report_url = '%s/%s' % (self.report_url, file_name)
          results_report = mytemplate.render(ensemble_tests=self.ensemble_tests,
                                                  prediction_date=self.prediction_date,
                                                  execution_date=self.execution_date,
                                                  report_url=report_url)
          report_out_file.write(results_report)
      except TypeError as e:
          logger.exception(makoExceptions.text_error_template().render())
      except (IOError,AttributeError,Exception) as e:
          logger.exception(e)
      else:
        try:
          logger.debug("Emailing output file: %s" % (out_filename))
          subject = self.subject % (self.prediction_date)
          #Now send the email.
          smtp = smtpClass(host=self.mailhost, user=self.user, password=self.password)
          smtp.rcpt_to(self.toaddrs)
          smtp.from_addr(self.fromaddr)
          smtp.subject(subject)
          smtp.message(results_report)
          smtp.send(content_type="html")
        except Exception as e:
            logger.exception(e)
    logger.debug("Finished email plugin run in %s seconds" % (time.time()-start_time))

  def finalize(self):
    return
