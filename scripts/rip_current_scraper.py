import sys
import logging.config
import requests
from bs4 import BeautifulSoup
from dateutil import parser as du_parser

sys.path.append('../../commonfiles/python')


class RipCurrentScraper:
    def __init__(self):
        self.logger = logging.getLogger()

    def directory_listing(self, url):
        session = requests.Session()
        session.trust_env = False  # Don't read proxy settings from OS

        req = session.get(url)
        if req.status_code == 200:
            page = req.text
            soup = BeautifulSoup(page, 'html.parser')
            ext = 'rip'
            file_list = []
            for node in soup.find_all('a'):
                if node.get('href').endswith(ext):
                    file_listing = {}
                    file_listing['file_url'] = url + '/' + node.get('href')
                    file_info = node.nextSibling.strip().split('  ')
                    #format the date time string into datetime object
                    date_time = du_parser.parse(file_info[0])
                    file_listing['last_modified'] = date_time
                    file_list.append(file_listing)
            return file_list
        else:
            self.logger.error("Request failed with status code: %d" % (req.status_code))
        return []


    def download_file(self, file_url):
        self.logger.debug("Requesting url: %s" % (file_url))
        session = requests.Session()
        session.trust_env = False  # Don't read proxy settings from OS
        req = session.get(file_url)
        if req.status_code == 200:
            page = req.text
            return page
        else:
            self.logger.error("URL request failed with status code: %d" % (req.status_code))
        return None

