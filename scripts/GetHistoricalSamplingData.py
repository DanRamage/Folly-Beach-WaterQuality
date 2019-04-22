import os
import sys
sys.path.append('../../commonfiles/python')
import logging.config
from data_collector_plugin import data_collector_plugin
import ConfigParser
import traceback
import geojson
import optparse
import ConfigParser

from dhecBeachAdvisoryReader import waterQualityAdvisory
from folly_wq_sites import folly_wq_sites

def main():
    parser = optparse.OptionParser()
    parser.add_option("--ConfigFile", dest="config_file",
                      help="INI Configuration file.")
    parser.add_option("--GetSampleData", dest="get_sample_data", action="store_true",
                      help="INI Configuration file.")
    parser.add_option("--GetStationMetadata", dest="get_station_metadata", action="store_true",
                      help="INI Configuration file.")
    parser.add_option("--DHECSOAPURL", dest="soap_url",
                      help="INI Configuration file.")
    parser.add_option("--DHECRESTURL", dest="rest_url",
                      help="INI Configuration file.")
    parser.add_option("--LogConfigFile", dest="log_config_file",
                      help="INI Configuration file.")
    parser.add_option("--SampleDataFile", dest="sample_data_file",
                      help="")
    parser.add_option("--SampleStationsFile", dest="sample_stations_file",
                      help="")

    (options, args) = parser.parse_args()

    try:
        config_file = ConfigParser.RawConfigParser()
        config_file.read(options.config_file)

        logging.config.fileConfig(options.log_config_file)
        logger = logging.getLogger('wq_processing_logger')
        logger.info("Log file opened.")

        sites_location_file = config_file.get('boundaries_settings', 'sample_sites')
        wq_sites = folly_wq_sites()
        wq_sites.load_sites(file_name=sites_location_file, boundary_file=None)

        logger.debug("Creating dhec sample query object.")
        advisoryObj = waterQualityAdvisory(options.soap_url, True)

        station_list = []
        for site in wq_sites:
            station_list.append(site.name)

        if options.get_sample_data:
            year_list = []
            for year in range(2005, 2019, 1):
                year_list.append(year)
            logger.debug("Beginning SOAP query.")
            data = advisoryObj.get_sample_data(station_list, year_list)
            for station in station_list:
                full_path = os.path.join(options.sample_data_file, "%s.csv" % (station))
                with open(full_path, "w") as sample_file:
                    sample_file.writelines('Station,Date,Value\n')
                    station_data = data[station]
                    for rec in station_data['results']:
                        sample_file.writelines('%s,%s,%s\n' % (rec['station'], rec['date'], rec['value']))
            logger.debug("Finished SOAP query.")

        if options.get_station_metadata:
            with open(options.sample_stations_file, "w") as samp_stations_file:
                samp_stations_file.writelines('WKT,EPAbeachID,SPLocation,Description,County,Boundary\n')
                metadata = advisoryObj.get_station_data_from_dhec(options.rest_url)
                for station in station_list:
                    for meta_rec in metadata:
                        if station == str(meta_rec['attributes']["STATION"]):
                            if len(options.sample_stations_file):
                                wkt_point = 'POINT(%f %f)' % (meta_rec['attributes']['LONGITUDE'], meta_rec['attributes']['LATITUDE'])
                                desc = "%s" % (meta_rec['attributes']['LOCATION'])
                                samp_stations_file.writelines('\"%s\",%s,%s,%s,%s,%s\n' % (
                                    wkt_point,
                                    meta_rec['attributes']['EPA_ID'],
                                    meta_rec['attributes']['STATION'],
                                    desc,
                                    meta_rec['attributes']['DISTRICT'],
                                    ''))


    except (IOError, Exception) as e:
        logger.exception(e)


if __name__ == "__main__":
    main()