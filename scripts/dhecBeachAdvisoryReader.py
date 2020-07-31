"""
Revisions
Date: 2015-03-16
Function: __scrapeResults
Changes: Use the newly implemented SOAP response.

Date: 2014-07-11
Function: __scrapeResults
Changes: Verify securityParams object is valid before attempting to use it.

Date: 2013-07-09
Function: waterQualityAdvisor:processData
Changes: The web page we were scraping to get the actual sample data has been taken down. Added code to handle the
webquery when nothing is returned. We look use the historical data for this case as well.
"""
import sys
sys.path.append('../commonfiles/python')
#import requests
import logging.config
import optparse
import ConfigParser
import copy
from lxml import etree    
import geojson
import json
#import urllib
#import urllib2
import requests
import socket 
import datetime
from pytz import timezone
from dateutil import parser as du_parser

import csv
from decimal import *
import httplib

import suds
from suds.client import Client
from suds.xsd.doctor import Import, ImportDoctor

"""
Class: waterQualityAdvisory
Purpose: Used to scrape the DHEC website for the station data as well as importing the station metadata from their csv file(from excel sheet).
"""
class waterQualityAdvisory(object):
  def __init__(self, baseUrl, logger=True):
    self.baseUrl = baseUrl    #The URL to the DHEC page to get the station data. 
                              #This is without the POST parameter 'station' which is added at the point the request is created.

    #httplib.HTTPConnection.debuglevel = 1                         
    if(logger):
      self.logger = logging.getLogger(type(self).__name__)
      #requests_log = logging.getLogger("requests.packages.urllib3")
      #requests_log.setLevel(logging.DEBUG)
      #requests_log.propagate = True      
    
    self.pageDataDict = {}
    self.pageDataDict['results'] = {
        'each' : "//table[contains(@id, 'GridView1')]/tr",
        'date' : "td[@align='left']//text()",
        'value' : "td[@align='center']//text()"
    }
  """
  Function: createStationGeoJSON
  Purpose: Creates a geoJSON file containing all the station data read from a CSV file.
  Parameters:
    csvFile - String with the full path to the csv file we are going to import.
    geoJSONOutfile - String with full path to the geoJSON file we are creating from the csv file.
  Return:
    None
  """
  def createStationGeoJSON(self, csvFile, geoJSONOutfile):
    import csv
    import math
    if(self.logger):
      self.logger.info("Creating station geoJson file.")
    try:
      colNames = ['station',
                  'address',
                  'city-county',
                  'epabeachid',
                  'beachname',
                  'beachlen',
                  'latitude',
                  'longitude',
                  'permanentsign'
                 ]
      srcFileObj = open(csvFile, 'rU')
      srcFile = csv.DictReader(srcFileObj, colNames)
      if(self.logger):
        self.logger.info("Opened input file: %s" % (csvFile))
      destFile = open(geoJSONOutfile, "w")
      if(self.logger):
        self.logger.info("Opened output file: %s" % (geoJSONOutfile))
        
    except IOError as e:
      if(self.logger):
        self.logger.exception(e)
    else:
      lineCnt = 0
      features = []
      try:
        locale = None
        beach = None
        for line in srcFile:
          #Bump past first 2 lines since they are the header rows.
          if(lineCnt > 1):
            #Verify we have valid coordinates.
            if(len(line['latitude']) and len(line['longitude'])):
              try:              
                latitude = float(line['latitude'].strip())
                longitude = float(line['longitude'].strip())

                geometry = geojson.Point(coordinates=[longitude, latitude])
              except ValueError as e:
                if(self.logger):
                  self.logger.error("Line: %d invalid value for either latitude or longitude" % (lineCnt))
                continue
              #Now let's get the rest of the properties.
              else:
                properties = {}
                if(len(line['city-county'].strip()) and locale != line['city-county']):
                  locale = line['city-county']
                if(len(line['beachname'].strip()) and beach != line['beachname']):
                  beach = line['beachname']
                properties['station'] = line['station']
                properties['desc'] = line['address']
                properties['locale'] = locale
                properties['epaid'] = line['epabeachid']
                properties['beach'] = beach
                properties['len'] = line['beachlen']
                properties['sign'] = False
                if(line['permanentsign'].lower() == 'yes'):
                  properties['sign'] = True
                #Create the geoJson feature object
                feature = geojson.Feature(id=properties['station'], geometry=geometry, properties=properties)
                features.append(feature)

                if(self.logger):
                  self.logger.info("Adding station: %s" % (properties['station']))
          lineCnt += 1
        featureColl = geojson.FeatureCollection(features=features)

        destFile.write(geojson.dumps(featureColl, separators=(',', ':')))
        #destFile.write(geojson.dumps(featureColl, sort_keys=True, indent=4 * ' '))

        if(self.logger):
          self.logger.info("JSON results written to file.")
      except Exception as e:
        if(self.logger):
          self.logger.exception(e)
      destFile.close()
      srcFileObj.close()

  """
  Function: createHistoricalJSON
  Purpose: From a CSV file of historical water quality data, create/append to a JSON file of the data.
  Parameters: 
    inputFile - String with the filename of the CSV file to import
    outputFile - Filename of the JSON file to create/append to.
  Returns:
    JSON object with the data.
  """
  def createHistoricalJSON(self, inputFilename, outputFilename):
    waterQualityJson = None
    """
    fieldNames = ["Station",
                  "Inspection Date",
                  "Insp Time",
                  "Lab Number",
                  "Inspection Type",
                  "E Sign",
                  "ETCOC",
                  "Tide",
                  "Wind/Curr",
                  "Weather"]
    """
    fieldNames = [
      "COLLECTION DATE",
      "COLLECTOR",
      "STATION",
      "INSPECTION TIME",
      "LABSAMPLENUMBER",
      "ETCOCSIGN",
      "ETCOC",
      "RAINFALL",
      "WIND",
      "TIDE",
      "WEATHER",
      "INSPECTIONTYPE"
    ]
    try:
      inputFile = open(inputFilename, 'rU')
      dataFile = csv.DictReader(inputFile, fieldNames)
      outFile = open(outputFilename, 'w')
    except IOError as e:
      if(self.logger):
        self.logger.exception(e)
    else:
      jsonObj = {}
      lineNum = 0
      for line in dataFile:
        if(lineNum > 0):
          stationData = []
          if(line['STATION'] in jsonObj):
          #if(line['Station'] in jsonObj):
            stationData = jsonObj[line['STATION']]
            #stationData = jsonObj[line['Station']]
          else:
            jsonObj[line['STATION']] = stationData
            #jsonObj[line['Station']] = stationData

          #dateVal = datetime.datetime.strptime(line['Inspection Date'], "%d-%b-%y")
          #dateVal = datetime.datetime.strptime(line['Inspection Date'], "%m/%d/%Y")
          dateVal = datetime.datetime.strptime(line['COLLECTION DATE'], "%m/%d/%y %H:%M")

          #timeVal = datetime.datetime.strptime(line['Insp Time'], "%H%M")
          #dateVal += (' ' + timeVal.strftime("%H:%M:00"))
          #dateVal = datetime.datetime.strptime(dateVal, "%Y-%m-%d %H:%M:00")
          #stationDict[dateVal] = line['ETCOC']
          wqObj = {'date' : dateVal, 'value' : line['ETCOC']}
          stationData.append(wqObj)

        lineNum += 1
    #Sort
    for index,dataObj in enumerate(jsonObj):
      stationEntries = jsonObj[dataObj]
      stationEntries.sort(key=lambda r: r['date'])
      #Convert the datetime objects into strings for the JSONification process.
      for index2,stationObj in enumerate(stationEntries):
        #stationObj['date'] = stationObj['date'].strftime("%Y-%m-%d %H:%M:00")
        stationObj['date'] = stationObj['date'].strftime("%Y-%m-%d")
    try:
      outFile.write(geojson.dumps(jsonObj, sort_keys=True, indent=4 * ' '))
    except Exception as e:
      if(self.logger):
        self.logger.exception(e)
    outFile.close()
    inputFile.close()
    return(jsonObj)
  """
  Function: processData
  Purpose: Scrapes the DHEC web pages for each station pulling in all the test results. The pages are created
    through a POST command and the data is in a table in the returned page.
  Parameters:
    stationNfoList - A FeatureCollection object that has geoJSON data for all the stations. 
    jsonOutputFilepath - String with the output path where each stations results geoJSON file is created.
      These results differ from the station geoJSON object in that there is an extra 'properties' parameter, 'test', that has a 
      key 'beachadvisories' that is an array of results data.
  Return:
    None
  """
  def processData(self, **kwargs):
    stationGeoJsonFile = kwargs['geo_json_file']
    jsonOutputFilepath  = kwargs['json_file_path']
    historyWQ = kwargs['historical_wq']
    dhec_rest_url = kwargs['dhec_url']
    sample_data_post_url = kwargs.get('post_data_url', None)
    sampling_stations = kwargs.get('sampling_stations', None)
    if(self.logger):
      self.logger.info("Begin data processing.")

    self.logger.debug("Opening station metadata file: %s" % (stationGeoJsonFile))
    stationNfoList = {}
    try:
      with open(stationGeoJsonFile, "r") as stationDataFile:
        stationNfoList = geojson.load(stationDataFile)
        features = stationNfoList['features']
        #If we passed in a sampling stations object, let's only use the stations
        #from that, pruning out everything else.
        if sampling_stations is not None:
          station_list = []
          for station in sampling_stations:
            station_list.append(station.name)
          delete_list = []
          for ndx, feature in enumerate(features):
            if feature['id'] not in station_list:
              delete_list.append(ndx)
          if len(delete_list):
            delete_list.sort(reverse=True)
            for item in delete_list:
              del features[item]
    except (IOError, Exception) as e:
      self.logger.exception(e)
    else:
      dhec_rest_results = self.get_station_data_from_dhec(dhec_rest_url)
      local_metadata_updated = self.update_local_station_metadata(stationNfoList, dhec_rest_results)
      resultsData = self.__scrapeResults(stationNfoList)
      #DWR 2012-12-05
      if(len(resultsData)):
        for index,stationName in enumerate(resultsData):
          if(len(resultsData[stationName]['results']) == 0):
            if(self.logger):
              self.logger.debug("Station: %s no results from webquery, adding in historical." % (stationName))
            if(stationName in historyWQ):
              resultsData[stationName]['results'] = historyWQ[stationName]
          if sample_data_post_url is not None:
            station_nfo = resultsData[stationName]['results']
            for result in station_nfo:
              sub_dict = {'station_name': stationName,
                                             'date': result['date'],
                                             'value': result['value']}
              url = sample_data_post_url.format(**sub_dict)
              try:
                requests.post(url)
              except Exception as e:
                self.logger.exception(e)
      #DWR 2013-07-09
      #No result at all.
      else:
        resultsData = {}
        if(self.logger):
          self.logger.debug("Web query failed.")
        for feature in stationNfoList['features']:
          stationName = feature['id']
          resultsData[stationName] = {'results' : {}}
          if(self.logger):
            self.logger.debug("Station: %s no results from webquery, adding in historical." % (stationName))
          if(stationName in historyWQ):
              resultsData[stationName]['results'] = historyWQ[stationName]
          else:
            if(self.logger):
              self.logger.debug("Station: %s not found in historical." % (stationName))


      self.__outputGeoJson(stationNfoList, resultsData, jsonOutputFilepath)
      if local_metadata_updated:
        try:
          self.logger.debug("Local metadata updated, saving changes.")
          with open(stationGeoJsonFile, "w") as stationDataFile:
            geojson.dump(stationNfoList, stationDataFile)
        except (IOError, TypeError) as e:
          self.logger.exception(e)

    if(self.logger):
      self.logger.info("Data processing completed.")


  def get_station_data_from_dhec(self, dhec_rest_url):
    station_features = None
    try:
      self.logger.debug("Querying DHEC rest.")
      req = requests.get(dhec_rest_url)
      recs = json.loads(req.content)
      station_features = recs['features']
    except Exception as e:
      self.logger.exception(e)
    return station_features

  def update_local_station_metadata(self, internal_station_metadata, dhec_station_metadata):
    local_metadata_updated = False
    for local_meta in internal_station_metadata['features']:
      station_name = local_meta['properties']['station']
      self.logger.debug("Station: %s search in DHEC REST metadata" % (station_name))
      dhec_rec = [rec for rec in dhec_station_metadata if rec['attributes']['STATION'] == station_name]
      if len(dhec_rec):
        local_properties = local_meta['properties']
        dhec_properties = dhec_rec[0]['attributes']
        if local_properties['beach'] != dhec_properties['BEACH_NAME']:
          local_properties['beach']=dhec_properties['BEACH_NAME']
          local_metadata_updated = True
        if local_properties['epaid'] != dhec_properties['EPA_ID']:
          local_properties['epaid']=dhec_properties['EPA_ID']
          local_metadata_updated = True
        if local_properties['desc'] != dhec_properties['LOCATION']:
          local_properties['desc']=dhec_properties['LOCATION']
          local_metadata_updated = True
        if dhec_properties['Advisory'] == 'No Advisory':
          if local_properties['sign'] != False:
            self.logger.debug('Station: %s advisory status changed from True to False.' % (station_name))
            local_metadata_updated = True
          local_properties['sign']=False
        else:
          if local_properties['sign'] != True:
            self.logger.debug('Station: %s advisory status changed from False to True.' % (station_name))
            local_metadata_updated = True
          local_properties['sign']=True

      else:
        self.logger.error("Unable to find station: %s in dhec rest data" % (station_name))

    return local_metadata_updated
  """
  Function: __scrapeResults
  Purpose: This is the function that loops through the station list, queries the webpage for the results creating the individual
    station geoJSON files, as well as a comprehensive  geoJSON file for all the stations with the most recent results.
  Parameters:
    stationNfoList - A FeatureCollection object that has geoJSON data for all the stations. 
  Return:
    None
  """
  #2015-03-16 DWR
  #Implement function to use the SOAP response.
  def __scrapeResults(self, stationNfoList):
    results = {}
    utc_tz = timezone('UTC')
    if self.logger:
      logging.getLogger('suds.client').setLevel(logging.DEBUG)
      self.logger.info("SOAP request for beach data.")
    try:
      #schema_url = "http://gisweb01.dhec.sc.gov/beachservice/beachservice.asmx?schema=beach"
      schema_url = "http://gis.dhec.sc.gov/beachservice/beachservice.asmx?schema=beach"
      schema_import = Import(schema_url)
      schema_doctor = ImportDoctor(schema_import)

      #soap_client = Client(url=self.baseUrl, doctor=schema_doctor)
      soap_client = Client(url=self.baseUrl, doctor=schema_doctor)
      if self.logger:
        self.logger.debug("Client: %s" % (soap_client))
      for year_cnt in range(1, -1, -1):
        year = datetime.datetime.now()
        if self.logger:
          self.logger.debug("SOAP GetBeachData request for year: %d" % (year.year - year_cnt))
        response = soap_client.service.GetBeachData(year=year.year - year_cnt)

        for dgram in response.diffgram:
          for DocumentElement in dgram.DocumentElement:
            for beachTable in DocumentElement.BeachTable:
              data = {
                'date': "",
                'value': ""
              }
              """
              data = {
                'id': "",
                'date': "",
                'value': "",
                'sign': "",
                'station': ""
              }
              """
              #data['station'] = beachTable.STATION[0]
              data['station'] = beachTable.Name[0]
              if data['station'] not in results:
                results[data['station']] = {'results': []}

              #data['id'] = beachTable._id
              #2014-05-13T00:00:00-04:00
              #date_parts = beachTable.START_DATE[0].split('T')
              date_parts = beachTable.SamplingDate[0].split('T')
              #Get date and time with TZ
              utc_date = du_parser.parse(beachTable.SamplingDate[0]).astimezone(utc_tz)
              #data['date'] = date_parts[0]
              data['date'] = utc_date.strftime('%Y-%m-%dT%H:%M:%SZ')
              data['value'] = beachTable.ETCOC[0]
              #data['sign'] = beachTable.ETCOC_MOD[0]
              results[data['station']]['results'].append(data)
        for station_name in results:
          station_data = results[station_name]
          sorted_data = sorted(station_data['results'], key=lambda rec: rec['date'])
          station_data['results'] = sorted_data
        #with open("/Users/danramage/tmp/response.txt", "w") as outfile:
        #  outfile.write(str(response))
    except Exception as e:
      if self.logger:
        self.logger.exception(e)
    if self.logger:
      self.logger.debug("SOAP request completed.")
    return results

  """
  Function: __scrapeResults
  Purpose: This is the function that loops through the station list, queries the webpage for the results creating the individual
    station geoJSON files, as well as a comprehensive  geoJSON file for all the stations with the most recent results.
  Parameters:
    stationNfoList - A FeatureCollection object that has geoJSON data for all the stations. 
    resultsData - A dictionary keyed on the station name. Contains an array of date,value entries for the sampling results available.
    jsonOutputFilepath - A String that is the path to store the individual station geoJSON files as well as the comprehensive file.
  Return:
    None
  """
  def __outputGeoJson(self, stationNfoList, resultsData, jsonOutputFilepath):
    if(self.logger):
      self.logger.info("Outputting JSON file.")
    features = []
    missing_stations = []
    for stationData in stationNfoList['features']:
      stationName = stationData['properties']['station']
      if self.logger:
        self.logger.debug("Station: %s" % (stationName))
      properties = {}
      properties['station'] = stationData['properties']['station']
      properties['desc'] = stationData['properties']['desc']
      properties['locale'] = stationData['properties']['locale']
      properties['epaid'] = stationData['properties']['epaid']
      properties['beach'] = stationData['properties']['beach']
      properties['len'] = stationData['properties']['len']
      properties['sign'] = stationData['properties']['sign']
      if stationName in resultsData:
        properties['test'] = {'beachadvisories' : resultsData[stationName]['results']}
        if(len(resultsData[stationName]['results']) == 0):
          properties['message'] = "Bacteria sample data currently unavailable."
      else:
        properties['test'] = {'beachadvisories' : []}
        properties['message'] = "Bacteria sample data currently unavailable."
        #2015-03-16 DWR
        #Log out the missing stations. SInce an email is sent on any errors, we only
        #want to send one email if we have multiple misses.
        missing_stations.append(stationName)
      geometry = stationData['geometry']
      feature = geojson.Feature(id=stationName, geometry=geometry, properties=properties)
      features.append(feature)

      try:
        fullPath = "%s/%s.json" % (jsonOutputFilepath, stationName)
        jsonFile = open(fullPath, "w")
        if(self.logger):
          self.logger.info("Opened JSON file: %s" % (fullPath))
        jsonFile.write(geojson.dumps(feature, separators=(',', ':')))
        #jsonFile.write(geojson.dumps(feature, indent=4 * ' '))
        if(self.logger):
          self.logger.info("JSON results written to file.")
        jsonFile.close()
      except IOError as e:
        if(self.logger):
          self.logger.exception(e)

      #After the individual station file is written, we reset the properties['test']['beachadvisories'] to contain
      #only the latest sample data.
      if stationName in resultsData:
        if(len(resultsData[stationName]['results'])):
          properties['test']['beachadvisories'] = resultsData[stationName]['results'][-1]
    #2015-03-16 DWR
    #Log out the missing stations. SInce an email is sent on any errors, we only
    #want to send one email if we have multiple misses.
    if len(missing_stations):
      if self.logger:
        self.logger.error("Stations: %s not in results data." % (" ,".join(missing_stations)))

    collection = geojson.FeatureCollection(features=features)
    try:
      fullPath = "%s/beachAdvisoryResults.json" % (jsonOutputFilepath)
      jsonFile = open(fullPath, "w")
      if(self.logger):
        self.logger.info("Opened JSON file: %s" % (fullPath))

      jsonFile.write(geojson.dumps(collection, separators=(',', ':')))
      #json_ret = {'status' : {'http_code': 200},
      #            'contents': collection}
      #jsonFile.write(geojson.dumps(json_ret, separators=(',', ':')))
      #jsonFile.write(geojson.dumps(collection, separators=(',', ':')))
      if(self.logger):
        self.logger.info("JSON results written to file.")
      jsonFile.close()
    except IOError as e:
      if(self.logger):
        self.logger.exception(e)

  def get_sample_data(self, station_list, year_list):
    results = {}
    station_results = {}
    utc_tz = timezone('UTC')
    logging.getLogger('suds.client').setLevel(logging.DEBUG)
    self.logger.info("SOAP request for beach data.")
    try:
      #schema_url = "http://gisweb01.dhec.sc.gov/beachservice/beachservice.asmx?schema=beach"
      schema_url = "http://gis.dhec.sc.gov/beachservice/beachservice.asmx?schema=beach"
      schema_import = Import(schema_url)
      schema_doctor = ImportDoctor(schema_import)

      #soap_client = Client(url=self.baseUrl, doctor=schema_doctor)
      soap_client = Client(url=self.baseUrl, doctor=schema_doctor)
      if self.logger:
        self.logger.debug("Client: %s" % (soap_client))
      for year in year_list:
        #year = datetime.datetime.now()
        if self.logger:
          self.logger.debug("SOAP GetBeachData request for year: %d" % (year))
        response = soap_client.service.GetBeachData(year=year)

        for dgram in response.diffgram:
          for DocumentElement in dgram.DocumentElement:
            for beachTable in DocumentElement.BeachTable:
              data = {
                'date': "",
                'value': ""
              }
              """
              data = {
                'id': "",
                'date': "",
                'value': "",
                'sign': "",
                'station': ""
              }
              """
              #data['station'] = beachTable.STATION[0]
              data['station'] = beachTable.Name[0]
              if data['station'] not in results:
                results[data['station']] = {'results': []}

              #data['id'] = beachTable._id
              #2014-05-13T00:00:00-04:00
              #date_parts = beachTable.START_DATE[0].split('T')
              date_parts = beachTable.SamplingDate[0].split('T')
              #Get date and time with TZ
              utc_date = du_parser.parse(beachTable.SamplingDate[0]).astimezone(utc_tz)
              #data['date'] = date_parts[0]
              data['date'] = utc_date.strftime('%Y-%m-%dT%H:%M:%SZ')
              data['value'] = beachTable.ETCOC[0]
              #data['sign'] = beachTable.ETCOC_MOD[0]
              results[data['station']]['results'].append(data)
        for station_name in results:
          if station_name in station_list:
            station_data = results[station_name]
            sorted_data = sorted(station_data['results'], key=lambda rec: rec['date'])
            station_data['results'] = sorted_data
            station_results[station_name] = station_data
    except Exception as e:
      self.logger.exception(e)
    self.logger.debug("SOAP request completed.")
    return station_results


def main():

  parser = optparse.OptionParser()
  parser.add_option("-c", "--ConfigFile", dest="configFile",
                    help="INI Configuration file." )
  parser.add_option("-i", "--ImportStationsFile", dest="importStations",
                    help="Stations file to import." )
  parser.add_option("-t", "--ImportStationsTestResultsFile", dest="importStationsTestResultsFile",
                    help="Stations file to import." )
  (options, args) = parser.parse_args()

  if(options.configFile is None):
    parser.print_help()
    sys.exit(-1)

  try:
    configFile = ConfigParser.RawConfigParser()
    configFile.read(options.configFile)

    logger = None
    logConfFile = configFile.get('logging', 'scraperConfigFile')
    if(logConfFile):
      logging.config.fileConfig(logConfFile)
      logger = logging.getLogger("dhec_beach_advisory_app")
      logger.info("Log file opened.")
  except ConfigParser.Error as e:
    print("No log configuration file given, logging disabled.")
  except Exception as e:
    import traceback
    traceback.print_exc(e)
    sys.exit(-1)

  try:
    #Base URL to the page that house an individual stations results.
    baseUrl = configFile.get('websettings', 'baseAdvisoryPageUrl')

    #output Filename for the JSON data.
    jsonFilepath = configFile.get('output', 'outputDirectory')

    #Filepath to the geoJSON file that contains the station data for all the stations.
    stationGeoJsonFile = configFile.get('stationData', 'stationGeoJsonFile')

    #The past WQ results.
    stationWQHistoryFile = configFile.get('stationData', 'stationWQHistoryFile')

    dhec_rest_url = configFile.get('websettings', 'dhec_rest_url')

    sample_data_post_url = configFile.get('sample_data_rest', 'url')
  except ConfigParser.Error as e:
    if(logger):
      logger.exception(e)

  else:
    """
    #Connect to the DHEC sqlite database. We query the monitoring station platforms so we have a list
    #to then use to query the website with.
    dbObj = dhecDB(dhecDBName,"dhec_beach_advisory_app")
    platformWhere = "WHERE platform_handle LIKE '%.%.monitorstation'"
    monitorStationsCursor = dbObj.getPlatforms(platformWhere)
    
    if(monitorStationsCursor):
      stationList = []
      for row in monitorStationsCursor:
        nfo = {}
        nfo['short_name'] = row['short_name']
        nfo['fixed_latitude'] = row['fixed_latitude'] 
        nfo['fixed_longitude'] = row['fixed_longitude'] 
        nfo['description'] = row['description']
        stationList.append(nfo)
        
      monitorStationsCursor.close()
    else:
      if(logger):
        logger.error("Unable to retrieve the monitoring stations from the database, cannot continue.")
        sys.exit(-1)
        
    dbObj.DB.close()
    """
    try:
      advisoryObj = waterQualityAdvisory(baseUrl, True)
      if(options.importStationsTestResultsFile):
        advisoryObj.createHistoricalJSON(options.importStationsTestResultsFile, "/Users/danramage/tmp/dhec/monitorstations/historicalWQ.json")
      if(options.importStations):
        advisoryObj.createStationGeoJSON(options.importStations, stationGeoJsonFile)
      else:
        #See if we have a historical WQ file, if so let's use that as well.
        historyWQFile = open(stationWQHistoryFile, "r")
        historyWQ = geojson.load(historyWQFile)

        #advisoryObj.processData(stationList, jsonFilepath, historyWQ, dhec_rest_url)
        advisoryObj.processData(geo_json_file=stationGeoJsonFile,
                                json_file_path=jsonFilepath,
                                historical_wq=historyWQ,
                                dhec_url=dhec_rest_url,
                                post_data_url=sample_data_post_url)
    except IOError as e:
      if(logger):
        logger.exception(e)
    except Exception as e:
      if(logger):
        logger.exception(e)
    if(logger):
      logger.info("Closing logfile.") 
if __name__ == "__main__":
  main()      


