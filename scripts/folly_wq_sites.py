import sys
sys.path.append('../commonfiles/python')
from wqHistoricalData import station_geometry,sampling_sites,geometry_list
import csv
from wq_sites import wq_sample_sites, wq_site

class folly_wq_site(wq_site):
  def __init__(self, **kwargs):
    wq_site.__init__(self, **kwargs)
    self.run_model = kwargs.get('run_model', True)

class folly_wq_sites(wq_sample_sites):
  def __init(self, **kwargs):
    wq_sample_sites.__init__(self, **kwargs)

  def load_sites(self, **kwargs):
    if 'file_name' in kwargs:
      if 'boundary_file' in kwargs:
        wq_boundaries = geometry_list(use_logger=True)
        if kwargs['boundary_file'] is not None:
          wq_boundaries.load(kwargs['boundary_file'])

      try:
        header_row = ["WKT","EPAbeachID","SPLocation","Description","County","Boundary","RunModel"]
        if self.logger:
          self.logger.debug("Reading sample sites file: %s" % (kwargs['file_name']))

        sites_file = open(kwargs['file_name'], "rU")
        dict_file = csv.DictReader(sites_file, delimiter=',', quotechar='"', fieldnames=header_row)
      except IOError as e:
        if self.logger:
          self.logger.exception(e)
      else:
        line_num = 0
        for row in dict_file:
          if line_num > 0:
            add_site = False
            #The site could be in multiple boundaries, so let's search to see if it is.
            station = self.get_site(row['SPLocation'])
            if station is None:
              add_site = True
              extents_wkt = None
              station = folly_wq_site(name=row['SPLocation'],
                                        wkt=row['WKT'],
                                        epa_id=row['EPAbeachID'],
                                        description=row['Description'],
                                        county=row['County'],
                                        run_model=row['RunModel'])
              if self.logger:
                self.logger.debug("Processing sample site: %s" % (row['SPLocation']))
              self.append(station)
              try:
                if len(row['Boundary']):
                  boundaries = row['Boundary'].split(',')
                  for boundary in boundaries:
                    if self.logger:
                      self.logger.debug("Sample site: %s Boundary: %s" % (row['SPLocation'], boundary))
                    boundary_geometry = wq_boundaries.get_geometry_item(boundary)
                    if add_site:
                      #Add the containing boundary
                      station.contained_by.append(boundary_geometry)
              except AttributeError as e:
                self.logger.exception(e)
          line_num += 1
        return True
    return False

