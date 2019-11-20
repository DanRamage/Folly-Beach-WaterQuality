import logging.config
from collections import OrderedDict
from datetime import datetime
import time
import numpy as np
from enterococcus_wq_test import EnterococcusPredictionTest
from wq_prediction_tests import wqEquations
import pickle

class prediction_levels(object):
  DISABLED = -2
  NO_TEST = -1
  LOW = 1
  HIGH = 3
  def __init__(self, value):
    self.__value = value
  def __str__(self):
    if self.value == self.LOW:
      return "LOW"
    elif self.value == self.HIGH:
      return "HIGH"
    elif self.value == self.DISABLED:
      return "TEST DISABLED"
    else:
      return "NO TEST"

  @property
  def value(self):
    return self.__value

  @value.setter
  def value(self, value):
    self.__value = value

class xgb_model(EnterococcusPredictionTest):
    def __init__(self, **kwargs):
        self._logger = logging.getLogger(__name__)
        self._name = kwargs['name']

        self._classifier_type = kwargs['classifier_type']

        self.low_limit = kwargs.get('low_limit', 104.0)
        self.high_limit = kwargs.get('high_limit', 500.0)

        self.data_used = OrderedDict()
        self._test_time = 0
        self.logger = logging.getLogger(type(self).__name__)
        self._result = None
        self.platform_settings = kwargs['platforms']
        self.tide_settings = kwargs['tide_station_nfo']
        model_file = kwargs['model_file']
        model_file_type = kwargs['model_file_type']
        if model_file_type == 'pickle':
            try:
                self._xgb_model = pickle.load(open(model_file, "rb"))
            except Exception as e:
                self._logger.exception(e)
                raise e

        self._prediction_level = prediction_levels(prediction_levels.NO_TEST)
        self._model_data = None

    @property
    def name(self):
        return self._name
    @property
    def classifier_type(self):
        return self._classifier_type
    @property
    def result(self):
        return self._result
    @property
    def test_time(self):
        return self._test_time
    @property
    def prediction_level(self):
        return self._prediction_level
    @property
    def model_data(self):
        return self._model_data

    def runTest(self, site_data):
        try:
            start_time = time.time()
            self.logger.debug("Model: %s test" % (self._name))
            self._model_data = site_data.copy()
            # Data comes in as an OrderedDict, we convert over to numpy array for xgboost.
            test_list = []
            for item in self._model_data.values():
                test_list.append(item)
            # X_test = np.array(list([item for item in data.values()[1:]]))
            X_test = np.array([test_list])

            self._result = self._xgb_model.predict(X_test)
            if self._classifier_type == 'binary':
                if self._result:
                    self._prediction_level.value = prediction_levels.HIGH
                    self._result = True
                else:
                    self._prediction_level.value  = prediction_levels.LOW
                    self._result = False

            else:
                if self._result > self.high_limit:
                    self._prediction_level.value  = prediction_levels.HIGH
                else:
                    self._prediction_level.value  = prediction_levels.LOW

            self._test_time = time.time() - start_time
        except Exception as e:
            self.logger.exception(e)
            self._prediction_level.value  = prediction_levels.NO_TEST

        self.logger.debug("Model: %s result: %s finished in %f seconds." % (self._name, self._result, self.test_time))
        return


class xgb_ensemble(wqEquations):
    def __init__(self, station, model_list):
        self.station = station  # The station that this object represents.
        self._models = model_list
        self._ensemble_prediction = prediction_levels(prediction_levels.NO_TEST)
        self.logger = logging.getLogger(__name__)

    @property
    def ensemble_predicition(self):
        return self._ensemble_prediction
    @property
    def models(self):
        return self._models

    def overall_prediction(self):
        '''
        Purpose: From the models used, determine the model type and come up with overall prediction level. Some models
        are binary, so either Pass/Fail, some are linear.
        :param self:
        :return:     A predictionLevels value.

        '''
        executed_tests = 0
        if len(self._models):
          sum = 0
          for model in self._models:
            if model.prediction_level.value == prediction_levels.LOW or\
                model.prediction_level.value != prediction_levels.HIGH:
                sum += model.prediction_level.value
                executed_tests += 1

          if executed_tests:
            self._ensemble_prediction.value = int(round(sum / executed_tests))


        self.logger.debug("Overall Prediction: %d(%s)" %(self._ensemble_prediction.value, self._ensemble_prediction.__str__()))
        return self._ensemble_prediction
