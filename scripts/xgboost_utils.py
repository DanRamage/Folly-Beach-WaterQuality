import os
import pickle
import xgboost
import configparser
def main():

    test_config_file = "/Users/danramage/Documents/workspace/WaterQuality/FollyBeach-WaterQuality/config/models/TRI-64-debug.ini"

    model_config_file = configparser.RawConfigParser()
    model_config_file.read(test_config_file)
    # Get the number of prediction models we use for the site.
    model_count = model_config_file.getint("xgboost_models", "model_count")

    for cnt in range(model_count):
        section_name = "xgboost_model_%d" % (cnt + 1)
        model_pickle_file = model_config_file.get(section_name, "pickle_file")

        directory, filename_part = os.path.split(model_pickle_file)
        filename, ext = os.path.splitext(filename_part)
        new_model_save_filename = os.path.join(directory, f"{filename}_newsave.json")
        old_json = os.path.join(directory, f"{filename}.json")
        xgb_model = xgboost.XGBModel()
        xgb_model.load_model(old_json)
        if xgb_model.objective == 'binary:logistic':
            new_model = xgboost.XGBClassifier()
        else:
            new_model = xgboost.XGBRegressor()
        new_model.load_model(old_json)
        new_model.save_model(new_model_save_filename)

        #xgb_model.save_model(new_model_save_filename)
        #old_model = pickle.load(open(old_json, "rb"))
        #old_model.save_model(new_model_save_filename)

    return


if __name__ == "__main__":
    main()