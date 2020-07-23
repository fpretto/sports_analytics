#####################################################################################################################
#
# Purpose: Helper functions
# Creator: Fabricio Pretto
#
#  TODO:
#######################################################################################################################

for name in dir():
    if not name.startswith('_'):
        del globals()[name]


import json
import os
import sys
cwd = os.getcwd() + '/Squash/'
sys.path.insert(0, cwd)



class classSquashConfig:

    # Usamos el constructor de la clase para leer el archivo JSON
    def __init__(self):
        with open(cwd + 'squashConfig.json') as config_file:
            config = json.load(config_file)

        # General
        self.root = config['general']['master_path']
        self.video_path = config['general']['video_path']
        self.video_name = config['general']['video_name']
        self.username = config['general']['username']

        # Sport Classifier
        self.clf_path = config['sport_clf']['model_path']
        self.clf_model = config['sport_clf']['model']
        self.clf_labels = config['sport_clf']['labels']

        # Player Tracking
        self.pl_track_path = config['player_tracking']['model_path']
        self.pl_track_model = config['player_tracking']['model']

    # Directorio donde estan ubicados los datasets
    def get_path_video(self):
        return self.root + self.video_path

    # Directorio donde esta ubicado el modelo de Sports Classifier
    def get_path_clf(self):
        return self.root + self.clf_path

    # Directorio donde esta el modelo de Player Detection
    def get_path_pl_detection(self):
        return self.root + self.pl_track_path

    # Directorio donde esta ubicado el modelo de Court Mapping
    def get_path_mapping(self):
        return self.root + self.tree_mapping

    # Directorio donde estan ubicados las funciones Helpers
    def get_path_utils(self):
        return self.root + self.tree_utils

    # Directorio donde estan ubicados los outputs de las ejecuciones
    def get_path_outputs(self):
        return self.root + self.tree_outputs



