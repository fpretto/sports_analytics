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

        # Archivos
        self._video_name = config['input_video_name']

        # Directorios
        self._root = config['root']
        self._tree_datasets = config['tree_datasets']
        self.tree_clf = config['tree_clf']
        self.tree_pl_detect = config['tree_pl_detect']
        self.tree_mapping = config['tree_mapping']
        self.tree_utils = config['tree_utils']
        self.tree_outputs = config['tree_outputs']

    @property
    def video_name(self):
        return self._video_name

    @property
    def root(self):
        return self._root

    @property
    def tree_datasets(self):
        return self._tree_datasets

    ######################################################################
    # METODOS PARA OBTENER NOMBRES COMPUESTOS
    ######################################################################

    # Directorio donde estan ubicados los datasets
    def get_path_datasets(self):
        return self.root + self.tree_datasets

    # Directorio donde esta ubicado el modelo de Sports Classifier
    def get_path_clf(self):
        return self.root + self.tree_clf

    # Directorio donde esta el modelo de Player Detection
    def get_path_pl_detection(self):
        return self.root + self.tree_pl_detect

    # Directorio donde esta ubicado el modelo de Court Mapping
    def get_path_mapping(self):
        return self.root + self.tree_mapping

    # Directorio donde estan ubicados las funciones Helpers
    def get_path_utils(self):
        return self.root + self.tree_utils

    # Directorio donde estan ubicados los outputs de las ejecuciones
    def get_path_outputs(self):
        return self.root + self.tree_outputs



