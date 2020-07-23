#####################################################################################################################
#
# Purpose: Detectar e Identificar a los jugadores
#
#   Inputs:
#       Video
#
#   Outputs:
#       Prediccion
#
#######################################################################################################################

from keras.models import load_model
from shapely.geometry import box, Point
import importlib
import pickle as pkl
import cv2
import sys

sys.path.insert(0, 'C:/Repo/Ludis/Squash/03 - Player Detection/')
import YOLOv3 as yolo3
importlib.reload(yolo3)

class PlayerIdentification:
    def __init__(self, output_path=None):
        self.output_path = output_path
