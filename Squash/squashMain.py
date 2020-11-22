#####################################################################################################################
#
# Purpose: Predecir si el video dado como input es de Squash
#
#   Inputs:
#       Video
#
#   Outputs:
#       Prediccion
#
#TODO: el objeto creado se guarda en pkl pero necesita la definición de clase para el load. Se necesita
#######################################################################################################################

for name in dir():
    if not name.startswith('_'):
        del globals()[name]

import os
import sys
import importlib
import pickle as pkl
import cv2
import json
from datetime import datetime
from keras.models import load_model

cwd = os.getcwd() + '/Squash/'

sys.path.insert(0, cwd)
import utils

#sys.path.insert(0, cwd)
#from classConfig import classSquashConfig as config_manager

sys.path.insert(0, cwd + '/01 - Sport Classifier/')
import classSportClassifier

sys.path.insert(0, cwd + '/02 - Court Detection/')
import classCourtDetection

sys.path.insert(0, cwd + '/03 - Player Detection/')
import classPlayerDetection

sys.path.insert(0, cwd + '/04 - Court Mapping/')
import classCourtMapping

sys.path.insert(0, cwd + '/05 - Report Generation/')
import classStatsGeneration

importlib.reload(classSportClassifier)
importlib.reload(classCourtDetection)
importlib.reload(classPlayerDetection)
importlib.reload(classCourtMapping)
importlib.reload(classStatsGeneration)
importlib.reload(utils)

#config = json.load(open(cwd + 'squashConfig.json'))

##########################################################################
#  DEFINITIONS
##########################################################################
with open(cwd + 'parameterCatalog.json') as f:
  paramsCatalog = json.load(f)

master_path = paramsCatalog['general']['master_path']
video_path = master_path + paramsCatalog['general']['video_path']
video_name = paramsCatalog['general']['video_name']
username = paramsCatalog['general']['username']

path = master_path + '99 - Ejecuciones/'
date = datetime.now().strftime('%d-%m-%Y')
date_suffix = '_' + str(datetime.now().year) + str('%02d' % datetime.now().month) + str('%02d' % datetime.now().day)
project_folder = video_name.split('.')[0] + date_suffix + '/'
output_path = path + project_folder
utils.make_dir(output_path)

##########################################################################
#  SPORT CLASSIFIER
##########################################################################

clf_model = load_model(master_path + '01 - Sport Classifier/squash_classifier.h5')
labels = pkl.loads(open(master_path + '01 - Sport Classifier/label_binarizer', "rb").read())

sport_clf = classSportClassifier.SportClassifier(clf_model, labels, pct_sample=0.005, output_path=output_path)

sport_clf.predictSport(video_path + video_name)

##########################################################################
#  COURT DETECTION
##########################################################################

CD = classCourtDetection.CourtDetection()
src_pts = CD.detectCourt(video_path, video_name, output_path, video_name.split('.')[0] + date_suffix)

##########################################################################
#  PLAYER DETECTION AND TRACKING
##########################################################################

dictPlayers = {'player_A': {'label': 'Player A', 'player_coords': [], 'player_torso': [], 'tracking_coords': [], '2d_court_coords': []},
               'player_B': {'label': 'Player B', 'player_coords': [], 'player_torso': [], 'tracking_coords': [], '2d_court_coords': []}}

# Instancia de Player Detection
yolo_model = load_model(master_path + '03 - Player Detection and Tracking/yolo_model.h5')
PDT = classPlayerDetection.PlayerDetection(yolo_model)

# Instancia de Court Mapping
court_coords = pkl.load(open('C:/GoogleDrive/LudisAI/05 - Court Mapping/squash_court_coords.pkl', 'rb'))
court_img = cv2.imread('C:/GoogleDrive/LudisAI/05 - Court Mapping/squash_court.jpg')
CM = classCourtMapping.CourtMapping(court_img, court_coords)

# Player Identification
dictPlayers = PDT.identifyPlayers(video_path, video_name, dictPlayers, username, src_pts, output_path, video_name.split('.')[0] + date_suffix)

dictPlayers['player_B']['label'] = 'Rodriguez'

# Player Tracking
start_time = datetime.now()
dictPlayers, video_duration = PDT.detectPlayers(video_path, video_name, dictPlayers, username, src_pts, output_path, video_name.split('.')[0] + date_suffix, CM)
end_time = datetime.now()

end_time-start_time
##########################################################################
#  COURT MAPPING
##########################################################################

#dictPlayers = pkl.load(open('C:/GoogleDrive/LudisAI/99 - Ejecuciones/squash-trim_20200602/03_player_coords_squash-trim_20200602.pkl', 'rb'))

heatmap = CM.createHeatmap(dictPlayers['player_A']['2d_court_coords'], username, video_duration, bins=15)

##########################################################################
#  REPORT GENERATION
##########################################################################

#output_path = 'C:/GoogleDrive/LudisAI/99 - Ejecuciones/squash-trim_20200602/'
#username = 'Nick Matthew'
#video_name = 'squash-trim.avi'
#video_duration = 791.8620689655172

username = 'Matthew'

RG = classStatsGeneration.ReportGeneration()
RG.generateReport(dictPlayers, output_path, username, heatmap, video_name, video_duration, date)

dict_stats = RG.generateStats(dictPlayers['player_A']['2d_court_coords'], dictPlayers['player_B']['2d_court_coords'], video_duration)

t_control_score, points = RG.calculateTControlScore(dictPlayers['player_A']['2d_court_coords'])
RG.plotTControlScore(points)

with open(output_path + '06_stats_' + username + '.json', 'w') as outfile:
    json.dump(dict_stats, outfile)




