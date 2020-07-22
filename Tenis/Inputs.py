#####################################################################################################################
#
# Purpose: Generacion de Inputs para el ETL
# Creator: Fabricio Pretto
#
#   Inputs:
#       Parametros de entrada
#
#   Outputs:
#       #
#  TODO:
#######################################################################################################################

######################################################################
# LOAD LIBRARIES
######################################################################

from datetime import datetime

######################################################################
# INPUTS
######################################################################

##### Configuration #####

nombre_ejecucion = 'GrandSlam'
proceso_etl      = 'T'               # Tipo de proceso a realizar. Posibles valores: [E, T, L]
json_torneos     = 'tournaments_2019_12.json'

##### Filters #####
# Diccionario para filtrar la ejecucion.
# Si un campo no se completa, se ejecutan todos los posibles valores (dictTorneos)

dictEjecucion = {'tr_gender'        : ['men'],
                 'tr_type'          : ['singles'],
                 'tr_category_name' : ['ATP'],
                 'tr_category_level': ['grand_slam']
                 }

dictTorneos = {'men': {'Challenger': 'Challenger',
                       'ITF Men': 'ITF Men',
                       'ATP': {'grand_slam', 'atp_250', 'atp_500', 'atp_1000', 'atp_next_generation',
                               'atp_world_tour_finals'}
                       },
               'women': {'ITF Women': 'ITF Women',
                         'WTA 125K': 'WTA 125K',
                         'WTA': {'wta_championships', 'wta_international', 'wta_premier', 'wta_elite_trophy'}
                         }
               }

dictConnParams = {'user'    : "postgres",
                  'password': "1082",
                  'host'    : "127.0.0.1",
                  'port'    : "5432",
                  'database': "postgres"}


##### Paths #####

path_master = 'C:/GoogleDrive/Datawarehouse/Tenis/'

##### Logging #####

log_format = "%(asctime)s | %(levelname)s | %(processName)s | %(funcName)s | %(message)s" # Formato del archivo de logs

