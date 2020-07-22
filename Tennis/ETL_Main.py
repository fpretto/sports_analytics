#####################################################################################################################
#
# Purpose: Main para realizar el ETL de Tenis.
#
#######################################################################################################################

######################################################################
# MEMORY CLEAN UP
######################################################################
for name in dir():
    if not name.startswith('_'):
        del globals()[name]


def main():

    ######################################################################
    # LOAD LIBRARIES
    ######################################################################
    import sys
    sys.path.insert(0, 'C:/Repo/Github/sports_analytics/Tennis/')
    import TennisClassETL as etl
    import Inputs as inputs
    import Helpers as helpers
    import json
    import requests
    import pandas as pd
    import numpy as np
    from datetime import datetime
    import os
    import time
    import joblib
    import logging
    import importlib
    import psycopg2
    from psycopg2 import Error
    importlib.reload(etl)
    importlib.reload(inputs)
    importlib.reload(helpers)

    pd.set_option('display.max_columns', 500)
    pd.set_option('display.float_format', '{:.4f}'.format)
    seed = 1984

    start_time = time.time()

    ######################################################################
    # LOAD LIBRARIES
    ######################################################################

    classETL = etl.ETL_Tenis(user_key = 'kb6dv75rtq3pk55uumfkxczc')

    ######################################################################
    # LOGGING CONFIGURATION
    ######################################################################

    # Create and configure logger
    logging.basicConfig(
            filename=inputs.path_master + 'LogsETL' + "_" + str(datetime.now().year) + '_' +
                     str('%02d' % datetime.now().month) + "_" + inputs.nombre_ejecucion + '.log',
            filemode='w',
            level=logging.INFO,
            format=inputs.log_format,
            datefmt='%Y/%m/%d %H:%M:%S')

    # Add messages in console
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

    logging.info('##### Inicio Proceso ETL #####')
    logging.info('Fecha Ejecucion: {}'.format(datetime.now()))

    ######################################################################
    # LOAD AND VALIDATE INPUTS
    ######################################################################

    helpers.logInputs()

    #logging.info('### Validacion de Paths e Inputs')

    #helpers.validatePathsAndFiles()

    #logging.info('### Todos los paths estan correctos')

    ######################################################################
    # EXTRACT, TRANSFORM AND LOAD
    ######################################################################

    ##### Torneos #####
    output_path = inputs.path_master + 'Tablas/Tournaments/'

    #### GUARDAR EN UN ARCHIVO LOS TORNEOS/TEMPORADAS/ANOS YA RELEVADOS PARA EXCLUIR DEL ETL

    logging.info('## Tabla Tournaments')
    logging.info('Carga JSON con torneos')
    # Si en inputs se provee un archivo json de torneos, se transforman esos registros. Sino se hace el request
    if inputs.json_torneos == '':
        tournaments = classETL.get_tournaments(output_path)
    else:
        tournaments = classETL.open_json(output_path, inputs.json_torneos)

    logging.info('# Generación DataFrame de torneos a procesar')

    df_tournaments = classETL.generate_df_tournaments(tournaments)

    # Generacion de lista de torneos a procesar, filtrando segun inputs
    if len(inputs.dictEjecucion['tr_gender']) > 0:
        df_tournaments = df_tournaments[df_tournaments['tr_gender'].isin(inputs.dictEjecucion['tr_gender'])].copy()

    if len(inputs.dictEjecucion['tr_type']) > 0:
        df_tournaments = df_tournaments[df_tournaments['tr_type'].isin(inputs.dictEjecucion['tr_type'])].copy()

    if len(inputs.dictEjecucion['tr_category_name']) > 0:
        df_tournaments = df_tournaments[df_tournaments['category_name'].isin(inputs.dictEjecucion['tr_category_name'])].copy()

    if len(inputs.dictEjecucion['tr_category_level']) > 0:
        df_tournaments = df_tournaments[df_tournaments['category_level'].isin(inputs.dictEjecucion['tr_category_level'])].copy()

    tournament_list = df_tournaments['tournament_id'].unique()

    logging.info('Total de torneos a procesar: {}'.format(len(tournament_list)))

    df_grouped = df_tournaments.groupby(['category_name', 'category_level',
                                         'tr_gender', 'tr_type'])['tournament_id'].count().reset_index()

    for cat in range(len(df_grouped)):
        logging.info('{} - {}/{}/{}: {} torneos'.format(df_grouped[df_grouped.index == cat]['category_name'].values[0],
                                                        df_grouped[df_grouped.index == cat]['category_level'].values[0],
                                                        df_grouped[df_grouped.index == cat]['tr_gender'].values[0],
                                                        df_grouped[df_grouped.index == cat]['tr_type'].values[0],
                                                        str(df_grouped[df_grouped.index == cat]['tournament_id'].values[0])))

    logging.info('### Procesamiento Torneos ###')
    # Para cada torneo de la lista se busca cada temporada con sus resultados
    for torneo in tournament_list:
            tr_gender = df_tournaments[df_tournaments['tournament_id'] == torneo]['tr_gender'].values[0]
            tr_type = df_tournaments[df_tournaments['tournament_id'] == torneo]['tr_type'].values[0]
            tr_category_name = df_tournaments[df_tournaments['tournament_id'] == torneo]['category_name'].values[0]
            tr_category_level = df_tournaments[df_tournaments['tournament_id'] == torneo]['category_level'].values[0]

            logging.info('### Torneo: {} - {}/{}/{}/ {} - {}'.format(tr_category_name,
                                                          tr_category_level,
                                                          tr_gender,
                                                          tr_type,
                                                          torneo,
                                                          df_tournaments[df_tournaments['tournament_id'] == torneo]['tr_name'].values[0]))

            if tr_category_level == 'otros':
                output_path = inputs.path_master + 'Tablas/Tournaments/' + tr_gender + '/' + tr_type + '/' + tr_category_name + '/'
            else:
                output_path = inputs.path_master + 'Tablas/Tournaments/' + tr_gender + '/' + tr_type + '/' + tr_category_name + '/' + tr_category_level + '/'

            # Si se realiza el proceso de extraccion, se hacen los requests a la API
            if inputs.proceso_etl == 'E':
                ######################################################################
                # EXTRACT
                ######################################################################

                # Extraccion de torneos via API
                logging.info('## Inicio proceso de Extraccion')

                seasons = classETL.get_tournament_seasons(torneo, output_path)
                time.sleep(2)

                for season in seasons['seasons']:
                    logging.info(
                        'Inicio Extraccion Temporada: {} - {}'.format(season['id'].split(':')[2], season['name']))

                    # Request de Season Info
                    if os.path.isfile(
                            output_path + 'T' + torneo + '_S' + season['id'].split(':')[2] + '_season_info.json'):
                        logging.info('Extraccion Season Info: El archivo ya existe')
                    else:
                        classETL.get_season_info('sr:tournament:' + torneo, season['id'], output_path)
                        logging.info('Extraccion Season Info: OK')
                        time.sleep(2)

                    # Request de Season Results
                    if os.path.isfile(
                            output_path + 'T' + torneo + '_S' + season['id'].split(':')[2] + '_season_results.json'):
                        logging.info('Extraccion Season Results: El archivo ya existe')
                    else:
                        classETL.get_season_results('sr:tournament:' + torneo, season['id'], output_path)
                        logging.info('Extraccion Season Results: OK')
                        time.sleep(2)

                time.sleep(2)

            else:
                ######################################################################
                # TRANSFORM
                ######################################################################

                logging.info('## Inicio proceso de Transformacion')

                # Transformacion de torneos a DataFrames
                tournament_seasons_file = 'T' + torneo + '_seasons.json'
                seasons = classETL.open_json(output_path, tournament_seasons_file)

                logging.info('# Transform: Tabla Seasons')
                # Tabla Seasons
                df_seasons = classETL.generate_df_seasons(seasons, torneo, output_path)

                logging.info('# Transform: Tabla Matches')
                # Tabla Matches
                df_matches = classETL.generate_df_season_matches(seasons, torneo, output_path)

                logging.info('# Transform: Tabla Tournament Categories')
                # Tabla Tournament Categories
                df_categories = classETL.generate_df_categories(df_tournaments)

                logging.info('# Transform: Tabla Parent Tournaments')
                # Tabla Parent Tournaments
                df_parent_tours = classETL.generate_df_parent_tours(df_tournaments)

                logging.info('# Transform: Tabla Venues')
                # Tabla Venues
                df_venues = classETL.generate_df_venues(seasons, torneo, output_path)

                logging.info('# Transform: Tabla Countries')
                # Tabla Countries
                df_countries = classETL.generate_df_countries(seasons, torneo, output_path)

                ######################################################################
                # LOAD
                ######################################################################

                logging.info('## Inicio proceso de Carga a DWH')

                logging.info('# Carga Tabla ParentTournments')
                classETL.load_table_parents(df_parent_tours, 'insert', inputs.dictConnParams)

                logging.info('# Carga Tabla TournamentCategories')
                classETL.load_table_categories(df_categories, 'insert', inputs.dictConnParams)

                logging.info('# Carga Tabla Tournaments')
                classETL.load_table_tournaments(df_tournaments, 'insert', inputs.dictConnParams)

                logging.info('# Carga Tabla Seasons')
                classETL.load_table_seasons(df_seasons, 'insert', inputs.dictConnParams)

                logging.info('# Carga Tabla Countries')
                classETL.load_table_countries(df_countries, 'insert', inputs.dictConnParams)

                logging.info('# Carga Tabla Venues')
                classETL.load_table_venues(df_venues, 'insert', inputs.dictConnParams)

                logging.info('# Carga Tabla Matches')
                classETL.load_table_matches(df_matches, 'insert', inputs.dictConnParams)

                logging.info('# Agregando torneo al total procesado')
                helpers.agregarTorneosAnalizados(torneo, df_seasons, df_matches)

    logging.info('### Procesamiento Jugadores ###')



    end_time = time.time()
    logging.info('### Duracion Ejecucion: {} segundos'.format(np.round(end_time - start_time, 0)))
    logging.info('##### Fin Proceso #####')


if __name__ == "__main__":
    main()

#importlib.reload(etl)
#classETL = etl.ETL_Tenis(user_key = 'kb6dv75rtq3pk55uumfkxczc')