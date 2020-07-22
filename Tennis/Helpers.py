
######################################################################
# LOAD LIBRARIES
######################################################################

import pandas as pd
import numpy as np
from datetime import datetime
import logging
import os
import sys
sys.path.insert(0, 'C:/Repo/Github/sports_analytics/Tennis/')
import Inputs as inputs
import logging
import importlib
importlib.reload(inputs)

######################################################################
# INPUTS
######################################################################

def logInputs():
    """
    Realiza el logueo de todos los parametros ingresados en el modulo de Inputs

    Parameters
        None
    Return
        Logs
    """
    logging.info('### Inputs ingresados para la ejecucion')
    logging.info('Proceso ETL: {}'.format(inputs.proceso_etl))
    logging.info('Archivo Torneos: {}'.format(inputs.path_master + inputs.json_torneos))

    if len(inputs.dictEjecucion['tr_gender']) > 0:
        logging.info('Generos procesados: {}'.format(inputs.dictEjecucion['tr_gender']))
    else:
        logging.info('Generos procesados: Todos')

    if len(inputs.dictEjecucion['tr_type']) > 0:
        logging.info('Tournament Type procesados: {}'.format(inputs.dictEjecucion['tr_type']))
    else:
        logging.info('Tournament Type procesados: Todos')

    if len(inputs.dictEjecucion['tr_category_name']) > 0:
        logging.info('Categorias procesadas: {}'.format(inputs.dictEjecucion['tr_category_name']))
    else:
        logging.info('Categorias procesadas: Todas')

    if len(inputs.dictEjecucion['tr_category_level']) > 0:
        logging.info('Niveles de Categorias procesados: {}'.format(inputs.dictEjecucion['tr_category_level']))
    else:
        logging.info('Niveles de Categorias procesados: Todos')


def importInputs(path, filename, indexcol = False):
    """
    Realiza el import de los datos a ser levantados en el main.

    Parameters:
        path: debe contener el path para el archivo a levantar. el mismo idealmente debería estar en el modulo inputs.
        filename: nombre de archivo con extension en formato string.
        indexcol = parametro booleano que indica si el archivo a levantar tiene indice o no.

    Returns:
        df
    """
    #import csv
    #sniffer = csv.Sniffer()

    if os.path.isfile(path + filename):
        try:
            df = pd.read_csv(path + filename, encoding = 'utf-8', sep = ',', decimal = '.', index_col = indexcol)
        except UnicodeDecodeError:
            logging.error('El encoding del archivo "{}{}" debe ser "utf-8"'.format(path, filename))
            sys.exit(1)

        decimal_cols = sum([df[i].apply(lambda x: x.replace('.', '').isdigit()).sum() > 0 for i in df.columns if
                           df[i].dtype == np.object])

        # Check que el separador sea ";"
        if len(df.columns) == 1:
            logging.error('El separador del archivo "{}{}" debe ser ";"'.format(path, filename))
            sys.exit(1)
            #dialect = sniffer.sniff(str(df))
            #if dialect.delimiter != '|' and dialect.delimiter.isalpha() == False:
            #logging.error('El separador del archivo "{}{}" debe ser "|"'.format(path, filename))
            #sys.exit()
        # Check que el decimal sea "."
        elif decimal_cols > 0:
            logging.error('Los decimales del archivo "{}{}" deben estar expresados con ","'.format(path, filename))
            sys.exit(1)
    else:
        logging.error('El archivo "{}" no se encuentra en {}'.format(filename, path))
        sys.exit(1)

    return df

def excluirTorneosAnalizados(torneo):
    """
    Excluye los torneos analizados en la ejecución del proceso al archivo con el total de los torneos analizados.

    Parameters:
        df: df a filtrar.
        sucursal: sucursal a la que pertenece el contrato cotizado.

    Returns:
        Archivo CSV con contratos analizados.
    """

    # Carga total torneos cargados en lotes previos
    torneos_excluir = importInputs(inputs.path_master, 'TorneosCargados.csv', indexcol=False)


def agregarTorneosAnalizados(torneo, df_seasons, df_matches):
        """
        Anexa los torneos analizados en la ejecución del proceso al archivo con el total de los torneos analizados.
        Solo se agregan al listado los torneos que hayan concluido, es decir, aquellos que tengan un campeon y un resultado
        en la final y por lo tanto se cuente con toda la informacion del torneo.

        Parameters:
            torneo: nro de torneo a agregar
            df_seasons: DataFrame con las temporadas a agregar del torneo
            df_matches: DataFrame con los resultados de los partidos de las temporadas del torneo

        Returns:
            Archivo CSV con torneos procesados.
        """

        # Carga total torneos cargados en lotes previos
        torneos_excluir = importInputs(inputs.path_master, 'TorneosCargados.csv', indexcol=False)

        # Torneos analizados en el corriente lote
        torneos_agregar = pd.DataFrame(columns=torneos_excluir.columns)

        for season in df_seasons['season_id'].unique():
            year = df_seasons[df_seasons['season_id'] == season]['year'].values[0]
            try:
                winner = df_matches[(df_matches['season_id'] == season) & (df_matches['tr_round_name'] == 'final')][
                    'winner_id'].values[0]
            except:
                winner = ''
            try:
                result = df_matches[(df_matches['season_id'] == season) & (df_matches['tr_round_name'] == 'final')][
                    'match_result'].values[0]
            except:
                result = ''

            registro = pd.DataFrame([torneo, season, year, winner, result]).transpose()
            registro.columns = torneos_agregar.columns
            torneos_agregar = torneos_agregar.append(registro)

        # Adicion de torneos a los ya analizados
        torneos_nuevos = set(torneos_agregar['temporada']).difference(set(torneos_excluir['temporada']))
        lote_agregar = torneos_agregar[(torneos_agregar['temporada'].isin(torneos_nuevos))
                                       & (torneos_agregar['campeon'] != '')]

        lote_final = pd.concat([torneos_excluir, lote_agregar], sort=False)

        logging.info('Temporadas agregadas: {}'.format(len(torneos_nuevos)))

        # Export de torneos analizados
        if os.path.isdir(inputs.path_master):
            lote_final.to_csv(inputs.path_master + 'TorneosCargados.csv', header=True, encoding='utf-8', sep=',', decimal='.', index=False)
        else:
            logging.error('No existe el path {}'.format(inputs.path_master))
            sys.exit(1)


