#####################################################################################################################
#
# Purpose: Definición de clase usada realizar el ETL de Tenis.
#
#######################################################################################################################
import os
import sys
import json
import requests
import pandas as pd
import numpy as np
from datetime import datetime
import logging

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)

class ETL_Tenis:
    def __init__(self, user_key):
        self.user_key = user_key

    ######################################################################
    # EXTRACT
    ######################################################################

    def get_tournaments(self, location):
        """
        Genera el request de los torneos via API y los almacena en el directorio especificado como parametro

        Parameters:
            location: directorio de destino del JSON crudo

        Return:
            JSON: JSON crudo extraido vía API
        """
        # Generacion request
        resp = requests.get('http://api.sportradar.us/tennis-t2/en/tournaments.json?api_key=' + self.user_key)
        if resp.status_code != 200:
            logging.error('El request de los torneos fallo con el siguiente codigo de status: {} - {}'.format(resp.status_code, resp.reason))
            raise NameError('El request de los torneos fallo con el siguiente codigo de status: {} - {}'.format(resp.status_code, resp.reason))

        # Export del JSON crudo
        date = "_" + str(datetime.now().year) + '_' + str('%02d' % datetime.now().month)
        with open(location + 'tournaments' + date + '.json', 'w') as outfile:
            json.dump(resp.json(), outfile)

        return resp.json()

    def get_tournament_seasons(self, tournament_id, location):
        """
        Genera el request de las temporadas de un torneo via API y los almacena en el directorio especificado como parametro

        Parameters:
            tournament_id: id del torneo para el cual buscar las temporadas
            location: directorio de destino del JSON crudo

        Return:
            JSON: JSON crudo extraido vía API
        """
        # Si el torneo ya existe en la carpeta, no hace el request
        if os.path.isfile(location + 'T' + tournament_id + '_seasons.json'):
            return
        else:
            # Generacion request
            resp = requests.get('http://api.sportradar.us/tennis-t2/en/tournaments/sr:tournament:' + tournament_id + '/seasons.json?api_key=' + self.user_key)
            if resp.status_code != 200:
                logging.error('El request de las temporadas del torneo {} fallo con el siguiente codigo de status: {} - {}'.format(tournament_id, resp.status_code, resp.reason))
                raise NameError('El request de las temporadas del torneo {} fallo con el siguiente codigo de status: {} - {}'.format(tournament_id, resp.status_code, resp.reason))

            # Export del JSON crudo
            with open(location + 'T' + tournament_id + '_seasons.json', 'w') as outfile:
                json.dump(resp.json(), outfile)

            return resp.json()

    def get_season_info(self, tournament_id, season_id, location):
        """
        Genera el request de la informacion de una temporada de un torneo via API y los almacena en el directorio especificado como parametro

        Parameters:
            tournament_id: id del torneo
            season_id: id de la temporada
            location: directorio de destino del JSON crudo

        Return:
            JSON: JSON crudo extraido vía API
        """
        # Generacion request
        resp = requests.get('http://api.sportradar.us/tennis-t2/en/tournaments/' + season_id + '/info.json?api_key=' + self.user_key)
        if resp.status_code != 200:
            logging.error('El request de la informacion de la temporada {} del torneo {} fallo con el siguiente codigo de status: {} - {}'.format(season_id, tournament_id, resp.status_code, resp.reason))
            raise NameError('El request de la informacion de la temporada {} del torneo {} fallo con el siguiente codigo de status: {} - {}'.format(season_id, tournament_id, resp.status_code, resp.reason))

        # Export del JSON crudo
        with open(location + 'T' + tournament_id.split(':')[2] + '_S' + season_id.split(':')[2] + '_season_info.json', 'w') as outfile:
            json.dump(resp.json(), outfile)

        return resp.json()

    def get_season_results(self, tournament_id, season_id, location):
        """
        Genera el request de los resultados de un torneo/temporada via API y los almacena en el directorio especificado como parametro

        Parameters:
            tournament_id: id del torneo
            season_id: id de la temporada
            location: directorio de destino del JSON crudo

        Return:
            JSON: JSON crudo extraido vía API
        """
        # Generacion request
        resp = requests.get('http://api.sportradar.us/tennis-t2/en/tournaments/' + season_id + '/results.json?api_key=' + self.user_key)
        if resp.status_code != 200:
            logging.error('El request de los resultados de la temporada {} del torneo {} fallo con el siguiente codigo de status: {} - {}'.format(season_id, tournament_id, resp.status_code, resp.reason))
            raise NameError('El request de los resultados de la temporada {} del torneo {} fallo con el siguiente codigo de status: {} - {}'.format(season_id, tournament_id, resp.status_code, resp.reason))

        # Export del JSON crudo
        with open(location + 'T' + tournament_id.split(':')[2] + '_S' + season_id.split(':')[2] + '_results.json', 'w') as outfile:
            json.dump(resp.json(), outfile)

        return resp.json()

    ######################################################################
    # TRANSFORM
    ######################################################################

    def open_json(self, path, file):
        """
        Levanta un archivo JSON

        Parameters:
            path: directorio del archivo
            file: nombre del archivo

        Return:
            json: archivo JSON
        """
        if os.path.isfile(path + file) == False:
            logging.error('No existe el archivo {}'.format(path + file))
            sys.exit()
        else:
            with open(path + file) as json_file:
                data = json.load(json_file)

        return data

    def add_tournament(self, df, json_add):
        """
        Anexa a un DataFrame la informacion de un torneo de acuerdo a los campos del DWH

        Parameters:
            df: DataFrame con la estructura donde anexar los registros del torneo
            json_add: JSON con la informacion del torneo

        Return:
            df: DataFrame original con los datos del torneo anexados
        """
        # Columnas del DataFrame final
        cols_torneos = ['tournament_id', 'parent_id', 'category_pk',
                        'tr_name', 'tr_type', 'tr_gender']

        # Agregamos el registro solo si no esta en la tabla del DW
        if json_add['id'] not in df['tournament_id'].unique():
            # Generacion del registro
            registro = []
            registro.append(json_add['id'].split(':')[2])
            if 'parent_id' in json_add.keys():
                registro.append(json_add['parent_id'].split(':')[2])
            else:
                registro.append('SinDatos')

            if 'level' in json_add['category'].keys():
                category_pk = json_add['category']['id'].split(':')[2] + '-' + \
                              json_add['category']['name'] + '-' + \
                              json_add['category']['level']
            else:
                category_pk = json_add['category']['id'].split(':')[2] + '-' + \
                              json_add['category']['name'] + '-otros'

            registro.append(category_pk)
            registro.append(json_add['name'])
            registro.append(json_add['type'])
            registro.append(json_add['gender'])

            df_registro = pd.DataFrame(registro).transpose()
            df_registro.columns = cols_torneos

            # Append del registro
            df = df.append(df_registro)

        return df

    def add_sports(self, df, json_add):
        """
        Anexa a un DataFrame la informacion de un deporte de acuerdo a los campos del DWH

        Parameters:
            df: DataFrame con la estructura donde anexar los registros del deporte
            json_add: JSON con la informacion del deporte

        Return:
            df: DataFrame original con los datos del deporte anexados
        """
        # Nos quedamos con los diccionarios unicos de categorias de torneos
        dicts = []
        for dic in json_add['tournaments']:
            dicts.append(dic['sport'])

        unique_sports = list({v['id']: v for v in dicts}.values())

        # Columnas del DataFrame final
        cols_sports = ['sport_id', 'sport_name']

        # Generacion del registro
        for sport in unique_sports:
            # Agregamos el registro solo si no esta en la tabla del DW
            if sport['id'].split(':')[2] not in df['sport_id'].unique():
                registro = []
                registro.append(sport['id'].split(':')[2])
                registro.append(sport['name'])

                df_registro = pd.DataFrame(registro).transpose()
                df_registro.columns = cols_sports

                # Append del registro
                df = df.append(df_registro)

        return df

    def add_categories(self, df, json_add):
        """
        Anexa a un DataFrame la informacion de una categoria de torneo de acuerdo a los campos del DWH

        Parameters:
            df: DataFrame con la estructura donde anexar los registros de la categoria de torneo
            json_add: JSON con la informacion de la categoria de torneo

        Return:
            df: DataFrame original con los datos de la categoria de torneo anexados
        """
        # Nos quedamos con los diccionarios unicos de categorias de torneos
        dicts = []
        for dic in json_add['tournaments']:

            if 'level' in dic['category'].keys():
                dic['category']['concat'] = dic['category']['name'] + '_' + dic['category']['level']
            else:
                dic['category']['concat'] = dic['category']['name']
            dicts.append(dic['category'])

        unique_categories = list({v['concat']: v for v in dicts}.values())

        if '' in unique_categories:
            unique_categories.remove('')

        # Columnas del DataFrame final
        cols_categories = ['category_pk', 'category_id', 'category_name', 'category_level']

        # Generacion de registros

        for category in unique_categories:
            # Agregamos el registro solo si no esta en la tabla del DW
            if category['id'].split(':')[2] not in df['category_pk'].unique():
                registro = []
                registro.append(category['id'].split(':')[2])
                registro.append(category['name'])

                if 'level' in category.keys():
                    registro.append(category['level'])

                df_registro = pd.DataFrame(registro).transpose()
                if 'level' not in category.keys():
                    df_registro['level'] = 'otro'

                df_registro.columns = cols_categories

                df_registro['category_pk'] = df_registro['category_id'] + '-' + df_registro['category_name'] + '-' + \
                                             df_registro['category_level']

                # Append del registro
                df = df.append(df_registro[['category_pk', 'category_id', 'category_name', 'category_level']])

        return df

    def add_seasons(self, df, season, json_add_season, json_add_season_info):
        """
        Anexa a un DataFrame la informacion de una temporada de acuerdo a los campos del DWH

        Parameters:
            df: DataFrame con la estructura donde anexar los registros de la temporada
            season: diccionario con la temporada a agregar
            json_add_season: JSON con la informacion de las temporadas del torneo
            json_add_season_info: JSON con la Season Info de la temporada

        Return:
            df: DataFrame original con los datos de la temporada anexados
        """
        # Generamos variables comunes a todas las temporadas del torneo
        tournament_id = json_add_season['tournament']['id'].split(':')[2]

        if 'level' in json_add_season['tournament']['category'].keys():
            category_pk = json_add_season['tournament']['category']['id'].split(':')[2] + '-' + \
                          json_add_season['tournament']['category']['name'] + '-' + \
                          json_add_season['tournament']['category']['level']
        else:
            category_pk = json_add_season['tournament']['category']['id'].split(':')[2] + '-' + \
                          json_add_season['tournament']['category']['name'] + '-otros'


        # Agregamos el registro solo si no esta en la tabla del DW
        if season['id'].split(':')[2] not in df['season_id'].unique():
            # Columnas del DataFrame final
            cols_seasons = ['season_id', 'season_name', 'start_date', 'end_date', 'year',
                            'tournament_id', 'category_pk', 'prize_money_amt',
                            'prize_currency', 'surface', 'complex', 'q_competitors', 'q_qualified_competitors',
                            'q_scheduled_matches']

            # Generacion del registro
            registro = []
            registro.append(season['id'].split(':')[2])
            registro.append(season['name'])
            registro.append(season['start_date'])
            registro.append(season['end_date'])
            registro.append(season['year'])
            registro.append(tournament_id)
            registro.append(category_pk)

            if 'prize_money' in json_add_season_info['info'].keys():
                registro.append(json_add_season_info['info']['prize_money'])
            else:
                registro.append(-1)

            if 'prize_currency' in json_add_season_info['info'].keys():
                registro.append(json_add_season_info['info']['prize_currency'])
            else:
                registro.append('')

            if 'surface' in json_add_season_info['info'].keys():
                registro.append(json_add_season_info['info']['surface'])
            else:
                registro.append('')

            if 'complex' in json_add_season_info['info'].keys():
                registro.append(json_add_season_info['info']['complex'])
            else:
                registro.append('')

            if 'number_of_competitors' in json_add_season_info['info'].keys():
                registro.append(json_add_season_info['info']['number_of_competitors'])
            else:
                registro.append(-1)

            if 'number_of_qualified_competitors' in json_add_season_info['info'].keys():
                registro.append(json_add_season_info['info']['number_of_qualified_competitors'])
            else:
                registro.append(-1)

            if 'number_of_scheduled_matches' in json_add_season_info['info'].keys():
                registro.append(json_add_season_info['info']['number_of_scheduled_matches'])
            else:
                registro.append(-1)

            df_registro = pd.DataFrame(registro).transpose()
            df_registro.columns = cols_seasons

            # Append del registro
            df = df.append(df_registro)

        return df

    def add_matches(self, df, json_add):
        """
        Anexa a un DataFrame los resultados de un torneo de acuerdo a los campos del DWH

        Parameters:
            df: DataFrame con la estructura donde anexar los registros del torneo
            json_add: JSON con la informacion del torneo

        Return:
            df: DataFrame original con los resutados del torneo anexados
        """
        # Generamos variables comunes a todas los partidos del torneo
        tournament_id = json_add['tournament']['id'].split(':')[2]

        if 'level' in json_add['tournament']['category'].keys():
            category_pk = json_add['tournament']['category']['id'].split(':')[2] + '-' + \
                          json_add['tournament']['category']['name'] + '-' + \
                          json_add['tournament']['category']['level']
        else:
            category_pk = json_add['tournament']['category']['id'].split(':')[2] + '-' + \
                          json_add['tournament']['category']['name'] + '-otros'

        # Generamos variables propias de los partidos
        for partido in json_add['results']:

            # Agregamos el registro solo si no esta en la tabla del DW
            if partido['sport_event']['id'].split(':')[2] not in df['match_id'].unique():

                # Columnas del DataFrame final
                cols_matches = ['match_id', 'tournament_id', 'season_id', 'category_pk', 'scheduled',
                                'venue_id', 'tr_round_type', 'tr_round_number', 'tr_round_name', 'home_competitor_id',
                                'home_seed', 'away_competitor_id', 'away_seed', 'match_mode', 'weather_conditions',
                                'event_status', 'match_status', 'match_ended', 'match_duration',
                                'home_score', 'away_score', 'winner_id', 'match_result']

                # Generacion del registro
                registro = []
                registro.append(partido['sport_event']['id'].split(':')[2])
                registro.append(tournament_id)
                registro.append(partido['sport_event']['season']['id'].split(':')[2])
                registro.append(category_pk)
                registro.append(partido['sport_event']['scheduled'])

                if 'venue' in partido['sport_event'].keys():
                    registro.append(partido['sport_event']['venue']['id'].split(':')[2])
                else:
                    registro.append('SinDatos')

                # Tournament Round
                if 'tournament_round' in partido['sport_event'].keys():

                    if 'type' in partido['sport_event']['tournament_round'].keys():
                        registro.append(partido['sport_event']['tournament_round']['type'])
                    else:
                        registro.append('')

                    if 'number' in partido['sport_event']['tournament_round'].keys():
                        registro.append(partido['sport_event']['tournament_round']['number'])
                    else:
                        registro.append(-1)

                    if 'name' in partido['sport_event']['tournament_round'].keys():
                        registro.append(partido['sport_event']['tournament_round']['name'])
                    else:
                        registro.append('')
                else:
                    registro.append('')
                    registro.append(-1)
                    registro.append('')


                # Jugador Home
                registro.append(partido['sport_event']['competitors'][0]['id'].split(':')[2])
                if 'seed' in partido['sport_event']['competitors'][0].keys():
                    registro.append(partido['sport_event']['competitors'][0]['seed'])
                else:
                    registro.append(-1)
                # Jugador Away
                registro.append(partido['sport_event']['competitors'][1]['id'].split(':')[2])
                if 'seed' in partido['sport_event']['competitors'][1].keys():
                    registro.append(partido['sport_event']['competitors'][1]['seed'])
                else:
                    registro.append(-1)

                if 'match_mode' in partido['sport_event_conditions'].keys():
                    registro.append(partido['sport_event_conditions']['match_mode'])
                else:
                    registro.append('')

                if 'weather_info' in partido['sport_event_conditions'].keys():
                    registro.append(partido['sport_event_conditions']['weather_info']['weather_conditions'])
                else:
                    registro.append('')

                if 'status' in partido['sport_event_status'].keys():
                    registro.append(partido['sport_event_status']['status'])
                else:
                    registro.append('')

                if 'match_status' in partido['sport_event_status'].keys():
                    registro.append(partido['sport_event_status']['match_status'])
                else:
                    registro.append('')

                if 'match_ended' in partido['sport_event_status'].keys():
                    registro.append(partido['sport_event_status']['match_ended'])
                    match_duration = (pd.to_datetime(partido['sport_event_status']['match_ended']) - \
                                      pd.to_datetime(partido['sport_event']['scheduled'])).seconds
                    registro.append(match_duration)
                else:
                    registro.append('')
                    registro.append(-1)

                if 'home_score' in partido['sport_event_status'].keys():
                    registro.append(partido['sport_event_status']['home_score'])
                    registro.append(partido['sport_event_status']['away_score'])
                else:
                    registro.append(-1)
                    registro.append(-1)

                # Agregamos el resultado del partido solo si se jugo hasta el final
                if 'winner_id' in partido['sport_event_status'].keys():
                    registro.append(partido['sport_event_status']['winner_id'].split(':')[2])

                    # Resultado del partido
                    if 'period_scores' in partido['sport_event_status'].keys():
                        cant_sets = len(partido['sport_event_status']['period_scores'])
                        counter = 1
                        result = ''
                        #
                        if partido['sport_event_status']['winner_id'].split(':')[2] == \
                                partido['sport_event']['competitors'][0]['id'].split(':')[2]:
                            for set_result in partido['sport_event_status']['period_scores']:
                                result = result + str(set_result['home_score']) + '-' + str(set_result['away_score'])
                                if counter < cant_sets:
                                    result = result + '/'
                                counter += 1
                            registro.append(result)
                        else:
                            for set_result in partido['sport_event_status']['period_scores']:
                                result = result + str(set_result['away_score']) + '-' + str(set_result['home_score'])
                                if counter < cant_sets:
                                    result = result + '/'
                                counter += 1
                            registro.append(result)
                    else:
                        registro.append('')
                else:
                    registro.append('')
                    registro.append('')

                df_registro = pd.DataFrame(registro).transpose()
                df_registro.columns = cols_matches

                # Append del registro
                df = df.append(df_registro)

        return df

    def add_venues(self, df, json_add):
        """
        Anexa a un DataFrame la informacion de los estadios de acuerdo a los campos del DWH

        Parameters:
            df: DataFrame con la estructura donde anexar los registros de los estadios
            json_add: JSON con la informacion de los estadios

        Return:
            df: DataFrame original con la informacion de los estadios anexados
        """
        # Nos quedamos con los diccionarios unicos de Venues
        dicts = []
        for dic in json_add['results']:
            if 'venue' in dic['sport_event'].keys():
                dicts.append(dic['sport_event']['venue'])

        unique_venues = list({v['id']: v for v in dicts}.values())

        if '' in unique_venues:
            unique_venues.remove('')

        # Columnas del DataFrame final
        cols_venues = ['venue_id', 'venue_name', 'city_name', 'country_code']

        # Generacion del registro
        for venue in unique_venues:
            # Agregamos el registro solo si no esta en la tabla del DW
            if venue['id'].split(':')[2] not in df['venue_id'].unique():
                registro = []
                registro.append(venue['id'].split(':')[2])
                registro.append(venue['name'])
                if 'city_name' in venue.keys():
                    registro.append(venue['city_name'])
                    registro.append(venue['country_code'])
                else:
                    registro.append('SinDatos')
                    registro.append('SinDatos')

                # Append del registro
                df_registro = pd.DataFrame(registro).transpose()
                df_registro.columns = cols_venues
                df = df.append(df_registro)

        return df

    def add_countries(self, df, json_add):
        """
        Anexa a un DataFrame la informacion de los paises de acuerdo a los campos del DWH

        Parameters:
            df: DataFrame con la estructura donde anexar los registros de los paises
            json_add: JSON con la informacion de los paises

        Return:
            df: DataFrame original con la informacion de los paises anexados
        """
        # Nos quedamos con los diccionarios unicos de Paises
        dicts = []
        for dic in json_add['results']:
            if 'venue' in dic['sport_event'].keys():
                dicts.append(dic['sport_event']['venue'])

        unique_countries = list({v['country_code']: v for v in dicts if 'country_code' in v.keys()}.values())

        if '' in unique_countries:
            unique_countries.remove('')

        # Columnas del DataFrame final
        cols_countries = ['country_code', 'country_name']

        # Generacion del registro
        for country in unique_countries:
            if country['country_code'] not in df['country_code'].unique():
                registro = []
                registro.append(country['country_code'])
                registro.append(country['country_name'])

                # Append del registro del Pais
                df_registro = pd.DataFrame(registro).transpose()
                df_registro.columns = cols_countries
                df = df.append(df_registro)

        return df

    def add_player(self, df, json_add):
        # Columnas del DataFrame final
        cols_players_profile = ['player_id', 'name', 'abbreviation', 'nationality', 'country_code', 'gender',
                                'date_of_birth', 'pro_year', 'handedness', 'height', 'weight',
                                'highest_singles_ranking', 'dt_highest_singles_ranking',
                                'highest_doubles_ranking', 'dt_highest_doubles_ranking']

        # Generacion del registro
        registro = []
        registro.append(json_add['player']['id'].split(':')[2])
        registro.append(json_add['player']['name'])
        registro.append(json_add['player']['abbreviation'])
        registro.append(json_add['player']['nationality'])
        registro.append(json_add['player']['country_code'])
        registro.append(json_add['player']['gender'])
        registro.append(json_add['player']['date_of_birth'])
        registro.append(json_add['player']['pro_year'])
        registro.append(json_add['player']['handedness'])
        registro.append(json_add['player']['height'])
        registro.append(json_add['player']['weight'])
        registro.append(json_add['player']['highest_singles_ranking'])
        registro.append(json_add['player']['date_highest_singles_ranking'])
        registro.append(json_add['player']['highest_doubles_ranking'])
        registro.append(json_add['player']['date_highest_doubles_ranking'])

        df_registro = pd.DataFrame(registro).transpose()
        df_registro.columns = cols_players_profile

        # Append del registro
        df = df.append(df_registro)

        return df

    def add_player_stats(self, df, json_add):

        # Anios de historia de juego
        import collections
        unique_years = collections.Counter(e['year'] for e in json_add['statistics']['periods'])
        lista_years = list(unique_years.keys())

        # Columnas del DataFrame final
        cols_players_stats = ['player_id', 'year', 'surface',
                              'tournaments_played', 'tournaments_won', 'pct_tournaments_won',
                              'matches_played', 'matches_won', 'pct_matches_won']

        # Generacion del registro
        player_id = json_add['player']['id'].split(':')[2]

        # Para cada anio de juego se agregan las estadisticas de cada superficie
        for year in lista_years:
            json_year = next((item for item in json_add['statistics']['periods'] if item["year"] == year))

            # Superficies donde jugo el jugador en el anio 'year'
            unique_surfaces = collections.Counter(e['type'] for e in json_year['surfaces'])
            lista_surfaces = list(unique_surfaces.keys())

            for surface in lista_surfaces:
                json_surface = next((item for item in json_year['surfaces'] if item["type"] == surface))
                registro = []
                registro.append(player_id)
                registro.append(year)
                registro.append(surface)
                registro.append(json_surface['statistics']['tournaments_played'])
                registro.append(json_surface['statistics']['tournaments_won'])

                pct_tournaments_won = json_surface['statistics']['tournaments_won'] / json_surface['statistics'][
                    'tournaments_played']
                registro.append(np.round(pct_tournaments_won, 4))

                registro.append(json_surface['statistics']['matches_played'])
                registro.append(json_surface['statistics']['matches_won'])

                pct_matches_won = json_surface['statistics']['matches_won'] / json_surface['statistics'][
                    'matches_played']
                registro.append(np.round(pct_matches_won, 4))

                df_registro = pd.DataFrame(registro).transpose()
                df_registro.columns = cols_players_stats

                # Append del registro
                df = df.append(df_registro)

                return df

    def generate_df_tournaments(self, json):
        """
        Genera un nuevo DataFrame de torneos con la estructura del DWH segun un JSON de entrada

        Parameters:
            json: JSON con la informacion de los torneos

        Return:
            df: DataFrame con la informacion de los torneos en formato de DWH
        """
        cols_torneos = ['tournament_id', 'parent_id', 'category_pk',
                        'tr_name', 'tr_type', 'tr_gender']
        df_tournaments = pd.DataFrame(columns=cols_torneos)

        for torneo in json['tournaments']:
            df_tournaments = self.add_tournament(df_tournaments, torneo).reset_index(drop=True)

        # Filtro de torneos
        df_tournaments['category_name'] = df_tournaments['category_pk'].apply(lambda x: x.split('-')[1])
        df_tournaments['category_level'] = df_tournaments['category_pk'].apply(lambda x: x.split('-')[2])
        excluded_categories = ['Exhibition', 'Wheelchairs', 'Juniors']

        df_tournaments_extract = df_tournaments[
            (df_tournaments['tr_type'] == 'singles') & (df_tournaments['tr_gender'] != 'mixed') &
            (~df_tournaments['category_name'].isin(excluded_categories))].copy()
        df_tournaments_extract = df_tournaments_extract.sort_values(['tr_gender', 'category_name']).reset_index(
            drop=True)

        return df_tournaments_extract

    def generate_df_categories(self, df_tournaments):
        """
        Genera un nuevo DataFrame de categorias de torneos con la estructura del DWH segun un DF de torneos de entrada

        Parameters:
            df_tournaments: DataFrame con la informacion de torneos

        Return:
            df: DataFrame con la informacion de las categorias de los torneos en formato de DWH
        """
        cols_categories = ['category_pk', 'category_id', 'category_name', 'category_level']
        df_categories = pd.DataFrame(columns=cols_categories)

        categories = df_tournaments['category_pk'].drop_duplicates().tolist()

        for cat in categories:
            registro = []
            registro.append(cat)
            registro.append(cat.split('-')[0])
            registro.append(cat.split('-')[1])
            registro.append(cat.split('-')[2])

            df_registro = pd.DataFrame(registro).transpose()
            df_registro.columns = cols_categories

            df_categories = df_categories.append(df_registro)

        return df_categories

    def generate_df_parent_tours(self, df_tournaments):
        """
        Genera un nuevo DataFrame de categorias de torneos con la estructura del DWH segun un DF de torneos de entrada

        Parameters:
            df_tournaments: DataFrame con la informacion de torneos

        Return:
            df: DataFrame con la informacion de las categorias de los torneos en formato de DWH
        """
        cols_parents = ['parent_id', 'category_pk']
        df_parents = pd.DataFrame(columns=cols_parents)

        parents = df_tournaments['parent_id'].drop_duplicates().tolist()

        for i in parents:
            registro = []
            registro.append(i)
            registro.append(df_tournaments[df_tournaments['parent_id'] == i]['category_pk'].values[0])

            df_registro = pd.DataFrame(registro).transpose()
            df_registro.columns = cols_parents

            df_parents = df_parents.append(df_registro)

        return df_parents

    def generate_df_seasons(self, json_season, torneo, path):
        """
        Genera un nuevo DataFrame de temporadas con la estructura del DWH segun un JSON de entrada

        Parameters:
            json: JSON con la informacion de todas las temporadas del torneo
            torneo: torneo al que pertenecen las temporadas
            path: directorio donde se encuentra el JSON con la Season Info

        Return:
            df: DataFrame con la informacion de las temporadas en formato de DWH
        """
        cols_seasons = ['season_id', 'season_name', 'start_date', 'end_date', 'year',
                        'tournament_id', 'category_pk', 'prize_money_amt',
                        'prize_currency', 'surface', 'complex', 'q_competitors', 'q_qualified_competitors',
                        'q_scheduled_matches']

        df_seasons = pd.DataFrame(columns=cols_seasons)

        for season in json_season['seasons']:
            logging.info('Temporada: T{} - S{}'.format(torneo, season['id'].split(':')[2]))
            season_info_file = 'T' + torneo + '_S' + season['id'].split(':')[2] + '_season_info.json'
            season_info = self.open_json(path, season_info_file)
            df_seasons = self.add_seasons(df_seasons, season, json_season, season_info).reset_index(drop = True)

        return df_seasons

    def generate_df_season_matches(self, json_season, torneo, path):
        """
        Genera un nuevo DataFrame con los partidos de las temporadas de un torneo con la estructura del DWH segun un JSON de entrada

        Parameters:
            json: JSON con la informacion de todas las temporadas del torneo
            torneo: torneo al que pertenecen las temporadas
            path: directorio donde se encuentra el JSON con la Season Info

        Return:
            df: DataFrame con la informacion de las temporadas en formato de DWH
        """
        cols_matches = ['match_id', 'tournament_id', 'season_id', 'category_pk', 'scheduled', 'venue_id',
                        'tr_round_type', 'tr_round_number', 'tr_round_name', 'home_competitor_id', 'home_seed',
                        'away_competitor_id', 'away_seed', 'match_mode', 'weather_conditions', 'event_status',
                        'match_status', 'match_ended', 'match_duration',
                        'home_score', 'away_score', 'winner_id', 'match_result']

        df_matches = pd.DataFrame(columns=cols_matches)

        for season in json_season['seasons']:
            logging.info('Resultados Temporada: T{} - S{}'.format(torneo, season['id'].split(':')[2]))
            season_results_file = 'T' + torneo + '_S' + season['id'].split(':')[2] + '_results.json'
            season_results = self.open_json(path, season_results_file)
            df_matches = self.add_matches(df_matches, season_results).reset_index(drop = True)

        return df_matches

    def generate_df_venues(self, json_season, torneo, path):
        """
        Genera un nuevo DataFrame los estadios de los torneos con la estructura del DWH segun un DF de torneos de entrada

        Parameters:
            df_tournaments: DataFrame con la informacion de torneos

        Return:
            df: DataFrame con la informacion de los estadios de los torneos en formato de DWH
        """
        cols_venues = ['venue_id', 'venue_name', 'city_name', 'country_code']

        df_venues = pd.DataFrame(columns=cols_venues)

        for season in json_season['seasons']:
            logging.info('Estadios Temporada: T{} - S{}'.format(torneo, season['id'].split(':')[2]))
            season_results_file = 'T' + torneo + '_S' + season['id'].split(':')[2] + '_results.json'
            season_results = self.open_json(path, season_results_file)
            df_venues = self.add_venues(df_venues, season_results).reset_index(drop = True)

        return df_venues

    def generate_df_countries(self, json_season, torneo, path):
        """
        Genera un nuevo DataFrame los estadios de los torneos con la estructura del DWH segun un DF de torneos de entrada

        Parameters:
            df_tournaments: DataFrame con la informacion de torneos

        Return:
            df: DataFrame con la informacion de las categorias de los torneos en formato de DWH
        """
        cols_countries = ['country_code', 'country_name']

        df_countries = pd.DataFrame(columns=cols_countries)

        for season in json_season['seasons']:
            logging.info('Paises Temporada: T{} - S{}'.format(torneo, season['id'].split(':')[2]))
            season_results_file = 'T' + torneo + '_S' + season['id'].split(':')[2] + '_results.json'
            season_results = self.open_json(path, season_results_file)
            df_countries = self.add_countries(df_countries, season_results).reset_index(drop = True)

        return df_countries

    def generate_ranking_atp(self, json_add):
        # Columnas del DataFrame final
        cols_rank_atp = ['rank', 'points', 'player_id', 'name', 'abbreviation', 'nationality',
                         'country_code', 'rank_movement', 'tournaments_played', 'week', 'year']

        df_rank_atp = pd.DataFrame(columns=cols_rank_atp)

        year = json_add['rankings'][1]['year']
        week = json_add['rankings'][1]['week']

        for rank in json_add['rankings'][1]['player_rankings']:
            # Generacion del registro
            registro = []
            registro.append(rank['rank'])
            registro.append(rank['points'])
            registro.append(rank['player']['id'].split(':')[2])
            registro.append(rank['player']['name'])
            registro.append(rank['player']['abbreviation'])
            registro.append(rank['player']['nationality'])
            registro.append(rank['player']['country_code'])
            registro.append(rank['ranking_movement'])
            registro.append(rank['tournaments_played'])
            registro.append(week)
            registro.append(year)

            df_registro = pd.DataFrame(registro).transpose()
            df_registro.columns = cols_rank_atp

            # Append del registro
            df_rank_atp = df_rank_atp.append(df_registro)

            return df_rank_atp

    def generate_ranking_wta(self, json_add):
        # Columnas del DataFrame final
        cols_rank_wta = ['rank', 'points', 'player_id', 'name', 'abbreviation', 'nationality',
                         'country_code', 'rank_movement', 'tournaments_played', 'week', 'year']

        df_rank_wta = pd.DataFrame(columns=cols_rank_wta)

        year = json_add['rankings'][0]['year']
        week = json_add['rankings'][0]['week']

        for rank in json_add['rankings'][0]['player_rankings']:
            # Generacion del registro
            registro = []
            registro.append(rank['rank'])
            registro.append(rank['points'])
            registro.append(rank['player']['id'].split(':')[2])
            registro.append(rank['player']['name'])
            registro.append(rank['player']['abbreviation'])
            registro.append(rank['player']['nationality'])
            registro.append(rank['player']['country_code'])
            registro.append(rank['ranking_movement'])
            registro.append(rank['tournaments_played'])
            registro.append(week)
            registro.append(year)

            df_registro = pd.DataFrame(registro).transpose()
            df_registro.columns = cols_rank_wta

            # Append del registro
            df_rank_wta = df_rank_wta.append(df_registro)

            return df_rank_wta


    ######################################################################
    # LOAD
    ######################################################################

    def crearTablasDW(self, dictConnParams, listTablas = False):

        if listTablas == False:

            print('# Tabla ParentTournments')
            cols_parents = ['parent_id', 'category_pk']
            df_parent_tours = pd.DataFrame(columns=cols_parents)
            self.load_table_parents(df_parent_tours, 'create', dictConnParams)

            print('# Tabla TournamentCategories')
            cols_categories = ['category_pk', 'category_id', 'category_name', 'category_level']
            df_categories = pd.DataFrame(columns=cols_categories)
            self.load_table_categories(df_categories, 'create', dictConnParams)

            print('# Tabla Tournaments')
            cols_torneos = ['tournament_id', 'parent_id', 'category_pk', 'tr_name', 'tr_type', 'tr_gender']
            df_tournaments = pd.DataFrame(columns=cols_torneos)
            self.load_table_tournaments(df_tournaments, 'create', dictConnParams)

            print('# Tabla Seasons')
            cols_seasons = ['season_id', 'season_name', 'start_date', 'end_date', 'year',
                            'tournament_id', 'category_pk', 'prize_money_amt',
                            'prize_currency', 'surface', 'complex', 'q_competitors', 'q_qualified_competitors',
                            'q_scheduled_matches']
            df_seasons = pd.DataFrame(columns=cols_seasons)
            self.load_table_seasons(df_seasons, 'create', dictConnParams)

            print('# Tabla Countries')
            cols_countries = ['country_code', 'country_name']
            df_countries = pd.DataFrame(columns=cols_countries)
            self.load_table_countries(df_countries, 'create', dictConnParams)

            print('# Tabla Venues')
            cols_venues = ['venue_id', 'venue_name', 'city_name', 'country_code']
            df_venues = pd.DataFrame(columns=cols_venues)
            self.load_table_venues(df_venues, 'create', dictConnParams)

            print('# Tabla Matches')
            cols_matches = ['match_id', 'tournament_id', 'season_id', 'category_pk', 'scheduled', 'venue_id',
                            'tr_round_type', 'tr_round_number', 'tr_round_name', 'home_competitor_id', 'home_seed',
                            'away_competitor_id', 'away_seed', 'match_mode', 'weather_conditions', 'event_status',
                            'match_status', 'match_ended', 'match_duration',
                            'home_score', 'away_score', 'winner_id', 'match_result']
            df_matches = pd.DataFrame(columns=cols_matches)
            self.load_table_matches(df_matches, 'create', dictConnParams)

    def updateTablaDW(self, tabla, pk, nombre_campo, valor_nuevo):
        classETL = etl.ETL_Tenis(user_key='kb6dv75rtq3pk55uumfkxczc')

        print('# Actualización Tabla ', tabla)

    def load_table_tournaments(self, df, operation, dictConnParams):
        """
        Realiza operaciones de carga en DWH los registros de la tabla Tournaments

        Parameters:
            df: DataFrame con los torneos a cargar
            operation: tipo de operacion a realizar (create, insert, update)
            dictConnParams: parametros de conexion a PostgreSQL

        Return:
            Mensaje de exito o error en la carga
        """
        import psycopg2
        from psycopg2 import Error

        try:
            connection = psycopg2.connect(user      = dictConnParams['user'],
                                          password  = dictConnParams['password'],
                                          host      = dictConnParams['host'],
                                          port      = dictConnParams['port'],
                                          database  = dictConnParams['database'])

            cursor = connection.cursor()

            if operation == 'create':
                try:
                    create_table_query = '''
                    CREATE TABLE "Tenis"."Tournaments"
                    (
                     "tournament_id" varchar(50) NOT NULL,
                     "parent_id"     varchar(50) NOT NULL,
                     "category_pk"   varchar(50) NOT NULL,
                     "tr_name"       varchar(50) NOT NULL,
                     "tr_type"       varchar(50) NOT NULL,
                     "tr_gender"     varchar(50) NOT NULL,
                     CONSTRAINT "FK_50" FOREIGN KEY ( "category_pk" ) REFERENCES "Tenis"."TournamentCategories" ( "category_pk" ),
                     CONSTRAINT "FK_53" FOREIGN KEY ( "parent_id" ) REFERENCES "Tenis"."ParentTournaments" ( "parent_id" )
                    );
                    CREATE UNIQUE INDEX "PK_Tournaments" ON "Tenis"."Tournaments"
                    (
                     "tournament_id"
                    );     
                    CREATE INDEX "fkIdx_50" ON "Tenis"."Tournaments"
                    (
                     "category_pk"
                    );
                    CREATE INDEX "fkIdx_53" ON "Tenis"."Tournaments"
                    (
                     "parent_id"
                    );
                    '''

                    cursor.execute(create_table_query)
                    connection.commit()
                    logging.info("Table Tournaments creada satisfactoriamente en PostgreSQL - Tenis")

                except (Exception, psycopg2.DatabaseError) as error:
                    logging.error("Error al crear en PostgreSQL la tabla Tournaments: {}".format(error))
                    raise Exception("Error al crear en PostgreSQL la tabla Tournaments: {}".format(error))

            elif operation == 'insert':
                try:
                    cursor.execute('''SELECT * FROM "Tenis"."Tournaments"''')
                    all = cursor.fetchall()

                    ids_excluir = []
                    for id in all:
                        ids_excluir.append(id[0])

                    df = df[~df['tournament_id'].isin(ids_excluir)].reset_index(drop=True).copy()

                    postgres_insert_query = """ INSERT INTO "Tenis"."Tournaments" 
                                (tournament_id, parent_id, category_pk, tr_name, tr_type, tr_gender) 
                                VALUES (%s,%s,%s,%s,%s,%s) """

                    if len(df) > 0:
                        count = 0
                        pct_logueado = 0
                        for registro in range(len(df)):
                            # Logueamos el avance de la carga
                            if (np.round(registro/len(df), 3) in [0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90, 1.00]) & (np.round(registro/len(df) * 100, 0) > pct_logueado):
                                logging.info('Avance carga: {} / {} ({}%)'.format(registro, len(df),np.round(registro / len(df) * 100,0)))
                                pct_logueado = np.round(registro / len(df) * 100, 0)

                            # Generamos el registro a insertar
                            record_to_insert = (df[df.index == registro]['tournament_id'].values[0],
                                                df[df.index == registro]['parent_id'].values[0],
                                                df[df.index == registro]['category_pk'].values[0],
                                                df[df.index == registro]['tr_name'].values[0],
                                                df[df.index == registro]['tr_type'].values[0],
                                                df[df.index == registro]['tr_gender'].values[0])

                            cursor.execute(postgres_insert_query, record_to_insert)
                            connection.commit()
                            count += 1
                        logging.info('Registros insertados satisfactoriamente en Table Seasons: {}'.format(count))
                    else:
                        logging.info('No hay registros nuevos para agregar')

                except (Exception, psycopg2.Error) as error:
                    if (connection):
                        logging.error("Fallo al insertar registro en la Tabla Tournaments: {}".format(error))
                        raise Exception("Fallo al insertar registro en la Tabla Tournaments: {}".format(error))

            elif operation == 'update':
                print('update')

        except:
            logging.error('Falló la conexión con PostgreSQL')
            raise Exception('Falló la conexión con PostgreSQL')

        finally:
            # closing database connection.
            if (connection):
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")

    def load_table_seasons(self, df, operation, dictConnParams):
        """
        Realiza operaciones de carga en DWH los registros de la tabla Seasons

        Parameters:
            df: DataFrame con las temporadas a cargar
            operation: tipo de operacion a realizar (create, insert, update)
            dictConnParams: parametros de conexion a PostgreSQL

        Return:
            Mensaje de exito o error en la carga
        """
        import psycopg2
        from psycopg2 import Error

        try:
            connection = psycopg2.connect(user=dictConnParams['user'],
                                          password=dictConnParams['password'],
                                          host=dictConnParams['host'],
                                          port=dictConnParams['port'],
                                          database=dictConnParams['database'])

            cursor = connection.cursor()

            if operation == 'create':
                try:
                    create_table_query = '''
                        CREATE TABLE "Tenis"."Seasons"
                        (
                         "season_id"               varchar(50) NOT NULL,
                         "season_name"             varchar(50) NOT NULL,
                         "start_date"              date NULL,
                         "end_date"                date NULL,
                         "year"                    int NULL,
                         "tournament_id"           varchar(50) NOT NULL,
                         "category_pk"             varchar(50) NOT NULL,
                         "prize_money_amt"         int NULL,
                         "prize_currency"          varchar(50) NULL,
                         "surface"                 varchar(50) NULL,
                         "complex"                 varchar(50) NULL,
                         "q_competitors"           int NULL,
                         "q_qualified_competitors" int NULL,
                         "q_scheduled_matches"     int NULL,
                         CONSTRAINT "FK_128" FOREIGN KEY ( "tournament_id" ) REFERENCES "Tenis"."Tournaments" ( "tournament_id" )
                        );
                        
                        CREATE UNIQUE INDEX "PK_Seasons" ON "Tenis"."Seasons"
                        (
                         "season_id"
                        );
                        
                        CREATE INDEX "fkIdx_128" ON "Tenis"."Seasons"
                        (
                         "tournament_id"
                        );
                    '''

                    cursor.execute(create_table_query)
                    connection.commit()
                    logging.info("Table Seasons creada satisfactoriamente en PostgreSQL - Tenis")

                except (Exception, psycopg2.DatabaseError) as error:
                    logging.error("Error al crear en PostgreSQL la tabla Seasons: {}".format(error))
                    raise Exception("Error al crear en PostgreSQL la tabla Seasons: {}".format(error))

            elif operation == 'insert':
                try:
                    cursor.execute('''SELECT * FROM "Tenis"."Seasons"''')
                    all = cursor.fetchall()

                    ids_excluir = []
                    for id in all:
                        # Eliminamos los registros de torneos del 2020 para actualizarlos
                        if id[4] == 2020:
                            cursor.execute('''DELETE FROM "Tenis"."Seasons" WHERE season_id = %s''', (id[0],))
                            connection.commit()
                        # Añadimos los ids de partidos existentes para excluirlos del Load
                        ids_excluir.append(id[0])

                    df = df[~df['season_id'].isin(ids_excluir)].reset_index(drop=True).copy()

                    postgres_insert_query = """ INSERT INTO "Tenis"."Seasons" 
                                (season_id, season_name, start_date, end_date, year, tournament_id, category_pk,
                                 prize_money_amt, prize_currency, surface, complex, q_competitors, 
                                 q_qualified_competitors, q_scheduled_matches) 
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """

                    if len(df) > 0:
                        count = 0
                        pct_logueado = 0
                        for registro in range(len(df)):
                            # Logueamos el avance de la carga
                            if (np.round(registro/len(df), 3) in [0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90, 1.00]) & (np.round(registro/len(df) * 100, 0) > pct_logueado):
                                logging.info('Avance carga: {} / {} ({}%)'.format(registro, len(df),np.round(registro / len(df) * 100,0)))
                                pct_logueado = np.round(registro / len(df) * 100, 0)

                            # Generamos el registro a insertar
                            record_to_insert = (df[df.index == registro]['season_id'].values[0],
                                                df[df.index == registro]['season_name'].values[0],
                                                df[df.index == registro]['start_date'].values[0],
                                                df[df.index == registro]['end_date'].values[0],
                                                df[df.index == registro]['year'].values[0],
                                                df[df.index == registro]['tournament_id'].values[0],
                                                df[df.index == registro]['category_pk'].values[0],
                                                df[df.index == registro]['prize_money_amt'].values[0],
                                                df[df.index == registro]['prize_currency'].values[0],
                                                df[df.index == registro]['surface'].values[0],
                                                df[df.index == registro]['complex'].values[0],
                                                df[df.index == registro]['q_competitors'].values[0],
                                                df[df.index == registro]['q_qualified_competitors'].values[0],
                                                df[df.index == registro]['q_scheduled_matches'].values[0]
                                                )

                            cursor.execute(postgres_insert_query, record_to_insert)
                            connection.commit()
                            count += 1
                        logging.info('Registros insertados satisfactoriamente en Table Seasons: {}'.format(count))
                    else:
                        logging.info('No hay registros nuevos para agregar')

                except (Exception, psycopg2.Error) as error:
                    if (connection):
                        logging.error("Fallo al insertar registro en la Tabla Seasons: {}".format(error))
                        raise Exception("Fallo al insertar registro en la Tabla Seasons: {}".format(error))

            elif operation == 'update':
                print('update')

        except:
            logging.error('Falló la conexión con PostgreSQL')
            raise Exception('Falló la conexión con PostgreSQL')

        finally:
            # closing database connection.
            if (connection):
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")

    def load_table_parents(self, df, operation, dictConnParams):
        """
        Realiza operaciones de carga en DWH los registros de la tabla ParentTournaments

        Parameters:
            df: DataFrame con los torneos a cargar
            operation: tipo de operacion a realizar (create, insert, update)
            dictConnParams: parametros de conexion a PostgreSQL

        Return:
            Mensaje de exito o error en la carga
        """
        import psycopg2
        from psycopg2 import Error

        try:
            connection = psycopg2.connect(user=dictConnParams['user'],
                                          password=dictConnParams['password'],
                                          host=dictConnParams['host'],
                                          port=dictConnParams['port'],
                                          database=dictConnParams['database'])

            cursor = connection.cursor()

            if operation == 'create':
                try:
                    create_table_query = '''
                        CREATE TABLE "Tenis"."ParentTournaments"
                        (
                         "parent_id"   varchar(50) NOT NULL,
                         "parent_name" varchar(50) NULL,
                         "category_pk" varchar(50) NOT NULL
                        );

                        CREATE UNIQUE INDEX "PK_ParentTournaments" ON "Tenis"."ParentTournaments"
                        (
                         "parent_id"
                        );
                    '''

                    cursor.execute(create_table_query)
                    connection.commit()
                    logging.info("Table ParentTournaments creada satisfactoriamente en PostgreSQL - Tenis")

                except (Exception, psycopg2.DatabaseError) as error:
                    logging.error("Error al crear en PostgreSQL la tabla ParentTournaments: {}".format(error))
                    raise Exception("Error al crear en PostgreSQL la tabla ParentTournaments: {}".format(error))

            elif operation == 'insert':
                try:
                    cursor.execute('''SELECT * FROM "Tenis"."ParentTournaments"''')
                    all = cursor.fetchall()

                    ids_excluir = []
                    for id in all:
                        ids_excluir.append(id[0])

                    if 'SinDatos' not in ids_excluir:
                        df = df.append(pd.DataFrame(np.array(['SinDatos', 'SinDatos', 'SinDatos']).reshape(1,-1),
                                  columns = ['parent_id', 'parent_name', 'category_pk']))

                    df = df[~df['parent_id'].isin(ids_excluir)].reset_index(drop=True).copy()

                    postgres_insert_query = """ INSERT INTO "Tenis"."ParentTournaments" 
                                (parent_id, parent_name, category_pk) 
                                VALUES (%s,%s,%s) """

                    if len(df) > 0:
                        count = 0
                        pct_logueado = 0
                        for registro in range(len(df)):
                            # Logueamos el avance de la carga
                            if (np.round(registro/len(df), 3) in [0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90, 1.00]) & (np.round(registro/len(df) * 100, 0) > pct_logueado):
                                logging.info('Avance carga: {} / {} ({}%)'.format(registro, len(df),np.round(registro / len(df) * 100,0)))
                                pct_logueado = np.round(registro / len(df) * 100, 0)

                            # Generamos el registro a insertar
                            record_to_insert = (df[df.index == registro]['parent_id'].values[0],
                                                "",
                                                df[df.index == registro]['category_pk'].values[0])

                            cursor.execute(postgres_insert_query, record_to_insert)
                            connection.commit()
                            count += 1
                        logging.info('Registros insertados satisfactoriamente en la Tabla ParentTournaments: {}'.format(count))
                    else:
                        logging.info('No hay registros nuevos para agregar')

                except (Exception, psycopg2.Error) as error:
                    if (connection):
                        logging.error("Fallo al insertar registro en la Tabla ParentTournaments: {}".format(error))
                        raise Exception("Fallo al insertar registro en la Tabla ParentTournaments: {}".format(error))

            elif operation == 'update':
                print('update')

        except:
            logging.error('Falló la conexión con PostgreSQL')
            raise Exception('Falló la conexión con PostgreSQL')

        finally:
            # closing database connection.
            if (connection):
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")

    def load_table_categories(self, df, operation, dictConnParams):
        """
        Realiza operaciones de carga en DWH los registros de la tabla TournamentCategories

        Parameters:
            df: DataFrame con las categorias de torneo a cargar
            operation: tipo de operacion a realizar (create, insert, update)
            dictConnParams: parametros de conexion a PostgreSQL

        Return:
            Mensaje de exito o error en la carga
        """
        import psycopg2
        from psycopg2 import Error

        try:
            connection = psycopg2.connect(user=dictConnParams['user'],
                                          password=dictConnParams['password'],
                                          host=dictConnParams['host'],
                                          port=dictConnParams['port'],
                                          database=dictConnParams['database'])

            cursor = connection.cursor()

            if operation == 'create':
                try:
                    create_table_query = '''
                            CREATE TABLE "Tenis"."TournamentCategories"
                            (
                             "category_pk"    varchar(50) NOT NULL,
                             "category_id"    varchar(50) NOT NULL,
                             "category_name"  varchar(50) NOT NULL,
                             "category_level" varchar(50) NULL
                            );

                            CREATE UNIQUE INDEX "PK_TournamentCategories" ON "Tenis"."TournamentCategories"
                            (
                             "category_pk"
                            );
                    '''

                    cursor.execute(create_table_query)
                    connection.commit()
                    logging.info("Table TournamentCategories creada satisfactoriamente en PostgreSQL - Tenis")

                except (Exception, psycopg2.DatabaseError) as error:
                    logging.error("Error al crear en PostgreSQL la tabla TournamentCategories: {}".format(error))
                    raise Exception("Error al crear en PostgreSQL la tabla TournamentCategories: {}".format(error))

            elif operation == 'insert':
                try:
                    cursor.execute('''SELECT * FROM "Tenis"."TournamentCategories"''')
                    all = cursor.fetchall()

                    ids_excluir = []
                    for id in all:
                        ids_excluir.append(id[0])

                    if 'SinDatos' not in ids_excluir:
                        df = df.append(pd.DataFrame(np.array(['SinDatos', 'SinDatos', 'SinDatos', 'SinDatos']).reshape(1,-1),
                                  columns = ['category_pk', 'category_id', 'category_name', 'category_level']))

                    df = df[~df['category_pk'].isin(ids_excluir)].reset_index(drop=True).copy()


                    postgres_insert_query = """ INSERT INTO "Tenis"."TournamentCategories" 
                                (category_pk, category_id, category_name, category_level) 
                                VALUES (%s,%s,%s,%s) """

                    if len(df) > 0:
                        count = 0
                        pct_logueado = 0
                        for registro in range(len(df)):
                            # Logueamos el avance de la carga
                            if (np.round(registro/len(df), 3) in [0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90, 1.00]) & (np.round(registro/len(df) * 100, 0) > pct_logueado):
                                logging.info('Avance carga: {} / {} ({}%)'.format(registro, len(df),np.round(registro / len(df) * 100,0)))
                                pct_logueado = np.round(registro / len(df) * 100, 0)

                            # Generamos el registro a insertar
                            record_to_insert = (df[df.index == registro]['category_pk'].values[0],
                                                df[df.index == registro]['category_id'].values[0],
                                                df[df.index == registro]['category_name'].values[0],
                                                df[df.index == registro]['category_level'].values[0])

                            cursor.execute(postgres_insert_query, record_to_insert)
                            connection.commit()
                            count += 1
                        logging.info('Registros insertados satisfactoriamente en Table TournamentCategories: {}'.format(count))
                    else:
                        logging.info('No hay registros nuevos para agregar')

                except (Exception, psycopg2.Error) as error:
                    if (connection):
                        logging.error("Fallo al insertar registro en la Tabla TournamentCategories: {}".format(error))
                        raise Exception("Fallo al insertar registro en la Tabla TournamentCategories: {}".format(error))

            elif operation == 'update':
                print('update')

        except:
            logging.error('Falló la conexión con PostgreSQL')
            raise Exception('Falló la conexión con PostgreSQL')

        finally:
            # closing database connection.
            if (connection):
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")

    def load_table_venues(self, df, operation, dictConnParams):
        """
        Realiza operaciones de carga en DWH los registros de la tabla Venues

        Parameters:
            df: DataFrame con los estadios a cargar
            operation: tipo de operacion a realizar (create, insert, update)
            dictConnParams: parametros de conexion a PostgreSQL

        Return:
            Mensaje de exito o error en la carga
        """
        import psycopg2
        from psycopg2 import Error

        try:
            connection = psycopg2.connect(user=dictConnParams['user'],
                                          password=dictConnParams['password'],
                                          host=dictConnParams['host'],
                                          port=dictConnParams['port'],
                                          database=dictConnParams['database'])

            cursor = connection.cursor()

            if operation == 'create':
                try:
                    create_table_query = '''
                        CREATE TABLE "Tenis"."Venues"
                        (
                         "venue_id"     varchar(50) NOT NULL,
                         "venue_name"   varchar(50) NOT NULL,
                         "city_name"    varchar(50) NOT NULL,
                         "country_code" varchar(50) NOT NULL,
                         CONSTRAINT "FK_109" FOREIGN KEY ( "country_code" ) REFERENCES "Tenis"."Countries" ( "country_code" )
                        );
                        
                        CREATE UNIQUE INDEX "PK_Venues" ON "Tenis"."Venues"
                        (
                         "venue_id"
                        );
                        
                        CREATE INDEX "fkIdx_109" ON "Tenis"."Venues"
                        (
                         "country_code"
                        );
                    '''

                    cursor.execute(create_table_query)
                    connection.commit()
                    logging.info("Table Venues creada satisfactoriamente en PostgreSQL - Tenis")

                except (Exception, psycopg2.DatabaseError) as error:
                    logging.error("Error al crear en PostgreSQL la tabla Venues: {}".format(error))
                    raise Exception("Error al crear en PostgreSQL la tabla Venues: {}".format(error))

            elif operation == 'insert':
                try:
                    cursor.execute('''SELECT * FROM "Tenis"."Venues"''')
                    all = cursor.fetchall()

                    ids_excluir = []
                    for id in all:
                        ids_excluir.append(id[0])

                    if 'SinDatos' not in ids_excluir:
                        df = df.append(pd.DataFrame(np.array(['SinDatos', 'SinDatos', 'SinDatos', 'SinDatos']).reshape(1,-1),
                                  columns = ['venue_id', 'venue_name', 'city_name', 'country_code']))

                    df = df[~df['venue_id'].isin(ids_excluir)].reset_index(drop=True).copy()

                    postgres_insert_query = """ INSERT INTO "Tenis"."Venues" 
                                (venue_id, venue_name, city_name, country_code) 
                                VALUES (%s,%s,%s,%s) """

                    if len(df) > 0:
                        count = 0
                        pct_logueado = 0
                        for registro in range(len(df)):
                            # Logueamos el avance de la carga
                            if (np.round(registro/len(df), 3) in [0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90, 1.00]) & (np.round(registro/len(df) * 100, 0) > pct_logueado):
                                logging.info('Avance carga: {} / {} ({}%)'.format(registro, len(df),np.round(registro / len(df) * 100,0)))
                                pct_logueado = np.round(registro / len(df) * 100, 0)

                            # Generamos el registro a insertar
                            record_to_insert = (df[df.index == registro]['venue_id'].values[0],
                                                df[df.index == registro]['venue_name'].values[0],
                                                df[df.index == registro]['city_name'].values[0],
                                                df[df.index == registro]['country_code'].values[0])

                            cursor.execute(postgres_insert_query, record_to_insert)
                            connection.commit()
                            count += 1
                        logging.info('Registros insertados satisfactoriamente en Table Venues: {}'.format(count))
                    else:
                        logging.info('No hay registros nuevos para agregar')

                except (Exception, psycopg2.Error) as error:
                    if (connection):
                        logging.error("Fallo al insertar registro en la Tabla Venues: {}".format(error))
                        raise Exception("Fallo al insertar registro en la Tabla Venues: {}".format(error))

            elif operation == 'update':
                logging.info('')

                dictUpdate = {'pk': ['1587'],
                              'campo': ['city_name'],
                              'valor_nuevo': ['Melbourne']}

        except:
            logging.error('Falló la conexión con PostgreSQL')
            raise Exception('Falló la conexión con PostgreSQL')

        finally:
            # closing database connection.
            if (connection):
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")

    def load_table_countries(self, df, operation, dictConnParams):
        """
        Realiza operaciones de carga en DWH los registros de la tabla Countries

        Parameters:
            df: DataFrame con los paises a cargar
            operation: tipo de operacion a realizar (create, insert, update)
            dictConnParams: parametros de conexion a PostgreSQL

        Return:
            Mensaje de exito o error en la carga
        """
        import psycopg2
        from psycopg2 import Error

        try:
            connection = psycopg2.connect(user=dictConnParams['user'],
                                          password=dictConnParams['password'],
                                          host=dictConnParams['host'],
                                          port=dictConnParams['port'],
                                          database=dictConnParams['database'])

            cursor = connection.cursor()

            if operation == 'create':
                try:
                    create_table_query = '''
                        CREATE TABLE "Tenis"."Countries"
                        (
                         "country_code" varchar(50) NOT NULL,
                         "country_name" varchar(50) NOT NULL
                        
                        );
                        
                        CREATE UNIQUE INDEX "PK_Countries" ON "Tenis"."Countries"
                        (
                         "country_code"
                        );
                    '''

                    cursor.execute(create_table_query)
                    connection.commit()
                    logging.info("Table Countries creada satisfactoriamente en PostgreSQL - Tenis")

                except (Exception, psycopg2.DatabaseError) as error:
                    logging.error("Error al crear en PostgreSQL la tabla Countries: {}".format(error))
                    raise Exception("Error al crear en PostgreSQL la tabla Countries: {}".format(error))

            elif operation == 'insert':
                try:
                    cursor.execute('''SELECT * FROM "Tenis"."Countries"''')
                    all = cursor.fetchall()

                    ids_excluir = []
                    for id in all:
                        ids_excluir.append(id[0])

                    if 'SinDatos' not in ids_excluir:
                        df = df.append(pd.DataFrame(np.array(['SinDatos', 'SinDatos']).reshape(1,-1),
                                  columns = ['country_code', 'country_name']))

                    df = df[~df['country_code'].isin(ids_excluir)].reset_index(drop=True).copy()

                    postgres_insert_query = """ INSERT INTO "Tenis"."Countries" 
                                (country_code, country_name) 
                                VALUES (%s,%s) """

                    if len(df) > 0:
                        count = 0
                        pct_logueado = 0
                        for registro in range(len(df)):
                            # Logueamos el avance de la carga
                            if (np.round(registro/len(df), 3) in [0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90, 1.00]) & (np.round(registro/len(df) * 100, 0) > pct_logueado):
                                logging.info('Avance carga: {} / {} ({}%)'.format(registro, len(df),np.round(registro / len(df) * 100,0)))
                                pct_logueado = np.round(registro / len(df) * 100, 0)

                            # Generamos el registro a insertar
                            record_to_insert = (df[df.index == registro]['country_code'].values[0],
                                                df[df.index == registro]['country_name'].values[0])

                            cursor.execute(postgres_insert_query, record_to_insert)
                            connection.commit()
                            count += 1
                        logging.info('Registros insertados satisfactoriamente en Table Countries: {}'.format(count))
                    else:
                        logging.info('No hay registros nuevos para agregar')

                except (Exception, psycopg2.Error) as error:
                    if (connection):
                        logging.error("Fallo al insertar registro en la Tabla Countries: {}".format(error))
                        raise Exception("Fallo al insertar registro en la Tabla Countries: {}".format(error))

            elif operation == 'update':
                print('update')

        except:
            logging.error('Falló la conexión con PostgreSQL')
            raise Exception('Falló la conexión con PostgreSQL')

        finally:
            # closing database connection.
            if (connection):
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")

    def load_table_matches(self, df, operation, dictConnParams):
        """
        Realiza operaciones de carga en DWH los registros de la tabla Matches

        Parameters:
            df: DataFrame con los partidos a cargar
            operation: tipo de operacion a realizar (create, insert, update)
            dictConnParams: parametros de conexion a PostgreSQL

        Return:
            Mensaje de exito o error en la carga
        """
        import psycopg2
        from psycopg2 import Error

        try:
            connection = psycopg2.connect(user=dictConnParams['user'],
                                          password=dictConnParams['password'],
                                          host=dictConnParams['host'],
                                          port=dictConnParams['port'],
                                          database=dictConnParams['database'])

            cursor = connection.cursor()

            if operation == 'create':
                try:
                    create_table_query = '''
                        CREATE TABLE "Tenis"."Matches"
                        (
                         "match_id"           varchar(50) NOT NULL,
                         "tournament_id"      varchar(50) NOT NULL,
                         "season_id"          varchar(50) NOT NULL,
                         "category_pk"        varchar(50) NOT NULL,
                         "scheduled"          varchar(50) NULL,
                         "venue_id"           varchar(50) NULL,
                         "tr_round_type"      varchar(50) NULL,
                         "tr_round_number"    int NULL,
                         "tr_round_name"      varchar(50) NULL,
                         "home_competitor_id" varchar(50) NOT NULL,
                         "home_seed"          int NULL,
                         "away_competitor_id" varchar(50) NOT NULL,
                         "away_seed"          int NULL,
                         "match_mode"         varchar(50) NOT NULL,
                         "weather_conditions" varchar(50) NULL,
                         "event_status"       varchar(50) NOT NULL,
                         "match_status"       varchar(50) NOT NULL,
                         "match_ended"        varchar(50) NULL,
                         "match_duration"     int NULL,
                         "home_score"         int NULL,
                         "away_score"         int NULL,
                         "winner_id"          varchar(50) NULL,
                         "match_result"       varchar(50) NULL,
                         CONSTRAINT "FK_115" FOREIGN KEY ( "tournament_id" ) REFERENCES "Tenis"."Tournaments" ( "tournament_id" ),
                         CONSTRAINT "FK_122" FOREIGN KEY ( "venue_id" ) REFERENCES "Tenis"."Venues" ( "venue_id" )
                        );
                        
                        CREATE UNIQUE INDEX "PK_Matches" ON "Tenis"."Matches"
                        (
                         "match_id"
                        );
                        
                        CREATE INDEX "fkIdx_115" ON "Tenis"."Matches"
                        (
                         "tournament_id"
                        );
                        
                        CREATE INDEX "fkIdx_122" ON "Tenis"."Matches"
                        (
                         "venue_id"
                        );
                    '''

                    cursor.execute(create_table_query)
                    connection.commit()
                    logging.info("Table Matches creada satisfactoriamente en PostgreSQL - Tenis")

                except (Exception, psycopg2.DatabaseError) as error:
                    logging.error("Error al crear en PostgreSQL la tabla Matches: {}".format(error))
                    raise Exception("Error al crear en PostgreSQL la tabla Matches: {}".format(error))

            elif operation == 'insert':
                try:
                    cursor.execute('''SELECT * FROM "Tenis"."Matches"''')
                    all = cursor.fetchall()

                    ids_excluir = []
                    for id in all:
                        # Eliminamos los registros de torneos del 2020 para actualizarlos
                        if id[4][0:4] == '2020':
                            cursor.execute('''DELETE FROM "Tenis"."Seasons" WHERE season_id = %s''', (id[0],))
                            connection.commit()
                        # Añadimos los ids de partidos existentes para excluirlos del Load
                        ids_excluir.append(id[0])

                    df = df[~df['match_id'].isin(ids_excluir)].reset_index(drop=True).copy()

                    postgres_insert_query = """ INSERT INTO "Tenis"."Matches" 
                                (match_id, tournament_id, season_id, category_pk, scheduled,
                                 venue_id, tr_round_type, tr_round_number, tr_round_name, home_competitor_id,
                                 home_seed, away_competitor_id, away_seed, match_mode, weather_conditions,
                                 event_status, match_status, match_ended, match_duration,
                                 home_score, away_score, winner_id, match_result) 
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """

                    if len(df) > 0:
                        count = 0
                        pct_logueado = 0
                        for registro in range(len(df)):
                            # Logueamos el avance de la carga
                            if (np.round(registro/len(df), 3) in [0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90, 1.00]) & (np.round(registro/len(df) * 100, 0) > pct_logueado):
                                logging.info('Avance carga: {} / {} ({}%)'.format(registro, len(df), np.round(registro/len(df) * 100, 0)))
                                pct_logueado = np.round(registro/len(df) * 100, 0)

                            # Generamos el registro a insertar
                            record_to_insert = (df[df.index == registro]['match_id'].values[0],
                                                df[df.index == registro]['tournament_id'].values[0],
                                                df[df.index == registro]['season_id'].values[0],
                                                df[df.index == registro]['category_pk'].values[0],
                                                df[df.index == registro]['scheduled'].values[0],
                                                df[df.index == registro]['venue_id'].values[0],
                                                df[df.index == registro]['tr_round_type'].values[0],
                                                df[df.index == registro]['tr_round_number'].values[0],
                                                df[df.index == registro]['tr_round_name'].values[0],
                                                df[df.index == registro]['home_competitor_id'].values[0],
                                                df[df.index == registro]['home_seed'].values[0],
                                                df[df.index == registro]['away_competitor_id'].values[0],
                                                df[df.index == registro]['away_seed'].values[0],
                                                df[df.index == registro]['match_mode'].values[0],
                                                df[df.index == registro]['weather_conditions'].values[0],
                                                df[df.index == registro]['event_status'].values[0],
                                                df[df.index == registro]['match_status'].values[0],
                                                df[df.index == registro]['match_ended'].values[0],
                                                df[df.index == registro]['match_duration'].values[0],
                                                df[df.index == registro]['home_score'].values[0],
                                                df[df.index == registro]['away_score'].values[0],
                                                df[df.index == registro]['winner_id'].values[0],
                                                df[df.index == registro]['match_result'].values[0])

                            cursor.execute(postgres_insert_query, record_to_insert)
                            connection.commit()
                            count += 1
                        logging.info('Registros insertados satisfactoriamente en Tabla Matches: {}'.format(count))
                    else:
                        logging.info('No hay registros nuevos para agregar')

                except (Exception, psycopg2.Error) as error:
                    if (connection):
                        logging.error("Fallo al insertar registro en la Tabla Matches: {}".format(error))
                        raise Exception("Fallo al insertar registro en la Tabla Matches: {}".format(error))

            elif operation == 'update':
                print('update')

        except:
            logging.error('Falló la conexión con PostgreSQL')
            raise Exception('Falló la conexión con PostgreSQL')

        finally:
            # closing database connection.
            if (connection):
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")