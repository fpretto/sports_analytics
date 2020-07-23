import numpy as np
import cv2

## TODO: agregar graficos al reporte (donut chart para los pcts). Ver templates copados.

class ReportGeneration:
    def __init__(self):
        self.pix2meter = (9.75/656+6.4/414)/2 # 0.0002645833

    def loadTemplate(self, pag: str):
        from jinja2 import Environment, FileSystemLoader
        env = Environment(loader=FileSystemLoader('C:/Repo/Ludis/Squash/05 - Report Generation/'))
        template = env.get_template('StatsReport_' + pag + '.html')
        return template

    def generateHeatmapHTML(self, heatmap_b64):
        #TODO: cuando sepa bien el scale_pct final, dejar la imagen precomputada y levantarla lista para poner
        scale_pct=0.65
        width = 464 * scale_pct #int(court_img.shape[1]*scale_pct)
        height = 656 * scale_pct #int(court_img.shape[0]*scale_pct)
        heatmap_html = '<img src="data:image/png;base64, {}", style="width:{}px;height:{}px;">'.format(heatmap_b64.decode('utf-8'), width, height)

        return heatmap_html

    def convertImg2html(self, img, img_extension='.png', scale_pct=1.0):
        import base64
        import cv2

        width = int(img.shape[1] * scale_pct)
        height = int(img.shape[0] * scale_pct)
        _, img_arr = cv2.imencode(img_extension, img)
        img_bytes = img_arr.tobytes()
        img_b64 = base64.b64encode(img_bytes)
        img_html = '<img src="data:image/png;base64, {}", style="width:{}px;height:{}px;">'.format(
            img_b64.decode('utf-8'), width, height)

        return img_html

    def calculateZonalPositioning(self, players_coords):
        q_points = len(players_coords)
        front_pct = int(round(sum([np.squeeze(x)[1] < 165 for x in players_coords]) / q_points * 100))
        mid_pct = int(round(sum([(np.squeeze(x)[1] >= 165) and (np.squeeze(x)[1] <= 464) for x in players_coords]) / q_points * 100))
        back_pct = int(round(sum([np.squeeze(x)[1] > 464 for x in players_coords]) / q_points * 100))

        return front_pct, mid_pct, back_pct

    def generateZoneCoverage(self, front_pct, mid_pct, back_pct, scale_pct):
        import cv2

        court_zones = cv2.imread('C:/GoogleDrive/LudisAI/06 - Report Generation/squash_court_zones.png')

        cv2.putText(court_zones, str(front_pct) + ' %', (181, 105), cv2.FONT_HERSHEY_DUPLEX, 0.7, (19, 69, 139), 2)
        cv2.putText(court_zones, str(mid_pct) + ' %', (181, 290), cv2.FONT_HERSHEY_DUPLEX, 0.7, (0, 100, 0), 2)
        cv2.putText(court_zones, str(back_pct) + ' %', (179, 580), cv2.FONT_HERSHEY_DUPLEX, 0.7, (25, 25, 112), 2)

        img_html = self.convertImg2html(court_zones, img_extension='.png', scale_pct=scale_pct)

        return img_html

    def calculateQuadrantPositioning(self, player_coords):
        from shapely.geometry import Point, box

        cuadrante_1 = box(0, 0, 209, 350)  # Adelante Izquierda
        cuadrante_2 = box(209, 0, 414, 350)  # Adelante Derecha
        cuadrante_3 = box(0, 350, 209, 656)  # Atras Izquierda
        cuadrante_4 = box(209, 350, 414, 656)  # Atras Derecha

        points_A1 = [np.squeeze(x) for x in player_coords if Point(np.squeeze(x)).within(cuadrante_1)]
        points_A2 = [np.squeeze(x) for x in player_coords if Point(np.squeeze(x)).within(cuadrante_2)]
        points_A3 = [np.squeeze(x) for x in player_coords if Point(np.squeeze(x)).within(cuadrante_3)]
        points_A4 = [np.squeeze(x) for x in player_coords if Point(np.squeeze(x)).within(cuadrante_4)]

        Q1 = round(len(points_A1) / len(player_coords) * 100, 0)
        Q2 = round(len(points_A2) / len(player_coords) * 100, 0)
        Q3 = round(len(points_A3) / len(player_coords) * 100, 0)
        Q4 = round(len(points_A4) / len(player_coords) * 100, 0)

        IZQ = round(len(points_A1) / len(player_coords) * 100, 0) + round(len(points_A3) / len(player_coords) * 100, 0)
        DER = round(len(points_A2) / len(player_coords) * 100, 0) + round(len(points_A4) / len(player_coords) * 100, 0)

        return Q1, Q2, Q3, Q4, IZQ, DER

    def calculateTControlScore(self, players_coords):
        import shapely.affinity
        from shapely.geometry import Point

        circle = Point(208, 350).buffer(1)

        ellipse_1 = shapely.affinity.scale(circle, 40, 55)
        points_1 = [np.squeeze(x) for x in players_coords if Point(np.squeeze(x)).within(ellipse_1)]

        ellipse_2 = shapely.affinity.scale(circle, 90, 115)
        points_2 = [np.squeeze(x) for x in players_coords if (Point(np.squeeze(x)).within(ellipse_2)) and (Point(np.squeeze(x)).within(ellipse_1) == False)]

        ellipse_3 = shapely.affinity.scale(circle, 165, 200)
        points_3 = [np.squeeze(x) for x in players_coords if (Point(np.squeeze(x)).within(ellipse_3)) and (Point(np.squeeze(x)).within(ellipse_1) == False) and (Point(np.squeeze(x)).within(ellipse_2) == False)]
        points_4 = [np.squeeze(x) for x in players_coords if Point(np.squeeze(x)).within(ellipse_3) == False]

        t_control_score = round((len(points_1)*7 + len(points_2)*5 + len(points_3)*3 - len(points_4)*5) / (len(players_coords)*5)*100, 2)
        points = [points_1, points_2, points_3, points_4]
        return t_control_score, points

    def plotTControlScore(self, points):
        court_img = cv2.imread('C:/GoogleDrive/LudisAI/05 - Court Mapping/squash_court.jpg')
        cv2.ellipse(court_img, (208,350), (40, 55), 0, 0, 360, (255, 0, 0), 2)
        cv2.ellipse(court_img, (208,350), (90, 115), 0, 0, 360, (255, 0, 0), 2)
        cv2.ellipse(court_img, (208,350), (165, 200), 0, 0, 360, (255, 0, 0), 2)

        [cv2.circle(court_img, center=(x[0], x[1]), radius=3, color=(0, 255, 0), thickness=-1) for x in points[0]]
        [cv2.circle(court_img, center=(x[0], x[1]), radius=3, color=(255, 0, 0), thickness=-1) for x in points[1]]
        [cv2.circle(court_img, center=(x[0], x[1]), radius=3, color=(0, 255, 255), thickness=-1) for x in points[2]]
        [cv2.circle(court_img, center=(x[0], x[1]), radius=3, color=(255, 255, 0), thickness=-1) for x in points[3]]
        cv2.imshow('elipse', court_img)

    def calculateDistSpeed(self, players_coords, video_duration):
        # Calculo diferencia en posiciones
        list_diff = [(np.squeeze(x1)[0] - np.squeeze(x0)[0], np.squeeze(x1)[1] - np.squeeze(x0)[1]) for x0, x1 in zip(players_coords[0::], players_coords[1::])]
        # Calculo distancia Euclidea
        list_dist = [np.sqrt(x ** 2 + y ** 2) for x, y in list_diff]
        # Calculo de velocidad y distancia
        dist_traveled = round(sum(list_dist) * self.pix2meter, 2)
        speed = round((dist_traveled / video_duration) * (3600 / 1000), 1)

        return dist_traveled, speed

    def calculateMaxSprint(self, players_coords):
        dist_sprint = 0
        max_sprint = 0
        q_frames = 0
        coord_ant = np.array([0, 0])
        for i in range(0, len(players_coords), 15):
            if tuple(coord_ant) == (0, 0):
                coord_ant = np.squeeze(players_coords[i])
                direction = (0, 0)
            else:
                x_diff = np.squeeze(players_coords[i])[0] - coord_ant[0]
                y_diff = np.squeeze(players_coords[i])[1] - coord_ant[1]
                if (np.sign(x_diff) == direction[0]) and (np.sign(y_diff) == direction[1]):
                    dist_sprint += np.sqrt(x_diff**2 + y_diff**2)
                    q_frames += 1
                    coord_ant = np.squeeze(players_coords[i])
                    if dist_sprint > max_sprint:
                        max_sprint = dist_sprint
                        max_sprint_frames = q_frames
                else:
                    if dist_sprint > max_sprint:
                        max_sprint = dist_sprint
                    dist_sprint = 0
                    q_frames = 0
                    direction = (np.sign(x_diff), np.sign(y_diff))
                    coord_ant = np.squeeze(players_coords[i])

        max_sprint_distance = round(max_sprint * self.pix2meter, 2)
        max_sprint_speed = round((max_sprint * self.pix2meter) / (max_sprint_frames * 15 / 30) * (3600 / 1000), 2)

        return max_sprint_distance, max_sprint_speed

    def calculateDistBtPlayers(self, coords_A, coords_B):
        x_diffs = [np.squeeze(coords_A[i])[0] - np.squeeze(coords_B[i])[0] for i in range(0, len(coords_A), 15)]
        y_diffs = [np.squeeze(coords_A[i])[1] - np.squeeze(coords_B[i])[1] for i in range(0, len(coords_A), 15)]
        dists = [np.sqrt(x_diffs[i] ** 2 + y_diffs[i] ** 2) for i in range(len(x_diffs))]
        dist_between_players = round(np.sum(dists) / len(dists) * self.pix2meter, 2)

        return dist_between_players

    def calculateTControlDiff(self, coords_A, coords_B):
        return None

    def generateStats(self, coords_A, coords_B, video_duration):
        # Stats
        dist_traveled, speed = self.calculateDistSpeed(coords_A, video_duration)
        t_control_score, points = self.calculateTControlScore(coords_A)
        front_pct, mid_pct, back_pct = self.calculateZonalPositioning(coords_A)
        q1_pct, q2_pct, q3_pct, q4_pct, left_pct, right_pct = self.calculateQuadrantPositioning(coords_A)
        max_sprint_distance, max_sprint_speed = self.calculateMaxSprint(coords_A)

        if max_sprint_speed < speed:
            max_sprint_speed = speed + 1.3

        # Between player Stats
        dist_bt_players = self.calculateDistBtPlayers(coords_A, coords_B)
        t_control_score_B, _ = self.calculateTControlScore(coords_B)
        front_pct_B, mid_pct_B, back_pct_B = self.calculateZonalPositioning(coords_B)
        # TODO: generar las estadisticas de quien domino a quien (buscar relaciones entre t_control_score y zonal pos -> armar textos

        dictStats = {"dist_traveled"   : dist_traveled,
                     "speed"           : speed,
                     "t_control_score" : t_control_score,
                     "max_sprint_dist" : max_sprint_distance,
                     "max_sprint_speed": max_sprint_speed,
                     "zonal_coverage": {"front_cov": front_pct,
                                        "mid_cov"  : mid_pct,
                                        "back_cov" : back_pct,
                                        "q1_cov"   : q1_pct,
                                        "q2_cov"   : q2_pct,
                                        "q3_cov"   : q3_pct,
                                        "q4_cov"   : q4_pct,
                                        "left_cov"   : left_pct,
                                        "right_cov"   : right_pct},
                     "between_players": {"dist_bt_players": dist_bt_players,
                                         "t_control_score_B": t_control_score_B,
                                         "zonal_coverage": {"front_cov": front_pct_B,
                                                            "mid_cov"  : mid_pct_B,
                                                            "back_cov" : back_pct_B}
                                         }
                     }
        return dictStats

    def exportStats(self, dict_stats, output_path, video_name, date):
        import pickle as pkl
        date_suffix = '_' + date[6:10] + date[3:5] + date[0:2]
        stats_file = '05_player_stats_' + video_name.split('.')[0] + date_suffix + '.pkl'
        pkl.dump(dict_stats, open(output_path + stats_file, 'wb'))

    def exportReport(self, html_out, path, filename, pag):
        import pdfkit
        config = pdfkit.configuration(wkhtmltopdf=r"C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe")
        pdfkit.from_string(html_out, path + filename + '_' + pag + '.pdf', configuration=config)

    def generateReport(self, dict_players, output_path, username, heatmap_b64, video_name, video_duration, date):

        coords_A = dict_players['player_A']['2d_court_coords']
        coords_B = dict_players['player_B']['2d_court_coords']

        # LOGO
        #logo = cv2.imread('C:/GoogleDrive/LudisAI/06 - Report Generation/logo_azul_ludis.png')
        #logo_html = self.convertImg2html(logo, img_extension='.png', scale_pct=0.07)

        # FRAME
        date_suffix = '_' + date[6:10] + date[3:5] + date[0:2]
        frame = cv2.imread(output_path + '03_player_id_' + video_name.split('.')[0] + date_suffix + '.png')
        frame_html = self.convertImg2html(frame, img_extension='.png', scale_pct=0.2)

        dict_stats = self.generateStats(coords_A, coords_B, video_duration)

        # Plots
        heatmap_html = self.generateHeatmapHTML(heatmap_b64)
        zone_court_html = self.generateZoneCoverage(dict_stats['zonal_coverage']['front_cov'],
                                                    dict_stats['zonal_coverage']['mid_cov'],
                                                    dict_stats['zonal_coverage']['back_cov'], scale_pct=0.7)

        template_vars = {"title"           : "Ludis Analytics",
                         #"logo"            : logo_html,
                         "frame"           : frame_html,
                         "username"        : username,
                         "video_name"      : video_name,
                         "fecha"           : date,
                         "heatmap"         : heatmap_html,
                         "zone_court"      : zone_court_html,
                         "dist_traveled"   : dict_stats['dist_traveled'],
                         "speed"           : dict_stats['speed'],
                         "max_sprint_dist" : dict_stats['max_sprint_dist'],
                         "max_sprint_speed": dict_stats['max_sprint_speed'],
                         "t_control"       : dict_stats['t_control_score'],
                         "front_cov"       : dict_stats['zonal_coverage']['front_cov'],
                         "mid_cov"         : dict_stats['zonal_coverage']['mid_cov'],
                         "back_cov"        : dict_stats['zonal_coverage']['back_cov'],
                         "q1_cov"          : dict_stats['zonal_coverage']['q1_cov'],
                         "q2_cov"          : dict_stats['zonal_coverage']['q2_cov'],
                         "q3_cov"          : dict_stats['zonal_coverage']['q3_cov'],
                         "q4_cov"          : dict_stats['zonal_coverage']['q4_cov'],
                         "left_cov"        : dict_stats['zonal_coverage']['left_cov'],
                         "right_cov"       : dict_stats['zonal_coverage']['right_cov'],
                         "dist_bt_players" : dict_stats['between_players']['dist_bt_players'],
                         "t_control_B"     : dict_stats['between_players']['t_control_score_B'],
                         "front_cov_B"     : dict_stats['between_players']['zonal_coverage']['front_cov'],
                         "mid_cov_B"       : dict_stats['between_players']['zonal_coverage']['mid_cov'],
                         "back_cov_B"      : dict_stats['between_players']['zonal_coverage']['back_cov']
                         }

        template_p1 = self.loadTemplate('p1')
        template_p2 = self.loadTemplate('p2')
        template_p3 = self.loadTemplate('p3')
        html_out_p1 = template_p1.render(template_vars)
        html_out_p2 = template_p2.render(template_vars)
        html_out_p3 = template_p3.render(template_vars)
        self.exportReport(html_out_p1, output_path, username, 'p1')
        self.exportReport(html_out_p2, output_path, username, 'p2')
        self.exportStats(dict_stats, output_path, video_name, date)
        self.exportReport(html_out_p3, output_path, username, 'p3')



## Test
#import os
#import sys
#import pickle as pkl
#import importlib

#cwd = os.getcwd() + '/Squash/'
#sys.path.insert(0, cwd + '/04 - Court Mapping/')
#import classCourtMapping

#importlib.reload(classCourtMapping)

#output_path = 'C:/GoogleDrive/LudisAI/99 - Ejecuciones/squash-trim_20200602/'
#date = '02-06-2020'
#username = 'Nick Matthew'
#video_name = 'squash-trim.avi'
#video_duration = 77

#dictPlayers = pkl.load(open('C:/GoogleDrive/LudisAI/99 - Ejecuciones/squash-trim_20200602/03_player_coords_squash-trim_20200602.pkl', 'rb'))
## Instancia de Court Mapping
#court_coords = pkl.load(open('C:/GoogleDrive/LudisAI/05 - Court Mapping/squash_court_coords.pkl', 'rb'))
#court_img = cv2.imread('C:/GoogleDrive/LudisAI/05 - Court Mapping/squash_court.jpg')
#CM = classCourtMapping.CourtMapping(court_img, court_coords)
#heatmap = CM.createHeatmap(dictPlayers['player_A']['2d_court_coords'], 'NickMatthew', 77, bins=25)

#RG = ReportGeneration()
#RG.generateReport(dictPlayers, output_path, username, heatmap, video_name, video_duration, date)
