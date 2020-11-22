#####################################################################################################################
#
# Purpose: Detectar e Identificar a los jugadores
#
#   Inputs:
#       Video
#
#   Outputs:
#       Prediccion
# TODO: Poner nombre del jugador en drawPlayerPosition() arriba del circulo
#######################################################################################################################

import cv2 as cv2
import numpy as np
import imutils
import matplotlib.pyplot as plt
import io
import base64

class CourtMapping:
    def __init__(self, court_img, dst_pts):
        self.court_img = court_img
        self.dst_pts = dst_pts

    def initVideoWriter(self, video_name, output_path, output_video_name, video_FourCC, video_fps):
        """
        Inicializa el streaming del video.

        :param video: video a procesar (path completo + nombre de archivo)
        :param output_path: path donde guardar el video procesado

        :return:
            cap: instancia de streaming de video
            video_frames: entero con el numero total de frames del video
            writer: si existe "output_path", devuelve instancia para guardar el video procesado
        """
        output_video = '04_court_mapping_' + output_video_name + '.' + video_name.split('.')[1]
        writer = cv2.VideoWriter(output_path + output_video, video_FourCC, video_fps,
                                 (self.court_img.shape[1], self.court_img.shape[0]), True)

        return writer

    def drawPlayerPosition(self, im, player_pos):

        cv2.circle(im, player_pos, radius=3, color=[255,0,0], thickness=3)

        return im

    def homographyTransform(self, img_src, src_pts, img_dst, dst_pts):
        src_pts = np.array(src_pts)
        h, status = cv2.findHomography(src_pts, dst_pts)
        img_out = cv2.warpPerspective(img_src, h, (img_dst.shape[1], img_dst.shape[0]))

        return img_out


    def getPlayersMask(self, im):
        lower_range = np.array([255, 0, 0])  # Set the Lower range value of blue in BGR
        upper_range = np.array([255, 155, 155])  # Set the Upper range value of blue in BGR
        mask = cv2.inRange(im, lower_range, upper_range)  # Create a mask with range
        result = cv2.bitwise_and(im, im, mask=mask)  # Performing bitwise and operation with mask in img variable

        return cv2.inRange(result, lower_range, upper_range)


    def drawPlayersOnCourt(self, im, coord, player, radius=10):
        for pos in coord:
            center_coordinates = (pos[0], pos[1])

            if player == 'player_A':
                color = [185, 16, 67]
            elif player == 'player_B':
                color = [24, 24, 230]

            cv2.circle(im, center_coordinates, radius, color, thickness=-1)

        return im

    def mapPlayer2Court(self, frame, src_pts, dictPlayers):

        court = self.court_img.copy()
        result = court.copy()

        for player in ['player_A', 'player_B']:
            # Generate copy of frame to track only one player at a time
            frame_mapping = frame.copy()
            # Draw player position on frame
            frame_mapping = self.drawPlayerPosition(frame_mapping, dictPlayers[player]['player_coords'][-1])
            # Apply homography transform
            frame_out = self.homographyTransform(frame_mapping, src_pts, court, self.dst_pts)
            # Get frame mask
            mask = self.getPlayersMask(frame_out)

            # Get the contours from the players position coordinates
            cnts = cv2.findContours(mask.copy(), cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
            cnts = imutils.grab_contours(cnts)

            if cnts is not None:
                for cnt in cnts:
                    result = self.drawPlayersOnCourt(court, cnt[0], player)
                    dictPlayers[player]['2d_court_coords'].append(cnt[0])

        return result

    def fig2base64(self, fig):
        img = io.BytesIO()
        fig.savefig(img, format='png', bbox_inches='tight')
        img.seek(0)

        return base64.b64encode(img.getvalue())

    def createHeatmap(self, coords, player, video_duration, bins=25, export=False):

        ## Heatmap plot
        court = self.court_img.copy()
        pos_x = [np.squeeze(pos)[0] for pos in coords]
        pos_y = [np.squeeze(pos)[1] for pos in coords]
        heatmap, xedges, yedges = np.histogram2d(pos_x, pos_y, bins=bins, normed=True)
        extent = [0, court.shape[1], court.shape[0], 0]

        fig, ax = plt.subplots(1, 1, figsize=(10, 10))
        ax.set_xlim(0, court.shape[1])
        ax.set_ylim(court.shape[0], 0)

        ax.imshow(court[:, :, [2, 1, 0]])
        ax.imshow(heatmap.T, cmap='hot_r', alpha=0.8, interpolation='gaussian', extent=extent)

        plt.axis('off')
        # plt.title(f'Heatmap: Player {idx}',fontsize=15)
        plt.title(f'Heatmap: {player}', fontsize=15)
        #plt.annotate(f'Court Coverage: {distTraveled} meters', (.19, .1), xycoords='figure fraction', fontsize=12)
        #plt.annotate(f'Speed: {speed} km/h', (.19, .08), xycoords='figure fraction', fontsize=12)
        #plt.show()

        # T-Control Ellipses
        #from matplotlib.patches import Ellipse

        #ellipse_1 = Ellipse(xy=(208, 350), width=40, height=55, fill=False)
        #ellipse_2 = Ellipse(xy=(208, 350), width=90, height=115, fill=False)
        #ellipse_3 = Ellipse(xy=(208, 350), width=165, height=200,fill=False)

        #ax.add_patch(ellipse_1)
        #ax.add_patch(ellipse_2)
        #ax.add_patch(ellipse_3)

        encoded_heatmap = self.fig2base64(fig)

        return encoded_heatmap