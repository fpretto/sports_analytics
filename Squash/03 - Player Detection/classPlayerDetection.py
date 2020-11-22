#####################################################################################################################
#
# Purpose: Detectar e Identificar a los jugadores
#
#   Inputs:
#       Video
#
#   Outputs:
#       Prediccion
# TODO: Ver forma de trackear los jugadores cuando tienen remera del mismo color
# TODO: Color Segmentation - identificar color desde un frame aleatorio (no necesariamente del 1ro), verificando que los bounding box esten separados (IoU)
#######################################################################################################################

from shapely.geometry import box, Point
from ffpyplayer.player import MediaPlayer
import random
import numpy as np
import importlib
import pickle as pkl
import cv2
import sys

sys.path.insert(0, 'C:/Repo/Ludis/Squash/03 - Player Detection/')
import YOLOv3 as yolo3
importlib.reload(yolo3)

class PlayerDetection:
    def __init__(self, model):
        self.yolo_model = model
        self.net_h = 416
        self.net_w = 416
        self.obj_thresh = 0.5
        self.nms_thresh = 0.45
        self.anchors = [[116,90,  156,198,  373,326],  [30,61, 62,45,  59,119], [10,13,  16,30,  33,23]]
        self.labels = ["person"]

    def initVideoCapture(self, video_path, video_name, output_path=None, output_video_name=None):
        """
        Inicializa el streaming del video.

        :param video: video a procesar (path completo + nombre de archivo)
        :param output_path: path donde guardar el video procesado

        :return:
            cap: instancia de streaming de video
            video_frames: entero con el numero total de frames del video
            writer: si existe "output_path", devuelve instancia para guardar el video procesado
        """
        # Initialize the video stream
        cap = cv2.VideoCapture(video_path+video_name)

        video_FourCC   = int(cap.get(cv2.CAP_PROP_FOURCC))
        video_fps      = int(cap.get(cv2.CAP_PROP_FPS))
        video_frames   = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        video_duration = video_frames / video_fps

        if output_path != None:
            video_size = (int(cap.get(cv2.CAP_PROP_FRAME_WIDTH)), int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT)))
            output_video = '03_tracking_' + output_video_name + '.' + video_name.split('.')[1]
            writer = cv2.VideoWriter(output_path + output_video, video_FourCC, video_fps, video_size)
        else:
            writer = None

        return cap, video_frames, video_fps, video_duration, writer

    def resizeFrame(self, frame, scale_pct):
        """
        Escala el tamaño del frame en un porcentaje scale_pct

        :param frame: frame a escalar
        :param scale_pct: int con el porcentaje a escalar
        :return:
            frame escalado
        """
        width = int(frame.shape[1] * scale_pct / 100)
        height = int(frame.shape[0] * scale_pct / 100)
        dim = (width, height)
        resizedFrame = cv2.resize(frame, dim, interpolation=cv2.INTER_AREA)

        return resizedFrame

    def predictYOLOboxes(self, frame):

        # Resize frames to match net dims
        frame_h, frame_w, _ = frame.shape
        new_frame = yolo3.preprocess_input(frame, self.net_h, self.net_w)

        # Predict YOLO boxes
        yolos = self.yolo_model.predict(new_frame)
        boxes = []

        for i in range(len(yolos)):
            # Decode the output of the network
            boxes += yolo3.decode_netout(yolos[i][0], self.anchors[i], self.obj_thresh,
                                         self.nms_thresh, self.net_h, self.net_w)

        # Correct the sizes of the bounding boxes
        yolo3.correct_yolo_boxes(boxes, frame_h, frame_w, self.net_h, self.net_w)

        # Suppress non-maximal boxes
        yolo3.do_nms(boxes, self.nms_thresh)

        return boxes

    def getBoxPolygon(self, tracking_coords):

        x_min = tracking_coords[0][1][0] # Coordenada X del punto mid-left
        y_min = tracking_coords[0][5][1] # Coordenada Y del punto upper-center
        x_max = tracking_coords[0][3][0] # Coordenada X del punto mid-right
        y_max = tracking_coords[0][0][1] # Coordenada Y del punto lower-center

        player_box = box(x_min, y_min, x_max, y_max)

        return player_box

    def clickEvent(self, event, x, y, flags, param):
        global seleccion, selectOK
        if event == cv2.EVENT_LBUTTONDOWN:
            seleccion = (x, y)

            if Point(seleccion).within(boxPoly_A):
                dictPl['player_A']['label'] = user
                selectOK = 1

            elif Point(seleccion).within(boxPoly_B):
                dictPl['player_B']['label'] = user
                selectOK = 1

    def identifyPlayers(self, video_path, video_name, dictPlayers, username, src_pts, output_path, output_video_name):
        global user, dictPl, boxPoly_A, boxPoly_B
        user = username
        dictPl = dictPlayers

        cap, _, _, _, _ = self.initVideoCapture(video_path, video_name)
        video_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        cap.set(1, int(video_frames * 0.25))  #random.randint(1, video_frames))
        grabbed, frame = cap.read()

        if grabbed:
            frame = self.resizeFrame(frame, scale_pct=50)

            # Predict YOLO boxes
            boxes = self.predictYOLOboxes(frame)

            # Draw bounding boxes on the image using labels
            frame = yolo3.draw_boxes(frame, 1, boxes, self.labels, self.obj_thresh, src_pts,
                                     dictPl, player=None)

            boxPoly_A = self.getBoxPolygon(dictPl['player_A']['tracking_coords'])
            boxPoly_B = self.getBoxPolygon(dictPl['player_B']['tracking_coords'])

            cv2.namedWindow("Player Identification")
            cv2.setMouseCallback("Player Identification", self.clickEvent)
            selectOK = 0

            while True:

                cv2.imshow("Player Identification", frame)

                key = cv2.waitKey(1) & 0xFF
                if (key == ord("c")) or (selectOK == 1):
                    break

            if output_path != None:
                cv2.imwrite(output_path + '03_player_id_' + output_video_name + '.png', frame)
            cv2.destroyAllWindows()

        return dictPl

    def mergeAudioandVideo(self):

        import ffmpeg

        input_video = ffmpeg.input('./test/test_video.webm')
        input_audio = ffmpeg.input('./test/test_audio.webm')

        ffmpeg.concat(input_video, input_audio, v=1, a=1).output('./processed_folder/finished_video.mp4').run()


    def detectPlayers(self, video_path, video_name, dictPlayers, username, src_pts, output_path, output_video_name, class_mapping):
        grabbed = True
        frame_id = 0

        cap, video_frames, video_fps, video_duration, writer = self.initVideoCapture(video_path, video_name,
                                                                          output_path=output_path,
                                                                          output_video_name=output_video_name)
        #player = MediaPlayer(video_path + video_name)

        writer_court_2d = class_mapping.initVideoWriter(video_name, output_path, output_video_name,
                                                        int(cap.get(cv2.CAP_PROP_FOURCC)), int(cap.get(cv2.CAP_PROP_FPS)))

        frames_list = list(range(1, video_frames, 1))
        # loop over frames from the video file stream (207)
        while grabbed:
            frame_n = frames_list[frame_id]
            #print(frame_n)
            # read the next frame from the file
            cap.set(1, frame_n)
            grabbed, frame = cap.read()
            #audio_frame, val = player.get_frame()

            if (grabbed) and (frame_n != frames_list[-1]):
                frame = self.resizeFrame(frame, scale_pct=50)

                # Predict YOLO boxes
                boxes = self.predictYOLOboxes(frame)

                # Draw bounding boxes on the image using labels
                frame = yolo3.draw_boxes(frame, frame_n, boxes, self.labels, self.obj_thresh, src_pts,
                                         dictPlayers, player=username)

                # Map player position to 2D court
                frame_court_2d = class_mapping.mapPlayer2Court(frame, src_pts, dictPlayers)

                # Write the output frame to disk
                if output_path != None:
                    frame = self.resizeFrame(frame, scale_pct=200)
                    writer.write(frame)
                    writer_court_2d.write(frame_court_2d)

                deciles = [int(x) for x in np.linspace(0, 1, 21) * frames_list[-1]]
                if frame_n in deciles:
                    print('Processed frames: {} de {} ({}%)'.format(frame_n, frames_list[-1],
                                                                    int(frame_n * 100 / frames_list[-1])))
                frame_id += 1

            else:
                grabbed = False

        if output_path != None:
            writer.release()
            writer_court_2d.release()

        cap.release()
        cv2.destroyAllWindows()

        # Save players coordinates dictionary
        if output_path != None:
            players_file = '03_player_coords_' + output_video_name + '.pkl'
            pkl.dump(dictPlayers, open(output_path + players_file, 'wb'))

        return dictPlayers, video_duration

