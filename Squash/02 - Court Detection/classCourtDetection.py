#####################################################################################################################
#
# Purpose: Delimitar la cancha de squash en el video
#
#   Inputs:
#       Video
#
#   Outputs:
#       Prediccion
#TODO: una vez que el usuario selecciona los puntos, validar q el poligono (o su aproximacion de convex hull) sea convexo
#TODO: Validar tambien que el area del poligono se corresponda con la de una cancha de squash (si se puede)
#######################################################################################################################

import cv2
import numpy as np
import pickle as pkl

class CourtDetection:
    def __init__(self, src_pts = None):
        self.src_pts = src_pts

    def initVideoCapture(self, video: str, output_path=None, output_video_name=None):
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
        cap = cv2.VideoCapture(video)

        video_FourCC   = int(cap.get(cv2.CAP_PROP_FOURCC))
        video_fps      = int(cap.get(cv2.CAP_PROP_FPS))
        video_frames   = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

        cap.set(1, int(video_frames * 0.25))

        if output_path != None:
            video_size = (int(cap.get(cv2.CAP_PROP_FRAME_WIDTH)), int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT)))
            writer = cv2.VideoWriter(output_path + output_video_name, video_FourCC, video_fps, video_size)
        else:
            writer = None

        return cap, video_frames, writer

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

    def clickEvent(self, event, x, y, flags, param):
        global im_poly
        if event == cv2.EVENT_LBUTTONDOWN:
            if len(self.src_pts) < 7: # Si ya se marcaron 7 puntos, no se capturan mas coordenadas
                # Se marca un circulo en la coordenada clickeada y se añade a la lista
                cv2.circle(img, center=(x, y), radius=3, color=(0, 255, 0), thickness=-1)
                self.src_pts.append((x, y))

                if len(self.src_pts) == 1:
                    cv2.imshow('Court Delimitation', img)

                # Si hay mas de un punto, va uniendo cada uno con el punto anterior de la lista
                if len(self.src_pts) >= 2:
                    cv2.line(img, pt1=self.src_pts[-1], pt2=self.src_pts[-2], color=(0, 255, 0), thickness=2)
                    cv2.imshow('Court Delimitation', img)

                # Si se marcaron los 7 puntos, se completa el poligono
                if len(self.src_pts) == 7:
                    im_poly = frame.copy()
                    cv2.polylines(im_poly, [np.array(self.src_pts)], isClosed=True, color=[0, 255, 255], thickness=2)
                    cv2.imshow('Court Delimitation', im_poly)

    def detectCourt(self, video_path, video_name, output_path=None, output_video_name=None):
        global frame, img

        cap, video_frames, writer = self.initVideoCapture(video_path + video_name)
        grabbed, frame = cap.read()

        frame = self.resizeFrame(frame, scale_pct=50)

        self.src_pts = []
        img = frame.copy()

        cv2.namedWindow("Court Delimitation")
        cv2.setMouseCallback("Court Delimitation", self.clickEvent)

        while True:

            if len(self.src_pts) < 7:
                # display the image and wait for a keypress
                cv2.imshow("Court Delimitation", img)

            key = cv2.waitKey(1) & 0xFF
            # if the 'r' key is pressed, reset the cropping region
            if key == ord("r"):
                img = frame.copy()
                self.src_pts = []
            # if the 'c' key is pressed, break from the loop
            elif key == ord("c"):
                # Write the output frame to disk
                if output_path != None:
                    cv2.imwrite(output_path + '02_court_' + output_video_name + '.png', im_poly)
                    pkl.dump(self.src_pts, open(output_path + '02_court_coords_' + output_video_name + '.pkl', 'wb'))
                break

        cap.release()
        cv2.destroyAllWindows()

        return self.src_pts


#path = 'C:/Users/PrettoF/Desktop/testLudis/'

#detector = CourtDetection()

#detector.detectCourt(path, 'squash-trim.avi')

# keep looping until the 'q' key is pressed
