#####################################################################################################################
#
# Purpose: Predecir si el video dado como input es de Squash
#
#   Inputs:
#       input_video: full path of video
#       sport_clf: full path of h5 file for the classification model
#       labels: full path of pickle file for the labels
#       pct_sample: percentage of frames to sample from video and predict sport
#       output_path: full path for saving output
#
#   Outputs:
#       video_label: label predicted for the video
#       video_label_pct: probability associated for predicted label
#       output video: if output_path provided, saved video of predicted frames in output_path
#
#######################################################################################################################

from datetime import datetime
import numpy as np
import cv2

class SportClassifier:
    def __init__(self, sport_clf, labels, pct_sample=0.25, output_path=None):
        self.model  = sport_clf
        self.labels = labels
        self.pct_sample = pct_sample
        self.output_path = output_path

    def initVideoCapture(self, video, output_path=None, output_video_name=None):
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

        if output_path != None:
            video_size = (int(cap.get(cv2.CAP_PROP_FRAME_WIDTH)), int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT)))
            writer = cv2.VideoWriter(output_path + output_video_name, video_FourCC, video_fps, video_size)
        else:
            writer = None

        return cap, video_frames, writer

    def sampleFrames(self, video_frames, pct_sample=0.25):
        """
        Toma una muestra de frames del video para clasificar si es de squash o no. Si el video tiene más de 500 frames,
        se toman una muestra de 75. Caso contrario, se toma una muestra de {pct_sample} del total.

        :param video_frames: entero con el numero total de frames del video
        :param pct_sample: float con el porcentaje de frames del video a muestrear
        :return:
            frames_predict: lista con los números de frames sobre los cuales predecir
        """
        np.random.seed(1984)
        # Muestreo de Frames sobre los cuales predecir
        if video_frames > 500:
            frames_predict = np.random.choice(range(1, video_frames), size=75, replace=False)
        else:
            frames_predict = np.random.choice(range(1, video_frames), size=int(video_frames*pct_sample), replace=False)

        return frames_predict

    def preprocessFrame(self, frame):
        """
        Preprocesamiento de los frames para llevarlos a un input válido para el modelo entrenado

        :param frame: frame (captura del video) a procesar

        :return:
            frame: frame preprocesado
        """
        # Media de normalizacion utilizada en el modelo (ResNet50) para Mean Subtraction
        norm_mean = np.array([123.68, 116.779, 103.939][::1], dtype="float32")

        # Convert frame from BGR to RGB, resize the frame to a fixed 224x224, and then
        # perform mean subtraction
        frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        frame = cv2.resize(frame, (224, 224)).astype("float32")
        frame -= norm_mean

        return frame

    def predictSport(self, video):

        start_time = datetime.now()

        cap, video_frames, writer = self.initVideoCapture(video, output_path = self.output_path)
        frames_predict = self.sampleFrames(video_frames, pct_sample = self.pct_sample)

        pred_list = []
        count = 0

        # Loop over frames from the video file stream
        print("[INFO] Classifying video...")
        for frame2read in frames_predict:
            # Set video frame to read
            cap.set(1, frame2read)
            # Read the next frame from the file
            grabbed, frame = cap.read()
            # Preprocessing frame
            frame = self.preprocessFrame(frame)
            # Make predictions on the frame and then update the predictions list
            # expand_dims con axis=0 agrega la dimension channels_first
            preds = self.model.predict(np.expand_dims(frame, axis=0))[0]
            i = int(np.squeeze(preds) > 0.5)
            label = self.labels.classes_[i]
            pred_list.append(label)

            # Save prediction
            if self.output_path != None:
                text = "Deporte: {}".format(label)
                cv2.puText(frame, text, (35, 50), cv2.FONT_HERSHEY_SIMPLEX, fontScale=1.25, color=(0, 255, 0), thickness=2)
                writer.write(frame)

            ##### VER si queda o no TODO: ver como queda el logging
            deciles = [int(x) for x in np.linspace(0, 1, 11) * video_frames]
            if count in deciles:
                print('Processed frames: {} de {} ({}%)'.format(count, frames_predict, int(count*100/frames_predict)))
            count += 1

        # release the file pointers
        print("[INFO] Cleaning up...")
        writer.release()
        cap.release()

        print("[INFO] Averaging predictions...")
        video_label_pct = int(sum([x == 'squash' for x in pred_list])/len(pred_list)*100)

        if video_label_pct > 50:
            video_label = 'Squash'
            print('El video es de Squash (prob: {}% )'.format(video_label_pct))
        else:
            video_label = 'No Squash'
            print('El video es no de Squash (prob: {}% )'.format(video_label_pct))
        ### Agregar todos los prints al log
        duration = datetime.now() - start_time
        print('Duracion: ', duration)

        return video_label, video_label_pct