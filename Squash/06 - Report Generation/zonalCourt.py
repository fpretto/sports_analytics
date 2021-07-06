import cv2
import numpy as np

def create_image(width, height, rgb_color=(0, 0, 0)):
    """Create new image(numpy array) filled with certain color in RGB"""
    # Create black blank image
    image = np.zeros((height, width, 3), np.uint8)

    # Since OpenCV uses BGR, convert the color first
    color = tuple(reversed(rgb_color))
    # Fill image with color
    image[:] = color

    return image

court_img = cv2.imread('C:/GoogleDrive/LudisAI/05 - Court Mapping/squash_court.jpg')

cv2.rectangle(court_img, (0,0), (414, 164), (0, 215, 255), 2)
cv2.rectangle(court_img, (0,166), (414, 466), (50,205,50), 2)
cv2.rectangle(court_img, (0,468), (414, 656), (60,20,220), 2)

x, y, w, h = 0, 0, 414, 164
front_0 = court_img[y:y+h, x:x+w]
front_rect = create_image(front_0.shape[1], front_0.shape[0], rgb_color=(255, 215, 0))
front_court = cv2.addWeighted(front_0, 0.5, front_rect, 0.5, 1.0)
court_img[y:y+h, x:x+w] = front_court
cv2.putText(court_img, 'Front-Court', (155, 32), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (19,69,139), 2)
cv2.putText(court_img, 'Defend/Counter-attack Zone', (62, 54), cv2.FONT_HERSHEY_DUPLEX, 0.6, (19,69,139), 1)

x, y, w, h = 0, 166, 414, 300
mid_0 = court_img[y:y+h, x:x+w]
mid_rect = create_image(mid_0.shape[1], mid_0.shape[0], rgb_color=(50,205,50))
mid_court = cv2.addWeighted(mid_0, 0.5, mid_rect, 0.5, 1.0)
court_img[y:y+h, x:x+w] = mid_court
cv2.putText(court_img, 'Mid-Court', (163, 198), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0,100,0), 2)
cv2.putText(court_img, 'Attack Zone', (152, 220), cv2.FONT_HERSHEY_DUPLEX, 0.6, (0,100,0), 1)

x, y, w, h = 0, 468, 414, 190
back_0 = court_img[y:y+h, x:x+w]
back_rect = create_image(back_0.shape[1], back_0.shape[0], rgb_color=(220,20,60))
back_court = cv2.addWeighted(back_0, 0.5, back_rect, 0.5, 1.0)
court_img[y:y+h, x:x+w] = back_court
cv2.putText(court_img, 'Back-Court', (157, 500), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (25,25,112), 2)
cv2.putText(court_img, 'Defend/Rally Zone', (121, 522), cv2.FONT_HERSHEY_DUPLEX, 0.6, (25,25,112), 1)

cv2.imshow('Zonal Mode', court_img)
cv2.waitKey(0)
cv2.destroyAllWindows()


# Save
cv2.imwrite('C:/GoogleDrive/LudisAI/06 - Report Generation/squash_court_zones.png', court_img)