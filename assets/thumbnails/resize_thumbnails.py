import os
import logging
from PIL import Image

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

THUMBNAIL_DIR = "/Users/umesh/Developer/projects/youtube/assets/thumbnails"
TARGET_WIDTH = 1080
TARGET_HEIGHT = 1920

def resize_thumbnails():
    """Resize all PNG files in THUMBNAIL_DIR to max 1080x1920, preserving aspect ratio."""
    if not os.path.exists(THUMBNAIL_DIR):
        logging.error(f"Directory not found: {THUMBNAIL_DIR}")
        return

    resized_count = 0
    for filename in os.listdir(THUMBNAIL_DIR):
        if filename.lower().endswith('.png'):
            filepath = os.path.join(THUMBNAIL_DIR, filename)
            try:
                with Image.open(filepath) as img:
                    original_size = img.size
                    img.thumbnail((TARGET_WIDTH, TARGET_HEIGHT), Image.Resampling.LANCZOS)
                    if img.size != original_size:
                        img.save(filepath)
                        resized_count += 1
                        logging.info(f"Resized {filename} from {original_size} to {img.size}")
            except Exception as e:
                logging.error(f"Failed to resize {filename}: {e}")

    logging.info(f"Completed resizing {resized_count} thumbnails")

if __name__ == "__main__":
    resize_thumbnails()
    