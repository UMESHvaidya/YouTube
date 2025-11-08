import os
import numpy as np
from moviepy.editor import VideoFileClip, CompositeVideoClip, ImageClip
import logging
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Define directories
INPUT_DIR = "/Users/umesh/Developer/projects/youtube/data/input_videos/wisdom_waves"
OUTPUT_DIR = "/Users/umesh/Developer/projects/youtube/data/output_videos/wisdom_waves"

# Watermark image paths
WATERMARK_PATH = "/Users/umesh/Developer/projects/youtube/assets/watermark/logo.png"
WATERMARK_PATH_LAST_5SEC = "/Users/umesh/Developer/projects/youtube/assets/watermark/subscribe.png"

# Watermark positions
WATERMARK_X = 515
WATERMARK_Y = 330

WATERMARK_X_LAST_5SEC = 235
WATERMARK_Y_LAST_5SEC = 960

# Threading configuration
MAX_WORKERS = min(multiprocessing.cpu_count(), 4)  # Limit to prevent memory issues
THREAD_LOCK = threading.Lock()  # For thread-safe logging

# Ensure output directory exists
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)


def thread_safe_log(message, level="info"):
    """Thread-safe logging function."""
    with THREAD_LOCK:
        if level == "info":
            logging.info(message)
        elif level == "error":
            logging.error(message)
        elif level == "warning":
            logging.warning(message)


def add_watermark_to_video(
    input_path, output_path,
    watermark_path_main, watermark_path_last5,
    x_pos=400, y_pos=300,
    x_pos_last=50, y_pos_last=50
):
    """Add PNG watermark to video at specified coordinates, with different watermark for last 5 seconds."""
    try:
        # Load video
        video = VideoFileClip(input_path)
        duration = video.duration

        # Check watermark files
        if not os.path.exists(watermark_path_main):
            thread_safe_log(f"Main watermark file not found: {watermark_path_main}", "error")
            return False
        if not os.path.exists(watermark_path_last5):
            thread_safe_log(f"Last 5 sec watermark not found: {watermark_path_last5}, using main instead.", "warning")
            watermark_path_last5 = watermark_path_main

        # Main watermark for entire video
        watermark_main = ImageClip(watermark_path_main, duration=duration)
        watermark_main = watermark_main.set_position((x_pos, y_pos))

        if duration <= 5:
            # If video is <=5s, use both watermarks throughout
            watermark_last = ImageClip(watermark_path_last5, duration=duration)
            watermark_last = watermark_last.set_position((x_pos_last, y_pos_last))
            final_video = CompositeVideoClip([video, watermark_main, watermark_last])
        else:
            # Use different watermark for last 5 seconds
            watermark_last = ImageClip(watermark_path_last5, duration=5)
            watermark_last = watermark_last.set_position((x_pos_last, y_pos_last)).set_start(duration - 5)
            final_video = CompositeVideoClip([video, watermark_main, watermark_last])


        # Write output video
        final_video.write_videofile(
            output_path,
            codec="libx264",
            audio_codec="aac",
            fps=video.fps,
            preset="medium",
            ffmpeg_params=["-crf", "23"],
            verbose=False,  # Reduce output clutter in multithreaded mode
            logger=None    # Disable moviepy logging to prevent conflicts
        )

        # Clean up
        video.close()
        watermark_main.close()
        watermark_last.close()
        final_video.close()

        thread_safe_log(f"✅ Watermarks added to: {os.path.basename(input_path)}")
        thread_safe_log(f"📍 Main watermark: ({x_pos}, {y_pos})")
        thread_safe_log(f"📍 Last 5 sec watermark: ({x_pos_last}, {y_pos_last})")
        return True

    except Exception as e:
        thread_safe_log(f"❌ Error processing {os.path.basename(input_path)}: {str(e)}", "error")

        # Clean up on error
        if "video" in locals():
            try:
                video.close()
            except:
                pass
        if "watermark_main" in locals():
            try:
                watermark_main.close()
            except:
                pass
        if "watermark_last" in locals():
            try:
                watermark_last.close()
            except:
                pass
        if "final_video" in locals():
            try:
                final_video.close()
            except:
                pass
        return False


def process_single_video_threaded(video_info):
    """Process a single video file - thread-safe version."""
    input_file, output_file, progress_info = video_info
    
    if not os.path.exists(input_file):
        thread_safe_log(f"Input file not found: {input_file}", "error")
        return False, input_file

    thread_safe_log(f"🎬 Processing {progress_info}: {os.path.basename(input_file)}")
    
    start_time = time.time()
    success = add_watermark_to_video(
        input_file,
        output_file,
        WATERMARK_PATH,
        WATERMARK_PATH_LAST_5SEC,
        WATERMARK_X,
        WATERMARK_Y,
        WATERMARK_X_LAST_5SEC,
        WATERMARK_Y_LAST_5SEC
    )
    
    processing_time = time.time() - start_time
    if success:
        thread_safe_log(f"✅ Completed {os.path.basename(input_file)} in {processing_time:.1f}s")
    else:
        thread_safe_log(f"❌ Failed {os.path.basename(input_file)}", "error")
    
    return success, input_file


def process_single_video(input_file, output_file=None):
    """Process a single video file - non-threaded version."""
    if not os.path.exists(input_file):
        logging.error(f"Input file not found: {input_file}")
        return False

    if output_file is None:
        filename = os.path.basename(input_file)
        name, ext = os.path.splitext(filename)
        output_file = os.path.join(OUTPUT_DIR, f"{name}_watermarked{ext}")

    return add_watermark_to_video(
        input_file,
        output_file,
        WATERMARK_PATH,
        WATERMARK_PATH_LAST_5SEC,
        WATERMARK_X,
        WATERMARK_Y,
        WATERMARK_X_LAST_5SEC,
        WATERMARK_Y_LAST_5SEC
    )


def process_directory():
    """Process all videos in the input directory with multithreading."""
    supported_extensions = [".mp4", ".avi", ".mov", ".mkv", ".m4v"]

    if not os.path.exists(INPUT_DIR):
        logging.error(f"Input directory not found: {INPUT_DIR}")
        return

    video_files = [
        f for f in os.listdir(INPUT_DIR)
        if any(f.lower().endswith(ext) for ext in supported_extensions)
    ]

    if not video_files:
        logging.warning("No videos found in input directory.")
        return

    logging.info(f"🎬 WATERMARK ADDER - MULTITHREADED MODE")
    logging.info(f"📁 Videos to process: {len(video_files)}")
    logging.info(f"🖼️ Main watermark: {WATERMARK_PATH}")
    logging.info(f"🖼️ Last 5 sec watermark: {WATERMARK_PATH_LAST_5SEC}")
    logging.info(f"📍 Main watermark: ({WATERMARK_X}, {WATERMARK_Y})")
    logging.info(f"📍 Last 5 sec watermark: ({WATERMARK_X_LAST_5SEC}, {WATERMARK_Y_LAST_5SEC})")
    logging.info(f"🧵 Max workers: {MAX_WORKERS}")
    logging.info(f"🚀 Starting processing...")

    # Prepare video processing tasks
    video_tasks = []
    for i, filename in enumerate(video_files, 1):
        input_path = os.path.join(INPUT_DIR, filename)
        name, ext = os.path.splitext(filename)
        output_path = os.path.join(OUTPUT_DIR, f"{name}_watermarked{ext}")
        progress_info = f"{i}/{len(video_files)}"
        video_tasks.append((input_path, output_path, progress_info))

    success_count = 0
    failed_files = []
    start_time = time.time()

    # Process videos using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all tasks
        future_to_video = {
            executor.submit(process_single_video_threaded, task): task[0] 
            for task in video_tasks
        }
        
        # Process completed tasks
        for future in as_completed(future_to_video):
            try:
                success, input_file = future.result()
                if success:
                    success_count += 1
                else:
                    failed_files.append(os.path.basename(input_file))
            except Exception as exc:
                input_file = future_to_video[future]
                thread_safe_log(f"Video {os.path.basename(input_file)} generated an exception: {exc}", "error")
                failed_files.append(os.path.basename(input_file))

    # Summary
    total_time = (time.time() - start_time) / 60  # minutes
    logging.info(f"\n🎉 MULTITHREADED PROCESSING COMPLETE!")
    logging.info(f"✅ Successful: {success_count}/{len(video_files)}")
    logging.info(f"❌ Failed: {len(failed_files)}/{len(video_files)}")
    if failed_files:
        logging.info(f"Failed files: {', '.join(failed_files)}")
    logging.info(f"⏱️ Total time: {total_time:.1f} minutes")
    if len(video_files) > 0:
        logging.info(f"🚀 Average time per video: {(total_time * 60) / len(video_files):.1f} seconds")


def process_directory_single_threaded():
    """Process all videos in the input directory - single threaded (original method)."""
    supported_extensions = [".mp4", ".avi", ".mov", ".mkv", ".m4v"]

    if not os.path.exists(INPUT_DIR):
        logging.error(f"Input directory not found: {INPUT_DIR}")
        return

    video_files = [
        f for f in os.listdir(INPUT_DIR)
        if any(f.lower().endswith(ext) for ext in supported_extensions)
    ]

    if not video_files:
        logging.warning("No videos found in input directory.")
        return

    logging.info(f"🎬 WATERMARK ADDER - SINGLE THREADED MODE")
    logging.info(f"📁 Videos to process: {len(video_files)}")
    logging.info(f"🖼️ Main watermark: {WATERMARK_PATH}")
    logging.info(f"🖼️ Last 5 sec watermark: {WATERMARK_PATH_LAST_5SEC}")
    logging.info(f"📍 Main watermark: ({WATERMARK_X}, {WATERMARK_Y})")
    logging.info(f"📍 Last 5 sec watermark: ({WATERMARK_X_LAST_5SEC}, {WATERMARK_Y_LAST_5SEC})")

    success_count = 0
    start_time = time.time()

    for i, filename in enumerate(video_files, 1):
        input_path = os.path.join(INPUT_DIR, filename)
        name, ext = os.path.splitext(filename)
        output_path = os.path.join(OUTPUT_DIR, f"{name}_watermarked{ext}")

        logging.info(f"Processing {i}/{len(video_files)}: {filename}")

        if add_watermark_to_video(
            input_path,
            output_path,
            WATERMARK_PATH,
            WATERMARK_PATH_LAST_5SEC,
            WATERMARK_X,
            WATERMARK_Y,
            WATERMARK_X_LAST_5SEC,
            WATERMARK_Y_LAST_5SEC
        ):
            success_count += 1

    total_time = (time.time() - start_time) / 60
    logging.info(f"\n🎉 SINGLE THREADED PROCESSING COMPLETE!")
    logging.info(f"✅ Successful: {success_count}/{len(video_files)}")
    logging.info(f"⏱️ Total time: {total_time:.1f} minutes")


def main():
    print("🎬 WATERMARK ADDER")
    print(f"🖼️ Main Watermark: {WATERMARK_PATH}")
    print(f"🖼️ Last 5s Watermark: {WATERMARK_PATH_LAST_5SEC}")
    print(f"📍 Main position: ({WATERMARK_X}, {WATERMARK_Y})")
    print(f"📍 Last 5s position: ({WATERMARK_X_LAST_5SEC}, {WATERMARK_Y_LAST_5SEC})")
    print(f"📁 Input: {INPUT_DIR}")
    print(f"📁 Output: {OUTPUT_DIR}")
    print(f"🧵 Available CPU cores: {multiprocessing.cpu_count()}")
    print(f"🧵 Max workers for threading: {MAX_WORKERS}\n")

    choice = input("Choose option:\n1. Process ALL videos (MULTITHREADED - Fast)\n2. Process ALL videos (SINGLE THREADED - Safe)\n3. Process SINGLE video\nEnter choice (1, 2, or 3): ").strip()

    if choice == "1":
        print("\n🚀 Processing ALL videos with MULTITHREADING...")
        process_directory()
    elif choice == "2":
        print("\n🐌 Processing ALL videos with SINGLE THREADING...")
        process_directory_single_threaded()
    elif choice == "3":
        video_path = input("Enter video file path: ").strip()

        # Remove surrounding quotes
        video_path = video_path.strip('"').strip("'")

        if not video_path:
            print("❌ No path provided. Exiting.")
            return

        if process_single_video(video_path):
            print("✅ Watermarks added successfully!")
        else:
            print("❌ Failed to add watermarks.")
    else:
        print("❌ Invalid choice. Exiting.")


if __name__ == "__main__":
    main()