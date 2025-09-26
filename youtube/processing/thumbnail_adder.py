import os
import logging
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing
from moviepy.editor import VideoFileClip, CompositeVideoClip, ImageClip

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Define directories
INPUT_DIR = "/Users/umesh/Developer/projects/youtube/data/input_videos/wisdom_waves/progress"
OUTPUT_DIR = "/Users/umesh/Developer/projects/youtube/data/output_videos/wisdom_waves/upload"
THUMBNAIL_DIR = "/Users/umesh/Developer/projects/youtube/assets/thumbnails"  # Default thumbnail directory

# Thumbnail settings
THUMBNAIL_DURATION = 0.1  # Duration in seconds to show thumbnail at end
THUMBNAIL_FADE_DURATION = 0.1  # Fade in/out duration

# Threading configuration
MAX_WORKERS = min(multiprocessing.cpu_count(), 4)
THREAD_LOCK = threading.Lock()

# Ensure directories exist
for directory in [OUTPUT_DIR, THUMBNAIL_DIR]:
    if not os.path.exists(directory):
        os.makedirs(directory)


def thread_safe_log(message, level="info"):
    """Thread-safe logging function."""
    with THREAD_LOCK:
        if level == "info":
            logging.info(message)
        elif level == "error":
            logging.error(message)
        elif level == "warning":
            logging.warning(message)


def add_thumbnail_to_video(input_path, output_path, thumbnail_path, duration=3.0, fade_duration=0.5):
    """Add thumbnail to the end of video with optional fade effect."""
    try:
        # Load video
        video = VideoFileClip(input_path)
        video_duration = video.duration
        video_size = video.size  # Get video dimensions (width, height)

        # Check thumbnail file
        if not os.path.exists(thumbnail_path):
            thread_safe_log(f"Thumbnail file not found: {thumbnail_path}", "error")
            video.close()
            return False

        # Load and resize thumbnail to match video size
        thumbnail = ImageClip(thumbnail_path, duration=duration)
        thumbnail = thumbnail.resized(video_size)  # Resize to match video dimensions
        
        # Add fade in/out effect to thumbnail (with compatibility handling)
        if fade_duration > 0 and fade_duration < duration / 2:
            try:
                # Try different fade method names for compatibility
                if hasattr(thumbnail, 'fadein'):
                    thumbnail = thumbnail.fadein(fade_duration).fadeout(fade_duration)
                elif hasattr(thumbnail, 'fade_in'):
                    thumbnail = thumbnail.fade_in(fade_duration).fade_out(fade_duration)
                elif hasattr(thumbnail, 'crossfadein'):
                    thumbnail = thumbnail.crossfadein(fade_duration).crossfadeout(fade_duration)
                else:
                    thread_safe_log("Fade effects not available, using thumbnail without fade", "warning")
            except Exception as fade_error:
                thread_safe_log(f"Could not apply fade effect: {fade_error}, continuing without fade", "warning")

        # Position thumbnail at the end of video
        thumbnail = thumbnail.with_start(video_duration)

        # Create final composite video
        final_video = CompositeVideoClip([video, thumbnail], size=video_size)
        
        # Write output video
        final_video.write_videofile(
            output_path,
            codec="libx264",
            audio_codec="aac",
            fps=video.fps,
            preset="medium",
            ffmpeg_params=["-crf", "23"],
            verbose=False,
            logger=None
        )

        # Clean up
        video.close()
        thumbnail.close()
        final_video.close()

        thread_safe_log(f"✅ Thumbnail added to: {os.path.basename(input_path)}")
        thread_safe_log(f"📸 Thumbnail: {os.path.basename(thumbnail_path)} ({duration}s duration)")
        return True

    except Exception as e:
        thread_safe_log(f"❌ Error processing {os.path.basename(input_path)}: {str(e)}", "error")
        
        # Clean up on error
        try:
            if 'video' in locals():
                video.close()
            if 'thumbnail' in locals():
                thumbnail.close()
            if 'final_video' in locals():
                final_video.close()
        except:
            pass
        return False


def process_single_video_threaded(video_info):
    """Process a single video file - thread-safe version."""
    input_file, output_file, thumbnail_file, progress_info = video_info
    
    if not os.path.exists(input_file):
        thread_safe_log(f"Input file not found: {input_file}", "error")
        return False, input_file

    thread_safe_log(f"🎬 Processing {progress_info}: {os.path.basename(input_file)}")
    
    start_time = time.time()
    success = add_thumbnail_to_video(
        input_file, 
        output_file, 
        thumbnail_file, 
        THUMBNAIL_DURATION, 
        THUMBNAIL_FADE_DURATION
    )
    
    processing_time = time.time() - start_time
    if success:
        thread_safe_log(f"✅ Completed {os.path.basename(input_file)} in {processing_time:.1f}s")
    else:
        thread_safe_log(f"❌ Failed {os.path.basename(input_file)}", "error")
    
    return success, input_file


def process_single_video(input_file, thumbnail_file, output_file=None):
    """Process a single video file - non-threaded version."""
    if not os.path.exists(input_file):
        logging.error(f"Input file not found: {input_file}")
        return False
    
    if not os.path.exists(thumbnail_file):
        logging.error(f"Thumbnail file not found: {thumbnail_file}")
        return False

    if output_file is None:
        filename = os.path.basename(input_file)
        name, ext = os.path.splitext(filename)
        output_file = os.path.join(OUTPUT_DIR, f"{name}_with_thumbnail{ext}")

    return add_thumbnail_to_video(
        input_file, 
        output_file, 
        thumbnail_file, 
        THUMBNAIL_DURATION, 
        THUMBNAIL_FADE_DURATION
    )


def find_matching_thumbnail(video_file, thumbnail_patterns):
    """Find matching thumbnail for a video file based on naming patterns."""
    video_name = os.path.splitext(os.path.basename(video_file))[0]
    
    # Try different naming patterns
    patterns = [
        f"{video_name}_thumb.png",
        f"{video_name}_thumbnail.png", 
        f"{video_name}.png",
        f"thumb_{video_name}.png"
    ]
    
    for pattern in patterns:
        for thumb_dir in thumbnail_patterns:
            thumb_path = os.path.join(thumb_dir, pattern)
            if os.path.exists(thumb_path):
                return thumb_path
    
    return None


def get_batch_thumbnail_strategy():
    """Get user's preferred strategy for batch thumbnail processing."""
    print("\n📸 BATCH THUMBNAIL STRATEGY:")
    print("1. Use ONE thumbnail for ALL videos")
    print("2. Auto-match thumbnails (video1.mp4 → video1_thumb.png)")
    print("3. Specify thumbnail directory (auto-find matching names)")
    print("4. Manual selection for each video")
    
    choice = input("Choose strategy (1-4): ").strip()
    return choice


def process_directory():
    """Process all videos in directory with multithreading."""
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

    strategy = get_batch_thumbnail_strategy()
    video_tasks = []
    
    if strategy == "1":
        # One thumbnail for all videos
        thumbnail_file = input("Enter thumbnail file path: ").strip().strip('"').strip("'")
        if not os.path.exists(thumbnail_file):
            logging.error("Thumbnail file not found!")
            return
            
        for i, filename in enumerate(video_files, 1):
            input_path = os.path.join(INPUT_DIR, filename)
            name, ext = os.path.splitext(filename)
            output_path = os.path.join(OUTPUT_DIR, f"{name}_with_thumbnail{ext}")
            progress_info = f"{i}/{len(video_files)}"
            video_tasks.append((input_path, output_path, thumbnail_file, progress_info))
    
    elif strategy == "2":
        # Auto-match thumbnails
        thumbnail_dirs = [THUMBNAIL_DIR, INPUT_DIR]  # Check both directories
        
        for i, filename in enumerate(video_files, 1):
            input_path = os.path.join(INPUT_DIR, filename)
            name, ext = os.path.splitext(filename)
            output_path = os.path.join(OUTPUT_DIR, f"{name}_with_thumbnail{ext}")
            
            # Find matching thumbnail
            thumbnail_file = find_matching_thumbnail(input_path, thumbnail_dirs)
            if thumbnail_file:
                progress_info = f"{i}/{len(video_files)}"
                video_tasks.append((input_path, output_path, thumbnail_file, progress_info))
            else:
                logging.warning(f"No matching thumbnail found for: {filename}")
    
    elif strategy == "3":
        # Specify thumbnail directory
        thumb_dir = input("Enter thumbnail directory path: ").strip().strip('"').strip("'")
        if not os.path.exists(thumb_dir):
            logging.error("Thumbnail directory not found!")
            return
            
        for i, filename in enumerate(video_files, 1):
            input_path = os.path.join(INPUT_DIR, filename)
            name, ext = os.path.splitext(filename)
            output_path = os.path.join(OUTPUT_DIR, f"{name}_with_thumbnail{ext}")
            
            thumbnail_file = find_matching_thumbnail(input_path, [thumb_dir])
            if thumbnail_file:
                progress_info = f"{i}/{len(video_files)}"
                video_tasks.append((input_path, output_path, thumbnail_file, progress_info))
            else:
                logging.warning(f"No matching thumbnail found for: {filename}")
    
    elif strategy == "4":
        # Manual selection for each video
        for i, filename in enumerate(video_files, 1):
            print(f"\nVideo {i}/{len(video_files)}: {filename}")
            thumbnail_file = input("Enter thumbnail file path (or 'skip'): ").strip().strip('"').strip("'")
            
            if thumbnail_file.lower() == 'skip':
                continue
                
            if not os.path.exists(thumbnail_file):
                logging.warning(f"Thumbnail not found, skipping: {filename}")
                continue
                
            input_path = os.path.join(INPUT_DIR, filename)
            name, ext = os.path.splitext(filename)
            output_path = os.path.join(OUTPUT_DIR, f"{name}_with_thumbnail{ext}")
            progress_info = f"{i}/{len(video_files)}"
            video_tasks.append((input_path, output_path, thumbnail_file, progress_info))
    
    else:
        logging.error("Invalid strategy choice!")
        return

    if not video_tasks:
        logging.warning("No valid video-thumbnail pairs found!")
        return

    # Process videos
    logging.info(f"🎬 THUMBNAIL ADDER - MULTITHREADED MODE")
    logging.info(f"📁 Videos to process: {len(video_tasks)}")
    logging.info(f"📸 Thumbnail duration: {THUMBNAIL_DURATION}s")
    logging.info(f"🎭 Fade duration: {THUMBNAIL_FADE_DURATION}s")
    logging.info(f"🧵 Max workers: {MAX_WORKERS}")
    logging.info(f"🚀 Starting processing...")

    success_count = 0
    failed_files = []
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_video = {
            executor.submit(process_single_video_threaded, task): task[0] 
            for task in video_tasks
        }
        
        for future in as_completed(future_to_video):
            try:
                success, input_file = future.result()
                if success:
                    success_count += 1
                else:
                    failed_files.append(os.path.basename(input_file))
            except Exception as exc:
                input_file = future_to_video[future]
                thread_safe_log(f"Video {os.path.basename(input_file)} generated exception: {exc}", "error")
                failed_files.append(os.path.basename(input_file))

    # Summary
    total_time = (time.time() - start_time) / 60
    logging.info(f"\n🎉 MULTITHREADED PROCESSING COMPLETE!")
    logging.info(f"✅ Successful: {success_count}/{len(video_tasks)}")
    logging.info(f"❌ Failed: {len(failed_files)}/{len(video_tasks)}")
    if failed_files:
        logging.info(f"Failed files: {', '.join(failed_files)}")
    logging.info(f"⏱️ Total time: {total_time:.1f} minutes")
    if len(video_tasks) > 0:
        logging.info(f"🚀 Average time per video: {(total_time * 60) / len(video_tasks):.1f} seconds")


def process_directory_single_threaded():
    """Process all videos in directory - single threaded."""
    # Similar to process_directory() but without threading
    # Implementation would be similar but using sequential processing
    logging.info("Single threaded batch processing - use multithreaded mode for better performance!")
    

def main():
    print("🎬 VIDEO THUMBNAIL ADDER")
    print(f"📁 Input directory: {INPUT_DIR}")
    print(f"📁 Output directory: {OUTPUT_DIR}")
    print(f"📁 Default thumbnail directory: {THUMBNAIL_DIR}")
    print(f"📸 Thumbnail duration: {THUMBNAIL_DURATION} seconds")
    print(f"🎭 Fade effect: {THUMBNAIL_FADE_DURATION} seconds")
    print(f"🧵 Available CPU cores: {multiprocessing.cpu_count()}")
    print(f"🧵 Max workers: {MAX_WORKERS}")
    print()

    choice = input("Choose option:\n1. Process ALL videos (MULTITHREADED - Batch mode)\n2. Process SINGLE video\n3. Change settings\nEnter choice (1, 2, or 3): ").strip()

    if choice == "1":
        print("\n🚀 Starting batch processing with multithreading...")
        process_directory()
        
    elif choice == "2":
        video_path = input("Enter video file path: ").strip().strip('"').strip("'")
        thumbnail_path = input("Enter thumbnail file path: ").strip().strip('"').strip("'")
        
        if not video_path or not thumbnail_path:
            print("❌ Path not provided. Exiting.")
            return

        if process_single_video(video_path, thumbnail_path):
            print("✅ Thumbnail added successfully!")
        else:
            print("❌ Failed to add thumbnail.")
            
    elif choice == "3":
        print("\n🔧 SETTINGS:")
        print(f"Current thumbnail duration: {THUMBNAIL_DURATION}s")
        print(f"Current fade duration: {THUMBNAIL_FADE_DURATION}s")
        print("Edit the script to change these values.")
        
    else:
        print("❌ Invalid choice. Exiting.")


if __name__ == "__main__":
    main()