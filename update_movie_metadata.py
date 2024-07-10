"""
Script to update metadata for movie files using Plex API and update file meta tags

Author: Damon Tavangar
Date: 2023-07-12

This script scans the specified directory and its subfolders to find movie files,
extracts metadata from the file names, updates the database, updates file meta tags,
and updates the Plex library accordingly. It also handles interruptions gracefully,
logs errors, and shows the progress of the metadata update process.

Dependencies:
- plexapi
- aiohttp
- aiofiles
- tenacity
- pillow
- tqdm
- mutagen
- pyyaml

Ensure all dependencies are installed before running the script:
pip install plexapi aiohttp aiofiles tenacity pillow tqdm mutagen pyyaml

"""

import os
import re
import yaml
import sqlite3
import logging
import threading
import asyncio
import aiohttp
from aiofiles import open as aio_open
from queue import Queue
from plexapi.server import PlexServer
from datetime import datetime
import fcntl
from contextlib import contextmanager
from tenacity import retry, wait_exponential, stop_after_attempt
from PIL import Image
from io import BytesIO
from tqdm import tqdm
from mutagen.mp4 import MP4, MP4Cover
from mutagen.easyid3 import EasyID3
from mutagen.id3 import ID3, APIC

# Load settings from YAML file
with open('settings.yaml', 'r') as f:
    config = yaml.safe_load(f)

PLEX_URL = config['plex']['url']
PLEX_TOKEN = config['plex']['token']
MEDIA_BASE_DIR = config['media']['base_dir']
DB_PATH = config['database']['path']
DEFAULT_DATE = config['default_date']
LOCK_FILE = config['lock_file']
NUM_THREADS = config['num_threads']  # Number of threads for parallel processing
BATCH_SIZE = config['batch_size']  # Number of updates to commit at once

# Configure logging
logging.basicConfig(filename=config['logging']['filename'], level=getattr(logging, config['logging']['level']), format=config['logging']['format'])
logger = logging.getLogger(__name__)

# Connect to Plex
plex = PlexServer(PLEX_URL, PLEX_TOKEN)

# Ensure the processed_files table exists with additional metadata columns
def setup_database():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute('PRAGMA busy_timeout = 30000')  # Set busy timeout to 30 seconds
        cursor = conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS processed_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_path TEXT UNIQUE,
            title TEXT,
            year TEXT,
            originally_available_at TEXT,
            title_sort TEXT,
            artist TEXT,
            genre TEXT,
            media_info TEXT,
            poster BLOB,
            thumbnail BLOB,
            error_message TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_file_path ON processed_files (file_path)')
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS metadata_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            originally_available_at TEXT,
            title_sort TEXT,
            genre TEXT
        )
        ''')
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS media_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            metadata_item_id INTEGER,
            file TEXT
        )
        ''')
        conn.commit()

@contextmanager
def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.execute('PRAGMA busy_timeout = 30000')  # Set busy timeout to 30 seconds
    try:
        yield conn
    finally:
        conn.close()

# Regex pattern to extract title and year from the filename
pattern = re.compile(r'^(.*?)(?:\s*-\s*\((\d{4})\)|\s*\((\d{4})\))?\.(mp4|mkv|avi)$')

def load_processed_files():
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT file_path FROM processed_files WHERE error_message IS NULL')
        return set(row[0] for row in cursor.fetchall())

def log_processed_file(file_path, metadata, batch):
    batch.append((file_path, metadata['title'], metadata['year'], metadata['originally_available_at'], metadata['title_sort'], metadata['artist'], metadata['genre'], metadata['media_info'], metadata['poster'], metadata['thumbnail']))

def log_error(file_path, error_message, batch):
    batch.append((file_path, error_message))

async def fetch_image(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                return await response.read()

def update_file_metadata(file_path, title, originally_available_at, title_sort, artist, genre, poster):
    try:
        if file_path.lower().endswith('.mp4'):
            video = MP4(file_path)
            video['\xa9nam'] = title
            video['\xa9day'] = originally_available_at
            video['\xa9ART'] = artist
            video['\xa9gen'] = genre
            if poster:
                video['covr'] = [MP4Cover(poster, imageformat=MP4Cover.FORMAT_JPEG)]
            video.save()
        elif file_path.lower().endswith('.mp3'):
            audio = EasyID3(file_path)
            audio['title'] = title
            audio['date'] = originally_available_at
            audio['artist'] = artist
            audio['genre'] = genre
            audio.save()
            audio = ID3(file_path)
            if poster:
                audio['APIC'] = APIC(
                    encoding=3,
                    mime='image/jpeg',
                    type=3,
                    desc='Cover',
                    data=poster
                )
            audio.save()
    except Exception as e:
        logger.error(f"Error updating file metadata for {file_path}: {e}")

async def update_meta_tags(file_path, batch_processed, batch_errors, progress, db_queue, db_lock):
    filename = os.path.basename(file_path)
    match = pattern.match(filename)
    if not match:
        logger.warning(f"Skipping file {file_path}, pattern not matched.")
        progress.update(1)
        return

    title = match.group(1)
    year = match.group(2) or match.group(3)
    originally_available_at = f"{DEFAULT_DATE}-{year}" if year else None
    title_sort = title
    artist = ""
    genre = ""

    parts = file_path.split(os.sep)
    if 'stars' in parts:
        star_index = parts.index('stars')
        if star_index + 1 < len(parts):
            artist = parts[star_index + 1]
            genre = f"star, star - {artist}"

    # Fetch media info and poster/thumbnail
    media_info = None
    poster = None
    thumbnail = None
    video = None  # Initialize video to None
    try:
        video = plex.library.search(title=title)[0]
        media_info = str(video)
        if video.thumb:
            poster_data = await fetch_image(plex.url(video.thumb, includeToken=True))
            poster = poster_data
            # Generate thumbnail
            image = Image.open(BytesIO(poster_data))
            image.thumbnail((128, 128))
            thumbnail_buffer = BytesIO()
            image.save(thumbnail_buffer, format='JPEG')
            thumbnail = thumbnail_buffer.getvalue()
    except Exception as e:
        logger.error(f"Error fetching media info for {file_path}: {e}")
    
    metadata = {
        'title': title,
        'year': year,
        'originally_available_at': originally_available_at,
        'title_sort': title_sort,
        'artist': artist,
        'genre': genre,
        'media_info': media_info,
        'poster': poster,
        'thumbnail': thumbnail
    }

    try:
        # Add to the database queue
        db_queue.put((file_path, metadata))

        # Update the video meta file
        try:
            if video:  # Ensure video is not None
                video.editTitle(title)
                video.editSortTitle(title_sort)
                if originally_available_at:
                    video.editOriginallyAvailableAt(datetime.strptime(originally_available_at, '%m/%d-%Y'))

                video.refresh()
        except Exception as e:
            logger.error(f"Error updating video metadata for {file_path}: {e}")
            log_error(file_path, f"Error updating video metadata: {e}", batch_errors)
            progress.update(1)
            return

        # Update the file meta tags
        update_file_metadata(file_path, title, originally_available_at, title_sort, artist, genre, poster)

        log_processed_file(file_path, metadata, batch_processed)
        logger.info(f"Processed file {file_path}")
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {e}")
        log_error(file_path, str(e), batch_errors)
        raise
    finally:
        progress.update(1)

async def process_directory(directory, queue, total_files):
    processed_files = load_processed_files()
    files_to_process = []

    for root, _, files in os.walk(directory):
        for file in files:
            if file.lower().endswith(('.mp4', '.mkv', '.avi')):
                file_path = os.path.join(root, file)
                if file_path not in processed_files:
                    files_to_process.append(file_path)
    
    total_files.update(len(files_to_process))

    for file_path in files_to_process:
        queue.put(file_path)

async def worker(queue, batch_processed, batch_errors, progress, db_queue, db_lock):
    while True:
        file_path = await queue.get()
        if file_path is None:
            break
        await update_meta_tags(file_path, batch_processed, batch_errors, progress, db_queue, db_lock)
        queue.task_done()

def db_worker(db_queue):
    setup_database()
    with get_db_connection() as conn:
        cursor = conn.cursor()
        while True:
            try:
                file_path, metadata = db_queue.get()
                cursor.execute('''
                    INSERT OR IGNORE INTO processed_files (file_path, title, year, originally_available_at, title_sort, artist, genre, media_info, poster, thumbnail)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (file_path, metadata['title'], metadata['year'], metadata['originally_available_at'], metadata['title_sort'], metadata['artist'], metadata['genre'], metadata['media_info'], metadata['poster'], metadata['thumbnail']))
                conn.commit()
            except sqlite3.OperationalError as e:
                logger.error(f"Database error: {e}")
                continue
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
            finally:
                db_queue.task_done()

async def main():
    setup_database()

    file_queue = Queue()
    db_queue = Queue()
    db_lock = threading.Lock()

    # Thread for database operations
    db_thread = threading.Thread(target=db_worker, args=(db_queue,))
    db_thread.daemon = True
    db_thread.start()

    processed_files = []
    errors = []

    total_files = tqdm(total=0, unit='files', desc='Total files')
    progress = tqdm(total=0, unit='files', desc='Processed files')

    await process_directory(MEDIA_BASE_DIR, file_queue, total_files)

    tasks = []
    for _ in range(NUM_THREADS):
        task = asyncio.create_task(worker(file_queue, processed_files, errors, progress, db_queue, db_lock))
        tasks.append(task)

    await file_queue.join()

    for _ in tasks:
        file_queue.put(None)
    await asyncio.gather(*tasks)

    db_queue.join()

    total_files.close()
    progress.close()

if __name__ == '__main__':
    with open(LOCK_FILE, 'w') as lockfile:
        try:
            fcntl.flock(lockfile, fcntl.LOCK_EX | fcntl.LOCK_NB)
            asyncio.run(main())
        except BlockingIOError:
            logger.error("Script is already running.")
        finally:
            fcntl.flock(lockfile, fcntl.LOCK_UN)
