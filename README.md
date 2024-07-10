# Update Movie Metadata Script

## Author
Damon Tavangar (Contact: tavangar2018@gmail.com)

## Project Title
Update Movie Metadata Script

## Project Description
This script updates metadata for movie files using the Plex API and updates file meta tags. It scans the specified directory and its subfolders to find movie files, extracts metadata from the file names, updates the database, updates file meta tags, and ensures changes are reflected in the Plex library. It handles interruptions gracefully, logs errors, and shows the progress of the metadata update process.


## Build Status
The project is in a stable state, with all core functionalities implemented and tested. Future updates may include additional features and improvements.

## Code Style
The project follows the PEP 8 style guide for Python code, ensuring consistency and readability.



## Tech/Framework Used
- Python 3.7+
- Plex API
- aiohttp
- aiofiles
- tenacity
- pillow
- tqdm
- mutagen
- pyyaml

## Features
- Scans directories and subfolders for movie files.
- Extracts metadata from file names.
- Updates the database with extracted metadata.
- Updates file meta tags.
- Updates metadata in Plex.
- Handles interruptions gracefully.
- Logs errors and progress.

## Credits
Thanks to the developers of the following libraries:

- plexapi
- aiohttp
- aiofiles
- tenacity
- pillow
- tqdm
- mutagen
- pyyaml


