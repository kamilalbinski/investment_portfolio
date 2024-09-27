import os
import shutil
from datetime import datetime

from etl_pipeline.parsers_files import parse_mbank, parse_pkotb
from etl_pipeline.loaders import load
from etl_pipeline.transformers import transform_holdings, transform
from utils.config import MBANK_FOLDER, PKOTB_FOLDER

''' GENERIC FUNCTIONS'''


def save_file(file_path, archive_folder='archive'):
    """
    Saves the file to the archive folder with a renamed format: YYYY-MM-DD_[existing_file_name].

    """
    # Get the folder where the file is saved
    folder_path = os.path.dirname(file_path)

    # Create archive folder if it doesn't exist
    archive_path = os.path.join(folder_path, archive_folder)
    if not os.path.exists(archive_path):
        os.makedirs(archive_path)

    # Rename the file by adding the date
    file_name = os.path.basename(file_path)
    new_file_name = f"{datetime.now().strftime('%Y-%m-%d')}_{file_name}"

    # Move the file to the archive folder
    new_file_path = os.path.join(archive_path, new_file_name)
    shutil.move(file_path, new_file_path)

    print(f"File {file_name} archived as {new_file_name} in {archive_folder}")


def process_file(file_path, parser, transformer, loader, saver):
    """
    Processes a single file through a given ETL pipeline.

    """
    try:
        # raw_data, is_edo = parser(file_path)
        raw_data, source, file_type = parser(file_path)
        transformed_data = transformer(raw_data, source, file_type)
        loader(transformed_data, file_type)

        # Save the file to the archive folder
        saver(file_path)

    except Exception as e:
        # logger.error(f"Error processing file {file_name}: {e}")
        print(f"Error processing file {file_path}: {e}")


def loop_through_files(folder_path, parser, transformer, loader, saver, ignore_folders=None):
    """
    Loops through each file in a specified folder and processes it through a given ETL pipeline,
    while ignoring specified folders like 'archive'.

    """
    if ignore_folders is None:
        ignore_folders = ['archive']  # By default, ignore the 'archive' folder

    for root, dirs, files in os.walk(folder_path):
        # Exclude any folders listed in ignore_folders from being walked into
        dirs[:] = [d for d in dirs if d not in ignore_folders]

        for file_name in files:
            file_path = os.path.join(root, file_name)
            print(f'Loading file: {file_name}')
            process_file(file_path, parser, transformer, loader, saver)


def etl_pipeline(process_name, folder_path, parser, transformer, loader, saver):
    """
    Executes an ETL pipeline for processing files within a specified folder.

    """
    print(f"\n### Running ETL process: {process_name}")
    loop_through_files(folder_path, parser, transformer, loader, saver)
    print(f"### Completed ETL process: {process_name}")


''' SPECIFIC FUNCTIONS'''


def run_etl_processes():
    """
    Runs the ETL processes for uploading holdings from files located in specified folders.

    """
    processes = {
        'Upload holdings for mBank': {
            'folder_path': MBANK_FOLDER,
            'parser': parse_mbank,
            'transformer': transform,
            'loader': load,
            'saver': save_file
        },
        'Upload holdings for PKO Treasury Bonds': {
            'folder_path': PKOTB_FOLDER,
            'parser': parse_pkotb,
            'transformer': transform,
            'loader': load,
            'saver': save_file
        },
    }

    for process_name, settings in processes.items():
        print(f"\n### Running ETL process: {process_name}")
        etl_pipeline(process_name, **settings)
        print(f"### Completed ETL process: {process_name}")