import os

from etl_pipeline.parsers_files import parse_mbank, parse_pkotb
from etl_pipeline.loaders import load
from etl_pipeline.transformers import transform_holdings, transform
from utils.config import MBANK_FOLDER, PKOTB_FOLDER

''' GENERIC FUNCTIONS'''


def process_file(file_path, parser, transformer, loader):
    """
    Processes a single file through a given ETL pipeline.

    """

    try:
        #raw_data, is_edo = parser(file_path)
        raw_data, source, file_type = parser(file_path)
        transformed_data = transformer(raw_data, source, file_type)
        loader(transformed_data, file_type)
    except Exception as e:
        # logger.error(f"Error processing file {file_name}: {e}")
        print(f"Error processing file {file_path}: {e}")


def loop_through_files(folder_path, parser, transformer, loader):
    """
    Loops through each file in a specified folder and processes it through a given ETL pipeline.

    """
    for root, dirs, files in os.walk(folder_path):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            # print(os.path.basename(root))
            print(f'Loading file: {file_name}')
            process_file(file_path, parser, transformer, loader)


def etl_pipeline(process_name, folder_path, parser, transformer, loader):
    """
    Executes an ETL pipeline for processing files within a specified folder.

    """

    print(f"\n### Running ETL process: {process_name}")
    loop_through_files(folder_path, parser, transformer, loader)
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
            'loader': load
        },
        'Upload holdings for PKO Treasury Bonds': {
            'folder_path': PKOTB_FOLDER,
            'parser': parse_pkotb,
            'transformer': transform,
            'loader': load
        },
    }

    for process_name, settings in processes.items():
        print(f"\n### Running ETL process: {process_name}")
        etl_pipeline(process_name, **settings)
        print(f"### Completed ETL process: {process_name}")
