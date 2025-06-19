import os
import shutil
from datetime import datetime

from etl_pipeline.parsers_files import parse_mbank, parse_pkotb
from etl_pipeline.loaders import load
from etl_pipeline.transformers import transform
from utils.config import MBANK_FOLDER, PKOTB_FOLDER, DOCS_WITH_ATTACHMENTS
from utils.misc_func import pdf_extractor
from pdfminer.pdfdocument import PDFPasswordIncorrect
from pdfminer.pdfparser import PDFSyntaxError

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
        if isinstance(getattr(e, '__context__', None), PDFPasswordIncorrect):
            print(f"Error processing file {file_path}: PDF password required. Please verify PDFs extraction list.")
        else:
            # logger.error(f"Error processing file {file_name}: {e}")
            print(f"Error processing file {file_path}: {e}")


def loop_through_files(folder_path, parser, transformer, loader, saver, extract_list=None, ignore_folders=None):
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
            if any(extract in file_name for extract in extract_list):
                outer_file_path = os.path.join(root, file_name)
                if file_name.endswith('.pdf'):
                    file_name = pdf_extractor(file_path)
                else:
                    raise ValueError("Unsupported file format.pdf")
                file_path = os.path.join(root, file_name)
                saver(outer_file_path)
            process_file(file_path, parser, transformer, loader, saver)


def etl_pipeline(process_name, folder_path, parser, transformer, loader, saver):
    """
    Executes an ETL pipeline for processing files within a specified folder.

    """
    # print(f"\n### Running ETL process: {process_name}")
    loop_through_files(folder_path, parser, transformer, loader, saver, DOCS_WITH_ATTACHMENTS)
    # print(f"### Completed ETL process: {process_name}")


''' SPECIFIC FUNCTIONS'''


def run_etl_processes():
    """
    Runs the ETL processes for uploading holdings from files located in specified folders.

    """
    processes = {
        'Upload transactions for mBank': {
            'folder_path': MBANK_FOLDER,
            'parser': parse_mbank,
            'transformer': transform,
            'loader': load,
            'saver': save_file
        },
        'Upload transactions for PKO Treasury Bonds': {
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