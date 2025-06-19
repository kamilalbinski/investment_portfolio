from utils.config import ROOT_PATH, pdf_password
import os
import fitz # PyMuPDF
import getpass

def pdf_extractor(pdf_file):

    password = pdf_password

    dir_path = os.path.dirname(pdf_file)

    if password == '':
        password = getpass.getpass("Enter PDF password: ")


    doc = fitz.open(pdf_file)
    if doc.is_encrypted:
        if not doc.authenticate(password):
            print("Wrong password!")
            exit()

    # Total number of embedded files
    count = doc.embfile_count()

    for i in range(count):
        info = doc.embfile_info(i)  # Get metadata
        actual_name = info["filename"].rsplit('_', maxsplit=1)[0]
        actual_name += '.pdf'

        print(f"Found attachment: {actual_name}")

        # Extract by index, not name
        file_data = doc.embfile_get(i)

        output_path = os.path.join(dir_path, actual_name)

        with open(output_path, "wb") as f:
            f.write(file_data)
        print(f"Saved: {actual_name}")

        return actual_name