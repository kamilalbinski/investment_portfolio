# config.py
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# === Project Root ===
ROOT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

# === Database Configuration ===
DATABASE_FILE = os.path.join(ROOT_PATH, os.getenv('DATABASE_FILE',''))

# === Folder Paths ===
MBANK_FOLDER = os.path.join(ROOT_PATH, os.getenv('MBANK_FOLDER',''))


PKOTB_FOLDER = os.path.join(ROOT_PATH, os.getenv('PKOTB_FOLDER',''))

# === URLs ===
BIZNESRADAR_URL = 'https://www.biznesradar.pl/notowania-historyczne/'
CPI_URL = 'https://stat.gov.pl/download/gfx/portalinformacyjny/pl/defaultstronaopisowa/4741/1/1/miesieczne_wskazniki_cen_towarow_i_uslug_konsumpcyjnych_od_1982_roku_2.csv'

# === API Configuration ===

# === Local Passwords ===
DOCS_WITH_ATTACHMENTS = ['document_template_1','document_template_2']
pdf_password = os.getenv('PDF_PASSWORD', '')

# === Optional Flags ===
DEBUG_MODE = os.getenv('DEBUG_MODE', 'False').lower() in ('1', 'true', 'yes')

