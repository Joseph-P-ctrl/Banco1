import os
import shutil


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SOURCE_BD_DIR = os.path.join(BASE_DIR, 'BD')

IS_VERCEL = os.environ.get('VERCEL') == '1' or bool(os.environ.get('VERCEL_ENV'))
DATA_ROOT = os.environ.get('BANCOS_DATA_DIR') or ('/tmp/bancos_data' if IS_VERCEL else BASE_DIR)

BD_DIR = os.path.join(DATA_ROOT, 'BD')
FILES_DIR = os.path.join(DATA_ROOT, 'files')
LOGS_DIR = os.path.join(DATA_ROOT, 'logs')
SESSION_DIR = os.path.join(DATA_ROOT, 'flask_session')
VOUCHERS_DIR = os.path.join(FILES_DIR, 'vouchers')


def ensure_data_dirs():
    for directory in [DATA_ROOT, BD_DIR, FILES_DIR, LOGS_DIR, SESSION_DIR, VOUCHERS_DIR]:
        os.makedirs(directory, exist_ok=True)


def bootstrap_bd_from_source():
    if not os.path.isdir(SOURCE_BD_DIR):
        return
    for file_name in os.listdir(SOURCE_BD_DIR):
        source_path = os.path.join(SOURCE_BD_DIR, file_name)
        destination_path = os.path.join(BD_DIR, file_name)
        if os.path.isfile(source_path) and not os.path.exists(destination_path):
            shutil.copy2(source_path, destination_path)


def bd_path(*parts):
    return os.path.join(BD_DIR, *parts)


def files_path(*parts):
    return os.path.join(FILES_DIR, *parts)


def logs_path(*parts):
    return os.path.join(LOGS_DIR, *parts)


def session_path(*parts):
    return os.path.join(SESSION_DIR, *parts)


def vouchers_path(*parts):
    return os.path.join(VOUCHERS_DIR, *parts)
