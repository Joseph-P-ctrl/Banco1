from flask import Flask, jsonify, render_template, request, send_file, session, redirect, url_for
from AccountService import AccountService
from InterbankService import InterbankService
from ProviderService import ProviderService
from TransferService import TransferService
from BaseDatosService import BaseDatosService
from AsientoService import AsientoService
from VoucherService import VoucherService
from io import BytesIO
from flask_session import Session
from flask_caching import Cache
from openpyxl import load_workbook
import pandas as pd
import os
import openpyxl
from openpyxl import Workbook
import re
import logging
import traceback
import json
import smtplib
import uuid
from email.message import EmailMessage
from dotenv import load_dotenv
from cryptography.fernet import Fernet
from storage_paths import ensure_data_dirs, bootstrap_bd_from_source, files_path, logs_path, SESSION_DIR, bd_path, vouchers_path

# setup logging
ensure_data_dirs()
bootstrap_bd_from_source()
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
logging.basicConfig(filename=logs_path('error.log'), level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')
app = Flask(__name__)
app.secret_key = 'AldoAbril1978'
app.config['SESSION_TYPE'] = 'filesystem'
app.config['SESSION_FILE_DIR'] = SESSION_DIR
Session(app)
cache = Cache(app, config={'CACHE_TYPE': 'simple'})

TRANSFER = "TRANSFER"
PROVIDERS = "PROVIDER"
INTERBANK = "INTERBAN"
CUENTA = "MOVIMIENT"
MOVIMIENTOS = "MOVIMIENTOS"
ASIENTO= "EXPORT"

COMPANY_KEYWORDS = {
    'SAC', 'S.A.C', 'SRL', 'S.R.L', 'SA', 'S.A', 'EIRL', 'E.I.R.L',
    'GERENCIA', 'DIRECCION', 'DIREC', 'REGIONAL', 'MUNICIPALIDAD',
    'MINISTERIO', 'GOBIERNO', 'UNIDAD', 'LOGISTICA', 'AGRICULTURA',
    'POLICIAL', 'HOSPITAL', 'UNIVERSIDAD', 'COLEGIO', 'EMPRESA',
    'SERVICIOS', 'AREA', 'OFICINA'
}


def _smtp_key_path():
    return files_path('smtp_credentials.key')


def _smtp_credentials_path():
    return files_path('smtp_credentials.json')


def _google_config_path():
    return files_path('google_config.json')


def _general_settings_path():
    return files_path('general_settings.json')


def _account_features_path():
    return files_path('account_features.json')


def _profile_photo_dir():
    return files_path('profile')


def _profile_photo_path(filename='profile_photo.png'):
    return os.path.join(_profile_photo_dir(), filename)


def _allowed_image_extension(filename):
    lower_name = str(filename or '').strip().lower()
    return lower_name.endswith('.png') or lower_name.endswith('.jpg') or lower_name.endswith('.jpeg') or lower_name.endswith('.webp')


def _safe_user_agent():
    try:
        return str(request.user_agent.string or '').strip()
    except Exception:
        return ''


def _client_ip():
    forwarded_for = request.headers.get('X-Forwarded-For', '').strip()
    if forwarded_for:
        return forwarded_for.split(',')[0].strip()
    return str(request.remote_addr or '').strip() or 'desconocido'


def _detect_os_family(user_agent_text):
    ua = str(user_agent_text or '').lower()
    if 'android' in ua:
        return 'Android'
    if 'windows' in ua:
        return 'Windows'
    if 'iphone' in ua or 'ipad' in ua or 'ios' in ua:
        return 'iOS'
    if 'mac os' in ua or 'macintosh' in ua:
        return 'macOS'
    if 'linux' in ua:
        return 'Linux'
    return 'Otro'


def _detect_device_type(user_agent_text):
    ua = str(user_agent_text or '').lower()
    if 'android' in ua or 'iphone' in ua or 'mobile' in ua:
        return 'Móvil'
    if 'ipad' in ua or 'tablet' in ua:
        return 'Tablet'
    return 'Computadora'


def _relative_time_label(timestamp_value):
    if not timestamp_value:
        return 'Sin registro'
    try:
        last_seen = pd.to_datetime(timestamp_value, format='%d/%m/%Y %H:%M:%S', errors='coerce')
        if pd.isna(last_seen):
            return str(timestamp_value)
        now_value = pd.Timestamp.now()
        diff = now_value - last_seen
        total_seconds = max(int(diff.total_seconds()), 0)

        if total_seconds < 60:
            return 'Hace unos segundos'
        if total_seconds < 3600:
            minutes = total_seconds // 60
            return f'Hace {minutes} min'
        if total_seconds < 86400:
            hours = total_seconds // 3600
            return f'Hace {hours} hora(s)'
        days = total_seconds // 86400
        return f'Hace {days} día(s)'
    except Exception:
        return str(timestamp_value)


def _get_fernet():
    env_key = os.environ.get('OUTLOOK_CREDENTIALS_KEY', '').strip()
    if env_key:
        key = env_key.encode('utf-8')
        return Fernet(key)

    key_path = _smtp_key_path()
    if os.path.exists(key_path):
        with open(key_path, 'rb') as key_file:
            key = key_file.read().strip()
    else:
        key = Fernet.generate_key()
        with open(key_path, 'wb') as key_file:
            key_file.write(key)

    return Fernet(key)


def normalize_sender_email(sender_value):
    sender_clean = str(sender_value or '').strip()
    sender_lower = sender_clean.lower()
    typo_domain = '@distrluz.com.pe'
    corrected_domain = '@distriluz.com.pe'

    if sender_lower.endswith(typo_domain):
        corrected = sender_clean[:-len(typo_domain)] + corrected_domain
        return corrected, True

    return sender_clean, False


def save_secure_smtp_credentials(sender_value, password_value, smtp_host_value=None, smtp_port_value=None, smtp_security_value=None):
    fernet = _get_fernet()
    payload = {
        'sender_encrypted': fernet.encrypt(sender_value.encode('utf-8')).decode('utf-8'),
        'password_encrypted': fernet.encrypt(password_value.encode('utf-8')).decode('utf-8')
    }
    if smtp_host_value is not None:
        payload['smtp_host'] = str(smtp_host_value).strip()
    if smtp_port_value is not None:
        payload['smtp_port'] = str(smtp_port_value).strip()
    if smtp_security_value is not None:
        payload['smtp_security'] = str(smtp_security_value).strip().lower()
    with open(_smtp_credentials_path(), 'w', encoding='utf-8') as out_file:
        json.dump(payload, out_file, ensure_ascii=False)


def load_secure_smtp_credentials():
    cred_path = _smtp_credentials_path()
    if not os.path.exists(cred_path):
        return {}

    try:
        with open(cred_path, 'r', encoding='utf-8') as in_file:
            payload = json.load(in_file)

        sender_encrypted = payload.get('sender_encrypted', '')
        password_encrypted = payload.get('password_encrypted', '')
        if not sender_encrypted or not password_encrypted:
            return {}

        fernet = _get_fernet()
        sender_value = fernet.decrypt(sender_encrypted.encode('utf-8')).decode('utf-8')
        password_value = fernet.decrypt(password_encrypted.encode('utf-8')).decode('utf-8')
        return {
            'sender': sender_value,
            'password': password_value,
            'smtp_host': str(payload.get('smtp_host', '')).strip(),
            'smtp_port': str(payload.get('smtp_port', '')).strip(),
            'smtp_security': str(payload.get('smtp_security', '')).strip().lower()
        }
    except Exception as ex:
        logging.error(f'No se pudo leer credenciales SMTP cifradas: {ex}')
        return {}


def save_google_config(config_values):
    payload = {
        'project_id': str(config_values.get('project_id', '')).strip(),
        'client_id': str(config_values.get('client_id', '')).strip(),
        'redirect_uri': str(config_values.get('redirect_uri', '')).strip(),
        'scopes': str(config_values.get('scopes', '')).strip(),
        'enabled': bool(config_values.get('enabled', False))
    }

    fernet = _get_fernet()

    client_secret = str(config_values.get('client_secret', '')).strip()
    api_key = str(config_values.get('api_key', '')).strip()

    if client_secret:
        payload['client_secret_encrypted'] = fernet.encrypt(client_secret.encode('utf-8')).decode('utf-8')
    if api_key:
        payload['api_key_encrypted'] = fernet.encrypt(api_key.encode('utf-8')).decode('utf-8')

    with open(_google_config_path(), 'w', encoding='utf-8') as out_file:
        json.dump(payload, out_file, ensure_ascii=False)


def load_google_config():
    config_path = _google_config_path()
    if not os.path.exists(config_path):
        return {
            'project_id': '',
            'client_id': '',
            'client_secret': '',
            'redirect_uri': '',
            'scopes': 'openid email profile',
            'api_key': '',
            'enabled': False
        }

    try:
        with open(config_path, 'r', encoding='utf-8') as in_file:
            payload = json.load(in_file)

        fernet = _get_fernet()

        client_secret = ''
        api_key = ''

        client_secret_encrypted = payload.get('client_secret_encrypted', '')
        if client_secret_encrypted:
            client_secret = fernet.decrypt(client_secret_encrypted.encode('utf-8')).decode('utf-8')

        api_key_encrypted = payload.get('api_key_encrypted', '')
        if api_key_encrypted:
            api_key = fernet.decrypt(api_key_encrypted.encode('utf-8')).decode('utf-8')

        return {
            'project_id': str(payload.get('project_id', '')).strip(),
            'client_id': str(payload.get('client_id', '')).strip(),
            'client_secret': client_secret,
            'redirect_uri': str(payload.get('redirect_uri', '')).strip(),
            'scopes': str(payload.get('scopes', 'openid email profile')).strip(),
            'api_key': api_key,
            'enabled': bool(payload.get('enabled', False))
        }
    except Exception as ex:
        logging.error(f'No se pudo leer configuración Google: {ex}')
        return {
            'project_id': '',
            'client_id': '',
            'client_secret': '',
            'redirect_uri': '',
            'scopes': 'openid email profile',
            'api_key': '',
            'enabled': False
        }


def load_general_settings():
    default_payload = {
        'inicio': {
            'mostrar_resumen': True,
            'notificaciones_activas': True
        },
        'perfil': {
            'nombre_mostrar': '',
            'cargo': '',
            'telefono': '',
            'foto_version': 0,
            'genero': '',
            'correo_personal': '',
            'correo_alterno': '',
            'fecha_nacimiento': '',
            'idioma': 'Español (España)',
            'direccion_casa': '',
            'direccion_trabajo': '',
            'otras_direcciones': ''
        },
        'seguridad': {
            'doble_factor': False,
            'cerrar_sesiones': True
        },
        'privacidad': {
            'compartir_datos': False,
            'analytics': False
        },
        'contactos': {
            'correo_soporte': '',
            'correo_copia': ''
        }
    }

    settings_path = _general_settings_path()
    if not os.path.exists(settings_path):
        return default_payload

    try:
        with open(settings_path, 'r', encoding='utf-8') as settings_file:
            payload = json.load(settings_file)

        if not isinstance(payload, dict):
            return default_payload

        merged = default_payload.copy()
        for section_key in default_payload.keys():
            section_value = payload.get(section_key, {})
            if isinstance(section_value, dict):
                merged_section = default_payload[section_key].copy()
                merged_section.update(section_value)
                merged[section_key] = merged_section
        return merged
    except Exception as ex:
        logging.error(f'No se pudo leer configuración general: {ex}')
        return default_payload


def save_general_settings(payload):
    with open(_general_settings_path(), 'w', encoding='utf-8') as settings_file:
        json.dump(payload, settings_file, ensure_ascii=False)


def sync_email_config_to_modules(sender_email):
    sender_clean = str(sender_email or '').strip().lower()
    if not sender_clean:
        return

    settings = load_general_settings()

    if 'perfil' not in settings or not isinstance(settings['perfil'], dict):
        settings['perfil'] = {}
    if 'contactos' not in settings or not isinstance(settings['contactos'], dict):
        settings['contactos'] = {}

    settings['perfil']['correo_personal'] = sender_clean

    soporte_actual = str(settings['contactos'].get('correo_soporte', '')).strip()
    if not soporte_actual:
        settings['contactos']['correo_soporte'] = sender_clean

    save_general_settings(settings)


def get_connected_account_email():
    secure_smtp = load_secure_smtp_credentials()
    sender = str(secure_smtp.get('sender', '')).strip()
    if sender:
        return sender

    settings = load_general_settings()
    perfil = settings.get('perfil', {}) if isinstance(settings, dict) else {}
    return str(perfil.get('correo_personal', '')).strip()


def load_account_features():
    default_payload = {
        'password': {
            'value_encrypted': '',
            'updated_at': ''
        },
        'devices': [
            {
                'id': 1,
                'name': 'Equipo principal',
                'location': 'Oficina',
                'active': True
            }
        ],
        'activity': [],
        'sessions': []
    }

    features_path = _account_features_path()
    if not os.path.exists(features_path):
        return default_payload

    try:
        with open(features_path, 'r', encoding='utf-8') as features_file:
            payload = json.load(features_file)

        if not isinstance(payload, dict):
            return default_payload

        merged = default_payload.copy()
        for key in ['password', 'devices', 'activity', 'sessions']:
            value = payload.get(key)
            if key == 'password' and isinstance(value, dict):
                password_payload = default_payload['password'].copy()
                password_payload.update(value)
                merged['password'] = password_payload
            elif key == 'devices' and isinstance(value, list):
                merged['devices'] = value
            elif key == 'activity' and isinstance(value, list):
                merged['activity'] = value
            elif key == 'sessions' and isinstance(value, list):
                merged['sessions'] = value

        return merged
    except Exception as ex:
        logging.error(f'No se pudo leer account_features: {ex}')
        return default_payload


def save_account_features(payload):
    with open(_account_features_path(), 'w', encoding='utf-8') as features_file:
        json.dump(payload, features_file, ensure_ascii=False)


def add_account_activity(action, detail=''):
    payload = load_account_features()
    activity_list = payload.get('activity', [])
    timestamp = pd.Timestamp.now().strftime('%d/%m/%Y %H:%M:%S')
    activity_list.insert(0, {
        'timestamp': timestamp,
        'action': str(action or '').strip(),
        'detail': str(detail or '').strip()
    })

    if len(activity_list) > 200:
        activity_list = activity_list[:200]

    payload['activity'] = activity_list
    save_account_features(payload)


def clear_account_activity(mode='all', keep_last=0):
    payload = load_account_features()
    if mode == 'last' and keep_last > 0:
        payload['activity'] = payload.get('activity', [])[:keep_last]
    else:
        payload['activity'] = []
    save_account_features(payload)


def _ensure_current_session_tracked():
    route_path = request.path or ''
    if route_path.startswith('/static') or route_path.startswith('/foto_perfil_actual'):
        return

    account_session_id = session.get('account_session_id', '')
    if not account_session_id:
        account_session_id = str(uuid.uuid4())
        session['account_session_id'] = account_session_id

    user_agent_text = _safe_user_agent()
    ip_value = _client_ip()
    now_text = pd.Timestamp.now().strftime('%d/%m/%Y %H:%M:%S')

    payload = load_account_features()
    sessions_list = payload.get('sessions', [])

    existing_index = -1
    for index, item in enumerate(sessions_list):
        if str(item.get('session_id', '')).strip() == account_session_id:
            existing_index = index
            break

    if existing_index >= 0:
        current = sessions_list[existing_index]
        current['last_seen'] = now_text
        current['ip'] = ip_value
        current['user_agent'] = user_agent_text
        current['os_family'] = _detect_os_family(user_agent_text)
        current['device_type'] = _detect_device_type(user_agent_text)
        current['active'] = True
        sessions_list[existing_index] = current
    else:
        sessions_list.insert(0, {
            'session_id': account_session_id,
            'first_seen': now_text,
            'last_seen': now_text,
            'ip': ip_value,
            'user_agent': user_agent_text,
            'os_family': _detect_os_family(user_agent_text),
            'device_type': _detect_device_type(user_agent_text),
            'active': True
        })

    sanitized_sessions = []
    now_value = pd.Timestamp.now()
    for item in sessions_list:
        last_seen_text = str(item.get('last_seen', '')).strip()
        is_active = False
        if last_seen_text:
            try:
                parsed_last_seen = pd.to_datetime(last_seen_text, format='%d/%m/%Y %H:%M:%S', errors='coerce')
                if pd.isna(parsed_last_seen):
                    raise ValueError('Fecha inválida')
                delta = now_value - parsed_last_seen
                is_active = delta.total_seconds() <= (7 * 24 * 3600)
            except Exception:
                is_active = bool(item.get('active', False))
        item['active'] = is_active
        sanitized_sessions.append(item)

    payload['sessions'] = sanitized_sessions[:200]
    save_account_features(payload)


@app.before_request
def _before_every_request_account_tracking():
    try:
        _ensure_current_session_tracked()
    except Exception as ex:
        logging.warning(f'No se pudo actualizar sesión de dispositivo: {ex}')


def save_account_password(new_password):
    if not new_password:
        return

    payload = load_account_features()
    fernet = _get_fernet()
    payload['password']['value_encrypted'] = fernet.encrypt(new_password.encode('utf-8')).decode('utf-8')
    payload['password']['updated_at'] = pd.Timestamp.now().strftime('%d/%m/%Y %H:%M:%S')
    save_account_features(payload)


def get_account_password_updated_at():
    payload = load_account_features()
    return str(payload.get('password', {}).get('updated_at', '')).strip()

def extract_emails_from_df(df):
    emails = set()
    email_regex = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
    for col in df.columns:
        for val in df[col].dropna():
            for m in email_regex.findall(str(val)):
                emails.add(m)
    return sorted(emails)

def extract_emails_without_voucher(df):
    if df is None or len(df) == 0:
        return []

    candidate_cols = []
    for col in df.columns:
        normalized = str(col).strip().lower().replace('°', 'º')
        if normalized in ['nº documento', 'nºdocumento', 'asientos', 'voucher contable']:
            candidate_cols.append(col)
        elif 'documento' in normalized or 'voucher' in normalized or 'asiento' in normalized:
            candidate_cols.append(col)

    if candidate_cols:
        voucher_col = candidate_cols[0]
        voucher_values = df[voucher_col]
        empty_mask = voucher_values.isna() | (voucher_values.astype(str).str.strip() == '')
        filtered_df = df.loc[empty_mask]
    else:
        filtered_df = df

    return extract_emails_from_df(filtered_df)

def normalize_reference(value):
    return str(value).strip().upper()

def extract_single_email(value):
    if pd.isna(value):
        return ''
    matches = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", str(value))
    if matches:
        return matches[0].strip().lower()
    return ''

def build_clientes_email_map(df_clientes):
    if df_clientes is None or len(df_clientes) == 0:
        return {}

    normalized_columns = [str(c).strip().lower() for c in df_clientes.columns]
    if 'referencia' not in normalized_columns or 'correo de contacto' not in normalized_columns:
        return {}

    referencia_col = df_clientes.columns[normalized_columns.index('referencia')]
    correo_col = df_clientes.columns[normalized_columns.index('correo de contacto')]

    df_map = df_clientes[[referencia_col, correo_col]].copy()
    df_map['referencia_key'] = df_map[referencia_col].apply(normalize_reference)
    df_map['correo_clean'] = df_map[correo_col].apply(extract_single_email)
    df_map = df_map[df_map['correo_clean'] != '']

    return dict(zip(df_map['referencia_key'], df_map['correo_clean']))

def load_clientes_email_map_from_bd():
    try:
        config_path = bd_path('config.json')
        if not os.path.exists(config_path):
            return {}
        import json
        with open(config_path, 'r', encoding='utf-8') as file:
            config = json.load(file)
        clientes_file_name = config.get('CLIENTES')
        if not clientes_file_name:
            return {}
        clientes_path = bd_path(clientes_file_name)
        if not os.path.exists(clientes_path):
            return {}
        df_clientes = pd.read_excel(clientes_path, header=0)
        return build_clientes_email_map(df_clientes)
    except Exception:
        return {}

def get_no_voucher_mask(df):
    if df is None or len(df) == 0:
        return pd.Series(dtype=bool)

    candidate_cols = []
    for col in df.columns:
        normalized = str(col).strip().lower().replace('°', 'º')
        if normalized in ['nº documento', 'nºdocumento', 'asientos', 'voucher contable']:
            candidate_cols.append(col)
        elif 'documento' in normalized or 'voucher' in normalized or 'asiento' in normalized:
            candidate_cols.append(col)

    if candidate_cols:
        voucher_col = candidate_cols[0]
        voucher_values = df[voucher_col]
        return voucher_values.isna() | (voucher_values.astype(str).str.strip() == '')

    return pd.Series([True] * len(df), index=df.index)

def collect_emails_without_voucher_using_clientes(df_movimientos, clientes_email_map):
    if df_movimientos is None or len(df_movimientos) == 0 or not clientes_email_map:
        return []

    no_voucher_mask = get_no_voucher_mask(df_movimientos)
    if len(no_voucher_mask) == 0:
        return []

    if 'Correo' not in df_movimientos.columns:
        df_movimientos['Correo'] = ''

    if 'Referencia' in df_movimientos.columns:
        for index, row in df_movimientos.loc[no_voucher_mask].iterrows():
            ref_key = normalize_reference(row.get('Referencia', ''))
            if ref_key in clientes_email_map:
                df_movimientos.at[index, 'Correo'] = clientes_email_map[ref_key]

    emails = set()
    for value in df_movimientos.loc[no_voucher_mask, 'Correo'].dropna():
        clean_email = extract_single_email(value)
        if clean_email:
            emails.add(clean_email)
    return sorted(emails)


def build_voucher_data_without_pdf(df_movimientos, clientes_email_map=None):
    if df_movimientos is None or len(df_movimientos) == 0:
        return []

    no_voucher_mask = get_no_voucher_mask(df_movimientos)
    if len(no_voucher_mask) == 0:
        return []

    voucher_records_by_email = {}

    for index, row in df_movimientos.loc[no_voucher_mask].iterrows():
        referencia = str(row.get('Referencia', row.get('referencia', f'REF-{index}'))).strip()

        email = ''
        if 'Correo' in row and pd.notna(row['Correo']) and str(row['Correo']).strip():
            email = extract_single_email(row['Correo'])
        elif 'Correos' in row and pd.notna(row['Correos']) and str(row['Correos']).strip():
            email = extract_single_email(str(row['Correos']).split(',')[0])
        elif clientes_email_map and referencia.upper() in clientes_email_map:
            email = extract_single_email(clientes_email_map[referencia.upper()])

        if not email:
            continue

        nombre_cliente = ''
        for candidate_col in [
            'Nombre Cliente', 'Nombre', 'Cliente', 'Razón Social', 'Razon Social',
            'Titular', 'Nombre y Apellido'
        ]:
            if candidate_col in row and pd.notna(row[candidate_col]) and str(row[candidate_col]).strip():
                nombre_cliente = str(row[candidate_col]).strip()
                break

        if not nombre_cliente:
            local_part = email.split('@')[0]
            name_tokens = [token for token in re.split(r'[._\-]+', local_part) if token and not token.isdigit()]
            if name_tokens:
                nombre_cliente = ' '.join(token.capitalize() for token in name_tokens[:3])

        if not nombre_cliente:
            nombre_cliente = 'Cliente'

        fecha_value = row.get('Fecha', row.get('fecha', ''))
        if isinstance(fecha_value, pd.Timestamp):
            fecha_value = fecha_value.strftime('%d/%m/%Y')

        voucher_records_by_email[email] = {
            'email': email,
            'referencia': referencia,
            'monto': row.get('Monto', row.get('monto', 0)),
            'nombre_cliente': nombre_cliente,
            'fecha': fecha_value,
            'descripcion': row.get('Descripción operación', row.get('descripcion', 'Operación bancaria')),
            'operacion_numero': row.get('Operación - Número', row.get('operacion', 'N/A')),
            'filepath': ''
        }

    return list(voucher_records_by_email.values())


def is_company_name(nombre_cliente):
    nombre = str(nombre_cliente or '').strip()
    if not nombre:
        return False

    nombre_upper = nombre.upper()
    has_company_word = any(word in nombre_upper for word in COMPANY_KEYWORDS)
    has_digits = bool(re.search(r'\d', nombre_upper))
    return has_company_word or has_digits


def build_saludo_cliente(nombre_cliente):
    nombre = str(nombre_cliente or '').strip()
    if not nombre or nombre.lower() == 'cliente':
        return 'Estimado Cliente,'
    if is_company_name(nombre):
        return 'Estimado Cliente,'
    return f'Estimado {nombre},'


def build_voucher_email_text(saludo):
    return f"""{saludo}

Nos complace informarle que hemos recibido un abono en nuestra cuenta corriente a su nombre:

Se adjunta el voucher.

Para proceder con sus recibos, le invitamos a acceder a nuestra plataforma de Oficina Virtual Distriluz: https://servicios.distriluz.com.pe/oficinavirtual.

En esta plataforma, podrá registrarse como Cliente Empresa para gestionar la cancelación de los suministros afiliados a su representada y agregar otros suministros. Podrá adjuntar la constancia del pago o transferencia realizada para completar el proceso.

Esperamos que esta herramienta le sea de gran utilidad. Agradecemos su atención y quedamos a su disposición para cualquier consulta adicional.
"""


def build_voucher_email_html(saludo, voucher_info=None):
    referencia = str((voucher_info or {}).get('referencia', '') or 'N/A')
    fecha = str((voucher_info or {}).get('fecha', '') or 'N/A')
    operacion = str((voucher_info or {}).get('operacion_numero', '') or 'N/A')
    descripcion = str((voucher_info or {}).get('descripcion', '') or 'Operación bancaria')
    monto = (voucher_info or {}).get('monto', 0)

    try:
        if isinstance(monto, str):
            monto_clean = monto.replace(',', '').strip()
            monto_value = float(monto_clean) if monto_clean else 0.0
        else:
            monto_value = float(monto)
        monto_text = f"S/ {monto_value:,.2f}"
    except Exception:
        monto_text = str(monto or 'S/ 0.00')

    return f"""
<html>
  <body style="font-family: Arial, sans-serif; color: #222; line-height: 1.5; font-size: 12px;">
    <div style="max-width: 760px; margin: 0 auto; border: 1px solid #e6e6e6; border-radius: 8px; overflow: hidden;">
      <div style="background: #1e5da8; color: #fff; padding: 14px 18px; font-size: 18px; font-weight: 700;">
        ENSA - Voucher de Abono Recibido
      </div>

      <div style="padding: 18px;">
        <p style="margin: 0 0 14px 0;">{saludo}</p>

        <p style="margin: 0 0 12px 0;">Nos complace informarle que hemos recibido un abono en nuestra cuenta corriente a su nombre:</p>

        <div style="border: 1px solid #d9e2ef; background: #f7fbff; border-radius: 8px; padding: 12px; margin: 0 0 12px 0;">
          <div><strong>Referencia:</strong> {referencia}</div>
          <div><strong>Fecha:</strong> {fecha}</div>
          <div><strong>Número de operación:</strong> {operacion}</div>
          <div><strong>Descripción:</strong> {descripcion}</div>
          <div style="margin-top: 6px;"><strong>Monto:</strong> {monto_text}</div>
        </div>

        <p style="margin: 0 0 12px 0;">Se adjunta el voucher.</p>

        <p style="margin: 0 0 12px 0; font-size: 12px;">
          Para proceder con sus recibos, le invitamos a
          <span style="font-size: 12px; font-weight: 700; color: #1e5da8;">acceder a nuestra plataforma de Oficina Virtual Distriluz</span>:
          <a href="https://servicios.distriluz.com.pe/oficinavirtual" style="font-size: 12px; color: #1e5da8;">https://servicios.distriluz.com.pe/oficinavirtual</a>.
        </p>

        <p style="margin: 0 0 12px 0;">
          En esta plataforma, podrá registrarse como Cliente Empresa para gestionar la cancelación de los suministros afiliados a su representada y agregar otros suministros.
          <span style="font-size: 12px; font-weight: 700; color: #1e5da8;">Podrá adjuntar la constancia del pago o transferencia realizada para completar el proceso.</span>
        </p>

        <p style="margin: 0;">Esperamos que esta herramienta le sea de gran utilidad. Agradecemos su atención y quedamos a su disposición para cualquier consulta adicional.</p>
      </div>
    </div>
  </body>
</html>
"""

def save_emails_cache(emails):
    try:
        cache_path = files_path('emails_cache.json')
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump({'emails': emails}, f, ensure_ascii=False)
    except Exception:
        pass

def load_emails_cache():
    try:
        cache_path = files_path('emails_cache.json')
        if not os.path.exists(cache_path):
            return []
        with open(cache_path, 'r', encoding='utf-8') as f:
            payload = json.load(f)
        emails = payload.get('emails', [])
        if isinstance(emails, list):
            return emails
        return []
    except Exception:
        return []


def count_vouchers_in_folder():
    try:
        base_dir = vouchers_path()
        if not os.path.exists(base_dir):
            return 0
        total = 0
        for filename in os.listdir(base_dir):
            if filename.lower().endswith('.pdf'):
                total += 1
        return total
    except Exception:
        return 0


def find_latest_voucher_for_email(recipient_email):
    recipient = str(recipient_email or '').strip().lower()
    if not recipient:
        return None

    encoded_email = recipient.replace('@', '_').replace('.', '_')
    expected_suffix = f"_{encoded_email}.pdf"

    try:
        base_dir = vouchers_path()
        if not os.path.exists(base_dir):
            return None

        candidate_paths = []
        for filename in os.listdir(base_dir):
            filename_lower = filename.lower()
            if not filename_lower.endswith('.pdf'):
                continue
            if filename_lower.endswith(expected_suffix):
                candidate_paths.append(os.path.join(base_dir, filename))

        if not candidate_paths:
            return None

        latest_path = max(candidate_paths, key=os.path.getmtime)
        return {
            'email': recipient,
            'referencia': None,
            'filepath': latest_path,
            'monto': None
        }
    except Exception:
        return None

def render_correos_page(emails=None, mensaje_exito=None, page=1):
    if emails is None:
        emails = []
    page_size = 50
    total = len(emails)
    total_pages = max(1, (total + page_size - 1) // page_size)
    if page < 1:
        page = 1
    if page > total_pages:
        page = total_pages
    start = (page - 1) * page_size
    end = start + page_size
    page_emails = emails[start:end]
    
    # Obtener información de vouchers disponibles
    vouchers_generados = session.get('vouchers_generados', [])
    total_vouchers = len(vouchers_generados)
    secure_smtp = load_secure_smtp_credentials()
    smtp_config = {
        'sender': secure_smtp.get('sender', ''),
        'smtp_host': secure_smtp.get('smtp_host', '') or 'owa.fonafe.gob.pe',
        'smtp_port': secure_smtp.get('smtp_port', '') or '587',
        'smtp_security': secure_smtp.get('smtp_security', '') or 'starttls'
    }
    google_config = load_google_config()
    general_settings = load_general_settings()
    photo_path = _profile_photo_path()
    has_profile_photo = os.path.exists(photo_path)
    profile_photo_version = general_settings.get('perfil', {}).get('foto_version', 0)
    
    return render_template(
        'correos.html',
        emails=emails,
        page_emails=page_emails,
        page=page,
        total_pages=total_pages,
        page_size=page_size,
        total_emails=total,
        mensaje_exito=mensaje_exito,
        total_vouchers=total_vouchers,
        vouchers_generados=vouchers_generados,
        smtp_config=smtp_config,
        google_config=google_config,
        general_settings=general_settings,
        has_profile_photo=has_profile_photo,
        profile_photo_url=f"/foto_perfil_actual?v={profile_photo_version}"
    )

def extract_emails_from_excel_upload(file_storage):
    emails = set()
    email_regex = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
    try:
        file_storage.stream.seek(0)
        workbook = load_workbook(filename=BytesIO(file_storage.read()), data_only=True)
        file_storage.stream.seek(0)

        for ws in workbook.worksheets:
            for row in ws.iter_rows():
                for cell in row:
                    if cell.value is not None:
                        for m in email_regex.findall(str(cell.value)):
                            emails.add(m)
                    if cell.hyperlink and cell.hyperlink.target:
                        target = str(cell.hyperlink.target)
                        if target.lower().startswith('mailto:'):
                            target = target[7:]
                        target = target.split('?', 1)[0]
                        for m in email_regex.findall(target):
                            emails.add(m)
    except Exception:
        # If the file cannot be opened with openpyxl (e.g., unsupported format), fallback to df extraction only
        try:
            file_storage.stream.seek(0)
        except Exception:
            pass
    return sorted(emails)

@app.route('/', methods=['POST','GET'])
def home():
    if request.method == 'POST':
        files = request.files.getlist('file')
        filtered_files = [x for x in files if x.filename!=""]
        if len(filtered_files) <= 1:
            return render_template('home.html', error_message= 'Debe subir por lo menos un archivo.')
        else:
            try:
                accountService = AccountService()    
                transferService = TransferService()
                interbankService = InterbankService()
                providerService = ProviderService()
                for file in files:
                    nombre =  file.filename.upper() 
                    if (nombre != ""):
                        if CUENTA in nombre:
                            accountService.process_movements(file)
                        elif TRANSFER in nombre:  
                            transferService.setMovimientos(accountService.movimientos)
                            transferService.process_transfers(file)
                        elif INTERBANK in nombre:
                            interbankService.setMovimientos(accountService.movimientos)
                            interbankService.process_interbanks(file)
                        elif PROVIDERS in nombre:
                            providerService.setMovimientos(accountService.movimientos)
                            providerService.process_providers(file)    
                        else:
                            raise Exception("Archivo no ubicado: "+nombre)    
                guardaMovimientos(accountService.movimientos)
                guardaRecaudos(accountService.recaudos)
                resumen = {"movements": accountService.error, "providers": providerService.error, "transfers": transferService.error, "interbanks": interbankService.error}
               
                return render_template("response.html", data= resumen) 
    
                #return redirect(url_for('upload'))
            except Exception as e:
                error_message = str(e)
                return render_template('home.html', error_message= error_message)
    else:
        return render_template('home.html')

def guardaMovimientos(movimientos):
    movimientos["Fecha"] = pd.to_datetime(movimientos["Fecha"], format="%d/%m/%Y").dt.strftime("%d/%m/%Y")
    excel_file = BytesIO()
    movimientos.to_excel(excel_file, index=False)
    excel_file.seek(0)
    workbook = openpyxl.load_workbook(excel_file)
    worksheet = workbook.active 
    worksheet.column_dimensions["A"].width = 20  
    worksheet.column_dimensions["C"].width = 30  
    worksheet.column_dimensions["K"].width = 40  
    worksheet.column_dimensions["L"].width = 40  
    worksheet.column_dimensions["M"].width = 35 
    worksheet.column_dimensions["N"].width = 40
    for col in worksheet.iter_cols(min_row=1, max_row=1):
        header_value = col[0].value
        if header_value == 'Correo':
            worksheet.column_dimensions[col[0].column_letter].width = 42
            break
    ruta_archivo = files_path('movimientos.xlsx')
    workbook.save(ruta_archivo)

def guardaRecaudos(recaudos):
    excel_file = BytesIO()
    recaudos.to_excel(excel_file, index=False)
    excel_file.seek(0)
    workbook = openpyxl.load_workbook(excel_file)
    worksheet = workbook.active 
    worksheet.column_dimensions["A"].width = 15  
    worksheet.column_dimensions["B"].width = 50  
    worksheet.column_dimensions["C"].width = 25  
    worksheet.column_dimensions["D"].width = 10  
    worksheet.column_dimensions["E"].width = 15 
    worksheet.column_dimensions["F"].width = 10 

    ruta_archivo = files_path('recaudos.xlsx')
    workbook.save(ruta_archivo)

@app.route('/basedatos', methods=['POST','GET'])
def basedatos():
    if request.method == 'POST':
        files    = request.files.getlist('file')
        
        try:
            filtered_files = [x for x in files if x.filename!=""]
                        
            if len(filtered_files) < 1:
                return render_template('base-datos.html', error_message= 'Debe subir por lo menos un archivo.')

            nombres = [f.filename.upper() for f in filtered_files]
            valid_patterns = ['RECAUDO', 'PREPAGO', 'TRABAJADOR', 'CLIENTE']
            invalid_files = [n for n in nombres if not any(pattern in n for pattern in valid_patterns)]
            if invalid_files:
                return render_template('base-datos.html', error_message='Archivo(s) no reconocido(s): ' + ', '.join(invalid_files))

            mensaje_exito = 'Base de datos actualizada correctamente.'
            
            base_datos_service = BaseDatosService()  
            base_datos_service.GuardarAchivos(files)  
            return render_template('base-datos.html',mensaje_exito=mensaje_exito)
                
        except Exception as e:
            error_message = str(e)
            return render_template('base-datos.html', error_message= error_message)

    else:
        nohay = 'Archivo subido correctamente.'
        return render_template('base-datos.html')
    
@app.route('/asiento', methods=['POST'])
def asiento_procesar():
    logging.error('asiento_procesar: start')
    files = request.files.getlist('file')
    logging.error('asiento_procesar: received %d files', len(files))
    filtered_files = [x for x in files if x.filename!=""]
    logging.error('asiento_procesar: filtered %d files', len(filtered_files))
    if len(filtered_files) <= 1:
        logging.error('asiento_procesar: not enough files, returning form')
        return render_template('asiento.html', error_message= 'Debe subir ambos archivo.')
    else:
        try:
            asientoService = AsientoService()    
            # detect files: movimientos y asientos
            movimientosfile = None
            asientosfile = None
            for file in files:
                nombre =  file.filename.upper()
                if (nombre != ""):
                    if MOVIMIENTOS in nombre or "MOVIMIENTO" in nombre:
                        movimientosfile = file
                    elif ASIENTO in nombre:
                        asientosfile = file
                    else:
                        # ignore unknown files for now
                        pass

            if movimientosfile is None or asientosfile is None:
                raise Exception('Faltan archivos requeridos: Movimientos o Asientos')

            clientes_email_map = load_clientes_email_map_from_bd()

            asientoService.conciliar(movimientosfile, asientosfile)
            #solo si hay asientos se completa en el cache
            if asientoService.df_movimientos is not None:
                emails = collect_emails_without_voucher_using_clientes(asientoService.df_movimientos, clientes_email_map)
                guardaAsientos(asientoService.df_movimientos)
                
                # PREPARAR INFORMACIÓN DE VOUCHERS EN HTML (SIN GENERAR PDF)
                try:
                    vouchers_generados = build_voucher_data_without_pdf(
                        asientoService.df_movimientos, 
                        clientes_email_map
                    )
                    session['vouchers_generados'] = vouchers_generados
                    logging.info(f'Vouchers HTML preparados: {len(vouchers_generados)}')
                except Exception as ve:
                    logging.error(f'Error preparando vouchers HTML: {str(ve)}')
                    session['vouchers_generados'] = []
                
                # guardar en session para uso posterior y redirigir al flujo de correos
                sorted_emails = sorted(set(emails))
                session['asiento_emails'] = sorted_emails
                save_emails_cache(sorted_emails)
                if len(sorted_emails) == 0:
                    session['asiento_email_warning'] = 'No se encontraron correos para líneas sin voucher. Verifique Referencia y CORREO DE CONTACTO en Base de Datos > Clientes.'
                else:
                    session.pop('asiento_email_warning', None)
                # guardaAsientos ya escribió files/asientos.xlsx, descargamos directamente
                ruta_archivo = files_path('asientos.xlsx')
                return send_file(ruta_archivo, as_attachment=True, download_name='asientos.xlsx')
            else: 
                #si hubiera error se pinta la misma pagina y no se redirecciona
                return render_template('asiento.html', error_message= 'No se encontro ningun asiento en el proceso')       
            
        except Exception as e:
            error_message = str(e)
            logging.error('asiento_procesar: exception: %s', error_message)
            return render_template('asiento.html', error_message= error_message)
    # Fallback: ensure the view always returns a response
    logging.error('asiento_procesar: reached end of function without explicit return')
    return render_template('asiento.html', error_message='Error inesperado en el procesamiento')



@app.route('/asiento', methods=['GET'])
def asiento_get():
    return render_template('asiento.html')


@app.route('/correos', methods=['GET','POST'])
def correos():
    sess_emails = session.get('asiento_emails', [])
    if not sess_emails:
        sess_emails = load_emails_cache()
        if sess_emails:
            session['asiento_emails'] = sess_emails
    warning_message = session.pop('asiento_email_warning', None)
    config_message = session.pop('config_message', None)
    page_message = config_message or warning_message

    try:
        page = int(request.args.get('page', '1'))
    except ValueError:
        page = 1
    if page < 1:
        page = 1

    return render_correos_page(emails=sess_emails, mensaje_exito=page_message, page=page)


@app.route('/gestor_contrasenas', methods=['GET'])
def gestor_contrasenas():
    add_account_activity('Gestor de contraseñas', 'Apertura de panel')
    return render_template('gestor_contrasenas.html')


@app.route('/mi_contrasena', methods=['GET'])
def mi_contrasena():
    add_account_activity('Mi contraseña', 'Apertura de panel')
    message = session.pop('password_message', None)
    updated_at = get_account_password_updated_at()
    return render_template('mi_contrasena.html', password_updated_at=updated_at, message=message)


@app.route('/mi_contrasena/guardar', methods=['POST'])
def mi_contrasena_guardar():
    new_password = request.form.get('new_password', '').strip()
    confirm_password = request.form.get('confirm_password', '').strip()

    if len(new_password) < 6:
        session['password_message'] = 'La contraseña debe tener al menos 6 caracteres.'
        return redirect(url_for('mi_contrasena'))

    if new_password != confirm_password:
        session['password_message'] = 'La confirmación no coincide con la contraseña.'
        return redirect(url_for('mi_contrasena'))

    try:
        save_account_password(new_password)
        add_account_activity('Mi contraseña', 'Contraseña actualizada')
        session['password_message'] = '✅ Contraseña actualizada correctamente.'
    except Exception as ex:
        session['password_message'] = f'Error guardando contraseña: {ex}'

    return redirect(url_for('mi_contrasena'))


@app.route('/dispositivos', methods=['GET'])
def dispositivos():
    add_account_activity('Dispositivos', 'Apertura de panel')
    payload = load_account_features()
    current_session_id = session.get('account_session_id', '')

    grouped = {}
    sessions_list = payload.get('sessions', [])
    now_value = pd.Timestamp.now()
    for item in sessions_list:
        os_family = str(item.get('os_family', 'Otro')).strip() or 'Otro'
        grouped.setdefault(os_family, [])

        last_seen_text = str(item.get('last_seen', '')).strip()
        inactive_days = 0
        if last_seen_text:
            try:
                parsed_last_seen = pd.to_datetime(last_seen_text, format='%d/%m/%Y %H:%M:%S', errors='coerce')
                if pd.isna(parsed_last_seen):
                    raise ValueError('Fecha inválida')
                inactive_days = max((now_value - parsed_last_seen).days, 0)
            except Exception:
                inactive_days = 0

        grouped[os_family].append({
            'session_id': str(item.get('session_id', '')).strip(),
            'device_type': str(item.get('device_type', '')).strip() or 'Equipo',
            'ip': str(item.get('ip', '')).strip() or 'desconocido',
            'last_seen': _relative_time_label(last_seen_text),
            'user_agent': str(item.get('user_agent', '')).strip(),
            'active': bool(item.get('active', False)),
            'inactive_days': inactive_days,
            'is_current': str(item.get('session_id', '')).strip() == current_session_id
        })

    message = session.pop('devices_message', None)
    return render_template(
        'dispositivos.html',
        devices=payload.get('devices', []),
        sessions_grouped=grouped,
        total_sessions=len(sessions_list),
        message=message
    )


@app.route('/dispositivos/agregar', methods=['POST'])
def dispositivos_agregar():
    name = request.form.get('name', '').strip()
    location = request.form.get('location', '').strip()

    if not name:
        session['devices_message'] = 'Indica el nombre del dispositivo.'
        return redirect(url_for('dispositivos'))

    payload = load_account_features()
    devices = payload.get('devices', [])
    next_id = max([int(d.get('id', 0) or 0) for d in devices], default=0) + 1
    devices.append({
        'id': next_id,
        'name': name,
        'location': location or 'Sin ubicación',
        'active': True
    })
    payload['devices'] = devices
    save_account_features(payload)

    add_account_activity('Dispositivos', f'Dispositivo agregado: {name}')
    session['devices_message'] = '✅ Dispositivo agregado correctamente.'
    return redirect(url_for('dispositivos'))


@app.route('/dispositivos/eliminar/<int:device_id>', methods=['POST'])
def dispositivos_eliminar(device_id):
    payload = load_account_features()
    devices = payload.get('devices', [])
    kept = [d for d in devices if int(d.get('id', 0) or 0) != int(device_id)]

    if len(kept) == len(devices):
        session['devices_message'] = 'No se encontró el dispositivo seleccionado.'
        return redirect(url_for('dispositivos'))

    payload['devices'] = kept
    save_account_features(payload)
    add_account_activity('Dispositivos', f'Dispositivo eliminado: id={device_id}')
    session['devices_message'] = '✅ Dispositivo eliminado correctamente.'
    return redirect(url_for('dispositivos'))


@app.route('/dispositivos/desconectar/<session_id_value>', methods=['POST'])
def dispositivos_desconectar(session_id_value):
    payload = load_account_features()
    sessions_list = payload.get('sessions', [])

    changed = False
    for item in sessions_list:
        if str(item.get('session_id', '')).strip() == str(session_id_value or '').strip():
            item['active'] = False
            changed = True
            break

    if changed:
        payload['sessions'] = sessions_list
        save_account_features(payload)
        add_account_activity('Dispositivos', f'Sesión desconectada: {session_id_value}')
        session['devices_message'] = '✅ Sesión desconectada correctamente.'
    else:
        session['devices_message'] = 'No se encontró la sesión seleccionada.'

    return redirect(url_for('dispositivos'))


@app.route('/mi_actividad', methods=['GET'])
def mi_actividad():
    payload = load_account_features()
    activity = payload.get('activity', [])

    query_text = request.args.get('q', '').strip().lower()
    action_filter = request.args.get('action', '').strip().lower()

    filtered = []
    for item in activity:
        action_value = str(item.get('action', '')).strip()
        detail_value = str(item.get('detail', '')).strip()
        timestamp_value = str(item.get('timestamp', '')).strip()

        haystack = f"{action_value} {detail_value} {timestamp_value}".lower()
        if query_text and query_text not in haystack:
            continue
        if action_filter and action_filter not in action_value.lower():
            continue
        filtered.append(item)

    action_types = sorted({str(item.get('action', '')).strip() for item in activity if str(item.get('action', '')).strip()})
    total = len(activity)
    total_filtered = len(filtered)

    return render_template(
        'mi_actividad.html',
        activity=filtered,
        total=total,
        total_filtered=total_filtered,
        action_types=action_types,
        current_query=query_text,
        current_action=action_filter
    )


@app.route('/mi_actividad/limpiar', methods=['POST'])
def mi_actividad_limpiar():
    mode = request.form.get('mode', '').strip().lower() or 'all'
    if mode == 'last50':
        clear_account_activity(mode='last', keep_last=50)
        add_account_activity('Mi actividad', 'Se conservaron solo 50 registros recientes')
    else:
        clear_account_activity(mode='all')
        add_account_activity('Mi actividad', 'Actividad eliminada por usuario')

    return redirect(url_for('mi_actividad'))


@app.route('/informacion_personal', methods=['GET'])
def informacion_personal():
    add_account_activity('Información personal', 'Apertura de panel')
    settings = load_general_settings()
    perfil = settings.get('perfil', {})
    message = session.pop('personal_info_message', None)
    photo_version = int(perfil.get('foto_version', 0) or 0)
    edit_field = request.args.get('edit', '').strip().lower()

    return render_template(
        'informacion_personal.html',
        perfil=perfil,
        sender_email=get_connected_account_email(),
        has_profile_photo=os.path.exists(_profile_photo_path()),
        profile_photo_url=f"/foto_perfil_actual?v={photo_version}",
        message=message,
        edit_field=edit_field
    )


@app.route('/informacion_personal/guardar', methods=['POST'])
def informacion_personal_guardar():
    settings = load_general_settings()
    perfil = settings.get('perfil', {})

    def update_field(field_name, default_value=''):
        if field_name in request.form:
            return request.form.get(field_name, '').strip()
        return str(perfil.get(field_name, default_value)).strip()

    perfil['nombre_mostrar'] = update_field('nombre_mostrar')
    perfil['genero'] = update_field('genero')
    perfil['correo_personal'] = update_field('correo_personal')
    perfil['correo_alterno'] = update_field('correo_alterno')
    perfil['telefono'] = update_field('telefono')
    perfil['fecha_nacimiento'] = update_field('fecha_nacimiento')
    perfil['idioma'] = update_field('idioma', 'Español (España)') or 'Español (España)'
    perfil['direccion_casa'] = update_field('direccion_casa')
    perfil['direccion_trabajo'] = update_field('direccion_trabajo')
    perfil['otras_direcciones'] = update_field('otras_direcciones')

    settings['perfil'] = perfil

    try:
        save_general_settings(settings)
        add_account_activity('Información personal', 'Datos personales actualizados')
        session['personal_info_message'] = '✅ Información personal guardada correctamente.'
    except Exception as ex:
        session['personal_info_message'] = f'Error guardando información personal: {ex}'

    return redirect(url_for('informacion_personal'))


@app.route('/correo_electronico', methods=['GET'])
def correo_electronico():
    add_account_activity('Correo electrónico', 'Apertura de panel')
    secure_smtp = load_secure_smtp_credentials()
    message = session.pop('email_settings_message', None)
    return render_template(
        'correo_electronico.html',
        sender=secure_smtp.get('sender', '') or get_connected_account_email(),
        smtp_host=secure_smtp.get('smtp_host', '') or 'owa.fonafe.gob.pe',
        smtp_port=secure_smtp.get('smtp_port', '') or '587',
        smtp_security=secure_smtp.get('smtp_security', '') or 'starttls',
        message=message
    )


@app.route('/correo_electronico/guardar', methods=['POST'])
def correo_electronico_guardar():
    existing = load_secure_smtp_credentials()
    sender = request.form.get('sender', '').strip() or existing.get('sender', '').strip()
    password = request.form.get('password', '').strip() or existing.get('password', '').strip()

    if not sender or not password:
        session['email_settings_message'] = 'Completa usuario y contraseña.'
        return redirect(url_for('correo_electronico'))

    try:
        save_secure_smtp_credentials(
            sender,
            password,
            smtp_host_value=existing.get('smtp_host', '') or 'owa.fonafe.gob.pe',
            smtp_port_value=existing.get('smtp_port', '') or '587',
            smtp_security_value=existing.get('smtp_security', '') or 'starttls'
        )
        sync_email_config_to_modules(sender)
        add_account_activity('Correo electrónico', f'Credenciales actualizadas para {sender}')
        session['email_settings_message'] = '✅ Configuración de correo actualizada correctamente.'
    except Exception as ex:
        session['email_settings_message'] = f'Error guardando correo: {ex}'

    return redirect(url_for('correo_electronico'))


@app.route('/configurar_correo', methods=['POST'])
def configurar_correo():
    emails = session.get('asiento_emails', [])

    existing = load_secure_smtp_credentials()
    sender = request.form.get('sender', '').strip() or existing.get('sender', '').strip()
    password = request.form.get('password', '').strip() or existing.get('password', '').strip()
    smtp_host = request.form.get('smtp_host', '').strip() or existing.get('smtp_host', '').strip() or 'owa.fonafe.gob.pe'
    smtp_port = request.form.get('smtp_port', '').strip() or existing.get('smtp_port', '').strip() or '587'
    smtp_security = request.form.get('smtp_security', '').strip().lower() or existing.get('smtp_security', '').strip().lower() or 'starttls'

    if smtp_security not in ('ssl', 'starttls', 'auto'):
        smtp_security = 'starttls'

    if not sender or not password:
        return render_correos_page(
            emails=emails,
            mensaje_exito='Completa remitente y clave para guardar la configuración de correo.',
            page=1
        )

    try:
        save_secure_smtp_credentials(
            sender,
            password,
            smtp_host_value=smtp_host,
            smtp_port_value=smtp_port,
            smtp_security_value=smtp_security
        )
        sync_email_config_to_modules(sender)
        add_account_activity('Configuración de correo', f'Credenciales guardadas para {sender}')
        session['config_message'] = '✅ Configuración de correo guardada correctamente.'
    except Exception as ex:
        session['config_message'] = f'Error guardando configuración de correo: {ex}'

    return redirect(url_for('correos'))


@app.route('/configurar_google', methods=['POST'])
def configurar_google():
    existing = load_google_config()

    project_id = request.form.get('project_id', '').strip()
    client_id = request.form.get('client_id', '').strip()
    client_secret = request.form.get('client_secret', '').strip() or existing.get('client_secret', '')
    redirect_uri = request.form.get('redirect_uri', '').strip()
    scopes = request.form.get('scopes', '').strip() or 'openid email profile'
    api_key = request.form.get('api_key', '').strip() or existing.get('api_key', '')
    enabled = request.form.get('google_enabled', '').strip().lower() == 'on'

    if not client_id or not redirect_uri:
        session['config_message'] = 'Completa Client ID y Redirect URI para guardar Google.'
        return redirect(url_for('correos'))

    try:
        save_google_config({
            'project_id': project_id,
            'client_id': client_id,
            'client_secret': client_secret,
            'redirect_uri': redirect_uri,
            'scopes': scopes,
            'api_key': api_key,
            'enabled': enabled
        })
        session['config_message'] = '✅ Configuración Google guardada correctamente.'
    except Exception as ex:
        session['config_message'] = f'Error guardando configuración Google: {ex}'

    return redirect(url_for('correos'))


@app.route('/configuracion_inicio', methods=['POST'])
def configuracion_inicio():
    settings = load_general_settings()
    settings['inicio']['mostrar_resumen'] = request.form.get('mostrar_resumen', '').strip().lower() == 'on'
    settings['inicio']['notificaciones_activas'] = request.form.get('notificaciones_activas', '').strip().lower() == 'on'

    try:
        save_general_settings(settings)
        add_account_activity('Inicio', 'Preferencias de inicio actualizadas')
        session['config_message'] = '✅ Configuración de Inicio guardada correctamente.'
    except Exception as ex:
        session['config_message'] = f'Error guardando Inicio: {ex}'

    return redirect(url_for('correos'))


@app.route('/configuracion_perfil', methods=['POST'])
def configuracion_perfil():
    settings = load_general_settings()
    settings['perfil']['nombre_mostrar'] = request.form.get('nombre_mostrar', '').strip()
    settings['perfil']['cargo'] = request.form.get('cargo', '').strip()
    settings['perfil']['telefono'] = request.form.get('telefono', '').strip()
    settings['perfil']['genero'] = request.form.get('genero', settings['perfil'].get('genero', '')).strip()
    settings['perfil']['correo_personal'] = request.form.get('correo_personal', settings['perfil'].get('correo_personal', '')).strip()
    settings['perfil']['correo_alterno'] = request.form.get('correo_alterno', settings['perfil'].get('correo_alterno', '')).strip()
    settings['perfil']['fecha_nacimiento'] = request.form.get('fecha_nacimiento', settings['perfil'].get('fecha_nacimiento', '')).strip()
    settings['perfil']['idioma'] = request.form.get('idioma', settings['perfil'].get('idioma', 'Español (España)')).strip()
    settings['perfil']['direccion_casa'] = request.form.get('direccion_casa', settings['perfil'].get('direccion_casa', '')).strip()
    settings['perfil']['direccion_trabajo'] = request.form.get('direccion_trabajo', settings['perfil'].get('direccion_trabajo', '')).strip()
    settings['perfil']['otras_direcciones'] = request.form.get('otras_direcciones', settings['perfil'].get('otras_direcciones', '')).strip()

    try:
        save_general_settings(settings)
        add_account_activity('Perfil', f"Perfil actualizado: {settings['perfil']['nombre_mostrar']}")
        session['config_message'] = '✅ Configuración de Perfil guardada correctamente.'
    except Exception as ex:
        session['config_message'] = f'Error guardando Perfil: {ex}'

    return redirect(url_for('correos'))


@app.route('/configuracion_seguridad', methods=['POST'])
def configuracion_seguridad():
    settings = load_general_settings()
    settings['seguridad']['doble_factor'] = request.form.get('doble_factor', '').strip().lower() == 'on'
    settings['seguridad']['cerrar_sesiones'] = request.form.get('cerrar_sesiones', '').strip().lower() == 'on'

    try:
        save_general_settings(settings)
        add_account_activity('Seguridad', 'Preferencias de seguridad actualizadas')
        session['config_message'] = '✅ Configuración de Seguridad guardada correctamente.'
    except Exception as ex:
        session['config_message'] = f'Error guardando Seguridad: {ex}'

    return redirect(url_for('correos'))


@app.route('/configuracion_privacidad', methods=['POST'])
def configuracion_privacidad():
    settings = load_general_settings()
    settings['privacidad']['compartir_datos'] = request.form.get('compartir_datos', '').strip().lower() == 'on'
    settings['privacidad']['analytics'] = request.form.get('analytics', '').strip().lower() == 'on'

    try:
        save_general_settings(settings)
        add_account_activity('Privacidad', 'Preferencias de privacidad actualizadas')
        session['config_message'] = '✅ Configuración de Privacidad guardada correctamente.'
    except Exception as ex:
        session['config_message'] = f'Error guardando Privacidad: {ex}'

    return redirect(url_for('correos'))


@app.route('/configuracion_contactos', methods=['POST'])
def configuracion_contactos():
    settings = load_general_settings()
    settings['contactos']['correo_soporte'] = request.form.get('correo_soporte', '').strip()
    settings['contactos']['correo_copia'] = request.form.get('correo_copia', '').strip()

    try:
        save_general_settings(settings)
        add_account_activity('Contactos', 'Contactos de configuración actualizados')
        session['config_message'] = '✅ Configuración de Contactos guardada correctamente.'
    except Exception as ex:
        session['config_message'] = f'Error guardando Contactos: {ex}'

    return redirect(url_for('correos'))


@app.route('/subir_foto_perfil', methods=['POST'])
def subir_foto_perfil():
    image_file = request.files.get('profile_image')
    next_url = request.form.get('next', '').strip()
    if image_file is None or image_file.filename == '':
        session['config_message'] = 'Selecciona una imagen para subir.'
        if next_url == 'informacion_personal':
            return redirect(url_for('informacion_personal'))
        return redirect(url_for('correos'))

    if not _allowed_image_extension(image_file.filename):
        session['config_message'] = 'Formato no válido. Usa PNG, JPG, JPEG o WEBP.'
        if next_url == 'informacion_personal':
            return redirect(url_for('informacion_personal'))
        return redirect(url_for('correos'))

    try:
        os.makedirs(_profile_photo_dir(), exist_ok=True)
        photo_path = _profile_photo_path()
        image_file.save(photo_path)

        settings = load_general_settings()
        current_version = int(settings.get('perfil', {}).get('foto_version', 0) or 0)
        settings['perfil']['foto_version'] = current_version + 1
        save_general_settings(settings)
        add_account_activity('Perfil', 'Foto de perfil actualizada')

        session['config_message'] = '✅ Foto de perfil actualizada correctamente.'
    except Exception as ex:
        session['config_message'] = f'Error subiendo foto de perfil: {ex}'

    if next_url == 'informacion_personal':
        if 'config_message' in session:
            session['personal_info_message'] = session.pop('config_message')
        return redirect(url_for('informacion_personal'))

    return redirect(url_for('correos'))


@app.route('/foto_perfil_actual', methods=['GET'])
def foto_perfil_actual():
    photo_path = _profile_photo_path()
    if not os.path.exists(photo_path):
        return '', 404
    return send_file(photo_path)

@app.route('/upload', methods=['POST','GET'])
def upload():
    
    if request.method == 'POST':
        ruta_archivo = files_path('movimientos.xlsx')
        return send_file(ruta_archivo, as_attachment=True, download_name="movimientos.xlsx")
   
    

@app.route('/download_recaudos', methods=['POST'])
def download_recaudos():
    ruta_archivo = files_path('recaudos.xlsx')
    return send_file(ruta_archivo, as_attachment=True, download_name="recaudos.xlsx")

def guardaAsientos(movimientosAsientos):
    movimientosAsientos["Fecha"] = pd.to_datetime(movimientosAsientos["Fecha"], format="%d/%m/%Y").dt.strftime("%d/%m/%Y")
    excel_file = BytesIO()
    movimientosAsientos.to_excel(excel_file, index=False)
    excel_file.seek(0)
    workbook = openpyxl.load_workbook(excel_file)
    worksheet = workbook.active 
    worksheet.column_dimensions["A"].width = 20  
    worksheet.column_dimensions["C"].width = 30  
    worksheet.column_dimensions["K"].width = 40  
    worksheet.column_dimensions["L"].width = 40  
    worksheet.column_dimensions["M"].width = 35 
    worksheet.column_dimensions["N"].width = 28 

    ruta_archivo = files_path('asientos.xlsx')
    workbook.save(ruta_archivo)


@app.route('/download_asientos', methods=['POST'])
def dowload_asientos():
    ruta_archivo = files_path('asientos.xlsx')
    return send_file(ruta_archivo, as_attachment=True, download_name="Asiento.xlsx")


@app.route('/send_emails', methods=['POST'])
def send_emails():
    emails = session.get('asiento_emails', [])
    if not emails:
        return render_correos_page(emails=[], mensaje_exito='No hay correos para enviar', page=1)

    manual_confirm = request.form.get('manual_confirm', '').strip().lower()
    if manual_confirm != 'yes':
        return render_correos_page(
            emails=emails,
            mensaje_exito='Marca la confirmación de envío manual antes de enviar correos.',
            page=1
        )

    selected_emails = request.form.getlist('selected_emails')
    if not selected_emails:
        return render_correos_page(
            emails=emails,
            mensaje_exito='Selecciona al menos un correo para enviar. No se envió nada automáticamente.',
            page=1
        )

    emails_to_send = sorted(set(selected_emails))
    
    # Obtener vouchers generados de la sesión
    vouchers_generados = session.get('vouchers_generados', [])
    
    # Log de diagnóstico
    logging.info(f'Total vouchers en sesión: {len(vouchers_generados)}')
    for v in vouchers_generados:
        logging.info(f"Voucher disponible: email={v.get('email')}, ref={v.get('referencia')}, path={v.get('filepath')}")
    
    # Crear diccionario para buscar voucher por email
    vouchers_por_email = {}
    for voucher in vouchers_generados:
        email = voucher.get('email', '').strip().lower()
        if email:
            vouchers_por_email[email] = voucher
            logging.info(f"Voucher indexado para: {email}")
    
    # Configuración para Microsoft 365 (Outlook)
    secure_smtp = load_secure_smtp_credentials()
    sender = os.environ.get('OUTLOOK_SENDER', '').strip() or secure_smtp.get('sender', '').strip()
    password = os.environ.get('OUTLOOK_PASSWORD', '').strip() or secure_smtp.get('password', '').strip()
    subject = os.environ.get('OUTLOOK_SUBJECT', 'Confirmación de Abono Recibido - DISTRILUZ ENSA')
    smtp_host = os.environ.get('OUTLOOK_SMTP_HOST', '').strip() or secure_smtp.get('smtp_host', '').strip() or 'owa.fonafe.gob.pe'
    smtp_port_raw = os.environ.get('OUTLOOK_SMTP_PORT', '').strip() or secure_smtp.get('smtp_port', '').strip() or '587'
    smtp_security = (os.environ.get('OUTLOOK_SMTP_SECURITY', '').strip() or secure_smtp.get('smtp_security', '').strip() or 'starttls').lower()
    try:
        smtp_port = int(smtp_port_raw)
    except ValueError:
        smtp_port = 587
        logging.warning(f"OUTLOOK_SMTP_PORT inválido ('{smtp_port_raw}'). Usando 587 por defecto.")

    if smtp_security not in ('ssl', 'starttls', 'auto'):
        logging.warning(f"OUTLOOK_SMTP_SECURITY inválido ('{smtp_security}'). Usando 'starttls'.")
        smtp_security = 'starttls'

    if not sender or not password:
        return render_correos_page(
            emails=emails,
            mensaje_exito='Falta configurar correo remitente y clave SMTP (archivo seguro o variables de entorno). No se envió nada.',
            page=1
        )

    sent_ok = []
    sent_fail = []
    sent_with_voucher_html = []
    
    try:
        # Conexión SMTP configurable (ssl | starttls | auto)
        used_security = smtp_security
        smtp_conn = None
        try:
            if smtp_security == 'ssl':
                smtp_conn = smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=30)
                smtp_conn.ehlo()
                used_security = 'ssl'
            else:
                smtp_conn = smtplib.SMTP(smtp_host, smtp_port, timeout=30)
                smtp_conn.ehlo()
                smtp_conn.starttls()
                smtp_conn.ehlo()
                used_security = 'starttls'
        except Exception as conn_ex:
            wrong_version = 'WRONG_VERSION_NUMBER' in str(conn_ex).upper()
            can_retry_starttls = smtp_security in ('ssl', 'auto') and wrong_version
            if can_retry_starttls:
                logging.warning(
                    f"SMTP SSL no compatible en {smtp_host}:{smtp_port} ({conn_ex}). Reintentando con STARTTLS."
                )
                smtp_conn = smtplib.SMTP(smtp_host, smtp_port, timeout=30)
                smtp_conn.ehlo()
                smtp_conn.starttls()
                smtp_conn.ehlo()
                used_security = 'starttls'
            else:
                raise

        with smtp_conn as smtp:
            try:
                smtp.login(sender, password)
            except smtplib.SMTPAuthenticationError as auth_ex:
                corrected_sender, sender_corrected = normalize_sender_email(sender)
                if sender_corrected and corrected_sender != sender:
                    try:
                        smtp.login(corrected_sender, password)
                        sender = corrected_sender
                        logging.warning(
                            'Se corrigió remitente con dominio typo para autenticación SMTP: %s',
                            sender
                        )
                    except smtplib.SMTPAuthenticationError:
                        return render_correos_page(
                            emails=emails,
                            mensaje_exito=(
                                'Error de autenticación SMTP (535). '
                                'El usuario guardado tiene dominio typo. Usa tu correo con @distriluz.com.pe en las credenciales SMTP. '
                                'No se envió ningún correo.'
                            ),
                            page=1
                        )
                else:
                    return render_correos_page(
                        emails=emails,
                        mensaje_exito=(
                            'Error de autenticación SMTP (535). '
                            'Verifica usuario/clave en credenciales SMTP o confirma con TI que la cuenta tenga SMTP AUTH habilitado en owa.fonafe.gob.pe. '
                            'No se envió ningún correo.'
                        ),
                        page=1
                    )

            for recipient in emails_to_send:
                try:
                    recipient_lower = recipient.strip().lower()
                    voucher_info = vouchers_por_email.get(recipient_lower)

                    nombre_cliente = ''
                    if voucher_info:
                        nombre_cliente = str(voucher_info.get('nombre_cliente', '')).strip()

                    if not nombre_cliente:
                        local_part = recipient.split('@')[0]
                        name_tokens = [token for token in re.split(r'[._\-]+', local_part) if token and not token.isdigit()]
                        if name_tokens:
                            nombre_cliente = ' '.join(token.capitalize() for token in name_tokens[:3])

                    if not nombre_cliente:
                        nombre_cliente = 'Cliente'

                    saludo = build_saludo_cliente(nombre_cliente)
                    body_text = build_voucher_email_text(saludo)
                    body_html = build_voucher_email_html(saludo, voucher_info)

                    msg = EmailMessage()
                    msg['Subject'] = subject
                    msg['From'] = sender
                    msg['To'] = recipient
                    # Agregar BCC para que el remitente reciba una copia de cada correo
                    msg['Bcc'] = sender
                    msg.set_content(body_text)
                    msg.add_alternative(body_html, subtype='html')

                    logging.info(f"Procesando email HTML: {recipient}")
                    
                    smtp.send_message(msg)

                    sender_lower = sender.strip().lower()
                    if sender_lower and recipient_lower != sender_lower:
                        try:
                            sender_copy = EmailMessage()
                            sender_copy['Subject'] = f"Copia de envío: {subject}"
                            sender_copy['From'] = sender
                            sender_copy['To'] = sender
                            sender_copy_text = (
                                f"Se envió un correo a: {recipient}\n"
                                f"Asunto: {subject}\n\n"
                                f"Contenido enviado:\n\n{body_text}"
                            )
                            sender_copy.set_content(sender_copy_text)
                            sender_copy.add_alternative(body_html, subtype='html')

                            smtp.send_message(sender_copy)
                        except Exception as sender_copy_ex:
                            logging.warning(
                                f"No se pudo enviar copia al remitente {sender}: {sender_copy_ex}"
                            )

                    sent_ok.append(recipient)
                    sent_with_voucher_html.append(recipient)
                    logging.info(f"📧 Email enviado con voucher HTML a: {recipient}")
                        
                except Exception as send_ex:
                    logging.error(f"Error enviando a {recipient}: {send_ex}")
                    sent_fail.append(f"{recipient}: {send_ex}")

        report_path = files_path('emails_send_report.csv')
        with open(report_path, 'w', encoding='utf-8') as report:
            report.write('estado,email,detalle\n')
            for ok in sent_ok:
                report.write(f'sent,{ok},ok\n')
            for fail in sent_fail:
                email_part = fail.split(':', 1)[0]
                detail_part = fail.split(':', 1)[1].strip() if ':' in fail else 'error'
                report.write(f'failed,{email_part},"{detail_part}"\n')

        # Identificar correos NO enviados (olvidados)
        emails_no_enviados = [email for email in emails if email not in emails_to_send]
        
        if len(sent_ok) == 0 and len(sent_fail) > 0:
            message = f"⚠️ Envío finalizado con errores. Enviados: {len(sent_ok)}. Fallidos: {len(sent_fail)}."
        else:
            message = f"✅ Envío finalizado. Enviados: {len(sent_ok)}. Fallidos: {len(sent_fail)}."
        
        # Información de vouchers HTML enviados
        if len(sent_with_voucher_html) > 0:
            message += f"\n📄 Con voucher HTML en el correo: {len(sent_with_voucher_html)}"
        
        if len(emails_no_enviados) > 0:
            message += f"\n\n⚠️ CORREOS NO ENVIADOS ({len(emails_no_enviados)}):\n"
            for email_no_enviado in emails_no_enviados:  # Mostrar TODOS sin límite
                message += f"• {email_no_enviado}\n"
        
        if len(sent_fail) > 0:
            message += '\n\nRevisa emails_send_report.csv para detalles de errores.'

        if len(sent_ok) > 0:
            message += '\n\nSi no lo ves en Bandeja de Entrada, revisa la carpeta Enviados del remitente.'
        
        return render_correos_page(emails=emails, mensaje_exito=message, page=1)
    except Exception as ex:
        return render_correos_page(
            emails=emails,
            mensaje_exito=f'Error enviando por Outlook/Microsoft 365 ({smtp_host}:{smtp_port}, {smtp_security}): {ex}',
            page=1
        )


@app.route('/vouchers')
def listar_vouchers():
    """
    Muestra la lista de vouchers generados en la sesión actual
    """
    vouchers = session.get('vouchers_generados', [])
    return render_template('vouchers.html', vouchers=vouchers)


@app.route('/voucher/<path:filename>')
def descargar_voucher(filename):
    """
    Descarga un voucher específico
    """
    try:
        filepath = vouchers_path(filename)
        if os.path.exists(filepath):
            return send_file(filepath, as_attachment=True, download_name=filename)
        else:
            return "Voucher no encontrado", 404
    except Exception as e:
        return f"Error descargando voucher: {str(e)}", 500


@app.route('/vouchers/descargar_todos')
def descargar_todos_vouchers():
    """
    Descarga todos los vouchers generados en un archivo ZIP
    """
    import zipfile
    from io import BytesIO
    
    vouchers = session.get('vouchers_generados', [])
    if not vouchers:
        return "No hay vouchers para descargar", 404
    
    try:
        # Crear archivo ZIP en memoria
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for voucher in vouchers:
                filepath = voucher['filepath']
                if os.path.exists(filepath):
                    filename = os.path.basename(filepath)
                    zip_file.write(filepath, filename)
        
        zip_buffer.seek(0)
        return send_file(
            zip_buffer,
            mimetype='application/zip',
            as_attachment=True,
            download_name='vouchers_asiento.zip'
        )
    except Exception as e:
        return f"Error creando archivo ZIP: {str(e)}", 500



@app.errorhandler(Exception)
def handle_exception(e):
     # Log full traceback to file
     tb = traceback.format_exc()
     logging.error('Unhandled exception:\n%s', tb)
     # return a friendly error page
     return render_template('error.html', message=str(e)), 500


if __name__ == '__main__':
    # Configuración para red local
    app.run(host='0.0.0.0', port=5000, debug=True)

    