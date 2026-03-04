from flask import Flask, render_template, request, send_file, session, redirect, url_for
from AccountService import AccountService
from InterbankService import InterbankService
from ProviderService import ProviderService
from TransferService import TransferService
from BaseDatosService import BaseDatosService
from AsientoService import AsientoService
from io import BytesIO
from flask_session import Session
from flask_caching import Cache
from openpyxl import load_workbook
import pandas as pd
import os
import openpyxl
import re
import logging
import traceback
import json
import smtplib
from email.message import EmailMessage
from dotenv import load_dotenv
from cryptography.fernet import Fernet
from storage_paths import ensure_data_dirs, bootstrap_bd_from_source, files_path, logs_path, SESSION_DIR, bd_path

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


def _general_settings_path():
    return files_path('general_settings.json')


def _account_features_path():
    return files_path('account_features.json')


def _profile_photo_dir():
    return files_path('profile')


def _profile_photo_path(filename='profile_photo.png'):
    return os.path.join(_profile_photo_dir(), filename)


def _profile_photo_context():
    settings = load_general_settings()
    photo_version = int(settings.get('perfil', {}).get('foto_version', 0) or 0)
    return {
        'has_profile_photo': os.path.exists(_profile_photo_path()),
        'profile_photo_url': f"/foto_perfil_actual?v={photo_version}"
    }


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


def _normalize_smtp_security(security_value):
    normalized = str(security_value or '').strip().lower()
    if normalized not in ('ssl', 'starttls', 'auto'):
        return 'starttls'
    return normalized


def _parse_smtp_port(port_value):
    try:
        return int(str(port_value or '').strip())
    except Exception:
        return 587


def _should_retry_with_port_25(smtp_port, conn_ex):
    if int(smtp_port or 0) != 587:
        return False

    error_text = str(conn_ex or '').lower()
    return any(token in error_text for token in (
        'winerror 10061',
        'connection refused',
        'actively refused',
        'timed out',
        'timeout',
    ))


def _validate_smtp_login(sender, password, smtp_host, smtp_port, smtp_security):
    sender_value = str(sender or '').strip()
    password_value = str(password or '')
    host_value = str(smtp_host or '').strip()
    security_value = _normalize_smtp_security(smtp_security)
    port_value = _parse_smtp_port(smtp_port)

    if not sender_value or not password_value or not host_value:
        return False, sender_value, security_value, port_value, 'Credenciales incompletas'

    try:
        smtp_conn = None
        used_security = security_value
        used_port = port_value

        try:
            if security_value == 'ssl':
                smtp_conn = smtplib.SMTP_SSL(host_value, port_value, timeout=20)
                smtp_conn.ehlo()
            else:
                smtp_conn = smtplib.SMTP(host_value, port_value, timeout=20)
                smtp_conn.ehlo()
                smtp_conn.starttls()
                smtp_conn.ehlo()
                used_security = 'starttls'
        except Exception as conn_ex:
            wrong_version = 'WRONG_VERSION_NUMBER' in str(conn_ex).upper()
            can_retry_starttls = security_value in ('ssl', 'auto') and wrong_version
            if can_retry_starttls:
                smtp_conn = smtplib.SMTP(host_value, port_value, timeout=20)
                smtp_conn.ehlo()
                smtp_conn.starttls()
                smtp_conn.ehlo()
                used_security = 'starttls'
            elif _should_retry_with_port_25(port_value, conn_ex):
                smtp_conn = smtplib.SMTP(host_value, 25, timeout=20)
                smtp_conn.ehlo()
                smtp_conn.starttls()
                smtp_conn.ehlo()
                used_security = 'starttls'
                used_port = 25
            else:
                return False, sender_value, security_value, port_value, str(conn_ex)

        with smtp_conn as smtp:
            try:
                smtp.login(sender_value, password_value)
            except smtplib.SMTPAuthenticationError as auth_ex:
                corrected_sender, sender_corrected = normalize_sender_email(sender_value)
                if sender_corrected and corrected_sender != sender_value:
                    smtp.login(corrected_sender, password_value)
                    sender_value = corrected_sender
                else:
                    return False, sender_value, used_security, used_port, str(auth_ex)

        return True, sender_value, used_security, used_port, ''
    except Exception as ex:
        return False, sender_value, security_value, port_value, str(ex)


def _require_worker_microsoft_login():
    session['system_authenticated'] = True
    session['smtp_authenticated'] = True
    session['quick_password_verified'] = True
    return None


def save_secure_smtp_credentials(sender_value, password_value, smtp_host_value=None, smtp_port_value=None, smtp_security_value=None, cc_value=None):
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
    if cc_value is None:
        existing = load_secure_smtp_credentials()
        existing_cc = str(existing.get('cc', '')).strip()
        if existing_cc:
            payload['cc'] = existing_cc
    else:
        payload['cc'] = str(cc_value).strip().lower()
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
            'smtp_security': str(payload.get('smtp_security', '')).strip().lower(),
            'cc': str(payload.get('cc', '')).strip().lower()
        }
    except Exception as ex:
        logging.error(f'No se pudo leer credenciales SMTP cifradas: {ex}')
        return {}


def load_general_settings():
    default_payload = {
        'inicio': {
            'mostrar_resumen': True,
            'notificaciones_activas': True
        },
        'perfil': {
            'foto_version': 0,
            'correo_personal': ''
        },
        'contactos': {
            'correo_soporte': '',
            'correo_copia': ''
        },
        'microsoft365': {
            'tenant_id': 'common',
            'client_id': '',
            'client_secret': '',
            'scope': 'https://graph.microsoft.com/User.Read openid profile email'
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
        'sessions': [],
        'security': {
            'skip_password_when_possible': False,
            'enhanced_browsing': False
        }
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
        for key in ['password', 'devices', 'activity', 'sessions', 'security']:
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
            elif key == 'security' and isinstance(value, dict):
                security_payload = default_payload['security'].copy()
                security_payload.update(value)
                merged['security'] = security_payload

        return merged
    except Exception as ex:
        logging.error(f'No se pudo leer account_features: {ex}')
        return default_payload


def save_account_features(payload):
    with open(_account_features_path(), 'w', encoding='utf-8') as features_file:
        json.dump(payload, features_file, ensure_ascii=False)


def add_account_activity(action, detail=''):
    return


def _ensure_current_session_tracked():
    return


@app.before_request
def _before_every_request_account_tracking():
    try:
        _ensure_current_session_tracked()
    except Exception as ex:
        logging.warning(f'No se pudo actualizar sesión de dispositivo: {ex}')


def extract_emails_from_df(df):
    emails = set()
    email_regex = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
    for col in df.columns:
        for val in df[col].dropna():
            for m in email_regex.findall(str(val)):
                emails.add(m)
    return sorted(emails)

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
            'operacion_numero': row.get('Operación - Número', row.get('operacion', 'N/A'))
        }

    return list(voucher_records_by_email.values())


def build_saludo_cliente(nombre_cliente):
    return 'Estimado Cliente,'


def build_voucher_email_text(saludo):
    return f"""{saludo}

Le informamos que hemos recibido un abono en nuestra cuenta corriente del BANCO DE CREDITO con los siguientes detalle:



https://servicios.distriluz.com.pe/oficinavirtual

En esta plataforma, podrá registrarse como Cliente Empresa para gestionar la cancelación de los suministros afiliados a su representada y agregar otros suministros. Podrá adjuntar la constancia del pago o transferencia realizada para completar el proceso.

Agradecemos su atención y quedamos a su disposición para cualquier consulta adicional al CEL. 979 450 731 o al correo electrónico: recaudacionensa@distriluz.com.pe
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
    <body style="margin:0; padding:0; background:#f3f6fb; font-family: 'Segoe UI', Arial, sans-serif; color:#1f2937; line-height:1.55;">
        <div style="max-width:760px; margin:20px auto; background:#ffffff; border:1px solid #e5e7eb; border-radius:12px; overflow:hidden;">
            <div style="background:linear-gradient(135deg, #1e5da8 0%, #274b8f 100%); color:#ffffff; padding:18px 22px;">
                <div style="font-size:13px; opacity:0.95; letter-spacing:0.2px;">ENSA</div>
                <div style="font-size:22px; font-weight:700; margin-top:2px;">ABONO RECIBIDO - DESCARGUE SU RECIBO EN PLATAFORMA OFICINA VIRTUAL CLIENTE EMPRESA</div>
            </div>

            <div style="padding:20px 22px 10px 22px;">
                <p style="margin:0 0 10px 0; font-size:18px; font-weight:700; color:#1e5da8;">
                    ENSA - ABONO RECIBIDO - DESCARGUE SU RECIBO EN PLATAFORMA OFICINA VIRTUAL CLIENTE EMPRESA
                </p>
                <p style="margin:0 0 14px 0; font-size:14px;">
                    <a href="https://servicios.distriluz.com.pe/oficinavirtual" style="color:#1e5da8; text-decoration:underline; font-weight:600;">
                        https://servicios.distriluz.com.pe/oficinavirtual
                    </a>
                </p>
                <p style="margin:0 0 14px 0; font-size:15px;">{saludo}</p>
                <p style="margin:0 0 14px 0; font-size:14px; color:#374151;">
                    Le informamos que hemos recibido un abono en nuestra cuenta corriente del BANCO DE CREDITO con los siguientes detalle:
                </p>

                <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border:1px solid #dbe6f5; border-radius:10px; margin:0 0 14px 0; overflow:hidden;">
                    <tr>
                        <td style="background:#eef4fd; padding:10px 14px; font-size:13px; font-weight:700; color:#1e3a8a;">Detalle del abono</td>
                    </tr>
                    <tr>
                        <td style="padding:0; background:#ffffff;">
                            <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="font-size:14px; color:#1f2937;">
                                <tr>
                                    <td style="width:185px; padding:10px 14px; border-top:1px solid #e5edf9; color:#4b5563;"><strong>Referencia</strong></td>
                                    <td style="padding:10px 14px; border-top:1px solid #e5edf9;">{referencia}</td>
                                </tr>
                                <tr>
                                    <td style="width:185px; padding:10px 14px; border-top:1px solid #e5edf9; background:#f9fbff; color:#4b5563;"><strong>Fecha</strong></td>
                                    <td style="padding:10px 14px; border-top:1px solid #e5edf9; background:#f9fbff;">{fecha}</td>
                                </tr>
                                <tr>
                                    <td style="width:185px; padding:10px 14px; border-top:1px solid #e5edf9; color:#4b5563;"><strong>Número de operación</strong></td>
                                    <td style="padding:10px 14px; border-top:1px solid #e5edf9;">{operacion}</td>
                                </tr>
                                <tr>
                                    <td style="width:185px; padding:10px 14px; border-top:1px solid #e5edf9; background:#f9fbff; color:#4b5563;"><strong>Descripción</strong></td>
                                    <td style="padding:10px 14px; border-top:1px solid #e5edf9; background:#f9fbff;">{descripcion}</td>
                                </tr>
                                <tr>
                                    <td style="width:185px; padding:12px 14px; border-top:1px solid #d7e3f7; background:#eef4fd; color:#1e3a8a;"><strong>Monto</strong></td>
                                    <td style="padding:12px 14px; border-top:1px solid #d7e3f7; background:#eef4fd; font-size:17px; font-weight:700; color:#1e5da8;">{monto_text}</td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                </table>

                <p style="margin:0 0 12px 0; font-size:14px; color:#374151;">
                    En esta plataforma, podrá registrarse como Cliente Empresa para gestionar la cancelación de los suministros afiliados a su representada y agregar otros suministros.
                    <strong style="color:#1e5da8;">Podrá adjuntar la constancia del pago o transferencia realizada para completar el proceso.</strong>
                </p>
            </div>

            <div style="border-top:1px solid #e5e7eb; background:#fafbfc; padding:14px 22px; font-size:13px; color:#4b5563;">
                Agradecemos su atención y quedamos a su disposición para cualquier consulta adicional al CEL. <a href="https://wa.me/51979450731" style="color:#25D366 !important; text-decoration:underline; font-weight:700;">🟢 WhatsApp 979 450 731</a> o al correo electrónico: <strong>recaudacionensa@distriluz.com.pe</strong>
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

def render_correos_page(emails=None, mensaje_exito=None, page=1, quick_password_message=None, force_quick_password=False):
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
        'smtp_security': secure_smtp.get('smtp_security', '') or 'starttls',
        'cc': secure_smtp.get('cc', '')
    }
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
        quick_password_message=quick_password_message,
        force_quick_password=force_quick_password,
        total_vouchers=total_vouchers,
        vouchers_generados=vouchers_generados,
        smtp_config=smtp_config,
        general_settings=general_settings,
        has_profile_photo=has_profile_photo,
        profile_photo_url=f"/foto_perfil_actual?v={profile_photo_version}"
    )

def _enforce_step_flow(current_step: str):
    return None

@app.route('/', methods=['POST','GET'])
def home():
    auth_redirect = _require_worker_microsoft_login()
    if auth_redirect is not None:
        return auth_redirect

    flow_redirect = _enforce_step_flow('home')
    if flow_redirect is not None:
        return flow_redirect

    if request.method == 'GET' and request.args.get('reset') == '1':
        session.pop('home_processing_result', None)
        session.pop('home_success_message', None)

    general_settings = load_general_settings()
    config_message = session.pop('config_message', None)
    quick_password_message = session.pop('quick_password_message', None)
    open_mail_settings = bool(session.pop('open_mail_settings', False)) or bool(quick_password_message)
    photo_path = _profile_photo_path()
    has_profile_photo = os.path.exists(photo_path)
    profile_photo_version = general_settings.get('perfil', {}).get('foto_version', 0)

    if request.method == 'POST':
        files = request.files.getlist('file')
        filtered_files = [x for x in files if x.filename!=""]
        if len(filtered_files) <= 1:
            return render_template(
                'home.html',
                error_message='Debe subir por lo menos un archivo.',
                processing_result=session.get('home_processing_result'),
                mensaje_exito=config_message or session.get('home_success_message'),
                quick_password_message=quick_password_message,
                open_mail_settings=open_mail_settings,
                has_profile_photo=has_profile_photo,
                profile_photo_url=f"/foto_perfil_actual?v={profile_photo_version}"
            )
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
                session['required_next_step'] = 'asiento'
                session['home_processing_result'] = resumen
                session['home_success_message'] = 'Proceso exitosamente.'
                return redirect(url_for('home'))
    
                #return redirect(url_for('upload'))
            except Exception as e:
                error_message = str(e)
                return render_template(
                    'home.html',
                    error_message=error_message,
                    processing_result=session.get('home_processing_result'),
                    mensaje_exito=config_message or session.get('home_success_message'),
                    quick_password_message=quick_password_message,
                    open_mail_settings=open_mail_settings,
                    has_profile_photo=has_profile_photo,
                    profile_photo_url=f"/foto_perfil_actual?v={profile_photo_version}"
                )
    else:
        return render_template(
            'home.html',
            processing_result=session.get('home_processing_result'),
            mensaje_exito=config_message or session.get('home_success_message'),
            quick_password_message=quick_password_message,
            open_mail_settings=open_mail_settings,
            has_profile_photo=has_profile_photo,
            profile_photo_url=f"/foto_perfil_actual?v={profile_photo_version}"
        )


@app.route('/menu', methods=['GET'])
def menu():
    auth_redirect = _require_worker_microsoft_login()
    if auth_redirect is not None:
        return auth_redirect

    tab = request.args.get('tab', 'home').strip().lower()
    tab_map = {
        'home': '/',
        'basedatos': '/basedatos',
        'asiento': '/asiento',
        'correos': '/correos'
    }
    if tab not in tab_map:
        tab = 'home'

    return render_template('menu.html', active_tab=tab, tab_map=tab_map, **_profile_photo_context())

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
    auth_redirect = _require_worker_microsoft_login()
    if auth_redirect is not None:
        return auth_redirect

    flow_redirect = _enforce_step_flow('basedatos')
    if flow_redirect is not None:
        return flow_redirect

    profile_photo_context = _profile_photo_context()
    config_message = session.pop('config_message', None)
    quick_password_message = session.pop('quick_password_message', None)
    open_mail_settings = bool(session.pop('open_mail_settings', False)) or bool(quick_password_message)

    if request.method == 'POST':
        files    = request.files.getlist('file')
        
        try:
            filtered_files = [x for x in files if x.filename!=""]
                        
            if len(filtered_files) < 1:
                return render_template('base-datos.html', error_message= 'Debe subir por lo menos un archivo.', **profile_photo_context)

            nombres = [f.filename.upper() for f in filtered_files]
            valid_patterns = ['RECAUDO', 'PREPAGO', 'TRABAJADOR', 'CLIENTE']
            invalid_files = [n for n in nombres if not any(pattern in n for pattern in valid_patterns)]
            if invalid_files:
                return render_template('base-datos.html', error_message='Archivo(s) no reconocido(s): ' + ', '.join(invalid_files), **profile_photo_context)

            mensaje_exito = 'Proceso exitosamente.'
            
            base_datos_service = BaseDatosService()  
            base_datos_service.GuardarAchivos(files)  
            session['required_next_step'] = 'home'
            return render_template('base-datos.html', mensaje_exito=mensaje_exito, **profile_photo_context)
                
        except Exception as e:
            error_message = str(e)
            return render_template('base-datos.html', error_message= error_message, **profile_photo_context)

    else:
        nohay = 'Archivo subido correctamente.'
        return render_template(
            'base-datos.html',
            mensaje_exito=config_message,
            quick_password_message=quick_password_message,
            open_mail_settings=open_mail_settings,
            **profile_photo_context
        )
    
@app.route('/asiento', methods=['POST'])
def asiento_procesar():
    auth_redirect = _require_worker_microsoft_login()
    if auth_redirect is not None:
        return auth_redirect

    flow_redirect = _enforce_step_flow('asiento')
    if flow_redirect is not None:
        return flow_redirect

    profile_photo_context = _profile_photo_context()

    logging.error('asiento_procesar: start')
    files = request.files.getlist('file')
    logging.error('asiento_procesar: received %d files', len(files))
    filtered_files = [x for x in files if x.filename!=""]
    logging.error('asiento_procesar: filtered %d files', len(filtered_files))
    if len(filtered_files) <= 1:
        logging.error('asiento_procesar: not enough files, returning form')
        return render_template('asiento.html', error_message= 'Debe subir ambos archivo.', **profile_photo_context)
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
                session['correos_ready'] = True
                save_emails_cache(sorted_emails)
                if len(sorted_emails) == 0:
                    session['asiento_email_warning'] = 'No se encontraron correos para líneas sin voucher. Verifique Referencia y CORREO DE CONTACTO en Base de Datos > Clientes.'
                else:
                    session.pop('asiento_email_warning', None)
                session['required_next_step'] = 'correos'
                return redirect(url_for('asiento_get', resultado_correo=1))
            else: 
                #si hubiera error se pinta la misma pagina y no se redirecciona
                return render_template('asiento.html', error_message= 'No se encontro ningun asiento en el proceso', **profile_photo_context)       
            
        except Exception as e:
            error_message = str(e)
            logging.error('asiento_procesar: exception: %s', error_message)
            return render_template('asiento.html', error_message= error_message, **profile_photo_context)
    # Fallback: ensure the view always returns a response
    logging.error('asiento_procesar: reached end of function without explicit return')
    return render_template('asiento.html', error_message='Error inesperado en el procesamiento', **profile_photo_context)



@app.route('/asiento', methods=['GET'])
def asiento_get():
    auth_redirect = _require_worker_microsoft_login()
    if auth_redirect is not None:
        return auth_redirect

    flow_redirect = _enforce_step_flow('asiento')
    if flow_redirect is not None:
        return flow_redirect

    show_result_mode = request.args.get('resultado_correo', '0') == '1'
    asiento_emails = session.get('asiento_emails', [])
    vouchers_generados = session.get('vouchers_generados', [])
    secure_smtp = load_secure_smtp_credentials()
    mail_cc = str(session.get('worker_cc', '')).strip() or str(secure_smtp.get('cc', '')).strip()
    page_message = session.pop('config_message', None)
    quick_password_message = session.pop('quick_password_message', None)
    open_mail_settings = bool(session.pop('open_mail_settings', False)) or bool(quick_password_message)

    return render_template(
        'asiento.html',
        show_result_mode=show_result_mode,
        asiento_emails=asiento_emails,
        total_vouchers=len(vouchers_generados),
        mail_cc=mail_cc,
        mensaje_exito=page_message,
        quick_password_message=quick_password_message,
        open_mail_settings=open_mail_settings,
        **_profile_photo_context()
    )


@app.route('/correos', methods=['GET','POST'])
def correos():
    auth_redirect = _require_worker_microsoft_login()
    if auth_redirect is not None:
        return auth_redirect

    flow_redirect = _enforce_step_flow('correos')
    if flow_redirect is not None:
        return flow_redirect

    arrived_from_asiento = session.get('required_next_step') == 'correos'
    if arrived_from_asiento:
        session.pop('required_next_step', None)

    ready_from_asiento = bool(session.pop('correos_ready', False))
    allow_results = arrived_from_asiento and ready_from_asiento

    if allow_results:
        sess_emails = session.get('asiento_emails', [])
    else:
        session.pop('asiento_emails', None)
        session.pop('vouchers_generados', None)
        session.pop('asiento_email_warning', None)
        sess_emails = []

    warning_message = session.pop('asiento_email_warning', None)
    config_message = session.pop('config_message', None)
    quick_password_message = session.pop('quick_password_message', None)
    force_quick_password = not bool(session.get('quick_password_verified', False))
    page_message = config_message or warning_message

    try:
        page = int(request.args.get('page', '1'))
    except ValueError:
        page = 1
    if page < 1:
        page = 1

    return render_correos_page(
        emails=sess_emails,
        mensaje_exito=page_message,
        page=page,
        quick_password_message=quick_password_message,
        force_quick_password=force_quick_password
    )


@app.route('/correo_electronico', methods=['GET'])
def correo_electronico():
    auth_redirect = _require_worker_microsoft_login()
    if auth_redirect is not None:
        return auth_redirect
    session.pop('email_settings_message', None)
    return redirect(url_for('correos'))


@app.route('/iniciar_sesion', methods=['GET'])
def iniciar_sesion():
    return redirect(url_for('basedatos'))


@app.route('/iniciar_sesion', methods=['POST'])
@app.route('/correo_electronico/guardar', methods=['POST'])
def correo_electronico_guardar():
    existing = load_secure_smtp_credentials()
    sender = request.form.get('sender', '').strip() or str(existing.get('sender', '')).strip()
    password = request.form.get('password', '').strip() or str(existing.get('password', '')).strip()
    confirm_password = request.form.get('confirm_password', '').strip()
    smtp_host = existing.get('smtp_host', '') or 'owa.fonafe.gob.pe'
    smtp_port = _parse_smtp_port(existing.get('smtp_port', '') or '587')
    smtp_security = _normalize_smtp_security(existing.get('smtp_security', '') or 'starttls')

    if not sender:
        session['login_message'] = 'Ingresa tu correo para continuar.'
        return redirect(url_for('basedatos'))

    if not password:
        session['login_message'] = 'No hay una contraseña guardada para ese correo. Configura la cuenta primero.'
        return redirect(url_for('basedatos'))

    if not confirm_password:
        confirm_password = password

    if password != confirm_password:
        session['login_message'] = 'Las contraseñas no coinciden. Verifica e intenta nuevamente.'
        return redirect(url_for('basedatos'))

    validated_sender = sender
    used_security = smtp_security

    try:
        save_secure_smtp_credentials(
            validated_sender,
            password,
            smtp_host_value=smtp_host,
            smtp_port_value=str(smtp_port),
            smtp_security_value=used_security
        )
        sync_email_config_to_modules(validated_sender)
        session['system_authenticated'] = True
        session['smtp_authenticated'] = True
        session['smtp_link_verified'] = True
        session['worker_sender'] = validated_sender
        session['worker_password'] = password
        session['worker_smtp_host'] = smtp_host
        session['worker_smtp_port'] = str(smtp_port)
        session['worker_smtp_security'] = used_security
        session['worker_login_at'] = pd.Timestamp.now().strftime('%d/%m/%Y %H:%M:%S')
        session['worker_auth_method'] = 'SMTP OWA'
        session['quick_password_verified'] = False
        add_account_activity('Correo electrónico', f'Sesión iniciada para {validated_sender}')
        session.pop('email_settings_message', None)
        session.pop('login_message', None)
    except Exception as ex:
        logging.warning(f'No se pudieron guardar credenciales en login: {ex}')
        session['system_authenticated'] = True
        session['smtp_authenticated'] = True
        session['smtp_link_verified'] = True
        session['worker_sender'] = validated_sender
        session['worker_password'] = password
        session['worker_smtp_host'] = smtp_host
        session['worker_smtp_port'] = str(smtp_port)
        session['worker_smtp_security'] = used_security
        session['worker_login_at'] = pd.Timestamp.now().strftime('%d/%m/%Y %H:%M:%S')
        session['worker_auth_method'] = 'SMTP OWA'
        session['quick_password_verified'] = False
        session.pop('email_settings_message', None)
        session.pop('login_message', None)

    return redirect(url_for('basedatos'))


@app.route('/correo_electronico/verificar_vinculo', methods=['POST'])
def correo_electronico_verificar_vinculo():
    sender = str(session.get('worker_sender', '')).strip() or str(load_secure_smtp_credentials().get('sender', '')).strip()
    used_security = _normalize_smtp_security(session.get('worker_smtp_security', '') or 'starttls')
    session['system_authenticated'] = True
    session['smtp_authenticated'] = True
    session['smtp_link_verified'] = True
    session['worker_sender'] = sender
    session['worker_smtp_security'] = used_security
    session['worker_login_at'] = pd.Timestamp.now().strftime('%d/%m/%Y %H:%M:%S')
    session['worker_auth_method'] = str(session.get('worker_auth_method', '')).strip() or 'SMTP OWA'
    session['email_settings_message'] = '✅ Vínculo de correo verificado correctamente.'
    add_account_activity('Correo electrónico', f'Vínculo verificado para {sender}')

    return redirect(url_for('correo_electronico'))


@app.route('/configurar_correo', methods=['POST'])
def configurar_correo():
    return_to = str(request.form.get('return_to', '')).strip().lower()
    redirect_to_correos = return_to == 'correos'
    redirect_to_home = return_to == 'home'
    redirect_to_basedatos = return_to == 'basedatos'
    redirect_to_asiento = return_to == 'asiento'

    def _redirect_after_password(message_text, is_error=False):
        if redirect_to_correos:
            if is_error:
                session['quick_password_message'] = message_text
                session['quick_password_verified'] = False
                return redirect(url_for('correos', open_quick_password='1'))
            session['config_message'] = message_text
            return redirect(url_for('correos'))

        if redirect_to_home or redirect_to_basedatos or redirect_to_asiento:
            if is_error:
                session['quick_password_message'] = message_text
                session['open_mail_settings'] = True
            else:
                session['config_message'] = message_text
            if redirect_to_home:
                return redirect(url_for('home'))
            if redirect_to_basedatos:
                return redirect(url_for('basedatos'))
            return redirect(url_for('asiento_get', resultado_correo=1))

        session['email_settings_message'] = message_text
        return redirect(url_for('correo_electronico'))

    existing = load_secure_smtp_credentials()
    sender = request.form.get('sender', '').strip() or existing.get('sender', '').strip()
    password = request.form.get('password', '').strip() or existing.get('password', '').strip()
    confirm_password_raw = request.form.get('confirm_password', None)
    confirm_password = '' if confirm_password_raw is None else str(confirm_password_raw).strip()
    cc_value = request.form.get('cc', '').strip() or existing.get('cc', '').strip()
    smtp_host = request.form.get('smtp_host', '').strip() or existing.get('smtp_host', '').strip() or 'owa.fonafe.gob.pe'
    smtp_port = _parse_smtp_port(request.form.get('smtp_port', '').strip() or existing.get('smtp_port', '').strip() or '587')
    smtp_security = _normalize_smtp_security(request.form.get('smtp_security', '').strip().lower() or existing.get('smtp_security', '').strip().lower() or 'starttls')

    cc_clean_items = [extract_single_email(item) for item in re.split(r'[;,]+', str(cc_value)) if str(item).strip()]
    cc_clean = ', '.join(dict.fromkeys(item for item in cc_clean_items if item))

    if not sender or not password:
        return _redirect_after_password('Completa remitente y contraseña para continuar.', is_error=True)

    if confirm_password_raw is not None:
        if not confirm_password:
            return _redirect_after_password('Repite la contraseña para continuar.', is_error=True)
        if password != confirm_password:
            return _redirect_after_password('Las contraseñas no coinciden. Intenta de nuevo.', is_error=True)

    validated_sender = sender
    used_security = smtp_security
    used_port = smtp_port

    is_valid_login, validated_sender, used_security, used_port, validation_error = _validate_smtp_login(
        sender,
        password,
        smtp_host,
        smtp_port,
        smtp_security
    )
    if not is_valid_login:
        logging.warning(f'Validación SMTP fallida en configurar_correo: {validation_error}')
        return _redirect_after_password('La contraseña es incorrecta. Verifica tus credenciales e intenta nuevamente.', is_error=True)

    try:
        save_secure_smtp_credentials(
            validated_sender,
            password,
            smtp_host_value=smtp_host,
            smtp_port_value=str(used_port),
            smtp_security_value=used_security,
            cc_value=cc_clean
        )
        sync_email_config_to_modules(validated_sender)
        session['system_authenticated'] = True
        session['smtp_authenticated'] = True
        session['smtp_link_verified'] = True
        session['worker_sender'] = validated_sender
        session['worker_password'] = password
        session['worker_smtp_host'] = smtp_host
        session['worker_smtp_port'] = str(used_port)
        session['worker_smtp_security'] = used_security
        session['worker_cc'] = cc_clean
        session['worker_auth_method'] = str(session.get('worker_auth_method', '')).strip() or 'SMTP OWA'
        session['quick_password_verified'] = True
        add_account_activity('Contraseña de correo', f'Sesión iniciada para {validated_sender}')
        return _redirect_after_password('✅ Contraseña de correo actualizada correctamente.')
    except Exception as ex:
        return _redirect_after_password('No se pudo guardar la contraseña de correo. Intenta nuevamente.', is_error=True)


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


@app.route('/download_asientos', methods=['GET', 'POST'])
def dowload_asientos():
    ruta_archivo = files_path('asientos.xlsx')
    return send_file(ruta_archivo, as_attachment=True, download_name="Asiento.xlsx")


@app.route('/send_emails', methods=['POST'])
def send_emails():
    auth_redirect = _require_worker_microsoft_login()
    if auth_redirect is not None:
        return auth_redirect

    request_source = str(request.form.get('source', '')).strip().lower()

    def _redirect_correos_with_message(message_text):
        session['config_message'] = message_text
        if request_source == 'asiento':
            return redirect(url_for('asiento_get', resultado_correo=1))
        return redirect(url_for('correos'))

    emails = session.get('asiento_emails', [])
    if not emails:
        return _redirect_correos_with_message('No hay correos para enviar')

    selected_emails = request.form.getlist('selected_emails')
    if not selected_emails:
        return _redirect_correos_with_message('Selecciona al menos un correo para enviar. No se envió nada automáticamente.')

    emails_to_send = sorted(set(selected_emails))
    
    # Obtener vouchers generados de la sesión
    vouchers_generados = session.get('vouchers_generados', [])
    
    # Log de diagnóstico
    logging.info(f'Total vouchers en sesión: {len(vouchers_generados)}')
    for v in vouchers_generados:
        logging.info(f"Voucher disponible: email={v.get('email')}, ref={v.get('referencia')}")
    
    # Crear diccionario para buscar voucher por email
    vouchers_por_email = {}
    for voucher in vouchers_generados:
        email = voucher.get('email', '').strip().lower()
        if email:
            vouchers_por_email[email] = voucher
            logging.info(f"Voucher indexado para: {email}")
    
    # Configuración SMTP
    secure_smtp = load_secure_smtp_credentials()
    form_sender = request.form.get('sender', '').strip()
    form_password = request.form.get('password', '').strip()
    form_smtp_host = request.form.get('smtp_host', '').strip()
    form_smtp_port = request.form.get('smtp_port', '').strip()
    form_smtp_security = request.form.get('smtp_security', '').strip().lower()

    session_sender = str(session.get('worker_sender', '')).strip()
    session_password = str(session.get('worker_password', '')).strip()
    session_smtp_host = str(session.get('worker_smtp_host', '')).strip()
    session_smtp_port = str(session.get('worker_smtp_port', '')).strip()
    session_smtp_security = str(session.get('worker_smtp_security', '')).strip().lower()
    session_cc = str(session.get('worker_cc', '')).strip()

    sender = form_sender or session_sender or os.environ.get('OUTLOOK_SENDER', '').strip() or secure_smtp.get('sender', '').strip()
    password = form_password or session_password or os.environ.get('OUTLOOK_PASSWORD', '').strip() or secure_smtp.get('password', '').strip()
    cc_raw = request.form.get('cc', '').strip() or session_cc or secure_smtp.get('cc', '').strip()
    subject_env = os.environ.get('OUTLOOK_SUBJECT', '').strip()
    subject = subject_env or 'Confirmación de Abono Recibido - ENSA'
    subject = re.sub(r'distriluz\s+ensa', 'ENSA', subject, flags=re.IGNORECASE).strip()
    smtp_host = form_smtp_host or session_smtp_host or os.environ.get('OUTLOOK_SMTP_HOST', '').strip() or secure_smtp.get('smtp_host', '').strip() or 'owa.fonafe.gob.pe'
    smtp_port_raw = form_smtp_port or session_smtp_port or os.environ.get('OUTLOOK_SMTP_PORT', '').strip() or secure_smtp.get('smtp_port', '').strip() or '587'
    smtp_security = (form_smtp_security or session_smtp_security or os.environ.get('OUTLOOK_SMTP_SECURITY', '').strip() or secure_smtp.get('smtp_security', '').strip() or 'starttls').lower()
    try:
        smtp_port = int(smtp_port_raw)
    except ValueError:
        smtp_port = 587
        logging.warning(f"OUTLOOK_SMTP_PORT inválido ('{smtp_port_raw}'). Usando 587 por defecto.")

    if smtp_security not in ('ssl', 'starttls', 'auto'):
        logging.warning(f"OUTLOOK_SMTP_SECURITY inválido ('{smtp_security}'). Usando 'starttls'.")
        smtp_security = 'starttls'

    cc_items = [extract_single_email(item) for item in re.split(r'[;,]+', str(cc_raw)) if str(item).strip()]
    cc_recipients = list(dict.fromkeys(item for item in cc_items if item))
    cc_clean = ', '.join(cc_recipients)

    should_save_smtp = request.form.get('save_smtp', 'yes').strip().lower() in ('yes', 'on', 'true', '1')
    if should_save_smtp and sender and password:
        try:
            save_secure_smtp_credentials(
                sender,
                password,
                smtp_host_value=smtp_host,
                smtp_port_value=str(smtp_port),
                smtp_security_value=smtp_security,
                cc_value=cc_clean
            )
            sync_email_config_to_modules(sender)
            session['worker_cc'] = cc_clean
        except Exception as save_ex:
            logging.warning(f'No se pudo persistir configuración SMTP desde envío manual: {save_ex}')

    if not sender or not password:
        return _redirect_correos_with_message(
            'Falta configurar correo remitente y clave SMTP. Ingresa Usuario y Contraseña en el panel de envío y vuelve a intentar.'
        )

    sent_ok = []
    sent_fail = []
    sent_with_voucher_html = []
    used_security = smtp_security
    used_port = smtp_port
    
    try:
        # Conexión SMTP configurable (ssl | starttls | auto)
        smtp_conn = None
        try:
            if smtp_security == 'ssl':
                smtp_conn = smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=30)
                smtp_conn.ehlo()
                used_security = 'ssl'
                used_port = smtp_port
            else:
                smtp_conn = smtplib.SMTP(smtp_host, smtp_port, timeout=30)
                smtp_conn.ehlo()
                smtp_conn.starttls()
                smtp_conn.ehlo()
                used_security = 'starttls'
                used_port = smtp_port
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
                used_port = smtp_port
            elif _should_retry_with_port_25(smtp_port, conn_ex):
                logging.warning(
                    f"SMTP {smtp_host}:{smtp_port} rechazó conexión ({conn_ex}). Reintentando por puerto 25 con STARTTLS."
                )
                smtp_conn = smtplib.SMTP(smtp_host, 25, timeout=30)
                smtp_conn.ehlo()
                smtp_conn.starttls()
                smtp_conn.ehlo()
                used_security = 'starttls'
                used_port = 25
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
                        return _redirect_correos_with_message(
                            'No se pudieron validar tus credenciales de correo. '
                            'El usuario guardado tiene dominio typo. Usa tu correo con @distriluz.com.pe en las credenciales SMTP. '
                            'No se envió ningún correo.'
                        )
                else:
                    return _redirect_correos_with_message(
                        'No se pudieron validar tus credenciales de correo. '
                        'Verifica usuario/clave en credenciales SMTP o confirma con TI que la cuenta tenga SMTP AUTH habilitado en owa.fonafe.gob.pe. '
                        'No se envió ningún correo.'
                    )

            if should_save_smtp and sender and password and (used_port != smtp_port or used_security != smtp_security):
                try:
                    save_secure_smtp_credentials(
                        sender,
                        password,
                        smtp_host_value=smtp_host,
                        smtp_port_value=str(used_port),
                        smtp_security_value=used_security
                    )
                    session['worker_smtp_port'] = str(used_port)
                    session['worker_smtp_security'] = used_security
                except Exception as save_ex:
                    logging.warning(f'No se pudo persistir fallback SMTP efectivo: {save_ex}')

            for recipient in emails_to_send:
                try:
                    recipient_lower = recipient.strip().lower()
                    voucher_info = vouchers_por_email.get(recipient_lower)

                    saludo = build_saludo_cliente('Cliente')
                    body_text = build_voucher_email_text(saludo)
                    body_html = build_voucher_email_html(saludo, voucher_info)

                    msg = EmailMessage()
                    msg['Subject'] = subject
                    msg['From'] = sender
                    msg['To'] = recipient
                    recipient_lower = recipient.strip().lower()
                    cc_for_message = [cc for cc in cc_recipients if cc != recipient_lower]
                    if cc_for_message:
                        msg['Cc'] = ', '.join(cc_for_message)
                    msg.set_content(body_text)
                    msg.add_alternative(body_html, subtype='html')

                    logging.info(f"Procesando email HTML: {recipient}")
                    
                    smtp.send_message(msg)

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

        sent_ok_set = {email.strip().lower() for email in sent_ok}
        remaining_emails = [email for email in emails if email.strip().lower() not in sent_ok_set]
        emails_no_enviados = remaining_emails[:]

        if len(sent_ok) > 0:
            session['asiento_emails'] = remaining_emails
            save_emails_cache(remaining_emails)

            vouchers_restantes = []
            for voucher in vouchers_generados:
                voucher_email = str(voucher.get('email', '')).strip().lower()
                if voucher_email not in sent_ok_set:
                    vouchers_restantes.append(voucher)
            session['vouchers_generados'] = vouchers_restantes

            if remaining_emails:
                session.pop('asiento_email_warning', None)
            else:
                session.pop('asiento_email_warning', None)
        
        if len(sent_ok) == 0 and len(sent_fail) > 0:
            message = f"⚠️ Envío finalizado con errores. Enviados: {len(sent_ok)}. Fallidos: {len(sent_fail)}."
        elif len(sent_ok) > 0 and len(sent_fail) == 0:
            message = "✅ Envío exitosamente"
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

        return _redirect_correos_with_message(message)
    except Exception as ex:
        return _redirect_correos_with_message(
            f'Error enviando por SMTP ({smtp_host}:{used_port}, {used_security}): {ex}'
        )


@app.route('/cerrar_sesion', methods=['GET'])
def cerrar_sesion():
    session.clear()
    return redirect(url_for('iniciar_sesion'))



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

    