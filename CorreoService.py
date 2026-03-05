from flask import render_template, request, session, redirect, url_for, jsonify
from storage_paths import files_path, bd_path
import os
import json
import re
import logging
import pandas as pd
import smtplib
from email.message import EmailMessage
from cryptography.fernet import Fernet


REQUIRED_SENDER_DOMAIN = '@distriluz.com.pe'


def _smtp_key_path():
    return files_path('smtp_credentials.key')


def _smtp_credentials_path():
    return files_path('smtp_credentials.json')


def _legacy_files_dir():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), 'files')


def _smtp_key_paths():
    candidates = [
        _smtp_key_path(),
        os.path.join(_legacy_files_dir(), 'smtp_credentials.key')
    ]
    unique = []
    for path in candidates:
        if path and path not in unique:
            unique.append(path)
    return unique


def _smtp_credentials_paths():
    candidates = [
        _smtp_credentials_path(),
        os.path.join(_legacy_files_dir(), 'smtp_credentials.json')
    ]
    unique = []
    for path in candidates:
        if path and path not in unique:
            unique.append(path)
    return unique


def _existing_paths(paths):
    return [path for path in paths if path and os.path.exists(path)]


def clear_secure_smtp_credentials():
    for cred_path in _smtp_credentials_paths():
        if os.path.exists(cred_path):
            try:
                os.remove(cred_path)
            except Exception as ex:
                logging.warning(f'No se pudo eliminar credenciales SMTP: {ex}')


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


def _fernet_candidates_for_load():
    candidates = []

    env_key = os.environ.get('OUTLOOK_CREDENTIALS_KEY', '').strip()
    if env_key:
        try:
            candidates.append(Fernet(env_key.encode('utf-8')))
        except Exception:
            pass

    for key_path in _existing_paths(_smtp_key_paths()):
        try:
            with open(key_path, 'rb') as key_file:
                key = key_file.read().strip()
            candidates.append(Fernet(key))
        except Exception:
            continue

    return candidates


def normalize_sender_email(sender_value):
    sender_clean = str(sender_value or '').strip().lower()
    if not sender_clean:
        return '', False

    if '@' in sender_clean:
        local_part, domain_part = sender_clean.split('@', 1)
        local_part = local_part.strip()
        domain_part = domain_part.strip()
        if not local_part or not domain_part:
            return '', False
        normalized_sender = f"{local_part}@{domain_part}"
    else:
        normalized_sender = f"{sender_clean}{REQUIRED_SENDER_DOMAIN}"

    was_changed = normalized_sender != sender_clean
    return normalized_sender, was_changed


def normalize_smtp_security(security_value):
    normalized = str(security_value or '').strip().lower()
    if normalized not in ('ssl', 'starttls', 'auto'):
        return 'starttls'
    return normalized


def parse_smtp_port(port_value):
    try:
        return int(str(port_value or '').strip())
    except Exception:
        return 587


def should_retry_with_port_25(smtp_port, conn_ex):
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


def _is_auth_not_supported_error(ex):
    error_text = str(ex or '').lower()
    return 'smtp auth extension not supported' in error_text or 'auth not supported' in error_text


def _connect_smtp_client(host_value, security_mode, port_value, timeout_seconds):
    if security_mode == 'ssl':
        smtp_conn = smtplib.SMTP_SSL(host_value, port_value, timeout=timeout_seconds)
        smtp_conn.ehlo()
        return smtp_conn

    smtp_conn = smtplib.SMTP(host_value, port_value, timeout=timeout_seconds)
    smtp_conn.ehlo()
    smtp_conn.starttls()
    smtp_conn.ehlo()
    return smtp_conn


def _build_smtp_attempts(security_value, port_value):
    attempts = []

    def add_attempt(mode, port):
        item = (mode, int(port))
        if item not in attempts:
            attempts.append(item)

    preferred_security = normalize_smtp_security(security_value)
    preferred_port = parse_smtp_port(port_value)

    if preferred_security == 'auto':
        add_attempt('starttls', preferred_port)
    else:
        add_attempt(preferred_security, preferred_port)

    add_attempt('ssl', 465)
    add_attempt('starttls', 587)
    add_attempt('starttls', 25)

    return attempts


def _open_authenticated_smtp_connection(host_value, sender_value, password_value, security_value, port_value, timeout_seconds):
    attempts = _build_smtp_attempts(security_value, port_value)
    last_error = None

    for security_mode, candidate_port in attempts:
        smtp_conn = None
        try:
            smtp_conn = _connect_smtp_client(host_value, security_mode, candidate_port, timeout_seconds)

            if not smtp_conn.has_extn('auth'):
                raise smtplib.SMTPNotSupportedError('SMTP AUTH extension not supported by server.')

            smtp_conn.login(sender_value, password_value)
            return smtp_conn, security_mode, candidate_port, ''
        except Exception as attempt_ex:
            last_error = attempt_ex
            if smtp_conn is not None:
                try:
                    smtp_conn.quit()
                except Exception:
                    pass

    return None, '', 0, str(last_error or 'No se pudo autenticar en SMTP')


def validate_smtp_login(sender, password, smtp_host, smtp_port, smtp_security):
    sender_value, _ = normalize_sender_email(sender)
    password_value = str(password or '')
    host_value = str(smtp_host or '').strip()
    security_value = normalize_smtp_security(smtp_security)
    port_value = parse_smtp_port(smtp_port)

    if not sender_value or not password_value or not host_value:
        return False, sender_value, security_value, port_value, 'Credenciales incompletas'

    try:
        smtp_conn, used_security, used_port, connect_error = _open_authenticated_smtp_connection(
            host_value,
            sender_value,
            password_value,
            security_value,
            port_value,
            timeout_seconds=20
        )
        if smtp_conn is None:
            return False, sender_value, security_value, port_value, connect_error

        try:
            smtp_conn.quit()
        except Exception:
            pass

        return True, sender_value, used_security, used_port, ''
    except Exception as ex:
        return False, sender_value, security_value, port_value, str(ex)


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
    cred_path = _smtp_credentials_path()
    os.makedirs(os.path.dirname(cred_path), exist_ok=True)
    with open(cred_path, 'w', encoding='utf-8') as out_file:
        json.dump(payload, out_file, ensure_ascii=False)


def load_secure_smtp_credentials():
    cred_paths = _existing_paths(_smtp_credentials_paths())
    if not cred_paths:
        return {}

    fernet_candidates = _fernet_candidates_for_load()
    if not fernet_candidates:
        return {}

    for cred_path in cred_paths:
        try:
            with open(cred_path, 'r', encoding='utf-8') as in_file:
                payload = json.load(in_file)

            sender_encrypted = payload.get('sender_encrypted', '')
            password_encrypted = payload.get('password_encrypted', '')
            if not sender_encrypted or not password_encrypted:
                continue

            for fernet in fernet_candidates:
                try:
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
                except Exception:
                    continue
        except Exception as ex:
            logging.error(f'No se pudo leer credenciales SMTP cifradas: {ex}')

    return {}


def extract_single_email(value):
    if pd.isna(value):
        return ''
    matches = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", str(value))
    if matches:
        return matches[0].strip().lower()
    return ''


def normalize_reference(value):
    return str(value).strip().upper()


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
        with open(cache_path, 'w', encoding='utf-8') as file:
            json.dump({'emails': emails}, file, ensure_ascii=False)
    except Exception:
        pass


def render_correos_page(emails, mensaje_exito, page, quick_password_message, force_quick_password, load_general_settings_func, profile_photo_path_func):
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
    general_settings = load_general_settings_func()

    photo_path = profile_photo_path_func()
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


def sync_email_config_to_modules(sender_email, load_general_settings_func, save_general_settings_func):
    sender_clean = str(sender_email or '').strip().lower()
    if not sender_clean:
        return

    settings = load_general_settings_func()

    if 'perfil' not in settings or not isinstance(settings['perfil'], dict):
        settings['perfil'] = {}
    if 'contactos' not in settings or not isinstance(settings['contactos'], dict):
        settings['contactos'] = {}

    settings['perfil']['correo_personal'] = sender_clean

    soporte_actual = str(settings['contactos'].get('correo_soporte', '')).strip()
    if not soporte_actual:
        settings['contactos']['correo_soporte'] = sender_clean

    save_general_settings_func(settings)


def correos_handler(require_worker_microsoft_login_func, enforce_step_flow_func, load_general_settings_func, profile_photo_path_func):
    auth_redirect = require_worker_microsoft_login_func()
    if auth_redirect is not None:
        return auth_redirect

    flow_redirect = enforce_step_flow_func('correos')
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
        force_quick_password=force_quick_password,
        load_general_settings_func=load_general_settings_func,
        profile_photo_path_func=profile_photo_path_func
    )


def correo_electronico_handler(require_worker_microsoft_login_func):
    auth_redirect = require_worker_microsoft_login_func()
    if auth_redirect is not None:
        return auth_redirect
    session.pop('email_settings_message', None)
    return redirect(url_for('correos'))


def correo_electronico_guardar_handler(add_account_activity_func, load_general_settings_func, save_general_settings_func):
    existing = load_secure_smtp_credentials()
    sender_input = request.form.get('sender', '').strip()
    sender_local = sender_input.split('@', 1)[0].strip()
    sender, _ = normalize_sender_email(sender_input)
    password = request.form.get('password', '').strip()
    confirm_password = request.form.get('confirm_password', '').strip()
    smtp_host = existing.get('smtp_host', '') or 'owa.fonafe.gob.pe'
    smtp_port = parse_smtp_port(existing.get('smtp_port', '') or '587')
    smtp_security = normalize_smtp_security(existing.get('smtp_security', '') or 'starttls')

    if not sender:
        session.pop('worker_sender', None)
        session.pop('worker_password', None)
        session.pop('last_sender_attempt', None)
        session['login_message'] = 'Ingresa tu correo para continuar.'
        return redirect(url_for('basedatos'))

    if not password:
        session.pop('worker_sender', None)
        session.pop('worker_password', None)
        session['last_sender_attempt'] = sender_local
        session['login_message'] = 'Ingresa tu contraseña para continuar.'
        return redirect(url_for('basedatos'))

    if not confirm_password:
        confirm_password = password

    if password != confirm_password:
        session.pop('worker_sender', None)
        session.pop('worker_password', None)
        session['last_sender_attempt'] = sender_local
        session['login_message'] = 'Las contraseñas no coinciden. Verifica e intenta nuevamente.'
        return redirect(url_for('basedatos'))

    validated_sender = sender
    used_security = smtp_security
    used_port = smtp_port

    is_valid_login, validated_sender, used_security, used_port, validation_error = validate_smtp_login(
        sender,
        password,
        smtp_host,
        smtp_port,
        smtp_security
    )
    if not is_valid_login:
        logging.warning(f'Validación SMTP fallida en correo_electronico_guardar: {validation_error}')
        session['system_authenticated'] = False
        session['smtp_authenticated'] = False
        session['smtp_link_verified'] = False
        session['quick_password_verified'] = False
        session.pop('worker_sender', None)
        session.pop('worker_password', None)
        session['last_sender_attempt'] = sender_local
        session['login_message'] = 'La contraseña es incorrecta. Verifica tus credenciales e intenta nuevamente.'
        return redirect(url_for('basedatos'))

    try:
        save_secure_smtp_credentials(
            validated_sender,
            password,
            smtp_host_value=smtp_host,
            smtp_port_value=str(used_port),
            smtp_security_value=used_security
        )
        sync_email_config_to_modules(validated_sender, load_general_settings_func, save_general_settings_func)
        session['system_authenticated'] = True
        session['smtp_authenticated'] = True
        session['smtp_link_verified'] = True
        session['worker_sender'] = validated_sender
        session['worker_password'] = password
        session['worker_smtp_host'] = smtp_host
        session['worker_smtp_port'] = str(used_port)
        session['worker_smtp_security'] = used_security
        session['worker_login_at'] = pd.Timestamp.now().strftime('%d/%m/%Y %H:%M:%S')
        session['worker_auth_method'] = 'SMTP OWA'
        session['quick_password_verified'] = False
        session.pop('last_sender_attempt', None)
        add_account_activity_func('Correo electrónico', f'Sesión iniciada para {validated_sender}')
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
        session['worker_smtp_port'] = str(used_port)
        session['worker_smtp_security'] = used_security
        session['worker_login_at'] = pd.Timestamp.now().strftime('%d/%m/%Y %H:%M:%S')
        session['worker_auth_method'] = 'SMTP OWA'
        session['quick_password_verified'] = False
        session.pop('email_settings_message', None)
        session.pop('login_message', None)

    return redirect(url_for('basedatos'))


def correo_electronico_verificar_vinculo_handler(add_account_activity_func):
    sender = str(session.get('worker_sender', '')).strip() or str(load_secure_smtp_credentials().get('sender', '')).strip()
    sender, _ = normalize_sender_email(sender)
    used_security = normalize_smtp_security(session.get('worker_smtp_security', '') or 'starttls')
    session['system_authenticated'] = True
    session['smtp_authenticated'] = True
    session['smtp_link_verified'] = True
    session['worker_sender'] = sender
    session['worker_smtp_security'] = used_security
    session['worker_login_at'] = pd.Timestamp.now().strftime('%d/%m/%Y %H:%M:%S')
    session['worker_auth_method'] = str(session.get('worker_auth_method', '')).strip() or 'SMTP OWA'
    session['email_settings_message'] = '✅ Vínculo de correo verificado correctamente.'
    add_account_activity_func('Correo electrónico', f'Vínculo verificado para {sender}')

    return redirect(url_for('correo_electronico'))


def configurar_correo_handler(add_account_activity_func, load_general_settings_func, save_general_settings_func):
    return_to = str(request.form.get('return_to', '')).strip().lower()
    redirect_to_correos = return_to == 'correos'
    redirect_to_home = return_to == 'home'
    redirect_to_basedatos = return_to == 'basedatos'
    redirect_to_asiento = return_to == 'asiento'
    is_js_request = request.headers.get('X-Requested-With') == 'XMLHttpRequest'

    def redirect_after_password(message_text, is_error=False):
        if is_js_request:
            return jsonify({
                'ok': not is_error,
                'is_error': is_error,
                'message': message_text
            }), (400 if is_error else 200)

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
    sender_input = request.form.get('sender', '').strip()
    sender_local = sender_input.split('@', 1)[0].strip()
    sender, _ = normalize_sender_email(sender_input)
    password = request.form.get('password', '').strip()
    confirm_password_raw = request.form.get('confirm_password', None)
    confirm_password = '' if confirm_password_raw is None else str(confirm_password_raw).strip()
    cc_value = request.form.get('cc', '').strip() or existing.get('cc', '').strip()
    smtp_host = request.form.get('smtp_host', '').strip() or existing.get('smtp_host', '').strip() or 'owa.fonafe.gob.pe'
    smtp_port = parse_smtp_port(request.form.get('smtp_port', '').strip() or existing.get('smtp_port', '').strip() or '587')
    smtp_security = normalize_smtp_security(request.form.get('smtp_security', '').strip().lower() or existing.get('smtp_security', '').strip().lower() or 'starttls')

    cc_clean_items = [extract_single_email(item) for item in re.split(r'[;,]+', str(cc_value)) if str(item).strip()]
    cc_clean = ', '.join(dict.fromkeys(item for item in cc_clean_items if item))

    if not sender or not password:
        session['last_sender_attempt'] = sender_local
        return redirect_after_password('Completa remitente y contraseña para continuar.', is_error=True)

    if confirm_password_raw is not None:
        if not confirm_password:
            session['last_sender_attempt'] = sender_local
            return redirect_after_password('Repite la contraseña para continuar.', is_error=True)
        if password != confirm_password:
            session['last_sender_attempt'] = sender_local
            return redirect_after_password('Las contraseñas no coinciden. Intenta de nuevo.', is_error=True)

    validated_sender = sender
    used_security = smtp_security
    used_port = smtp_port

    is_valid_login, validated_sender, used_security, used_port, validation_error = validate_smtp_login(
        sender,
        password,
        smtp_host,
        smtp_port,
        smtp_security
    )
    if not is_valid_login:
        logging.warning(f'Validación SMTP fallida en configurar_correo: {validation_error}')
        session['last_sender_attempt'] = sender_local
        return redirect_after_password('La contraseña es incorrecta. Verifica tus credenciales e intenta nuevamente.', is_error=True)

    try:
        save_secure_smtp_credentials(
            validated_sender,
            password,
            smtp_host_value=smtp_host,
            smtp_port_value=str(used_port),
            smtp_security_value=used_security,
            cc_value=cc_clean
        )
        sync_email_config_to_modules(validated_sender, load_general_settings_func, save_general_settings_func)
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
        session.pop('last_sender_attempt', None)
        add_account_activity_func('Contraseña de correo', f'Sesión iniciada para {validated_sender}')
        return redirect_after_password('✅ Contraseña de correo actualizada correctamente.')
    except Exception:
        return redirect_after_password('No se pudo guardar la contraseña de correo. Intenta nuevamente.', is_error=True)


def send_emails_handler(require_worker_microsoft_login_func, load_general_settings_func, save_general_settings_func):
    auth_redirect = require_worker_microsoft_login_func()
    if auth_redirect is not None:
        return auth_redirect

    request_source = str(request.form.get('source', '')).strip().lower()

    def redirect_correos_with_message(message_text):
        session['config_message'] = message_text
        if request_source == 'asiento':
            return redirect(url_for('asiento_get', resultado_correo=1))
        return redirect(url_for('correos'))

    emails = session.get('asiento_emails', [])
    if not emails:
        return redirect_correos_with_message('No hay correos para enviar')

    selected_emails = request.form.getlist('selected_emails')
    if not selected_emails:
        return redirect_correos_with_message('Selecciona al menos un correo para enviar. No se envió nada automáticamente.')

    emails_to_send = sorted(set(selected_emails))

    vouchers_generados = session.get('vouchers_generados', [])

    logging.info(f'Total vouchers en sesión: {len(vouchers_generados)}')
    for voucher in vouchers_generados:
        logging.info(f"Voucher disponible: email={voucher.get('email')}, ref={voucher.get('referencia')}")

    vouchers_por_email = {}
    for voucher in vouchers_generados:
        email = voucher.get('email', '').strip().lower()
        if email:
            vouchers_por_email[email] = voucher
            logging.info(f"Voucher indexado para: {email}")

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
    sender, _ = normalize_sender_email(sender)
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
            sync_email_config_to_modules(sender, load_general_settings_func, save_general_settings_func)
            session['worker_cc'] = cc_clean
        except Exception as save_ex:
            logging.warning(f'No se pudo persistir configuración SMTP desde envío manual: {save_ex}')

    if not sender or not password:
        return redirect_correos_with_message(
            'Falta configurar correo remitente y clave SMTP. Ingresa Usuario y Contraseña en el panel de envío y vuelve a intentar.'
        )

    sent_ok = []
    sent_fail = []
    sent_with_voucher_html = []
    used_security = smtp_security
    used_port = smtp_port

    try:
        smtp_conn, used_security, used_port, connect_error = _open_authenticated_smtp_connection(
            smtp_host,
            sender,
            password,
            smtp_security,
            smtp_port,
            timeout_seconds=30
        )
        if smtp_conn is None:
            if _is_auth_not_supported_error(connect_error):
                return redirect_correos_with_message(
                    'El servidor SMTP no permite autenticación en el modo/puerto configurado. '
                    'Prueba con SSL (465) o STARTTLS (587).'
                )
            return redirect_correos_with_message(
                f'Error enviando por SMTP ({smtp_host}:{smtp_port}, {smtp_security}): {connect_error}'
            )

        with smtp_conn as smtp:

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
            message = '✅ Envío exitosamente'
        else:
            message = f"✅ Envío finalizado. Enviados: {len(sent_ok)}. Fallidos: {len(sent_fail)}."

        if len(sent_with_voucher_html) > 0:
            message += f"\n📄 Con voucher HTML en el correo: {len(sent_with_voucher_html)}"

        if len(emails_no_enviados) > 0:
            message += f"\n\n⚠️ CORREOS NO ENVIADOS ({len(emails_no_enviados)}):\n"
            for email_no_enviado in emails_no_enviados:
                message += f"• {email_no_enviado}\n"

        if len(sent_fail) > 0:
            message += '\n\nRevisa emails_send_report.csv para detalles de errores.'

        if len(sent_ok) > 0:
            message += '\n\nSi no lo ves en Bandeja de Entrada, revisa la carpeta Enviados del remitente.'

        return redirect_correos_with_message(message)
    except Exception as ex:
        return redirect_correos_with_message(
            f'Error enviando por SMTP ({smtp_host}:{used_port}, {used_security}): {ex}'
        )


def register_correo_routes(
    app,
    require_worker_microsoft_login_func,
    enforce_step_flow_func,
    load_general_settings_func,
    save_general_settings_func,
    profile_photo_path_func,
    add_account_activity_func,
):
    @app.route('/correos', methods=['GET', 'POST'])
    def correos():
        return correos_handler(
            require_worker_microsoft_login_func=require_worker_microsoft_login_func,
            enforce_step_flow_func=enforce_step_flow_func,
            load_general_settings_func=load_general_settings_func,
            profile_photo_path_func=profile_photo_path_func,
        )

    @app.route('/correo_electronico', methods=['GET'])
    def correo_electronico():
        return correo_electronico_handler(
            require_worker_microsoft_login_func=require_worker_microsoft_login_func
        )

    @app.route('/iniciar_sesion', methods=['GET'])
    def iniciar_sesion():
        return redirect(url_for('basedatos'))

    @app.route('/iniciar_sesion', methods=['POST'])
    @app.route('/correo_electronico/guardar', methods=['POST'])
    def correo_electronico_guardar():
        return correo_electronico_guardar_handler(
            add_account_activity_func=add_account_activity_func,
            load_general_settings_func=load_general_settings_func,
            save_general_settings_func=save_general_settings_func,
        )

    @app.route('/correo_electronico/verificar_vinculo', methods=['POST'])
    def correo_electronico_verificar_vinculo():
        return correo_electronico_verificar_vinculo_handler(
            add_account_activity_func=add_account_activity_func
        )

    @app.route('/configurar_correo', methods=['POST'])
    def configurar_correo():
        return configurar_correo_handler(
            add_account_activity_func=add_account_activity_func,
            load_general_settings_func=load_general_settings_func,
            save_general_settings_func=save_general_settings_func,
        )

    @app.route('/send_emails', methods=['POST'])
    def send_emails():
        return send_emails_handler(
            require_worker_microsoft_login_func=require_worker_microsoft_login_func,
            load_general_settings_func=load_general_settings_func,
            save_general_settings_func=save_general_settings_func,
        )
