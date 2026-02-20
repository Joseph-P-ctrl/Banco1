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


def _smtp_key_path():
    return files_path('smtp_credentials.key')


def _smtp_credentials_path():
    return files_path('smtp_credentials.json')


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
    if total_vouchers == 0:
        total_vouchers = count_vouchers_in_folder()
    
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
        vouchers_generados=vouchers_generados
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
                
                # GENERAR VOUCHERS PARA CADA CLIENTE
                try:
                    voucher_service = VoucherService(output_dir=vouchers_path())
                    vouchers_generados = voucher_service.generar_vouchers_desde_dataframe(
                        asientoService.df_movimientos, 
                        clientes_email_map
                    )
                    # Guardar información de vouchers en sesión
                    session['vouchers_generados'] = vouchers_generados
                    logging.info(f'Vouchers generados: {len(vouchers_generados)}')
                except Exception as ve:
                    logging.error(f'Error generando vouchers: {str(ve)}')
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

    try:
        page = int(request.args.get('page', '1'))
    except ValueError:
        page = 1
    if page < 1:
        page = 1

    return render_correos_page(emails=sess_emails, mensaje_exito=warning_message, page=page)

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

    company_keywords = {
        'SAC', 'S.A.C', 'SRL', 'S.R.L', 'SA', 'S.A', 'EIRL', 'E.I.R.L',
        'GERENCIA', 'DIRECCION', 'DIREC', 'REGIONAL', 'MUNICIPALIDAD',
        'MINISTERIO', 'GOBIERNO', 'UNIDAD', 'LOGISTICA', 'AGRICULTURA',
        'POLICIAL', 'HOSPITAL', 'UNIVERSIDAD', 'COLEGIO', 'EMPRESA',
        'SERVICIOS', 'AREA', 'OFICINA'
    }

    def build_saludo(nombre_cliente):
        nombre = str(nombre_cliente or '').strip()
        if not nombre or nombre.lower() == 'cliente':
            return 'Estimado(a) cliente,'

        nombre_upper = nombre.upper()
        has_company_word = any(word in nombre_upper for word in company_keywords)
        has_digits = bool(re.search(r'\d', nombre_upper))
        words = [w for w in re.split(r'\s+', nombre.strip()) if w]
        looks_like_person = len(words) >= 2 and not has_company_word and not has_digits

        if has_company_word or has_digits:
            return f'Estimados señores de {nombre},'
        if looks_like_person:
            return f'Estimado(a) {nombre},'
        return f'Estimado(a) {nombre},'
    
    default_body = """{saludo}

Nos complace informarle que hemos recibido un abono en nuestra cuenta corriente a su nombre. Para proceder con sus recibos, le invitamos a acceder a nuestra plataforma de Oficina Virtual Distriluz: https://servicios.distriluz.com.pe/oficinavirtual.

En esta plataforma, podrá registrarse como Cliente Empresa para gestionar la cancelación de los suministros afiliados a su representada y agregar otros suministros. Podrá adjuntar la constancia del pago o transferencia realizada para completar el proceso.

Esperamos que esta herramienta le sea de gran utilidad. Agradecemos su atención y quedamos a su disposición para cualquier consulta adicional.
"""
    
    body = os.environ.get('OUTLOOK_BODY', default_body)

    if not sender or not password:
        return render_correos_page(
            emails=emails,
            mensaje_exito='Falta configurar correo remitente y clave SMTP (archivo seguro o variables de entorno). No se envió nada.',
            page=1
        )

    sent_ok = []
    sent_fail = []
    sent_with_voucher = []
    sent_without_voucher = []
    
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

                    if not voucher_info:
                        voucher_info = find_latest_voucher_for_email(recipient_lower)
                        if voucher_info:
                            vouchers_por_email[recipient_lower] = voucher_info
                            logging.info(
                                f"Voucher recuperado desde carpeta para {recipient_lower}: {voucher_info.get('filepath')}"
                            )

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

                    saludo = build_saludo(nombre_cliente)
                    body_personalizado = (
                        body
                        .replace('{saludo}', saludo)
                        .replace('{cliente_nombre}', nombre_cliente)
                        .replace('{cliente_email}', recipient)
                    )

                    msg = EmailMessage()
                    msg['Subject'] = subject
                    msg['From'] = sender
                    msg['To'] = recipient
                    # Agregar BCC para que el remitente reciba una copia de cada correo
                    msg['Bcc'] = sender
                    msg.set_content(body_personalizado)
                    
                    # Adjuntar voucher PDF si existe para este email
                    voucher_attached = False
                    
                    logging.info(f"Procesando email: {recipient} -> buscando voucher para: {recipient_lower}")
                    
                    if voucher_info:
                        voucher_path = voucher_info.get('filepath')
                        logging.info(f"Voucher encontrado para {recipient_lower}: {voucher_path}")
                        
                        if voucher_path and os.path.exists(voucher_path):
                            with open(voucher_path, 'rb') as pdf_file:
                                pdf_data = pdf_file.read()
                                pdf_filename = os.path.basename(voucher_path)
                                msg.add_attachment(pdf_data, maintype='application', 
                                                 subtype='pdf', filename=pdf_filename)
                            voucher_attached = True
                            logging.info(f"✅ Voucher {pdf_filename} adjuntado a {recipient}")
                        else:
                            logging.warning(f"⚠️ Archivo de voucher NO existe: {voucher_path}")
                    else:
                        logging.warning(f"⚠️ NO se encontró voucher para: {recipient_lower}")
                    
                    smtp.send_message(msg)

                    sender_lower = sender.strip().lower()
                    if sender_lower and recipient_lower != sender_lower:
                        try:
                            sender_copy = EmailMessage()
                            sender_copy['Subject'] = f"Copia de envío: {subject}"
                            sender_copy['From'] = sender
                            sender_copy['To'] = sender
                            sender_copy.set_content(
                                f"Se envió un correo a: {recipient}\n"
                                f"Asunto: {subject}\n\n"
                                f"Contenido enviado:\n\n{body_personalizado}"
                            )

                            if voucher_attached and voucher_info:
                                voucher_path = voucher_info.get('filepath')
                                if voucher_path and os.path.exists(voucher_path):
                                    with open(voucher_path, 'rb') as pdf_file:
                                        pdf_data = pdf_file.read()
                                        pdf_filename = os.path.basename(voucher_path)
                                        sender_copy.add_attachment(
                                            pdf_data,
                                            maintype='application',
                                            subtype='pdf',
                                            filename=pdf_filename
                                        )

                            smtp.send_message(sender_copy)
                        except Exception as sender_copy_ex:
                            logging.warning(
                                f"No se pudo enviar copia al remitente {sender}: {sender_copy_ex}"
                            )

                    sent_ok.append(recipient)
                    
                    if voucher_attached:
                        sent_with_voucher.append(recipient)
                        logging.info(f"📧 Email enviado con voucher a: {recipient}")
                    else:
                        sent_without_voucher.append(recipient)
                        logging.info(f"📧 Email enviado SIN voucher a: {recipient}")
                        
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
        
        # Información de vouchers adjuntados
        if len(sent_with_voucher) > 0:
            message += f"\n📄 Con voucher adjunto: {len(sent_with_voucher)}"
        if len(sent_without_voucher) > 0:
            message += f"\n⚠️ Sin voucher adjunto: {len(sent_without_voucher)}"
        
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

    