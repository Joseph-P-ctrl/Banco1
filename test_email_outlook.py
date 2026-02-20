"""
Script de prueba para enviar correo usando Microsoft 365 (Outlook)
Configuración para: u212prac01@distriluz.com.pe
"""
import smtplib
from email.message import EmailMessage
import os
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

# Configuración del correo
SENDER = os.environ.get('OUTLOOK_SENDER', '').strip()
PASSWORD = os.environ.get('OUTLOOK_PASSWORD', '').strip()
RECIPIENT = os.environ.get('OUTLOOK_RECIPIENT', SENDER).strip()  # Cambia a tu correo de destino para la prueba
SUBJECT = 'Prueba de correo - Microsoft 365'
BODY = '''Estimado(a),

Este es un correo de prueba enviado desde el sistema local usando Microsoft 365.

Si recibes este correo, la configuración está funcionando correctamente.

Saludos.
'''

def send_test_email():
    if not SENDER or not PASSWORD:
        print('Faltan OUTLOOK_SENDER/OUTLOOK_PASSWORD en variables de entorno o .env')
        return

    try:
        print(f"Conectando a smtp.office365.com:587...")
        print(f"Remitente: {SENDER}")
        print(f"Destinatario: {RECIPIENT}")
        
        # Crear el mensaje
        msg = EmailMessage()
        msg['Subject'] = SUBJECT
        msg['From'] = SENDER
        msg['To'] = RECIPIENT
        msg.set_content(BODY)
        
        # Conectar y enviar
        with smtplib.SMTP('smtp.office365.com', 587, timeout=30) as smtp:
            smtp.ehlo()
            print("Iniciando STARTTLS...")
            smtp.starttls()
            smtp.ehlo()
            print("Autenticando...")
            smtp.login(SENDER, PASSWORD)
            print("Enviando correo...")
            smtp.send_message(msg)
            print("✓ Correo enviado exitosamente!")
            
    except smtplib.SMTPAuthenticationError as e:
        print(f"✗ Error de autenticación: {e}")
        print("\nPosibles soluciones:")
        print("1. Verifica que el usuario y contraseña sean correctos")
        print("2. Verifica que la cuenta no requiera autenticación de dos factores")
        print("3. Verifica que SMTP esté habilitado en tu cuenta de Microsoft 365")
    except smtplib.SMTPException as e:
        print(f"✗ Error SMTP: {e}")
    except Exception as e:
        print(f"✗ Error general: {e}")

if __name__ == '__main__':
    print("=== Prueba de Envío de Correo - Microsoft 365 ===\n")
    send_test_email()
