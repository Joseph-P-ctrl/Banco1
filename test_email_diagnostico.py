"""
Script de diagnóstico para envío de correo Microsoft 365
"""
import smtplib
from email.message import EmailMessage
import socket
import os
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

SENDER = os.environ.get('OUTLOOK_SENDER', '').strip()
PASSWORD = os.environ.get('OUTLOOK_PASSWORD', '').strip()
RECIPIENT = os.environ.get('OUTLOOK_RECIPIENT', SENDER).strip()

def main():
    if not SENDER or not PASSWORD:
        print('Faltan OUTLOOK_SENDER/OUTLOOK_PASSWORD en variables de entorno o .env')
        return

    print("=== Diagnóstico de Conexión Microsoft 365 ===\n")

    print("1. Verificando conectividad de red a smtp.office365.com...")
    try:
        socket.create_connection(("smtp.office365.com", 587), timeout=10)
        print("   ✓ Conexión de red exitosa\n")
    except Exception as e:
        print(f"   ✗ Error de conexión de red: {e}\n")
        return

    print("2. Conectando a SMTP...")
    try:
        smtp = smtplib.SMTP('smtp.office365.com', 587, timeout=30)
        print("   ✓ Conexión SMTP establecida\n")

        print("3. Enviando EHLO inicial...")
        code, response = smtp.ehlo()
        print(f"   Código: {code}")
        print(f"   Respuesta: {response.decode()[:200]}...\n")

        print("4. Iniciando STARTTLS...")
        smtp.starttls()
        print("   ✓ STARTTLS iniciado\n")

        print("5. Enviando EHLO después de STARTTLS...")
        code, response = smtp.ehlo()
        print(f"   Código: {code}")
        print(f"   Respuesta: {response.decode()[:200]}...\n")

        print("6. Intentando autenticación...")
        print(f"   Usuario: {SENDER}")
        print(f"   Contraseña: {'*' * len(PASSWORD)}")
        try:
            smtp.login(SENDER, PASSWORD)
            print("   ✓ Autenticación exitosa!\n")

            print("7. Intentando enviar correo de prueba...")
            msg = EmailMessage()
            msg['Subject'] = 'Prueba diagnóstico - Microsoft 365'
            msg['From'] = SENDER
            msg['To'] = RECIPIENT
            msg.set_content('Este es un correo de prueba de diagnóstico.')

            smtp.send_message(msg)
            print("   ✓✓✓ CORREO ENVIADO EXITOSAMENTE! ✓✓✓\n")

        except smtplib.SMTPAuthenticationError as e:
            print(f"   ✗ Error de autenticación: {e}\n")
        except Exception as e:
            print(f"   ✗ Error al enviar: {e}\n")
        finally:
            smtp.quit()
    except Exception as e:
        print(f"   ✗ Error general: {e}\n")

    print("\n=== Fin del diagnóstico ===")


if __name__ == '__main__':
    main()
