"""
Script alternativo para Microsoft Exchange (sin autenticación SMTP externa)
Intentando usar el servidor Exchange local
"""
import smtplib
from email.message import EmailMessage
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

    print("=== Intentando con diferentes servidores SMTP ===\n")

    servers = [
        ('smtp.office365.com', 587, 'STARTTLS'),
        ('outlook.office365.com', 587, 'STARTTLS'),
        ('mail.distriluz.com.pe', 587, 'STARTTLS'),
        ('mail.distriluz.com.pe', 25, 'STARTTLS'),
        ('smtp.distriluz.com.pe', 587, 'STARTTLS'),
        ('smtp.distriluz.com.pe', 25, 'STARTTLS'),
        ('exchange.distriluz.com.pe', 587, 'STARTTLS'),
        ('exchange.distriluz.com.pe', 25, 'STARTTLS'),
    ]

    for server, port, method in servers:
        print(f"\nProbando: {server}:{port} ({method})")
        print("-" * 50)
        try:
            smtp = smtplib.SMTP(server, port, timeout=10)
            code, response = smtp.ehlo()
            print(f"✓ Conexión exitosa")
            print(f"  Respuesta EHLO: {code}")

            if method == 'STARTTLS':
                smtp.starttls()
                smtp.ehlo()
                print(f"✓ STARTTLS exitoso")

            try:
                smtp.login(SENDER, PASSWORD)
                print(f"✓✓ AUTENTICACIÓN EXITOSA!")

                msg = EmailMessage()
                msg['Subject'] = 'Prueba Exchange - Servidor encontrado'
                msg['From'] = SENDER
                msg['To'] = RECIPIENT
                msg.set_content('Correo de prueba exitoso desde el servidor correcto.')

                smtp.send_message(msg)
                print(f"✓✓✓ CORREO ENVIADO EXITOSAMENTE!")
                print(f"\n*** USAR ESTE SERVIDOR: {server}:{port} ***\n")
                smtp.quit()
                break

            except smtplib.SMTPAuthenticationError as e:
                print(f"✗ Error de autenticación: {e}")
            except Exception as e:
                print(f"✗ Error al enviar: {e}")

            smtp.quit()

        except ConnectionRefusedError:
            print(f"✗ Conexión rechazada (puerto cerrado o firewall)")
        except TimeoutError:
            print(f"✗ Timeout (servidor no responde)")
        except Exception as e:
            print(f"✗ Error: {e}")

    print("\n=== Fin de pruebas ===")


if __name__ == '__main__':
    main()
