"""
Prueba de envío con el servidor owa.fonafe.gob.pe
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

    print("=== Probando servidor: owa.fonafe.gob.pe ===\n")

    ports = [587, 25, 465]

    for port in ports:
        print(f"\n{'='*60}")
        print(f"Probando puerto: {port}")
        print('='*60)

        try:
            if port == 465:
                print("Usando SMTP_SSL...")
                smtp = smtplib.SMTP_SSL('owa.fonafe.gob.pe', port, timeout=30)
                smtp.ehlo()
            else:
                print("Usando SMTP con STARTTLS...")
                smtp = smtplib.SMTP('owa.fonafe.gob.pe', port, timeout=30)
                code, response = smtp.ehlo()
                print(f"✓ EHLO: {code}")
                smtp.starttls()
                print("✓ STARTTLS exitoso")
                smtp.ehlo()

            print("Intentando autenticación...")
            smtp.login(SENDER, PASSWORD)
            print("✓✓ AUTENTICACIÓN EXITOSA!")

            print("Enviando correo de prueba...")
            msg = EmailMessage()
            msg['Subject'] = 'Prueba exitosa - owa.fonafe.gob.pe'
            msg['From'] = SENDER
            msg['To'] = RECIPIENT
            msg.set_content('¡Correo de prueba enviado exitosamente desde el sistema!\n\nServidor: owa.fonafe.gob.pe\nPuerto: ' + str(port))

            smtp.send_message(msg)
            print("✓✓✓ CORREO ENVIADO EXITOSAMENTE!")
            print(f"\n*** CONFIGURACIÓN CORRECTA ***")
            print(f"Servidor: owa.fonafe.gob.pe")
            print(f"Puerto: {port}")
            print(f"Método: {'SSL' if port == 465 else 'STARTTLS'}")

            smtp.quit()
            break

        except Exception as e:
            print(f"✗ Error: {e}")
            print(f"  Tipo: {type(e).__name__}")

    print("\n" + "="*60)
    print("Fin de pruebas")
    print("="*60)


if __name__ == '__main__':
    main()
