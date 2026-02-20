import sys
import os
sys.path.insert(0, os.getcwd())
from app import app

client = app.test_client()
files = {}
base = os.path.join(os.getcwd(), 'files')
mov = os.path.join(base, 'movimientos.xlsx')
asiento = os.path.join(base, 'asientos.xlsx')
if os.path.exists(mov) and os.path.exists(asiento):
    with open(mov, 'rb') as f1, open(asiento, 'rb') as f2:
        data = {
            'file': [
                (f1, 'movimientos.xlsx'),
                (f2, 'asientos.xlsx')
            ]
        }
        resp = client.post('/asiento', data=data, content_type='multipart/form-data')
        print('STATUS', resp.status_code)
        print(resp.data.decode('utf-8')[:1000])
else:
    print('No hay archivos de prueba en files/ mov or asiento missing')
