import unittest

import pandas as pd
from AsientoService import AsientoService
from TransferService import TransferService
from ProviderService import ProviderService
from AccountService import AccountService

class TestReadExcel(unittest.TestCase):
    def test_proveedores(self):
        proveedores = {
            "Ordenante - Nombre o Razón Social": ["KALLPA GENERACION SA"] * 20,
            "Ordenante - RUC/DNI": ["RUC"] * 20,
            "Ordenante - Número": [20538810682] * 20,
            "Documento - Tipo": ["Factura del proveedor"] * 20,
            "Nº de documento": [
                "00F061-00013868", "00F061-00013870", "00F061-00013865", "00F061-00013872",
                "00F061-00013862", "00F061-00013860", "00F061-00013859", "00F061-00013855",
                "00F061-00013866", "00F061-00013863", "00F061-00013852", "00F061-00013858",
                "00F061-00013861", "00F061-00013853", "00F061-00013857", "00F061-00013871",
                "00F061-00013869", "00F061-00013856", "00F061-00013864", "00F061-00013854"
            ],
            "Fecha de pago": ['27/07/2023'] * 20,
            "Cuenta, crédito o tarjeta de crédito de destino - T": ["C"] * 20,
            "Cuenta, crédito o tarjeta de crédito de destino - M": ["S/"] * 20,
            "Cuenta, crédito o tarjeta de crédito de destino - Número": ["305-0037523-0-27"] * 20,
            "Monto abonado - Moneda": ["S/"] * 20,
            "Monto abonado": [
                12430.97, 11263.82, 9945.67, 15475.77, 3714.89, 1489.25, 14072.47, 4540.68,
                3308.18, 17776.66, 27077.20, 3273.67, 5731.50, 74744.11, 19755.35, 2170.02,
                2833.84, 3083.79, 1690.70, 43.24
            ],
            "Estado": ["Procesada"] * 20,
            "Observación": ["Ninguna"] * 20
        }
        movimientoProveedores = {
            'Fecha': ['27/07/2023'],
            'Fecha valuta': [''],
            'Descripción operación': ['VARIOS KALLPA GENERACI'],
            'Monto': [234421.78],
            'Saldo': [2672535.72],
            'Sucursal - agencia': ['111-008'],
            'Operación - Número': ['09789286'],
            'Operación - Hora': ['13:32:02'],
            'Usuario': ['TNP101'],
            'UTC': ['2401'],
            'Referencia2': ['0000010041']
        }
        proveedores_teste = pd.DataFrame(proveedores)    
        movimientos =  pd.DataFrame(movimientoProveedores)
        proveedoresService = ProviderService()
        accountService = AccountService()   
        accountService._process_movements_df(movimientos)
        proveedoresService.setMovimientos(accountService.movimientos)  
        proveedoresService._process_providers_df(proveedores_teste) 
        
          # Realiza las aserciones correspondientes para verificar los resultados
        self.assertEqual( proveedoresService.movimientos["Referencia"][0],"KALLPA GENERACION SA")  # Aserciones de prueba según lo que esperas
 
    def test_proveedores_06(self):
        proveedores = {
            "Ordenante - Nombre o Razón Social": ["CELEPSA RENOVABLES SRL"],
            "Ordenante - RUC/DNI": ["RUC"],
            "Ordenante - Número": ["20422764136"],
            "Documento - Tipo": ["Factura del proveedor"],
            "Nº de documento": ["000000000014007"],
            "Fecha de pago": ["01/08/2023"],
            "Cuenta, crédito o tarjeta de crédito de destino - T": ["C"],
            "Cuenta, crédito o tarjeta de crédito de destino - M": ["S/"],
            "Cuenta, crédito o tarjeta de crédito de destino - Número": ["305-0037523-0-27"],
            "Monto abonado - Moneda": ["S/"],
            "Monto abonado": [0.06],
            "Estado": ["Procesada"],
            "Observación": ["Ninguna"]
        }

        movimientoProveedores = {
            "Fecha": ["01/08/2023"],
            "Fecha valuta": [""],  # This field appears to be empty in the given data
            "Descripción operación": ["0000014007 CELEPSA REN"],
            "Monto": [0.06],
            "Saldo": ["11,834,790.39"],  # You might want to represent this as a float or integer based on use-case
            "Sucursal - agencia": ["111-008"],
            "Operación - Número": ["01248993"],
            "Operación - Hora": ["16:55:55"],
            "Usuario": ["TNP100"],
            "UTC": ["2401"],
            "Referencia2": ["Pago Fact.14007"]
        }
        proveedores_teste = pd.DataFrame(proveedores)    
        movimientos =  pd.DataFrame(movimientoProveedores)
        proveedoresService = ProviderService()
        accountService = AccountService()   
        accountService._process_movements_df(movimientos)
        proveedoresService.setMovimientos(accountService.movimientos)  
        proveedoresService._process_providers_df(proveedores_teste) 
        
          # Realiza las aserciones correspondientes para verificar los resultados
        self.assertEqual( proveedoresService.movimientos["Referencia"][0],"CELEPSA RENOVABLES SRL")  # Aserciones de prueba según lo que esperas


if __name__ == '__main__':
    unittest.main()
