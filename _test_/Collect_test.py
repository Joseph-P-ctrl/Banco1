import unittest
import pandas as pd
from unittest.mock import Mock
from unittest.mock import patch
from AccountService import AccountService


class Collect_test(unittest.TestCase):
    def test_recaudo_EFECTIVO00000027149820(self):
     
            dic_recaudo = {
                                    'Código del Depositante': ['27149820'],
                                    'Nombre del Depositante': ['EVASQUEZ VASQUEZ MANUEL'],
                                    'Información de Retorno': ['PAGO DEUDA TOTAL'],
                                    'FECHA GENERACION': '09.08.2023',
                                    'AREA SOLICITANTE': ['SUCURSALES'],
                                    'FECHA DEPOSITO BCP': ''
                                }
            # Create DataFrame
            df_recaudo = pd.DataFrame(dic_recaudo)
            
            movimientoProveedores = {
                "Fecha": ["09/08/2023"],
                "Fecha valuta": [""],  # This field appears to be empty in the given data
                "Descripción operación": ["EFECTIVO00000027149820"],
                "Monto": [2832.16],
                "Saldo": ["2,597,597.93"],  # You might want to represent this as a float or integer based on use-case
                "Sucursal - agencia": ["111-008"],
                "Operación - Número": ["01248993"],
                "Operación - Hora": ["16:55:55"],
                "Usuario": ["TNP100"],
                "UTC": ["2401"],
                "Referencia2": ["Pago Fact.14007"]
            }
            movimientos = pd.DataFrame(movimientoProveedores)
            accountService = AccountService()   
            with patch.object(AccountService, "read_recaudos", return_value=df_recaudo):
                accountService._process_movements_df(movimientos)
            self.assertEqual( accountService.movimientos["Procedencia"][0],"COD.RECAUDO-SUCURSALES")  # Aserciones de prueba según lo que esperas
            self.assertEqual(accountService.recaudos["fecha_dep"][0],"09/08/2023")
            
            # Realiza las aserciones correspondientes para verificar los resultados
    

    def test_prepago_EFECTIVO00000026456352(self):
            movimientoProveedores = {
                "Fecha": ["16/08/2023"],
                "Fecha valuta": [""],  # This field appears to be empty in the given data
                "Descripción operación": ["EFECTIVO00000026456352"],
                "Monto": [50],
                "Saldo": ["4,493,572.09"],  # You might want to represent this as a float or integer based on use-case
                "Sucursal - agencia": ["111-017"],
                "Operación - Número": ["00185908"],
                "Operación - Hora": ["16:55:55"],
                "Usuario": ["TNP100"],
                "UTC": ["2401"],
                "Referencia2": ["Pago Fact.14007"]
            }
            movimientos = pd.DataFrame(movimientoProveedores)
            accountService = AccountService()   
            accountService._process_movements_df(movimientos)
            self.assertEqual( accountService.movimientos["Procedencia"][0],"PREPAGO-26456352")  # Aserciones de prueba según lo que esperas
    
    def test_trabajador_EFECTIVO00000040554960(self):
            movimientoProveedores = {
                "Fecha": ["16/08/2023"],
                "Fecha valuta": [""],  # This field appears to be empty in the given data
                "Descripción operación": ["EFECTIVO00000040554960"],
                "Monto": [183.50],
                "Saldo": ["4,493,572.09"],  # You might want to represent this as a float or integer based on use-case
                "Sucursal - agencia": ["111-023"],
                "Operación - Número": ["03103898"],
                "Operación - Hora": ["16:55:55"],
                "Usuario": ["TNP100"],
                "UTC": ["2401"],
                "Referencia2": ["Pago Fact.14007"]
            }
            movimientos = pd.DataFrame(movimientoProveedores)
            accountService = AccountService()   
            accountService._process_movements_df(movimientos)
            self.assertEqual( accountService.movimientos["Procedencia"][0],"TRABAJADOR-40554960")  # Aserciones de prueba según lo que esperas