import unittest

import pandas as pd
from AsientoService import AsientoService

class AccountEntries_test(unittest.TestCase):
    def test_contraloria_gral_republica(self):
            movimientosAsientos ={
                "Fecha": ["04/07/2023"],
                "Fecha valuta": [""],
                "Descripción operación": ["BCO.NACI0000"],
                "Monto": ["5723.2"],
                "Saldo": ["4426163.58"],
                "Sucursal - agencia": ["191-000"],
                "Operación - Número": ["08555500"],
                "Operación - Hora": ["16:39:14"],
                "Usuario": ["RCJN"],
                "UTC": ["2014"],
                "Referencia2": [""],
                "Referencia": ["CONTRALORIA GRAL DE LA REPUBLICA 03"],
                "Procedencia": [""]
            }
            export_teste = {
                "Documento compras": [1041032011],
                "Icono part.abiertas/comp.": [""],
                "Acreedor": [""],
                "Cuenta": [7000013520],
                "Fecha de documento": ["4/07/2023"],
                "Fe.contabilización": ["6/07/2023"],
                "Nº documento": [7000013520],
                "Clase de documento": ["DI"],
                "Referencia": [600737402],
                "Doc.compensación": ["230707"],
                "Texto": [0.00],
                "Moneda del documento": ["PEN"],
                "Importe en moneda local": [5723.2],
                "División": [212],
                "Ejercicio / mes": ["2023/07"],
                "Nombre del usuario": ["INT-OPTIMUS"],
                "Clave contabiliz.": [40],
                "Asignación": ["555500"],
                "Indicador Debe/Haber": ["S"],
                "Importe en ML2": [1576.64],
                "Centro de coste": [""],
                "Centro de beneficio": [""]
            }
            
            movimiento_teste = pd.DataFrame(movimientosAsientos)
            export_teste = pd.DataFrame(export_teste)
            asientoService = AsientoService()
            asientoService._conciliar_df(movimiento_teste, export_teste)
            self.assertEqual(asientoService.df_movimientos["Asientos"][0],7000013520)

    def test_busqueda_por_fecha_y_operacion(self):
        movimientosAsientos = {
        "Fecha": ["07/03/2024", "05/03/2024", "05/03/2024", "04/03/2024"],
        "Fecha valuta": ["", "", "", ""],
        "Descripción operación": ["EFECTIVO00000025784519", "EFECTIVO00000025790310", "EFECTIVO00000026463900", "EFECTIVO00000025775289"],
        "Monto": ["50", "100", "50", "550"],
        "Saldo": ["2250031.21", "2048573.25", "2047514.96", "2217581.67"],
        "Sucursal - agencia": ["111-017", "111-017", "111-017", "111-017"],
        "Operación - Número": ["00133294", "00374868", "00133294", "00374868"],
        "Operación - Hora": ["13:20:21", "19:39:03", "15:02:07", "11:26:38"],
        "Usuario": ["996119", "993669", "981699", "986910"],
        "UTC": ["1013", "1013", "1013", "1013"],
        "Referencia2": ["", "", "", ""],
        "Referencia": ["MONTENEGRO DE SAMAME DONATILA", "CALDERON DE CARDENAS, GLADYS JUSTINA", "UGAZ TAPIA, LUIS DANTE", "IZASIGA BARCO, DINO  WILLIAN"],
        "Procedencia": ["PREPAGO", "PREPAGO", "PREPAGO", "PREPAGO"]
        }
        
        export_teste = {
            "Icono part.abiertas/comp.": ["1041032011", "1041032011", "1041032011", "1041032011"],
            "Cuenta": ["", "", "", ""],
            "Fecha de documento": ["04/03/2024", "05/03/2024", "5/03/2024", "7/03/2024"],
            "Fe.contabilización": ["05/03/2024", "07/03/2024", "06/03/2024", "11/03/2024"],
            "Importe en moneda local": ["550.00", "100.00", "50.00", "50.00"],
            "Asignación": ["0374868", "0374868", "0133294", "0133294"],
            "Nº documento": ["7000006805", "7000007123", "7000006892", "7000007296"],
            "Clase de documento": ["DI", "DI", "DI", "DI"],
            "Referencia": ["2403042510374868", "2403052510374868", "2403052510133294", "2403072510133294"],
            "Doc.compensación": ["600317724", "600317728", "600317726", "600317727"],
            "Texto": ["240305        0.00", "240307        0.00", "240306        0.00", "240311        0.00"],
            "Moneda del documento": ["PEN", "PEN", "PEN", "PEN"],
            "División": ["0212", "0212", "0212", "0212"],
            "Ejercicio / mes": ["2024/03", "2024/03", "2024/03", "2024/03"],
            "Nombre del usuario": ["INT-OPTIMUS", "INT-OPTIMUS", "INT-OPTIMUS", "INT-OPTIMUS"],
            "Clave contabiliz.": ["40", "40", "40", "40"],
            "Indicador Debe/Haber": ["S", "S", "S", "S"],
            "Importe en ML2": ["145.77", "26.52", "13.26", "13.38"]
        }


            
        movimiento_teste = pd.DataFrame(movimientosAsientos)
        export_teste = pd.DataFrame(export_teste)
        asientoService = AsientoService()
        asientoService._conciliar_df(movimiento_teste, export_teste)
        self.assertEqual(asientoService.df_movimientos["Asientos"][0],'7000007296')

    def test_asiento_guivar_silva(self):
                movimientos_data ={
        'Fecha': ['15/08/2023'],
        'Fecha valuta': [None],
        'Descripción operación': ['EFECTIVO00000025788302'],
        'Monto': [50],
        'Saldo': [4202405.12],
        'Sucursal - agencia': ['111-017'],
        'Operación - Número': [555716],
        'Operación - Hora': ['14:36:02'],
        'Usuario': [988379],
        'UTC': [1013],
        'Referencia2': [None],
        'Referencia': ['GUIVAR SILVA, MARIA CONSUELO'],
        'Procedencia': ['PREPAGO']
    }
                export_data = {
                    "Documento compras": [1041032011],
                    "Icono part.abiertas/comp.": [""],
                    "Acreedor": [""],
                    "Cuenta": [7000017736],
                    "Fecha de documento": ["4/07/2023"],
                    "Fe.contabilización": ["6/07/2023"],
                    "Nº documento": [7000017736],
                    "Clase de documento": ["DI"],
                    "Referencia": [600737402],
                    "Doc.compensación": ["230707"],
                    "Texto": [0.00],
                    "Moneda del documento": ["PEN"],
                    "Importe en moneda local": [5723.2],
                    "División": [212],
                    "Ejercicio / mes": ["2023/07"],
                    "Nombre del usuario": ["INT-OPTIMUS"],
                    "Clave contabiliz.": [40],
                    "Asignación": ["0555716"],
                    "Indicador Debe/Haber": ["S"],
                    "Importe en ML2": [1576.64],
                    "Centro de coste": [""],
                    "Centro de beneficio": [""]
                }
                
                movimiento_teste = pd.DataFrame(movimientos_data)
                export_teste = pd.DataFrame(export_data)
                asientoService = AsientoService()
                asientoService._conciliar_df(movimiento_teste, export_teste)
                self.assertEqual(asientoService.df_movimientos["Asientos"][0],7000017736)
