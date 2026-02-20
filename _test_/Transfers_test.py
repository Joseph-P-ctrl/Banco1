import unittest

import pandas as pd
from AsientoService import AsientoService
from TransferService import TransferService
from ProviderService import ProviderService
from AccountService import AccountService

class TestReadExcel(unittest.TestCase):
    
    def test_transferencias_consorcio_electrico_villacuri(self):
        
        traferedatos =   {
            "Ordenante": ["CONSORCIO ELECTRICO DE VILLACURI S.A.C."],
            "Fecha de abono": ["27/07/2023"],
            "Cuenta - T": ["C"],
            "Cuenta - M": ["S/"],
            "Cuenta - Número": ["305-0037523-0-27"],
            "Monto de operación - Moneda": ["S/"],
            "Monto de operación": [166916.42],
            "Monto de operación T/C": ["0.00"],
            "Monto abonado - Moneda": ["S/"],
            "Monto abonado": [166916.42]
        }
        movimientosTraferencias = {
            "Fecha": ["27/07/2023"],
            "Fecha valuta": [""],
            "Descripción operación": ["DE CONSORCIO ELECTRICO"],
            "Monto": [166916.42],
            "Saldo": [2438113.94],
            "Sucursal - agencia": ["111-008"],
            "Operación - Número": ["03070075"],
            "Operación - Hora": ["16:11:01"],
            "Usuario": ["TNP0UA"],
            "UTC": ["2401"],
            "Referencia2": [""]
        }
        traferencias =pd.DataFrame(traferedatos)
        
        movimientos = pd.DataFrame(movimientosTraferencias)
        inteService = TransferService()
        accountService = AccountService()   
        accountService._process_movements_df(movimientos)
        inteService.setMovimientos(accountService.movimientos)  
            
        inteService._process_transfers_df(traferencias)  
        self.assertEqual( inteService.movimientos["Referencia"][0],"CONSORCIO ELECTRICO DE VILLACURI S.A.C.")
    
    def test_emp_regional_serv_publico(self):
        
        transfer =   {
    "Ordenante": ["EMPRESA REGIONAL DE SERVICIO PUBLICO DE ELECTRICIDAD DEL NORTE S.A."],
    "Fecha de abono": ["09/08/2023"],
    "Cuenta - T": ["C"],
    "Cuenta - M": ["S/"],
    "Cuenta - Número": ["305-0037523-0-27"],
    "Monto de operación - Moneda": ["S/"],
    "Monto de operación": ["1,180,000.00"],
    "T/C": ["0.00"],
    "Monto abonado - Moneda": ["S/"],
    "Monto abonado": ["1,180,000.00"]
}
        movimientos = {
    "Fecha": ["09/08/2023"],
    "Fecha valuta": [""],
    "Descripción operación": ["DE EMP.REG.DE SERV.PUB"],
    "Monto": ["1,180,000.00"],
    "Saldo": ["4398085"],
    "Sucursal - agencia": ["111-008"],
    "Operación - Número": ["03026831"],
    "Operación - Hora": ["12:51:58"],
    "Usuario": ["TNP131"],
    "UTC": ["2406"],
    "Referencia2": [""],
    "Referencia": [""],
    "Procedencia": [""]
}
        df_transfer =pd.DataFrame(transfer)
        
        df_movimientos = pd.DataFrame(movimientos)
        transferService = TransferService()
        accountService = AccountService()   
        accountService._process_movements_df(df_movimientos)
        transferService.setMovimientos(accountService.movimientos)  
            
        transferService._process_transfers_df(df_transfer)  
        self.assertEqual( transferService.movimientos["Referencia"][0],"EMPRESA REGIONAL DE SERVICIO PUBLICO DE ELECTRICIDAD DEL NORTE S.A.")
    

    def test_cable_nortetv(self):
        
        transfer =   {
    "Ordenante": ["CABLENORTV SAC"],
    "Fecha de abono": ["04/08/2023"],
    "Cuenta - T": ["C"],
    "Cuenta - M": ["S/"],
    "Cuenta - Número": ["305-0037523-0-27"],
    "Monto de operación - Moneda": ["S/ "],
    "Monto de operación": ["2,390.37"],
    "T/C": ["0.00"],
    "Monto abonado - Moneda": ["S/"],
    "Monto abonado": ["2,390.37"]
}
        movimientos =  {
    "Fecha": ["04/08/2023"],
    "Fecha valuta": [""],
    "Descripción operación": ["DE CABLENORTV SAC"],
    "Monto": ["2,390.37"],
    "Saldo": ["1660931.09"],
    "Sucursal - agencia": ["111-008"],
    "Operación - Número": ["03032687"],
    "Operación - Hora": ["12:45:53"],
    "Usuario": ["TNP0R4"],
    "UTC": ["2401"],
    "Referencia2": [""],
    "Referencia": [""]
}
        df_transfer =pd.DataFrame(transfer)
        
        df_movimientos = pd.DataFrame(movimientos)
        transferService = TransferService()
        accountService = AccountService()   
        accountService._process_movements_df(df_movimientos)
        transferService.setMovimientos(accountService.movimientos)  
            
        transferService._process_transfers_df(df_transfer)  
        self.assertEqual( transferService.movimientos["Referencia"][0],"CABLENORTV SAC")
    
        


if __name__ == '__main__':
    unittest.main()
