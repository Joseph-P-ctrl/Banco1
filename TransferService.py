import os
import pandas as pd
from datetime import datetime

class MyCustomException(Exception):
        pass


class Error:
    def __init__(self):
        self.message = ""
        self.items = []
    def addItem(self, item):
        self.items.append(item)    
        

class TransferService:
     
    def __init__(self):
        self.error = Error()
        
    def setMovimientos(self,movimientos):
        self.movimientos = movimientos
        

    def _process_transfers_df(self, df_transferencias):
        print('inicio transferencias')
        df_transferencias = df_transferencias.copy()
        if len(df_transferencias.columns) < 10:
            raise MyCustomException("Archivo Transferencias no reconocido")
        if "Ordenante" not in df_transferencias.columns:
            raise MyCustomException("Archivo Transferencias no detectado")
        object_columns = df_transferencias.select_dtypes(include=["object"]).columns
        for column_name in object_columns:
            df_transferencias.loc[:, column_name] = df_transferencias[column_name].astype(str).str.strip()

        df_transferencias.loc[:, "Monto abonado"] = pd.to_numeric(
            df_transferencias["Monto abonado"].astype(str).str.replace(",", "", regex=False),
            errors='coerce'
        )
        df_transferencias.loc[:, "Fecha de abono"] =  pd.to_datetime(df_transferencias["Fecha de abono"], dayfirst=True, errors='coerce')
        print('transfer 1', df_transferencias)
          # Limpia la columna: quita espacios internos y externos
        df_transferencias.loc[:, "Monto abonado - Moneda"] = (
                df_transferencias["Monto abonado - Moneda"]
                .str.strip()           # elimina espacios al inicio y al final
                .str.replace(" ", "")  # elimina espacios internos
            )
        df_transferencias = df_transferencias.loc[df_transferencias["Monto abonado - Moneda"]=="S/"].copy()
        print('transfer 2', df_transferencias)
        self.movimientos = self.movimientos.copy()
        self.movimientos.loc[:, "Fecha"] = pd.to_datetime(self.movimientos["Fecha"], dayfirst=True)
        self.movimientos.loc[:, "Monto"] = pd.to_numeric(
            self.movimientos["Monto"].astype(str).str.replace(",", "", regex=False),
            errors='coerce'
        )
        print('transfer', df_transferencias)
        for index, row in df_transferencias.iterrows():
            fecha = row["Fecha de abono"]
            reg = self.movimientos.loc[(self.movimientos["Monto"]==row["Monto abonado"]) & (self.movimientos["Fecha"]==fecha)]
            
            print('los resultadops',reg)
            if len(reg)>1:
                self.error.message= "Mas de una coincidencia"
                self.error.addItem({"ordenante": row["Ordenante"], "monto": row["Monto abonado"], "fecha":row["Fecha de abono"]})   
            elif(len(reg)==1):
                self.movimientos.loc[(self.movimientos["Monto"]==row["Monto abonado"]) & (self.movimientos["Fecha"]==fecha), "Referencia"] = row["Ordenante"]
            else:
                 self.error.message = "Registros no ubicados"
                 self.error.addItem({"ordenante": row["Ordenante"], "monto": row["Monto abonado"], "fecha":row["Fecha de abono"]})   
    
    def process_transfers(self, transferFile):
        transferencias = pd.read_excel(transferFile, header=2)
        self._process_transfers_df(transferencias)