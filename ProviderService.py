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
        
class ProviderService:
     
    def __init__(self):
        self.error= Error()

    def setMovimientos(self,movimientos):
        self.movimientos = movimientos

    def _process_providers_df( self,df_proveedores):
        try:
            df_proveedores = df_proveedores.copy()
            if (len(df_proveedores.columns)<13):
                raise MyCustomException("Archivo Providers: Columnas no encontradas, elimine cabeceras innecesarias provecios ")
            if "Ordenante - Nombre o Razón Social" not in df_proveedores.columns:
                raise MyCustomException("Archivo Estado de Cuenta: Columnas no encontradas, elimine cabeceras innecesarias")

            df_proveedores.loc[:, "Monto abonado"] = pd.to_numeric(
                df_proveedores["Monto abonado"].astype(str).str.replace(",", "", regex=False),
                errors='coerce'
            )
            df_proveedores.loc[:, "Ordenante - Nombre o Razón Social"] = df_proveedores["Ordenante - Nombre o Razón Social"].astype(str).str.strip()
            df_proveedores.loc[:, "Fecha de pago"] = pd.to_datetime(df_proveedores["Fecha de pago"], dayfirst=True)
            new_proveedores = df_proveedores[["Monto abonado", "Ordenante - Nombre o Razón Social","Fecha de pago"]].copy()

            group_proveedores = new_proveedores.groupby(["Ordenante - Nombre o Razón Social","Fecha de pago"]).sum().round(2)
            self.movimientos = self.movimientos.copy()
            self.movimientos.loc[:, "Fecha"] = pd.to_datetime(self.movimientos["Fecha"], dayfirst=True)
            
            for index, row in group_proveedores.iterrows():
                fecha = index[1]
                monto_abonado = float(row["Monto abonado"])
                
                reg = self.movimientos.loc[
                    (self.movimientos["Fecha"]==fecha)
                    & ((self.movimientos["Monto"] - monto_abonado).abs() < 0.005)
                ]
               
                if len(reg)>1:
                    self.error.message = "Mas de una coincidencia"
                    self.error.addItem({"ordenante": index[0], "monto": monto_abonado, "fecha":fecha})
                elif(len(reg)==1):
                    self.movimientos.loc[
                        (self.movimientos["Fecha"]==fecha)
                        & ((self.movimientos["Monto"] - monto_abonado).abs() < 0.005),
                        "Referencia"
                    ] = index[0]
                    print("las posiciones ", index[0])
                else:
                    self.error.message = "Registros no ubicados"
                    self.error.addItem({"ordenante": index[0], "monto": monto_abonado, "fecha":fecha})
                    print("ordenante",index[0], "monto", monto_abonado, "fecha",fecha)   
            
        except Exception as ex:
            self.error.message = str(ex)
            
    def process_providers( self,providersFile):
        try:
            df_proveedores = pd.read_excel(providersFile,   header=2 )
            self._process_providers_df(df_proveedores)
            
        except Exception as ex:
            self.error.message = str(ex)
            