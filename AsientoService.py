import os
import pandas as pd
from datetime import datetime
import re

class MyCustomException(Exception):
        pass


class Error:
    def __init__(self):
        self.message = ""
        self.items = []
    def addItem(self, item):
        self.items.append(item)    
class AsientoService:
        
    def __init__(self):
        self.error= Error()
        
    def _conciliar_df( self, df_movimientos, df_asientos):
        self.df_movimientos = df_movimientos
        self.df_asientos = df_asientos
        try:
            if (len(self.df_movimientos.columns)<10):
               raise MyCustomException("Archivo movimientos: Columnas no encontradas, elimine cabeceras innecesarias provecios ")
            if "Fecha" not in self.df_movimientos.columns:
                raise MyCustomException( "Archivo movimientos: Columnas no encontradas")
            if (len(self.df_asientos.columns)<17):
                raise MyCustomException("Archivo asiento: Columnas no encontradas, elimine  cabeceras innecesarias ")
            if "Nº documento" not in self.df_asientos.columns:
                raise MyCustomException("Archivo Asientos: Columna Nro Documento no encontrada")
            
            #df1m = self.df_movimientos[["Monto","Saldo" ,"Sucursal - agencia" ,"Operación - Número" ,"Operación - Hora" ,"Usuario" ,"UTC"  ,"Referencia" ,"Procedencia"]].copy()
            #self.df_asientos = df_asientos.dropna(subset=["Asignación"])
            self.df_asientos['Asignacion_new'] = self.df_asientos['Asignación'].astype(str)
            print('df_asientos', self.df_asientos)
           
            def extract_decimal_part(value):
                decimal_position = value.find(".")
                if decimal_position != -1:
                    return value[decimal_position + 1:]
                else:
                    return ""
            def extract_integer_part(value):
                decimal_position = value.find(".")
                if decimal_position != -1:
                    return value[:decimal_position]
                else:
                    return value
            self.df_asientos['Asignacion_new']  = self.df_asientos['Asignacion_new'].apply(extract_integer_part)
            self.df_asientos['Asignacion_new']= self.df_asientos['Asignacion_new'].str.zfill(7).str[-6:]
            self.df_movimientos['Operacion_new'] = self.df_movimientos['Operación - Número'].astype(str).str.zfill(7).str[-6:]
            self.df_movimientos["Fecha"] = pd.to_datetime(self.df_movimientos["Fecha"], dayfirst=True)
            # Ensure Asientos column exists so downstream code/tests can access it
            if 'Asientos' not in self.df_movimientos.columns:
                self.df_movimientos['Asientos'] = ''
            # Persist detected email(s) from asiento rows into output file for downstream extraction
            if 'Correos' not in self.df_movimientos.columns:
                self.df_movimientos['Correos'] = ''

            email_regex = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")

            def extract_emails_from_row(row):
                emails = set()
                for value in row.values:
                    if pd.notna(value):
                        for m in email_regex.findall(str(value)):
                            emails.add(m)
                return sorted(emails)

            # normalize Fecha de documento to datetime for proper comparison
            self.df_asientos["Fecha de documento"] = pd.to_datetime(self.df_asientos["Fecha de documento"], dayfirst=True, errors='coerce')
            print('tipos', self.df_asientos.dtypes)
            for index, row in self.df_movimientos.iterrows():
                # try exact match by asignacion and date
                reg = self.df_asientos.loc[(self.df_asientos['Asignacion_new'] == row["Operacion_new"]) & (self.df_asientos["Fecha de documento"]==row["Fecha"])]
                if len(reg) == 1:
                    matched_row = reg.iloc[0]
                    self.df_movimientos.loc[index, "Asientos"] = matched_row['Nº documento']
                    matched_emails = extract_emails_from_row(matched_row)
                    if matched_emails:
                        self.df_movimientos.loc[index, "Correos"] = ", ".join(matched_emails)
                else:
                    # fallback: match by asignacion only (some records may not match by date)
                    reg2 = self.df_asientos.loc[self.df_asientos['Asignacion_new'] == row["Operacion_new"]]
                    if len(reg2) >= 1:
                        matched_row = reg2.iloc[0]
                        self.df_movimientos.loc[index, "Asientos"] = matched_row['Nº documento']
                        matched_emails = extract_emails_from_row(matched_row)
                        if matched_emails:
                            self.df_movimientos.loc[index, "Correos"] = ", ".join(matched_emails)
            self.df_movimientos = self.df_movimientos.drop('Operacion_new', axis=1)
               
        except Exception as ex:
            self.error.message = str(ex)
            raise ex
            
    
    def conciliar( self, movimientosfile, asientosfile):
        
        try:
            # Definir una función para convertir el número de serie de fecha en formato legible
            def excel_date_to_datetime(x):
                return pd.to_datetime('1900-01-01') + pd.to_timedelta(x-1, 'D')
            
            df_movimientos = pd.read_excel(movimientosfile,   header=0 )
            df_asientos = pd.read_excel(asientosfile,   header=0 )
            self._conciliar_df(df_movimientos, df_asientos)

               
        except Exception as ex:
            self.error.message = str(ex)
            raise ex
            
    