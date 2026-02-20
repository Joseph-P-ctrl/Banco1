import os
import pandas as pd
import re
import numpy as np
import json
from datetime import datetime
from storage_paths import bd_path

class MyCustomException(Exception):
        pass



class Error:
    def __init__(self):
        self.message = ""
        self.items = []
    def addItem(self, item):
        self.items.append(item)    
        

class AccountService:
    def __init__(self):
        self.error = Error()
     
    def read_recaudos(self,config):
        recaudos = bd_path(config["RECAUDOS"])
        return pd.read_excel(recaudos)
    def read_prepagos(self,config):
        prepagos = bd_path(config["PREPAGOS"])
        return pd.read_excel(prepagos, header=None)
    def read_trabajadores(self,config):
        trabajadores = bd_path(config["TRABAJADORES"])
        return pd.read_excel(trabajadores, header=None)
    
    def _process_movements_df(self, df_movimientos):
        self.movimientos= df_movimientos
        if (len(self.movimientos.columns)<11):
            raise MyCustomException("Archivo movimientos: Columnas no encontradas, elimine cabeceras innecesarias de movimientos")
        if "Fecha" not in self.movimientos.columns:
            raise MyCustomException("Columnas no encontradas, elimine cabeceras innecesarias")
        movimientos_efectivo = self.movimientos.loc[self.movimientos["Descripción operación"].str.contains('EFECTIVO', na=False)]

        
        
        

        with open(bd_path('config.json'), 'r') as file:
            config = json.load(file)
            
            # Leer el archivo Excel
            df_recaudos = self.read_recaudos(config)
            if (len(df_recaudos.columns)>6):
                raise MyCustomException('BD Recaudos debe tener 6 columnas, revisar')
            cols_recaudos = ['codigo', 'nombre', 'informacion','fecha_gen','area','fecha_dep']
            df_recaudos.columns = cols_recaudos
            self.recaudos = df_recaudos
            df_prepagos = self.read_prepagos(config)
            cols_prepagos = ['codigo','nombre']
            df_prepagos.columns = cols_prepagos
            df_trabajores = self.read_trabajadores(config)
            cols_trabajadores = ['codigo','nombre']
            df_trabajores.columns = cols_trabajadores
            self.movimientos['Referencia'] = ''
            self.movimientos['Procedencia'] = ''
            self.movimientos['info retorno'] = ''
        for index, row in movimientos_efectivo.iterrows():
            cod_recaudo = re.findall(r'\d+', row["Descripción operación"])
            cod_recaudo = [num.lstrip('0') for num in cod_recaudo if num.lstrip('0')]
            
            #search in recaudos
            reg = df_recaudos.loc[df_recaudos['codigo'].astype(str) == cod_recaudo[0]]
                
            if len(reg)>0:
                self.movimientos.at[index, "Referencia"] = str(reg['nombre'].iloc[0]) #lee la segunda columan el primer registro
                self.movimientos['Referencia'] = self.movimientos['Referencia'].astype(str)
                recaudos = "COD.RECAUDO-" + str(reg['area'].iloc[0])
                self.movimientos.at[index, "Procedencia"] = recaudos
                self.recaudos.loc[self.recaudos['codigo'].astype(str)==cod_recaudo[0],"fecha_dep"]=row["Fecha"]  

                self.movimientos.at[index, "info retorno"] = str(reg['informacion'].iloc[0])

                continue
             #search in prepagos
            reg = df_prepagos.loc[df_prepagos['codigo'].astype(str)==cod_recaudo[0]]   
            if len(reg)>0:
                self.movimientos.at[index, "Referencia"] = reg['nombre'].iloc[0] #lee la segunda columan el primer registro
                recaudos = "PREPAGO-" + str(reg['codigo'].iloc[0])
                self.movimientos.at[index, "Procedencia"] = recaudos
                
                continue
            #search in trabajadores
            reg = df_trabajores.loc[df_trabajores['codigo'].astype(str)==cod_recaudo[0]]   
            if len(reg)>0:
                self.movimientos.at[index, "Referencia"] = reg['nombre'].iloc[0] #lee la segunda columan el primer registro
                recaudos = "TRABAJADOR-" + str(reg['codigo'].iloc[0])
                self.movimientos.at[index, "Procedencia"] = recaudos
                continue

            
            
           
   
    def process_movements(self, movimientos):
        df_movimientos= pd.read_excel(movimientos,  header=4)
        self._process_movements_df(df_movimientos)
   


   

    