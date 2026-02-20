import os
import pandas as pd
from datetime import datetime
from flask_session import Session
from flask_caching import Cache
import json
from storage_paths import BD_DIR, bd_path, ensure_data_dirs

class MyCustomException(Exception):
        pass

class BaseDatosService:
    def removeFiles(self, fileName):
        ensure_data_dirs()
        directory_path = BD_DIR
        
        # List all files in the directory that contain the substring
        matching_files = [f for f in os.listdir(directory_path) if fileName in f and os.path.isfile(os.path.join(directory_path, f))]
        # Print the matching files
        for file_name in matching_files:
            path_file = os.path.join(directory_path, file_name)
            if os.path.exists(path_file):
                os.remove(path_file)
    def GuardarAchivos(self, files):
        RECAUDO = "RECAUDO"
        PREPAGO = "PREPAGO"
        TRABAJADOR = "TRABAJADOR"
        CLIENTE = "CLIENTE"
        default_config = {
            "RECAUDOS": "CODIGO RECAUDO.xlsx",
            "PREPAGOS": "PREPAGOS.xlsx",
            "TRABAJADORES": "TRABAJADORES.xlsx",
            "CLIENTES": "CLIENTES.xlsx"
        }
        try:
            ensure_data_dirs()
            config_path = bd_path('config.json')
            if os.path.exists(config_path):
                with open(config_path, 'r', encoding='utf-8') as existing:
                    config = json.load(existing)
            else:
                config = default_config.copy()

            for file in files:
                if file and file.filename:
                    filename = file.filename.upper()
                    if RECAUDO in filename:
                         config["RECAUDOS"] = filename
                         self.removeFiles(RECAUDO)
                    elif PREPAGO in filename:  
                        config["PREPAGOS"] = filename   
                        self.removeFiles(PREPAGO)
                    elif TRABAJADOR in filename : 
                        config["TRABAJADORES"] = filename   
                        self.removeFiles(TRABAJADOR)
                    elif CLIENTE in filename:
                        config["CLIENTES"] = filename
                        self.removeFiles(CLIENTE)
                    else:
                        continue
                    file_path = bd_path(filename)

                    file.save(file_path)
            
            # Save to a JSON file
            with open(config_path, 'w', encoding='utf-8') as myfile:
                json.dump(config, myfile, indent=4)  # The `indent` parameter makes the output more readable
              
        except Exception as ex:
            raise ex


