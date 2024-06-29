import os
import json

def reformat_json_files(directory):
    # Itera su tutti i file nella directory
    for filename in os.listdir(directory):
        if filename.endswith(".json"):
            filepath = os.path.join(directory, filename)
            
            # Leggi il contenuto del file JSON
            with open(filepath, 'r') as file:
                data = json.load(file)
            
            # Riscrivi il file JSON con indentazione di 4 spazi
            with open(filepath, 'w') as file:
                json.dump(data, file, indent=4)
            
            print(f"File {filename} riformattato.")

directory_path = './results'

reformat_json_files(directory_path)

