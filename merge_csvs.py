import os
import glob
import pandas as pd
import time

def merge_csv_files(dataset_dir, output_filename='merged_dataset.csv'):
    csv_files = glob.glob(os.path.join(dataset_dir, '*.csv'))

    if not csv_files:
        print("No se encontraron archivos CSV en el directorio.")
        return None
    
    start_time = time.time()  # ⏱️ inicio

    # Guardar el archivo combinado en la raíz del proyecto (misma altura que dataset_dir)
    project_root = os.path.dirname(os.path.abspath(dataset_dir))
    output_path = os.path.join(project_root, output_filename)
    if os.path.exists(output_path):
        print("El archivo combinado ya existe.")
        return output_path

    # Abre el archivo de salida para escribir
    with open(output_path, 'w') as output_file:
        first_file = True  # Para manejar la cabecera en el primer archivo
        for file in csv_files:
            try:
                # Leer el archivo en partes (chunks) para no sobrecargar la memoria
                for chunk in pd.read_csv(file, sep='|', chunksize=100000, low_memory=False):  # Cambiar chunksize si es necesario
                    if first_file:
                        # Escribir la cabecera solo una vez (en el primer archivo)
                        chunk.to_csv(output_file, header=True, index=False, sep='|')
                        first_file = False
                    else:
                        # Escribir solo los datos, sin cabecera
                        chunk.to_csv(output_file, header=False, index=False, sep='|')
                print(f"{os.path.basename(file)} procesado exitosamente.")
            except Exception as e:
                print(f"Error leyendo {file}: {e}")
    end_time = time.time()  # ⏱️ fin
    elapsed = end_time - start_time
    print(f"\n✅ Archivo combinado en {elapsed:.2f} segundos")
    return output_path
