import os
import glob
from kaggle.api.kaggle_api_extended import KaggleApi
import time

def download_dataset(dataset_dir, dataset):

    csv_files = glob.glob(os.path.join(dataset_dir, '*.csv'))
    if not csv_files:
        try:
            start_time = time.time()  # ⏱️ inicio
            if not os.path.exists(dataset_dir):
                os.makedirs(dataset_dir)
            api = KaggleApi()
            api.authenticate()
            api.dataset_download_files(dataset, path=dataset_dir, unzip=True)
            end_time = time.time()  # ⏱️ fin
            elapsed = end_time - start_time
            print(f"\n✅ Descarga completada en {elapsed:.2f} segundos")
        except ImportError:
            print("Por favor instala la librería kaggle: pip install kaggle")
            exit(1)
