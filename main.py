from download_kaggle import download_dataset
from merge_csvs import merge_csv_files
from analyze_dataset import analyze_dataset
import winsound

DATASET_DIR = './kaggle_dataset'
DATASET = 'agungpambudi/network-malware-detection-connection-analysis'
ANALYZE_DATASET = './merged_dataset.csv'
# ANALYZE_DATASET = './kaggle_dataset/CTU-IoT-Malware-Capture-1-1conn.log.labeled.csv'
MERGE = False
ALL = True

def main():
    download_dataset(DATASET_DIR, DATASET)
    if MERGE:
        merge_csv_files(DATASET_DIR)
        analyze_dataset(ANALYZE_DATASET)
    if ALL:
        analyze_dataset(DATASET_DIR)        

    # winsound.Beep(500, 1000)  # frecuencia en Hz, duración en ms
    

if __name__ == "__main__":
    main()
