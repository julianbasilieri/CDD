import os
import glob
import pandas as pd
from collections import Counter
import time

def analyze_dataset(dataset_path):
    # Si es un directorio, analiza todos los .csv dentro
    if os.path.isdir(dataset_path):
        csv_files = glob.glob(os.path.join(dataset_path, '*.csv'))
        if not csv_files:
            print("No se encontraron archivos CSV en la carpeta.")
            return
        for file in csv_files:
            print(f"\n--- Análisis de {os.path.basename(file)} ---")
            _analyze_single_csv(file)
    else:
        # Si es un archivo, analiza solo ese archivo
        _analyze_single_csv(dataset_path)
        
# def analyze_dataset(file_path):
#     try:
#         df = pd.read_csv(file_path, sep='|')
#         print(df.columns.tolist())

#         columnas_a_verificar = [
#             "ts",
#             "duration",
#             "orig_bytes", 
#             "resp_bytes",
#         ]

#         for col in columnas_a_verificar:
#             if col not in df.columns:
#                 print(f"⚠️  Columna '{col}' no encontrada en el archivo. Saltando...")
#                 continue

#             try:
#                 df[f'{col}_numeric'] = pd.to_numeric(df[col], errors='coerce')

#                 errores = df[df[f'{col}_numeric'].isna() & df[col].notna()]
#                 if not errores.empty:
#                     print(f"\n❌ Valores no numéricos detectados en columna '{col}':")
#                     # for idx, valor in errores[col].items():
#                     #     fila_real = idx + 2
#                     #     print(f"  → Fila {fila_real}: valor inválido → '{valor}'")

#             except Exception as col_err:
#                 print(f"❌ Error procesando columna '{col}' en archivo '{file_path}': {col_err}")

#     except Exception as e:
#         print(f"💥 Error general al analizar {file_path}: {e}")

def _analyze_single_csv(csv_path):
    try:
        start_time = time.time()  # ⏱️ inicio

        # Define tipos óptimos
        dtype_dict = {
            'ts': 'float32',
            'uid': 'object',
            'id.orig_h': 'object',
            'id.orig_p': 'uint32',
            'id.resp_h': 'object',
            'id.resp_p': 'uint32',
            'proto': 'category',
            'service': 'category',
            'duration': 'float32',
            'orig_bytes': 'float32',
            'resp_bytes': 'float32',
            'conn_state': 'category',
            'local_orig': 'bool',
            'local_resp': 'bool',
            'missed_bytes': 'uint32',
            'history': 'object',
            'orig_pkts': 'uint32',
            'orig_ip_bytes': 'uint32',
            'resp_pkts': 'uint32',
            'resp_ip_bytes': 'uint32',
            'tunnel_parents': 'object',
            'label': 'category',
            'detailed-label': 'category'
        }
        
        cols_utiles_unique = [
            "id.orig_h", "id.orig_p",
            "id.resp_h", "id.resp_p",
            "proto", "service",
            "duration",
            "orig_bytes", "resp_bytes",
            "conn_state", "missed_bytes", "history",
            "orig_pkts", "orig_ip_bytes",
            "resp_pkts", "resp_ip_bytes",
            "label", "detailed-label"
        ]
        
        cols_utiles_numeros = [
            "missed_bytes",
            "orig_pkts",
            "orig_ip_bytes",
            "resp_pkts",
            "resp_ip_bytes"
        ]

        # Conversión personalizada para booleanos y numéricos
        def custom_converter(col, series):
            if col in ['local_orig', 'local_resp']:
                return series.map({'T': True, 'F': False, 'True': True, 'False': False}).astype('bool')
            if col in ['duration', 'orig_bytes', 'resp_bytes']:
                return pd.to_numeric(series, errors='coerce').astype('float32')
            return series

        # Lee una muestra para obtener columnas
        df_head = pd.read_csv(csv_path, sep='|', nrows=100)
        usecols = df_head.columns

        # Lee el archivo completo con tipos optimizadosç
        numerical_exceptions = ['duration', 'orig_bytes', 'resp_bytes']
        safe_dtype_dict = {k: v for k, v in dtype_dict.items() if v != 'bool' and k not in numerical_exceptions}
        df = pd.read_csv(csv_path, sep='|', usecols=usecols, dtype=safe_dtype_dict, low_memory=False)
        for col in ['local_orig', 'local_resp', 'duration', 'orig_bytes', 'resp_bytes']:
            if col in df.columns:
                df[col] = custom_converter(col, df[col])

        # Selecciona columnas clave para análisis
        cols = list(df_head.columns)
        label_col = next((c for c in cols if c.lower() == 'label'), None)
        detailed_label_col = next((c for c in cols if c.lower() in ['detailed-label', 'detailed_label']), None)
        proto_col = next((c for c in cols if c.lower() == 'proto'), None)
        conn_state_col = next((c for c in cols if c.lower() == 'conn_state'), None)

        # Inicializa acumuladores para conteos usando Counter
        label_counts = Counter()
        detailed_label_counts = Counter()
        proto_counts = Counter()
        conn_state_counts = Counter()
        null_counts = pd.Series(0, index=cols)
        uniques = {col: set() for col in cols}
        stats_cols = df_head.select_dtypes(include='number').columns
        stats_sum = pd.Series(0.0, index=stats_cols)
        stats_count = pd.Series(0, index=stats_cols)
        stats_min = pd.Series(float('inf'), index=stats_cols)
        stats_max = pd.Series(float('-inf'), index=stats_cols)

        # Procesa el archivo por chunks
        for chunk in pd.read_csv(csv_path, sep='|', chunksize=100000, low_memory=False):
            # Conteo de nulos considerando "-" como nulo
            for col in cols:
                null_counts[col] += chunk[col].isnull().sum() + (chunk[col] == "-").sum()
            for col in uniques:
                uniques[col].update(chunk[col].dropna().unique())
            for col in stats_cols:
                # Convertir la columna a numérico de forma segura (ignora '-' y NaN)
                numeric_col = pd.to_numeric(chunk[col], errors='coerce')
                stats_sum[col] += numeric_col.sum()
                stats_count[col] += numeric_col.count()
                stats_min[col] = min(stats_min[col], numeric_col.min(skipna=True))
                stats_max[col] = max(stats_max[col], numeric_col.max(skipna=True))
            if label_col:
                label_counts.update(chunk[label_col].dropna())
            if detailed_label_col:
                detailed_label_counts.update(chunk[detailed_label_col].dropna())
            if proto_col:
                proto_counts.update(chunk[proto_col].dropna())
            if conn_state_col:
                conn_state_counts.update(chunk[conn_state_col].dropna())

        # Acumula la información para el PDF
        analysis_text = []
        analysis_text.append(f"Archivo analizado: {os.path.basename(csv_path)}\n")
        analysis_text.append("Columnas y tipos de datos:\n" + df_head.dtypes.to_string() + "\n")
        analysis_text.append("Valores nulos por columna:\n" + null_counts.to_string() + "\n")
        analysis_text.append("Valores únicos por columnas relevantes:\n" + "\n".join(f"{col}: {len(uniques[col])} únicos" for col in cols_utiles_unique if col in uniques) + "\n")
        analysis_text.append("Estadísticas descriptivas (numéricas):\n")
        for col in cols_utiles_numeros:
            if stats_count[col] > 0:
                analysis_text.append(f"{col}: mean={stats_sum[col]/stats_count[col]:.2f}, min={stats_min[col]}, max={stats_max[col]}")
            else:
                analysis_text.append(f"{col}: sin datos")
        if label_col:
            analysis_text.append(f"\nDistribución de la columna '{label_col}':\n{pd.Series(label_counts).sort_values(ascending=False).to_string()}")
        else:
            analysis_text.append("\nColumna 'label' no encontrada.")
        if detailed_label_col:
            analysis_text.append(f"\nDistribución de la columna '{detailed_label_col}':\n{pd.Series(detailed_label_counts).sort_values(ascending=False).to_string()}")
        else:
            analysis_text.append("\nColumna 'detailed-label' no encontrada.")
        if proto_col:
            analysis_text.append(f"\nProtocolos más frecuentes ({proto_col}):\n{pd.Series(proto_counts).sort_values(ascending=False).to_string()}")
        if conn_state_col:
            analysis_text.append(f"\nEstados de conexión más frecuentes ({conn_state_col}):\n{pd.Series(conn_state_counts).sort_values(ascending=False).to_string()}")

        # Generar gráficos y exportar PDF
        export_analysis_to_pdf(csv_path, "\n\n".join(analysis_text), label_counts, detailed_label_counts, proto_counts, conn_state_counts)
        
        end_time = time.time()  # ⏱️ fin
        elapsed = end_time - start_time
        print(f"\n✅ Análisis completado en {elapsed:.2f} segundos")

    except Exception as e:
        print(f"Error analizando {csv_path}: {e}")

def export_analysis_to_pdf(csv_path, analysis_text, label_counts, detailed_label_counts, proto_counts, conn_state_counts):
    try:
        from fpdf import FPDF
        import matplotlib.pyplot as plt

        # Carpeta analysis_reports al mismo nivel que kaggle_dataset
        base_dir = os.path.dirname(os.path.dirname(csv_path)) if os.path.basename(os.path.dirname(csv_path)) == "kaggle_dataset" else os.path.dirname(csv_path)
        reports_dir = os.path.join(base_dir, "analysis_reports")
        if not os.path.exists(reports_dir):
            os.makedirs(reports_dir)
        pdf_name = os.path.splitext(os.path.basename(csv_path))[0] + "_analysis.pdf"
        pdf_path = os.path.join(reports_dir, pdf_name)

        # Crear gráficos y guardarlos como imágenes temporales
        img_files = []
        def plot_and_save(counter, title, fname):
            if not counter or sum(counter.values()) == 0:
                return None
            plt.figure(figsize=(6, 4))
            items = pd.Series(counter).sort_values(ascending=False)
            items.plot(kind='bar', color='#4F81BD')
            plt.title(title)
            plt.ylabel('Cantidad')
            plt.tight_layout()
            img_path = os.path.join(reports_dir, fname)
            plt.savefig(img_path)
            plt.close()
            img_files.append(img_path)
            return img_path

        plot_and_save(label_counts, "Distribución de 'label'", "label_dist.png")
        plot_and_save(detailed_label_counts, "Distribución de 'detailed-label'", "detailed_label_dist.png")
        plot_and_save(proto_counts, "Protocolos más frecuentes", "proto_dist.png")
        plot_and_save(conn_state_counts, "Estados de conexión más frecuentes", "conn_state_dist.png")

        # Crear PDF con estilos
        pdf = FPDF()
        pdf.add_page()
        pdf.set_auto_page_break(auto=True, margin=15)
        pdf.set_font("Arial", size=11)
        pdf.set_text_color(40, 40, 40)
        pdf.set_fill_color(230, 230, 250)

        # Título
        pdf.set_font("Arial", 'B', 16)
        pdf.cell(0, 10, "Reporte de Análisis de Dataset", ln=True, align='C')
        pdf.ln(5)

        # Texto de análisis
        pdf.set_font("Arial", size=10)
        for line in analysis_text.split('\n'):
            pdf.multi_cell(0, 5, line)
        pdf.ln(5)

        # Insertar gráficos
        for img in img_files:
            pdf.add_page()
            pdf.set_font("Arial", 'B', 12)
            pdf.cell(0, 10, os.path.splitext(os.path.basename(img))[0].replace('_', ' ').capitalize(), ln=True)
            pdf.image(img, x=20, w=170)
            pdf.ln(5)

        pdf.output(pdf_path)

        # Eliminar imágenes temporales
        for img in img_files:
            if os.path.exists(img):
                os.remove(img)

    except ImportError:
        print("No se pudo exportar a PDF porque falta la librería fpdf o matplotlib. Instala con: pip install fpdf matplotlib")
