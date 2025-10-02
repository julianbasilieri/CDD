# Limpieza y Análisis de Datasets de Kaggle

## Requisitos

- Python 3.8+
- Instalar dependencias:
  ```
  pip install pandas kaggle fpdf matplotlib
  ```

- Configurar la API de Kaggle:
  1. Descargar `kaggle.json` desde tu perfil de Kaggle.
  2. Colocar el archivo en `C:\Users\<TU_USUARIO>\.kaggle\kaggle.json`.

## Uso

1. **Descargar los datasets**
   - El script descarga automáticamente los archivos desde Kaggle si no existen en la carpeta `kaggle_dataset`.

2. **Formatear archivos**
   - Si los archivos tienen datos en una sola columna, usa `format_datasets.py` para formatearlos en una carpeta nueva.

3. **Unir archivos**
   - Ejecuta el merge con:
     ```
     merge_csvs.py
     ```
   - El archivo combinado se guarda como `merged_dataset.csv` en la raíz del proyecto.

4. **Analizar datasets**
   - Ejecuta el análisis con:
     ```
     analyze_dataset.py
     ```
   - Puedes analizar un archivo individual o una carpeta completa.
   - Los reportes PDF se guardan en la carpeta `analysis_reports` en la raíz del proyecto.

5. **Ejecución principal**
   - Usa `main.py` para orquestar todo el flujo (descarga, merge, análisis).

## Ejemplo de ejecución

```
python main.py
```

## Notas

- El análisis es eficiente para grandes volúmenes de datos (procesa por chunks).
- Los reportes PDF incluyen gráficos y resumen de los datos.
- Personaliza las variables en `main.py` para ajustar el flujo según tus necesidades.

---
