from document_intelligence_functions import DocumentIntelligence
import os
import pandas as pd

def process_table(table):
    data = {}
    for index, row in table.iterrows():
        # Concatenar todos los elementos no nulos de la fila en una sola cadena
        full_row = ' '.join(str(item) for item in row if pd.notna(item))
        # Dividir la cadena completa en posibles pares clave-valor
        parts = full_row.split(':')
        current_key = None
        
        # Iterar sobre las partes para formar correctamente los pares clave-valor
        for i, part in enumerate(parts):
            if i == 0:
                # El primer elemento siempre es una clave
                current_key = part.strip()
            else:
                # Los siguientes elementos pueden ser valor de la clave anterior y clave del siguiente par
                # Dividir en el último espacio para separar el valor de la nueva clave
                last_space_index = part.rfind(' ')
                if last_space_index != -1:
                    value = part[:last_space_index].strip()
                    new_key = part[last_space_index:].strip()
                    
                    if current_key and value:
                        data[current_key] = value
                    current_key = new_key
                else:
                    # En caso de que no haya espacio, toda la parte es un valor
                    if current_key:
                        data[current_key] = part.strip()
                        
        # Asegurarse de añadir el último par si es necesario
        if current_key and i == len(parts) - 1 and parts[i]:
            data[current_key] = parts[i].strip()
    
    return data

# Asegúrate de que el directorio actual es donde está el script y el PDF
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# Ruta al documento PDF
file_path = "F6CR_Aprobada_Hamaca-100D_27052022.pdf"

# Crear una instancia de la clase DocumentIntelligence
doc_intelligence = DocumentIntelligence()

# Intentar llamar al método analyze_read para procesar el documento
try:
    text_content, tables = doc_intelligence.analyze_read(file_path=file_path)
except Exception as e:
    print(f"Error al procesar el documento: {e}")
    exit()

# Imprimir el contenido del documento
# for page_text in text_content:
#     print("Contenido de la página:", page_text)

# Preparar un DataFrame para guardar todos los datos
all_data = []

# Si hay tablas, procesarlas
if tables:
    for table in tables:
        data = process_table(table)
        if data:
            print(data)  # Opcional: imprimir para depuración
            all_data.append(data)

# Convertir todos los datos en un DataFrame y guardarlos en Excel
if all_data:
    df = pd.DataFrame(all_data)
    try:
        df.to_excel("datos_pozo.xlsx", index=False)
        print("Todos los datos han sido exportados a Excel exitosamente.")
    except Exception as e:
        print(f"Error al guardar en Excel: {e}")
else:
    print("No se encontraron datos para exportar.")


# import pandas as pd

# # Datos de ejemplo basados en tu descripción de la tabla
# data = {
#     "Compañia": ["Frontera Energy Colombia Corp Sucursal Colombia"],
#     "Contrato": ["CPE-6"],
#     "Campo": ["HAMACA"],
#     "Pozo": ["HAMACA-100D"],
#     "Clasificación": ["Desarrollo"],
#     "Estructura": ["Monoclinal"],
#     "Estado final": ["Taponado y Abandonado"],
#     "LOCALIZACIÓN DEFINITIVA, MAGNA, SIRGAS": [0.0],
#     "Torre": [None],
#     "Fondo (si es desviado)": [None],
#     "N (Y)": ["876.193,00"],
#     "N (Y) 2": ["875.276,00"],
#     "E (X)": ["874.318,00"],
#     "E (X) 2": ["873.934,00"],
#     "Perforación iniciada": ["21/04/2022"],
#     "Perforación concluida": ["1/05/2022"],
#     "Pozo terminado": ["1/05/2022"],
#     "Profundidad total iniciada": ["5.126,00 pies"],
#     "Elevación mesa rotaria": ["814,00 pies"],
#     "Profundidad total vertical": ["3.463,00 pies"],
#     "Elevación del terreno": ["814,00 pies"],
#     "Taponado hasta": ["0,00 pies"]
# }

# # Crear DataFrame
# df = pd.DataFrame(data)

# # Guardar el DataFrame a un archivo Excel
# df.to_excel("datos_pozo.xlsx", index=False)

# print("Datos exportados a Excel exitosamente.")

