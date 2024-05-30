import os
import pdb
import dotenv
import numpy as np
import pandas as pd
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential
from difflib import get_close_matches
import pdb

class DocumentIntelligence():
    """La clase DocumentIntelligence está diseñada para aprovechar las capacidades avanzadas del servicio Azure Form Recognizer,
       permitiendo el análisis profundo de documentos. La clase simplifica el proceso de conexión con Azure Form Recognizer utilizando
       credenciales almacenadas en variables de entorno y ofrece métodos intuitivos para el análisis de documentos. Sus funcionalidades principales incluyen:

       analyze_read: 
            Este método central ofrece una forma poderosa y flexible de analizar documentos PDF. 
            Puede procesar archivos tanto desde una ruta local como directamente desde objetos en memoria (ideal para flujos de trabajo donde
            los archivos no se almacenan en disco), extrayendo el texto y, opcionalmente, las tablas contenidas. Esta función es especialmente
            útil para aplicaciones que requieren procesamiento de documentos, OCR (Reconocimiento Óptico de Caracteres), y extracción de datos estructurados,
            como tablas, de archivos PDF.

        _extract_tables:
            Un método auxiliar dedicado a la extracción y transformación de tablas detectadas en los documentos analizados.
              Convierte las tablas identificadas por Azure Form Recognizer en DataFrames de Pandas, facilitando su manipulación y análisis posterior
                en aplicaciones de análisis de datos y machine learning."""
    
    def __init__(self, dotenv_path="../.env"):
        dotenv.load_dotenv(dotenv_path, override=True)
        self.endpoint = os.environ.get('AZURE_FORM_RECOGNIZER_ENDPOINT')
        self.key = os.environ.get('AZURE_FORM_RECOGNIZER_API_KEY')
        self.document_analysis_client = DocumentAnalysisClient(
            endpoint=self.endpoint, credential=AzureKeyCredential(key=self.key)
        )

    def analyze_read(self, file_obj=None, file_path=None, return_tables=False):
        """
        Analiza el contenido de un documento PDF, que puede ser proporcionado como un objeto de archivo en memoria
        o a través de una ruta de archivo.

        :param file_obj: Objeto de archivo en memoria para analizar (BytesIO, por ejemplo).
        :param file_path: Ruta del archivo PDF a analizar.
        :return: Texto del documento y las tablas encontradas.
        """
        if file_obj is not None:
            pdf_bytes = file_obj
        elif file_path is not None:
            with open(file_path, "rb") as f:
                pdf_bytes = f.read()
        else:
            raise ValueError("Debe proporcionarse 'file_obj' o 'file_path'.")

        full_content_list = []
        poller = self.document_analysis_client.begin_analyze_document("prebuilt-layout", document=pdf_bytes)
        result = poller.result()

        for page in result.pages:
            page_text_lines = [line.content for line in page.lines]
            page_text = ' '.join(page_text_lines)
            full_content_list.append(page_text)

        if result.tables:
            tables = self._extract_tables(result)
            return full_content_list, tables
        else:
            return full_content_list, None

    def _extract_tables(self, result):
        """
        Extrae tablas del resultado del análisis del documento.

        :param result: Resultado del análisis del documento.
        :return: Lista de DataFrames de pandas que representan las tablas encontradas.
        """
        tables = []
        for table in result.tables:
            max_idx = np.max([cell.row_index for cell in table.cells])
            max_jdx = np.max([cell.column_index for cell in table.cells])

            tb = pd.DataFrame(np.zeros((max_idx + 1, max_jdx + 1)), dtype=str)
            for cell in table.cells:
                tb.iloc[cell.row_index, cell.column_index] = str(cell.content)
            tb.columns = tb.iloc[0, :]
            tb.set_index(tb.iloc[0, 0])
            tb = tb.iloc[1:, :]

            tables.append(tb)

        return tables
    
    def identify_and_structure_tables(self, file_obj=None, file_path=None,
                                      list_string_in_columns=[], list_field_names=[],
                                        umbral=0.6,drop_rows=[0,1],min_len_df=4,set_names_columns=True):
        """
        Analiza un documento, extrae y procesa las tablas según criterios específicos. El documento puede ser proporcionado como
        un objeto de archivo en memoria o a través de una ruta de archivo.

        :param file_obj: Objeto de archivo en memoria para analizar (opcional).
        :param file_path: Ruta del archivo PDF a analizar (opcional).
        :param list_string_in_columns: Lista de cadenas para identificar tablas de interés.
        :param list_field_names: Lista de nombres de campos para buscar en las tablas.
        :param umbral: Umbral para la coincidencia de cadenas en la identificación de nombres de columnas.

        :return: DataFrame de pandas con los datos consolidados de todas las tablas de interés procesadas.
        """
        if file_obj:
            document = file_obj
        elif file_path:
            with open(file_path, "rb") as f:
                document = f.read()
        else:
            raise ValueError("Debe proporcionarse 'file_obj' o 'file_path'.")

        poller = self.document_analysis_client.begin_analyze_document("prebuilt-layout", document=document)
        result = poller.result()

        print(f"Tablas encontradas: {len(result.tables)}")


        ind_tables_obj = []
        list_df_obj = []
        columns_obj = []

        for i, table in enumerate(result.tables):

            df_ = self.table_to_dataframe(table)  # Utilizamos la función adaptada directamente aquí
            df_['page'] = str(table.bounding_regions[0].page_number)
            df_['document'] = str(table.bounding_regions[0].page_number)
           

            columns_row_0 = df_.iloc[0,:].to_list()
            columns_row_1 = df_.iloc[1,:].to_list() 
            # print(columns_row_0)
            # print(columns_row_1)

            # Filtramos los items que no son cadenas vacías
            filtered_list_row_0 = [item for item in columns_row_0 if item]
            filtered_list_row_1 = [item for item in columns_row_1 if item]

            filtered_list_row_0_and_1 = filtered_list_row_0+filtered_list_row_1

            # Concatenamos los items con comas
            str_row_0_and_1 = ', '.join(filtered_list_row_0_and_1)

            if len(df_)>=min_len_df:
                if any(elemento in str_row_0_and_1.lower() for elemento in list_string_in_columns):
                    
                    columns_obj.append(str_row_0_and_1)
                    ind_tables_obj.append(i)
                    list_df_obj.append(df_)
                    # print(df_)
                    print(f"Tabla de interés identificada: {i}")
                    # pdb.set_trace()
        list_df_processed = []
        for df_i in list_df_obj:
            df_ = df_i.copy()
            columns_row_0 = df_.iloc[0,:].to_list()
            columns_row_0 = [v for v in columns_row_0 if v is not None]

            columns_row_1 = df_.iloc[1,:].to_list()
            columns_row_1 = [v for v in columns_row_1 if v is not None]

            # print(columns_row_0)
            # rn.shuffle(columns_row_1)

            similar_field_row_0 = {}
            similar_field_row_1 = {}

            for i in range(len(list_field_names)):

                # Para cada elemento de la lista, buscamos la mejor coincidencia
                # La función get_close_matches retorna una lista de las mejores similar_field_row_x "cercanas"
                # Aquí tomamos solo la mejor coincidencia si existe alguna, de lo contrario None
                match_row_0 = get_close_matches(list_field_names[i],columns_row_0 , n=1, cutoff=umbral)
                match_row_1 = get_close_matches(list_field_names[i],columns_row_1 , n=1, cutoff=umbral)

                if match_row_0:
                    similar_field_row_0[list_field_names[i]] = {'similar':match_row_0[0],
                                                                'ind_col': columns_row_0.index(match_row_0[0])
                                                                }
                else:
                    similar_field_row_0[list_field_names[i]] = None

                if match_row_1:
                    similar_field_row_1[list_field_names[i]] = {'similar':match_row_1[0],
                                                                'ind_col': columns_row_1.index(match_row_1[0])
                                                                }
                else:
                    similar_field_row_1[list_field_names[i]] = None

            # Filtramos los elementos que no tienen None como valor
            items_filtered_row_0 = {clave: valor for clave, valor in similar_field_row_0.items() if valor is not None}
            items_filtered_row_1 = {clave: valor for clave, valor in similar_field_row_1.items() if valor is not None}

            # print(items_filtered_row_0)
            # print(items_filtered_row_1)

            if set_names_columns:
                for key in items_filtered_row_0.keys():
                    column_in = items_filtered_row_0[key]['ind_col']
                    df_.rename({column_in:key},axis=1,inplace=True)

                for key in items_filtered_row_1.keys():
                    column_in = items_filtered_row_1[key]['ind_col']
                    df_.rename({column_in:key},axis=1,inplace=True)
                    
                list_df_processed.append(df_)
                print(f"Dataframes procesados: {len(list_df_processed)}")
            # print(df_)
            
            if drop_rows:
                df_.drop(drop_rows,axis=0,inplace=True)
            else:
                print("No se elimino ninguna fila")
            # pdb.set_trace()
            

            

        

        # Concatenar los DataFrames en la lista
        df_concatenado = pd.concat(list_df_processed, ignore_index=True)
        print(f"Tamaño completo del df concatenado: {df_concatenado.shape}")

        return df_concatenado,list_df_processed

    def _identify_dataframe_structure(self,df):
        
        def is_header_row(row):
            """ Función para determinar si una fila puede ser usada como encabezado. """
            return all(isinstance(item, str) and len(item) > 1 for item in row)

        def convert_key_value(df):
            """ Convierte un DataFrame de formato clave-valor a diccionario. """
            return dict(zip(df.iloc[:, 0], df.iloc[:, 1]))

        def convert_with_headers(df):
            """ Convierte un DataFrame con encabezados en la primera fila a lista de diccionarios. """
            df.columns = df.iloc[0]
            df = df.drop(0)
            return df.to_dict(orient='records')

        def auto_convert_dataframe(df):
            """ Determina automáticamente el método de conversión y aplica la conversión. """
            if is_header_row(df.iloc[0]):
                return convert_with_headers(df)
            else:
                return convert_key_value(df)

        return(auto_convert_dataframe(df))
    

    def auto_identify_and_structure_tables(self, file_obj=None, file_path=None,
                                           list_string_in_columns=[]):
        """
        Analiza un documento, extrae y procesa las tablas según criterios específicos. El documento puede ser proporcionado como
        un objeto de archivo en memoria o a través de una ruta de archivo.

        :param file_obj: Objeto de archivo en memoria para analizar (opcional).
        :param file_path: Ruta del archivo PDF a analizar (opcional).
        :param list_string_in_columns: Lista de cadenas para identificar tablas de interés.
        :param list_field_names: Lista de nombres de campos para buscar en las tablas.
        :param umbral: Umbral para la coincidencia de cadenas en la identificación de nombres de columnas.
        :return: DataFrame de pandas con los datos consolidados de todas las tablas de interés procesadas.
        """
        if file_obj:
            document = file_obj
        elif file_path:
            with open(file_path, "rb") as f:
                document = f.read()
        else:
            raise ValueError("Debe proporcionarse 'file_obj' o 'file_path'.")

        poller = self.document_analysis_client.begin_analyze_document("prebuilt-layout", document=document)
        result = poller.result()

        print(f"Tablas encontradas: {len(result.tables)}")

        ind_tables_obj = []
        list_df_obj = []
        columns_obj = []
        list_df_processed=[]
        for i, table in enumerate(result.tables):

            df_ = self.table_to_dataframe(table)  # Utilizamos la función adaptada directamente aquí
                    
            columns_row_0 = df_.iloc[0,:].to_list()
            columns_row_1 = df_.iloc[1,:].to_list() 
            # print(columns_row_0)
            # print(columns_row_1)

            # Filtramos los items que no son cadenas vacías
            filtered_list_row_0 = [item for item in columns_row_0 if item]
            filtered_list_row_1 = [item for item in columns_row_1 if item]

            filtered_list_row_0_and_1 = filtered_list_row_0+filtered_list_row_1

            # Concatenamos los items con comas
            str_row_0_and_1 = ', '.join(filtered_list_row_0_and_1)

            if len(df_)>=2:
                if any(elemento in str_row_0_and_1.lower() for elemento in list_string_in_columns):
                    
                    columns_obj.append(str_row_0_and_1)
                    ind_tables_obj.append(i)
                    
                    if df_.isna().sum().sum()>2:
                        print("Tabla con campos vacios *")
                        print(df_)
                        # print(df_)
                        try:
                            print("replacing...")
                            for col_ in df_.columns:
                                value_default = df_[col_].dropna().loc[df_[col_] != ""].iloc[1]
                                if value_default:
                                    
                                    df_[col_] = df_[col_].replace("",value_default)
                                    df_[col_] = df_[col_].fillna(value_default)
                            print("Tabla completada *")
                            # print(df_)
                        except Exception as e:
                            print(f"No fué posible completar la tabla. Error: {e}")
                        
                    list_df_obj.append(df_)
                    
                    print(f"Tabla de interés identificada: {i}")
                    
                    df_result_json=self._identify_dataframe_structure(df_ )
                    print(f"Tamaño del JsonL: {len(df_result_json)}")
                    list_df_processed.append(df_result_json)
        
        return list_df_processed

    @staticmethod
    def table_to_dataframe(table):
        """
        Convierte una tabla extraída de un documento analizado en un DataFrame de pandas.

        :param table: Objeto de tabla del resultado del análisis de documentos.
        :return: DataFrame de pandas que representa la tabla.
        """
        data = [[] for _ in range(table.row_count)]
        for cell in table.cells:
            data[cell.row_index].append(cell.content)

        df = pd.DataFrame(data)
        return df