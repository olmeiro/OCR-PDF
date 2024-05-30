import os
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
import json

class BlobFunctions():
    """
    La clase BlobFunctions está diseñada para facilitar la interacción con Azure Blob Storage, ofreciendo métodos para cargar, 
    descargar y almacenar archivos y datos. Esta clase se encarga de la inicialización automática de la conexión con Azure Blob Storage 
    utilizando credenciales definidas en un archivo de variables de entorno.
    Una vez establecida la conexión, proporciona funcionalidades clave para trabajar con blobs, tales como:
    
    extract_file_from_blob: 
        Permite descargar archivos específicos de un contenedor de Azure Blob Storage.
        Los archivos a descargar se filtran por su ubicación dentro de una carpeta del contenedor y por su extensión o un sufijo en su nombre. 
        Este método devuelve una lista de diccionarios, cada uno con el nombre y el contenido en bytes del archivo descargado.

    save_json_to_blob: 
        Guarda un objeto de datos (usualmente un diccionario) en un archivo JSON dentro de un contenedor específico.
        Este método es útil para almacenar resultados de procesamiento, configuraciones u otro tipo de estructuras de datos JSON en Blob Storage.

    upload_blob:
        Ofrece flexibilidad para subir archivos a Blob Storage, permitiendo tanto la carga de archivos desde una ruta local como la subida directa
        de objetos de archivo en memoria. Esto es especialmente útil para aplicaciones que necesitan cargar archivos recibidos desde una interfaz web 
        o de otras fuentes no basadas en el sistema de archivos local.
    """
    def __init__(self):

        current_dir = os.path.dirname(os.path.abspath(__file__))
        dotenv_path = os.path.join(current_dir, '..', '.env')  # Sube un nivel en la estructura de directorios
        load_dotenv(dotenv_path, override=True)

        self.account_name = os.environ.get('AZURE_BLOB_STORAGE_BLOB_NAME')
        self.account_key = os.environ.get('AZURE_BLOB_STORAGE_KEY')
        self.container_name = os.environ.get('AZURE_BLOB_STORAGE_CONTAINER_NAME')

        connection_string = f"DefaultEndpointsProtocol=https;AccountName={self.account_name};AccountKey={self.account_key};EndpointSuffix=core.windows.net"
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        self.container_client = self.blob_service_client.get_container_client(self.container_name)

    def extract_file_from_blob(self, blob_folder_path, end):
        """
        Descarga archivos desde una carpeta específica en Azure Blob Storage.

        :param blob_folder_path: Ruta de la carpeta dentro del contenedor desde donde descargar los archivos.
        :param end: Extensión de archivo o cadena final para filtrar los archivos a descargar.
        :return: Lista de diccionarios con los nombres de archivos y su contenido en bytes.
        """
        print(f"Iniciando carga de archivos desde {blob_folder_path}")

        blob_list = [blob.name for blob in self.container_client.list_blobs(name_starts_with=f'{blob_folder_path}/')]
        files_list = []

        for blob_name in blob_list:
            if blob_name.lower().endswith(end):
                print(f"Loading {blob_name}")
                blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=blob_name)
                download_stream = blob_client.download_blob()
                pdf_bytes = download_stream.readall()

                files_list.append({
                    'file_name': blob_name.split('/')[-1],  # Asume que el nombre del archivo está después del último '/'
                    'file': pdf_bytes
                })

        return files_list

    def save_json_to_blob(self, blob_folder_path, file_name, content, file_name_json_out='texts_clausulados.json'):

        """
        Guarda contenido en un archivo dentro de Azure Blob Storage.

        :param blob_folder_path: Ruta de la carpeta dentro del contenedor donde se guardará el archivo.
        :param file_name: Nombre del archivo a guardar.
        :param content: Contenido del archivo a guardar.
        :param file_name_json_out: Nombre opcional del archivo de salida, por defecto 'texts_clausulados.json'.
        """
        print(f"Iniciando guardado de archivo en {blob_folder_path}/{file_name}")

        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=f"{blob_folder_path}/{file_name}")

        # Convertir el contenido (diccionario) a una cadena JSON
        content_as_string = json.dumps(content, ensure_ascii=False, indent=4)

        # Subir el contenido al Blob Storage como una cadena
        blob_client.upload_blob(content_as_string, overwrite=True)
        print(f"Archivo {file_name} guardado en {blob_folder_path}/")

    def upload_blob(self, file_name, dir_destiny, file_path=None, file_obj=None):
        """
        Sube un archivo al Azure Blob Storage, desde una ruta local o directamente desde un objeto en memoria.

        :param file_name: Nombre del archivo a subir.
        :param dir_destiny: Directorio de destino dentro del contenedor de Azure Blob Storage.
        :param file_path: Ruta local del archivo a subir (usada si file_obj no se proporciona).
        :param file_obj: Objeto de archivo en memoria para subir (usado si se proporciona, ignorando file_path).
        """
        blob_full_path = os.path.join(dir_destiny, file_name)

        if file_obj is not None:
            # Subir desde un objeto de archivo en memoria
            print(f"Subiendo archivo desde el objeto en memoria a {blob_full_path}")
            blob_client = self.container_client.upload_blob(name=blob_full_path, data=file_obj, overwrite=True)
        elif file_path is not None:
            # Subir desde una ruta local
            print(f"Subiendo archivo desde la ruta local {file_path} a {blob_full_path}")
            with open(file_path, "rb") as data:
                blob_client = self.container_client.upload_blob(name=blob_full_path, data=data, overwrite=True)
        else:
            raise ValueError("Debe proporcionarse file_path o file_obj.")

        print(f"Archivo subido con éxito como {blob_full_path} al contenedor {self.container_name}.")