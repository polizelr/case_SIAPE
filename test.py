import boto3
import zipfile
import os
from io import BytesIO

def zip_and_upload_to_s3(file_list, s3_bucket, s3_prefix):
    """
    Função que recebe uma lista de arquivos, compacta cada um e envia para um bucket do S3.

    :param file_list: Lista de arquivos obtidos com dbutils.fs.ls
    :param s3_bucket: Nome do bucket S3 onde os arquivos serão salvos
    :param s3_prefix: Prefixo (caminho) dentro do bucket S3 onde os arquivos serão salvos
    """
    # Inicializa o cliente S3
    s3_client = boto3.client('s3')

    for file_info in file_list:
        file_path = file_info.path
        file_name = os.path.basename(file_path)
        zip_file_name = f"{file_name}.zip"

        # Cria um buffer de memória para o arquivo ZIP
        zip_buffer = BytesIO()

        # Cria o arquivo ZIP
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
            # Adiciona o arquivo ao ZIP
            with open(file_path, 'rb') as file:
                zipf.writestr(file_name, file.read())

        # Move o cursor do buffer para o início
        zip_buffer.seek(0)

        # Define o caminho completo no S3
        s3_key = f"{s3_prefix}/{zip_file_name}"

        # Faz o upload do arquivo ZIP para o S3
        s3_client.upload_fileobj(zip_buffer, s3_bucket, s3_key)
        print(f"Arquivo {file_name} compactado e enviado para s3://{s3_bucket}/{s3_key}")

# Exemplo de uso
# Suponha que você tenha uma lista de arquivos obtidos com dbutils.fs.ls
# file_list = dbutils.fs.ls("/mnt/your_directory")

# Chama a função para compactar e enviar os arquivos para o S3
# zip_and_upload_to_s3(file_list, "your-s3-bucket", "your/s3/prefix")
