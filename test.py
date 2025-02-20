import zipfile
from io import BytesIO

def zip_and_upload_to_s3(file_list, s3_bucket, s3_prefix):
    """
    Função que recebe uma lista de arquivos, compacta cada um e envia para um bucket do S3 usando apenas dbutils.

    :param file_list: Lista de arquivos obtidos com dbutils.fs.ls
    :param s3_bucket: Nome do bucket S3 onde os arquivos serão salvos
    :param s3_prefix: Prefixo (caminho) dentro do bucket S3 onde os arquivos serão salvos
    """
    for file_info in file_list:
        file_path = file_info.path  # Caminho completo do arquivo
        file_name = file_info.name  # Nome do arquivo
        zip_file_name = f"{file_name}.zip"

        # Cria um buffer de memória para o arquivo ZIP
        zip_buffer = BytesIO()

        # Cria o arquivo ZIP
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
            # Lê o conteúdo do arquivo usando dbutils
            file_content = dbutils.fs.head(file_path)
            # Adiciona o arquivo ao ZIP
            zipf.writestr(file_name, file_content)

        # Move o cursor do buffer para o início
        zip_buffer.seek(0)

        # Converte o conteúdo binário para base64 (string)
        import base64
        zip_content_base64 = base64.b64encode(zip_buffer.getvalue()).decode('utf-8')

        # Define o caminho completo no S3
        s3_key = f"{s3_prefix}/{zip_file_name}"

        # Salva o arquivo ZIP no S3 usando dbutils
        dbutils.fs.put(f"s3a://{s3_bucket}/{s3_key}", zip_content_base64, overwrite=True)
        print(f"Arquivo {file_name} compactado e enviado para s3://{s3_bucket}/{s3_key}")

# Exemplo de uso
# Suponha que você tenha uma lista de arquivos obtidos com dbutils.fs.ls
file_list = dbutils.fs.ls("/mnt/your_directory")

# Chama a função para compactar e enviar os arquivos para o S3
zip_and_upload_to_s3(file_list, "your-s3-bucket", "your/s3/prefix")
