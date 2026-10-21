
from minio import Minio
from minio.error import S3Error

# Connexion à MinIO
client = Minio(
    "localhost:9000",  # Adresse de MinIO
    access_key="minioadmin",
    secret_key="minioadmin123",
    secure=False        # HTTP, pas HTTPS
)
bucket_name_list = ["bronze","silver","gold"]
for bucket_name in bucket_name_list:
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' créé avec succès !")
    else:
        print(f"Le bucket '{bucket_name}' existe déjà.")

bucket_name = "rayan"

try:
    client.remove_bucket(bucket_name)
    print(f"Bucket '{bucket_name}' supprimé avec succès.")

except S3Error as e:
    print(f"Erreur : {e}")