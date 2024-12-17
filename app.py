import os
import boto3
import subprocess

def download_sql_from_s3_and_import():
    # 환경 변수에서 S3 및 RDS 설정 읽기
    bucket_name = os.getenv("S3_BUCKET_NAME")
    s3_directory = os.getenv("S3_DIRECTORY", "sql_backups/")
    local_directory = os.path.join(os.getcwd(), "downloaded_sql")
    os.makedirs(local_directory, exist_ok=True)

    rds_host = os.getenv("RDS_HOST")
    rds_username = os.getenv("RDS_USERNAME")
    rds_password = os.getenv("RDS_PASSWORD")
    rds_database = os.getenv("RDS_DATABASE")

    # S3에서 SQL 파일 다운로드
    session = boto3.Session(profile_name="default")
    s3_client = session.client("s3")
    objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_directory)

    for obj in objects.get('Contents', []):
        s3_file = obj['Key']
        if s3_file.endswith('.sql'):
            local_path = os.path.join(local_directory, os.path.basename(s3_file))
            print(f"Downloading {s3_file} to {local_path}...")
            s3_client.download_file(bucket_name, s3_file, local_path)

            # RDS로 데이터 삽입
            print(f"Importing {local_path} into RDS...")
            try:
                cmd = f"mysql -h {rds_host} -u {rds_username} -p{rds_password} {rds_database} < {local_path}"
                subprocess.run(cmd, shell=True, check=True)
                print(f"Imported {local_path} into RDS successfully.")
            except subprocess.CalledProcessError as e:
                print(f"Error importing {local_path}: {e}")
