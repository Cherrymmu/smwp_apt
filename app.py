import os
import boto3
import mysql.connector

def download_and_import_to_rds():
    # S3 설정
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_REGION", "us-east-1")
    s3_bucket = os.getenv("S3_BUCKET_NAME")
    s3_directory = os.getenv("S3_DIRECTORY", "sql_backups/")

    # RDS 설정
    rds_host = os.getenv("RDS_HOST")
    rds_user = os.getenv("RDS_USERNAME")
    rds_password = os.getenv("RDS_PASSWORD")
    rds_database = os.getenv("RDS_DATABASE")

    # S3 클라이언트 생성
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region
    )

    # 다운로드 경로 설정
    download_dir = os.path.join(os.getcwd(), "downloaded_sql")
    os.makedirs(download_dir, exist_ok=True)

    try:
        # S3 버킷에서 SQL 파일 리스트 가져오기
        response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_directory)
        for obj in response.get("Contents", []):
            file_key = obj["Key"]
            file_name = os.path.basename(file_key)
            local_file_path = os.path.join(download_dir, file_name)

            print(f"Downloading {file_key} from S3 to {local_file_path}...")
            s3_client.download_file(s3_bucket, file_key, local_file_path)

            # RDS 연결 및 SQL 파일 실행
            print(f"Importing {local_file_path} into RDS database...")
            conn = mysql.connector.connect(
                host=rds_host,
                user=rds_user,
                password=rds_password,
                database=rds_database
            )
            cursor = conn.cursor()

            with open(local_file_path, "r") as sql_file:
                sql_script = sql_file.read()
                for statement in sql_script.split(";\n"):
                    if statement.strip():
                        cursor.execute(statement)
            conn.commit()
            print(f"Successfully imported {file_name} into RDS.")

            cursor.close()
            conn.close()

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    download_and_import_to_rds()
