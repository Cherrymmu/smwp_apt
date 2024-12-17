import os
import boto3
import mysql.connector
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger()

def validate_env_variables():
    """환경 변수 유효성 검사"""
    required_vars = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "S3_BUCKET_NAME",
                    "RDS_HOST", "RDS_USERNAME", "RDS_PASSWORD", "RDS_DATABASE"]
    for var in required_vars:
        if not os.getenv(var):
            raise EnvironmentError(f"환경 변수 '{var}'가 설정되지 않았습니다.")

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
        logger.info("S3 버킷에서 SQL 파일 리스트 가져오는 중...")
        response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_directory)
        
        for obj in response.get("Contents", []):
            file_key = obj["Key"]
            file_name = os.path.basename(file_key)
            local_file_path = os.path.join(download_dir, file_name)

            logger.info(f"Downloading {file_key} from S3 to {local_file_path}...")
            s3_client.download_file(s3_bucket, file_key, local_file_path)

            # RDS 연결 및 SQL 파일 실행
            logger.info(f"Importing {local_file_path} into RDS database...")
            conn = mysql.connector.connect(
                host=rds_host,
                user=rds_user,
                password=rds_password,
                database=rds_database
            )
            cursor = conn.cursor()

            with open(local_file_path, "r") as sql_file:
                sql_script = sql_file.read()
                cursor.execute(sql_script, multi=True)
            conn.commit()
            logger.info(f"Successfully imported {file_name} into RDS.")

            cursor.close()
            conn.close()

    except mysql.connector.Error as db_error:
        logger.error(f"RDS 데이터베이스 오류: {db_error}")
    except boto3.exceptions.Boto3Error as s3_error:
        logger.error(f"S3 접근 오류: {s3_error}")
    except Exception as e:
        logger.error(f"알 수 없는 오류 발생: {e}")

if __name__ == "__main__":
    validate_env_variables()
    download_and_import_to_rds()