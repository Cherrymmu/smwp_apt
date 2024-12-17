import os
import subprocess
import boto3

def validate_env_variables():
    """환경 변수 유효성 검사"""
    required_env_vars = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "S3_BUCKET_NAME", "EXPORT_DIR"]
    for var in required_env_vars:
        if not os.getenv(var):
            raise EnvironmentError(f"환경 변수 '{var}'가 설정되지 않았습니다.")

def run_localsql_script():
    """Shell 명령어로 localsql.py 실행"""
    localsql_path = r"C:\Users\User\Desktop\AWS\localsql.py"
    try:
        print("Executing localsql.py...")
        subprocess.run(["python", localsql_path], check=True)
        print("localsql.py executed successfully!")
    except subprocess.CalledProcessError as e:
        print(f"Error while executing localsql.py: {e}")
        raise

def upload_sql_to_s3():
    """로컬의 .sql 파일을 S3로 업로드"""
    export_dir = os.getenv("EXPORT_DIR")
    s3_bucket_name = os.getenv("S3_BUCKET_NAME")
    s3_directory = os.getenv("S3_DIRECTORY", "sql_backups/")

    # AWS S3 클라이언트 설정
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "us-east-1")
    )

    for file_name in os.listdir(export_dir):
        if file_name.endswith(".sql"):
            local_file_path = os.path.join(export_dir, file_name)
            s3_path = os.path.join(s3_directory, file_name).replace("\\", "/")
            print(f"Uploading '{local_file_path}' to s3://{s3_bucket_name}/{s3_path}...")
            s3_client.upload_file(local_file_path, s3_bucket_name, s3_path)

    print("SQL 파일 S3 업로드 완료!")

if __name__ == "__main__":
    validate_env_variables()  # 환경 변수 확인
    run_localsql_script()     # Shell 명령어로 localsql
