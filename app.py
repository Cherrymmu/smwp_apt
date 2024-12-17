import os
import subprocess
import boto3
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_env_variable(var_name):
    value = os.environ.get(var_name)
    if not value:
        raise ValueError(f"Environment variable '{var_name}' is not set.")
    return value

def upload_sql_to_s3(local_directory, bucket_name, s3_directory=""):
    session = boto3.Session(profile_name="default")
    s3_client = session.client("s3")
    for root, _, files in os.walk(local_directory):
        for file in files:
            if file.endswith(".sql"):
                local_path = os.path.join(root, file)
                s3_path = os.path.join(s3_directory, file).replace("\\", "/")
                s3_client.upload_file(local_path, bucket_name, s3_path)
                logging.info(f"Uploaded {file} to s3://{bucket_name}/{s3_path}")

def import_sql_to_rds(sql_directory):
    db_host = get_env_variable("DB_HOST")
    db_user = get_env_variable("DB_USER")
    db_password = get_env_variable("DB_PASSWORD")
    db_name = get_env_variable("DB_NAME")
    for file in os.listdir(sql_directory):
        if file.endswith(".sql"):
            file_path = os.path.join(sql_directory, file)
            cmd = f"mysql -h {db_host} -u {db_user} -p{db_password} {db_name} < {file_path}"
            subprocess.run(cmd, shell=True, check=True)
            logging.info(f"Executed SQL file: {file}")

if __name__ == "__main__":
    rawDB_dir = "./rawDB"
    backup_dir = "./backup"
    os.makedirs(backup_dir, exist_ok=True)

    # SQL 파일 RDS로 업로드 및 백업 디렉토리 이동
    import_sql_to_rds(rawDB_dir)
    for file in os.listdir(rawDB_dir):
        if file.endswith(".sql"):
            os.rename(os.path.join(rawDB_dir, file), os.path.join(backup_dir, file))
    logging.info("SQL files imported and moved to backup.")
