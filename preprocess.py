import os
import boto3
import mysql.connector

def test_mysql_connection():
    """MySQL 연결 확인"""
    LOCAL_DB_HOST = os.environ.get("LOCAL_DB_HOST")
    LOCAL_DB_NAME = os.environ.get("LOCAL_DB_NAME")
    LOCAL_DB_USER = os.environ.get("LOCAL_DB_USER")
    LOCAL_DB_PASSWORD = os.environ.get("LOCAL_DB_PASSWORD")

    try:
        conn = mysql.connector.connect(
            host=LOCAL_DB_HOST,
            user=LOCAL_DB_USER,
            password=LOCAL_DB_PASSWORD,
            database=LOCAL_DB_NAME
        )
        print("MySQL 연결 성공!")
        cursor = conn.cursor()
        cursor.execute("SHOW DATABASES")
        for db in cursor:
            print("DB:", db)
        cursor.close()
        conn.close()
    except Exception as e:
        print("MySQL 연결 실패:", e)
        raise  # 실패 시 프로그램 종료


def dump_and_upload_to_s3():
    # AWS S3 설정 (환경 변수에서 키 가져오기)
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_REGION", "us-east-1")

    # S3 클라이언트 생성
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region
    )

    # 로컬 DB 설정
    local_db_host = os.getenv("LOCAL_DB_HOST")
    local_db_user = os.getenv("LOCAL_DB_USER")
    local_db_password = os.getenv("LOCAL_DB_PASSWORD")
    local_db_name = os.getenv("LOCAL_DB_NAME")
    table_name = os.getenv("TABLE_NAME")
    chunk_size = 300  # 청크 크기

    # SQL 파일 저장 경로
    output_dir = os.path.join(os.getcwd(), "rawDB")
    os.makedirs(output_dir, exist_ok=True)

    # S3 설정
    bucket_name = os.getenv("S3_BUCKET_NAME")
    s3_directory = os.getenv("S3_DIRECTORY", "sql_backups/")

    try:
        # MySQL 연결
        conn = mysql.connector.connect(
            host=local_db_host,
            user=local_db_user,
            password=local_db_password,
            database=local_db_name
        )
        cursor = conn.cursor()

        # 총 데이터 개수 확인
        total_rows_query = f"SELECT COUNT(*) FROM {table_name};"
        cursor.execute(total_rows_query)
        total_rows = cursor.fetchone()[0]
        print(f"Total rows in table '{table_name}': {total_rows}")

        # 데이터 청크 단위로 덤프 및 S3 업로드
        for offset in range(0, total_rows, chunk_size):
            output_file = os.path.join(output_dir, f"{table_name}_chunk_{offset // chunk_size + 1}.sql")
            print(f"Dumping rows {offset} to {offset + chunk_size} into {output_file}...")
            dump_query = f"SELECT * FROM {table_name} LIMIT {chunk_size} OFFSET {offset};"
            cursor.execute(dump_query)
            rows = cursor.fetchall()

            # SQL 파일 저장
            with open(output_file, "w") as f:
                for row in rows:
                    row_data = ", ".join(
                        ["'{}'".format(str(item).replace("'", "\\'")) if item is not None else "NULL" for item in row]
                    )
                    f.write(f"INSERT INTO `{table_name}` VALUES ({row_data});\n")

            # S3로 업로드
            s3_path = os.path.join(s3_directory, os.path.basename(output_file)).replace("\\", "/")
            s3_client.upload_file(output_file, bucket_name, s3_path)
            print(f"Uploaded {output_file} to s3://{bucket_name}/{s3_path}")

    except mysql.connector.Error as e:
        print(f"MySQL Error: {e}")
    except Exception as e:
        print(f"Unexpected Error: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    print("** MySQL 연결 확인 시작 **")
    test_mysql_connection()  # MySQL 연결 확인
    print("** 데이터 덤프 및 S3 업로드 시작 **")
    dump_and_upload_to_s3()
