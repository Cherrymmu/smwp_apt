import os
import boto3
import mysql.connector

def dump_and_upload_to_s3():
    # 환경 변수에서 설정 읽기
    local_db_host = os.getenv("LOCAL_DB_HOST")
    local_db_user = os.getenv("LOCAL_DB_USER")
    local_db_password = os.getenv("LOCAL_DB_PASSWORD")
    local_db_name = os.getenv("LOCAL_DB_NAME")
    table_name = os.getenv("TABLE_NAME")
    chunk_size = 300  # 청크 크기

    # SQL 파일 저장 경로
    output_dir = os.path.join(os.getcwd(), "rawDB")
    os.makedirs(output_dir, exist_ok=True)

    # AWS S3 설정
    bucket_name = os.getenv("S3_BUCKET_NAME")
    s3_directory = os.getenv("S3_DIRECTORY", "sql_backups/")  # 기본 S3 디렉토리
    session = boto3.Session(profile_name="default")
    s3_client = session.client("s3")

    try:
        # 로컬 DB 연결
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
    dump_and_upload_to_s3()
