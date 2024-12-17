import os
import shutil  # 파일 이동을 위해 shutil 모듈 추가
import subprocess
import mysql.connector
from dotenv import load_dotenv

def dump_local_database_in_chunks():
    # 로컬 MySQL 설정
    local_db_host = os.getenv("LOCAL_DB_HOST")  # 로컬 MySQL 서버
    local_db_user = os.getenv("LOCAL_DB_USER")  # MySQL 사용자
    local_db_password = os.getenv("LOCAL_DB_PASSWORD")  # 비밀번호
    local_db_name = os.getenv("LOCAL_DB_NAME")  # 데이터베이스 이름
    table_name = os.getenv("TABLE_NAME")  # 테이블 이름
    chunk_size = 300             # 한 번에 내보낼 데이터 개수

    # SQL 파일 저장 경로 설정
    output_dir = os.path.join(os.getcwd(), "rawDB")
    os.makedirs(output_dir, exist_ok=True)

    try:
        # MySQL 데이터베이스 연결
        conn = mysql.connector.connect(
            host=local_db_host,
            user=local_db_user,
            password=local_db_password,
            database=local_db_name
        )
        cursor = conn.cursor()

        # 총 데이터 개수 확인
        print("Calculating total number of rows...")
        total_rows_query = f"SELECT COUNT(*) FROM {table_name};"
        cursor.execute(total_rows_query)
        total_rows = cursor.fetchone()[0]
        print(f"Total rows in table '{table_name}': {total_rows}")

        # 데이터를 청크 단위로 내보내기
        for offset in range(0, total_rows, chunk_size):
            output_file = os.path.join(output_dir, f"{table_name}_chunk_{offset // chunk_size + 1}.sql")
            print(f"Dumping rows {offset} to {offset + chunk_size} into {output_file}...")

            # 데이터를 청크 단위로 가져오기
            dump_query = f"SELECT * FROM {table_name} LIMIT {chunk_size} OFFSET {offset};"
            cursor.execute(dump_query)
            rows = cursor.fetchall()

            # SQL 파일에 저장
            with open(output_file, "w") as f:
                for row in rows:
                    row_data = ", ".join(["'{}'".format(str(item).replace("'", "\\'")) if item is not None else "NULL" for item in row])
                    f.write(f"INSERT INTO `{table_name}` VALUES ({row_data});\n")

            print(f"Chunk saved: {output_file}")

    except mysql.connector.Error as e:
        print(f"MySQL Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def upload_to_rds_and_cleanup():
    raw_dir = os.path.join(os.getcwd(), "rawDB")  # SQL 파일이 저장된 디렉토리
    backup_dir = os.path.join(os.getcwd(), "backup")

    # 백업 디렉토리 생성
    os.makedirs(backup_dir, exist_ok=True)

    # RDS 환경 변수 가져오기
    rds_host = os.getenv("RDS_HOST")
    rds_username = os.getenv("RDS_USERNAME")
    rds_password = os.getenv("RDS_PASSWORD")
    rds_database = os.getenv("RDS_DATABASE")

    if not all([rds_host, rds_username, rds_password, rds_database]):
        print("Error: One or more required environment variables are not set.")
        return

    # RDS 업로드
    for file in os.listdir(raw_dir):
        file_path = os.path.join(raw_dir, file)

        if file.endswith('.sql'):
            print(f"Uploading {file} to RDS...")
            try:
                with open(file_path, "r") as sql_file:
                    subprocess.run(
                        ["mysql", "-h", rds_host, "-u", rds_username, f"-p{rds_password}", rds_database],
                        stdin=sql_file,
                        check=True
                    )
                print(f"Uploaded {file} successfully.")

                # 백업 디렉토리로 이동
                backup_path = os.path.join(backup_dir, file)
                shutil.move(file_path, backup_path)
                print(f"Moved {file} to backup directory: {backup_path}")

            except subprocess.CalledProcessError as e:
                print(f"Error uploading {file_path} to RDS: {e}")
            except Exception as e:
                print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    dump_local_database_in_chunks()    # 로컬 데이터베이스에서 SQL 파일 덤프
    upload_to_rds_and_cleanup()        # RDS에 업로드 및 백업