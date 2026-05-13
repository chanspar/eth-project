import os
from dotenv import load_dotenv
from src.silver.spark_config import get_spark_session

# .env 파일 로드 (환경 변수 활성화)
load_dotenv()

def test_connection():
    # 스파크 세션 생성
    app_name = "GCS-Connection-Test"
    spark = get_spark_session(app_name)
    
    # 로그 레벨을 ERROR로 설정하여 불필요한 INFO/WARN 로그 숨김
    spark.sparkContext.setLogLevel("ERROR")
    
    # .env에서 버킷 이름 가져오기
    bucket_name = os.getenv("GCS_BUCKET_NAME")
    if not bucket_name:
        print("❌ Error: GCS_BUCKET_NAME이 .env에 설정되어 있지 않습니다.")
        return

    # 테스트용 경로 (루트보다는 하위 폴더 하나를 지정하는 것이 깔끔합니다)
    gcs_path = f"gs://{bucket_name}/bronze/blocks/"
    print(f"🔍 Connecting to GCS path: {gcs_path}")

    try:
        # GCS 경로가 유효한지 확인하기 위해 빈 Parquet 읽기 시도 (연결 테스트용)
        # 실제 파일이 없어도 커넥션 설정이 잘못되면 여기서 에러가 발생합니다.
        print(f"🚀 GCS 연결 테스트 시작...")
        
        # spark.read.parquet() 등은 경로가 아예 없으면 에러를 내므로 
        # 설정 오류(인증 등)와 경로 부재를 구분하기 위해 try-except로 감쌉니다.
        spark.read.text(gcs_path).limit(1).collect()
        print(f"✅ Success! 버킷 '{bucket_name}'에 성공적으로 연결되었습니다.")
            
    except Exception as e:
        if "Path does not exist" in str(e):
            print(f"✅ Success! 버킷 '{bucket_name}' 연결은 성공했으나, 현재 버킷이 비어있습니다.")
        else:
            print(f"❌ Connection Failed: {e}")
        print("\n💡 Tip: .env의 GOOGLE_APPLICATION_CREDENTIALS 경로가 실제 키 파일 위치와 맞는지 확인해 보세요.")
    finally:
        spark.stop()

if __name__ == "__main__":
    test_connection()
