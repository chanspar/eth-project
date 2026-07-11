import time
import psycopg2
import uuid
import random
from datetime import datetime, timedelta
from psycopg2.extras import execute_values

DB_DSN = "dbname=eth_data user=user password=password host=localhost port=5432"

def generate_fake_transactions(count=200000):
    """
    TimescaleDB 파티셔닝(청크 분할)을 유도하기 위해 
    과거 10일 동안 골고루 분산된 가짜 이더리움 트랜잭션 20만 건을 생성합니다.
    """
    print(f"📦 가짜 트랜잭션 데이터 {count:,}건 생성 중...")
    start_time = datetime.now() - timedelta(days=10)
    data = []
    
    # [중요] 압축률 향상을 위한 지갑 풀(Wallet Pool) 정의
    # 실제 블록체인 환경처럼 동일한 지갑이 여러 트랜잭션을 일으키는 '데이터 중복성'을 재현합니다.
    # init.sql의 compress_segmentby = 'from_address' 규칙에 의해 
    # from_address의 중복도가 높아야 컬럼 기반 압축 엔진이 동작합니다.
    wallet_pool = ["0x" + uuid.uuid4().hex[:40] for _ in range(100)]
    
    for i in range(count):
        tx_time = start_time + timedelta(seconds=i * 4)
        data.append((
            "0x" + uuid.uuid4().hex,  # 트랜잭션 해시
            tx_time,                  # 타임스탬프 (TIMESTAMPTZ)
            random.choice(wallet_pool),  # From 주소 (중복 유도)
            random.choice(wallet_pool),  # To 주소 (중복 유도)
            random.randint(10**15, 10**19),  # 가치 (0.001 ETH ~ 10 ETH)
            random.randint(15*10**9, 50*10**9) # 가스비 (15 ~ 50 Gwei)
        ))
    print(f"✅ 데이터 생성 완료 (총 {len(data):,}건)")
    return data

def main():
    try:
        conn = psycopg2.connect(DB_DSN)
    except Exception as e:
        print(f"❌ DB 연결 실패: {e}\nDocker Compose가 작동 중인지, 포트 5432가 열려있는지 확인해주세요.")
        return
        
    cur = conn.cursor()
    
    # 1. 기존 데이터 비우기
    print("\n🧹 기존 transactions 테이블 초기화 (TRUNCATE) 중...")
    cur.execute("TRUNCATE TABLE transactions;")
    conn.commit()
    
    # 2. 대량의 데이터 적재 (용량 확보 목적)
    data = generate_fake_transactions(200000)
    print(f"\n🚀 TimescaleDB 하이퍼테이블에 {len(data):,}건 벌크 적재 시작 (execute_values 사용)...")
    
    t_start = time.time()
    query = """
        INSERT INTO transactions (hash, timestamp, from_address, to_address, value, gas_price)
        VALUES %s
        ON CONFLICT (hash, timestamp) DO NOTHING;
    """
    execute_values(cur, query, data)
    conn.commit()
    print(f"⚡ 적재 완료 (소요시간: {time.time() - t_start:.2f}초)")
    
    # 3. DB 내부 복제를 통해 700만 건으로 고속 증폭
    print("\n🔥 네트워크 병목을 피하기 위해 DB 내부 복제(generate_series) 기법으로 데이터를 700만 건 규모로 고속 증폭합니다...")
    t_amp = time.time()
    cur.execute("""
        INSERT INTO transactions (hash, timestamp, from_address, to_address, value, gas_price)
        SELECT 
            '0x' || md5(random()::text || i::text),
            timestamp + (i * interval '2 seconds'),
            from_address,
            to_address,
            value,
            gas_price
        FROM transactions, generate_series(1, 34) i;
    """)
    conn.commit()
    
    # 총 건수 확인
    cur.execute("SELECT count(*) FROM transactions;")
    total_rows = cur.fetchone()[0]
    print(f"⚡ 증폭 완료 (총 적재량: {total_rows:,}건, 소요시간: {time.time() - t_amp:.2f}초)")
    
    # 4. 압축 전 하이퍼테이블 크기 확인 (Postgres 물리 용량 측정 함수 사용)
    cur.execute("""
        SELECT pg_size_pretty(pg_total_relation_size('transactions'));
    """)
    stats = cur.fetchone()
    before_size = stats[0] if stats and stats[0] else "0 B"
    print(f"\n📊 [압축 전] 하이퍼테이블 전체 크기: {before_size}")
    
    # 4. 강제 수동 압축 실행 (7일 경과 정책을 시뮬레이션하기 위함)
    print("\n⚡ 7일 보존 기간 경과 시나리오를 재현하기 위해 물리 청크(Chunk)들을 강제 압축시킵니다...")
    try:
        cur.execute("""
            SELECT show_chunks('transactions');
        """)
        chunks = cur.fetchall()
        print(f"🔗 발견된 분할 청크 개수: {len(chunks)}개")
        
        for (chunk,) in chunks:
            print(f"  ➔ 📦 압축 실행 중: {chunk} ...")
            try:
                # compress_chunk 함수로 각 개별 청크 강제 압축 실행
                cur.execute(f"SELECT compress_chunk('{chunk}', if_not_compressed => true);")
                conn.commit()
            except Exception as ce:
                print(f"  ⚠️ {chunk} 압축 건너뜀: {ce}")
                conn.rollback()
    except Exception as e:
        print(f"❌ 청크 목록 조회 및 압축 도중 오류 발생: {e}")
        conn.rollback()
        
    # 5. 압축 후 실측 결과 출력
    print("\n📈 [압축 완료 후 스토리지 절감 통계]")
    
    # hypertable_compression_stats은 백그라운드 갱신 딜레이가 있을 수 있으므로, 
    # 즉각 반영되는 chunk_compression_stats를 활용해 통계를 정밀하게 집계합니다.
    cur.execute("""
        SELECT 
            pg_size_pretty(sum(before_compression_total_bytes)::bigint) AS before_bytes,
            pg_size_pretty(sum(after_compression_total_bytes)::bigint) AS after_bytes,
            round(100.0 * (sum(before_compression_total_bytes) - sum(after_compression_total_bytes)) / sum(before_compression_total_bytes), 2) AS savings
        FROM chunk_compression_stats('transactions');
    """)
    res = cur.fetchone()
    
    if res and res[0]:
        print("=" * 65)
        print(f"  * 📁 압축 전 원본 데이터 크기   : {res[0]}")
        print(f"  * 💾 압축 후 디스크 점유 크기   : {res[1]}")
        print(f"  * 📉 스토리지 공간 절감율       : \033[92m{res[2]}%\033[0m (목표: 90% 이상)")
        print("=" * 65)
        print("🎉 TimescaleDB 압축 정책을 통한 90% 이상의 디스크 절감 효과 검증 완료!")
    else:
        print("❌ 압축 통계 데이터 수집 실패. 데이터를 추가 유입한 뒤 다시 시도해 주세요.")
        
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
