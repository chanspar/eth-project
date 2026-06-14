eth-etl stream -> kafka -> consumer -> [postgres 확장버전(timescaleDB),redis] -> fastapi -> websocket 


## init.sql 
- 블록체인 데이터 특성을 고려해 timescaleDB 활용
- 데이터 특성(Wei)을 고려한 NUMERIC 스키마로 오버플로우를 원천 차단

- 토큰&트랜스퍼 테이블의 pk를 복합키로 설정한 이유는 뭔가?
  - 트랜잭션해시만으로는 안되는건가? 트랜잭션 한개에 토큰 전송 이벤트는 2개 이상.
- 그러면 PK를 Auto increment로 하면 되지 않나? 
	- 카프카와 같은 분산 처리 환경에서는 컨슈머 재시작 시 At-Least-Once 전송으로 인해 데이터가 중복 유입될 수
	- 그래서 멱등성 보장을 위해 복합 기본키로 설계

- 인덱스 작업은 address에만 걸어 둠 (gas_price, value 제외)
	- 카프카 실시간 적재(Insert) 속도를 방어하기 위해 부가적인 조회용 컬럼(gas, value)의 인덱싱은 과감히 배제
	- 대신, 서비스의 알파이자 오메가인 '지갑 주소 검색(address)'을 위한 최소한의 필수 인덱스만 남기는 Trade-off 전략을 택함

- PK에 timestamp를 포함해야 하는 이유 (왜 timestamp 단독 PK는 안 되는가?)
	- TimescaleDB의 파티셔닝 기준이 시간이라 PK에 필수로 들어가야 하지만, 1개 블록 안의 수백 개 트랜잭션은 시간이 똑같아 단독 PK로 쓰면 중복 에러가 나므로 식별자(hash)와 결합함.

## Kafka Consumer 라이브러리 선정 (confluent-kafka vs aiokafka vs Kafka Streams/Faust)
- **선택: confluent-kafka (C-core 기반 동기식 처리)**
  - **이유**: 현재 컨슈머의 주 목적은 외부 API 대기 없이 파싱 후 DB에 100건씩 묶어(Batch) 적재하는 **"고속 순수 처리(Throughput)"**입니다. `confluent-kafka`는 C언어(`librdkafka`) 기반으로 버퍼와 네트워크 I/O를 백그라운드에서 처리하여 적재 속도가 압도적으로 높고, 현업 대용량 파이프라인의 표준입니다.
- **왜 aiokafka를 쓰지 않았나? (비동기 처리)**
  - 만약 메시지 건건이 Etherscan 등 외부 API를 호출해야 하는 I/O Bound 작업이었다면 `aiokafka`가 압승입니다. 하지만 단순 DB 적재이므로 Pure Python의 이벤트 루프 오버헤드보다 C 기반의 깡성능 배치가 훨씬 유리합니다.
  (파이썬의 GIL(멀티 스레딩 막음)과 Asyncio 이벤트 루프 오버헤드 이슈 있음)
- **왜 Kafka Streams, PySpark, Faust 같은 헤비 프레임워크를 쓰지 않았나?**
  - 복잡한 시간 단위 윈도우 집계(Window Aggregation)나 스트림 간 조인(Stream Join)이 필요 없고, 단순 파싱 및 UPSERT 멱등성 보장이 목적이므로, 무거운 프레임워크 오버헤드를 버리고 가장 가볍고 제어가 직관적인 기본 컨슈머 로직을 택했습니다.

## 도커 컴포즈 실행
/infra/kafka로 가서 make up ㄱㄱ혓



### BaseSettings, SettingsConfigDict
- BaseSettings: 이 클래스를 상속받아 데이터 모델을 만들면 파이썬이 실행될 때 `.env` 파일을 자동으로 읽어서 환경변수를 파이썬 변수로 가져올 수 있음(.env는 다 문자열임), 타입 힌트로 문자열로 들어온 변수 알잘딱 자동변환해주고, 값이 없거나 틀리면 에러 발생 int("hi")이런거 에러
- SettingConfigDict: BaseSettings 상속시 사용, env 파일 경로, 인코딩, missing_ok 등 설정 가능. 



### pool.ThreadedConnectionPool을 사용한 이유

- 하수 방식: 배치 100건을 저장할 때마다 psycopg2.connect()로 연결을 열고 닫습니다. (속도 최악)

- 커넥션 풀(Pool) 방식: 앱이 시작될 때 미리 DB 연결 통로를 1개~10개 만들어 두고 대기시킵니다. 데이터가 오면 만들어둔 통로 중 하나를 '빌려 쓰고(getconn) 다시 반납(putconn)' 합니다. 속도가 비약적으로 빨라집니다.

- 멀티 스레드 환경 대비 스레드 세이프 설계, IO작업은 GIL 아님

- 참고: psycopg2의 ThreadedConnectionPool은 보통 늘어난 Connection을 자동으로 축소하지 않는다.


