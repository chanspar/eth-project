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



### token & transfer에는 timestamp가 없다?
- 블록 시간 테이블(blocks)을 따로 캐싱


## 엔드투엔드(End-to-End) 파이프라인 통합 테스트 
1. cd infra/kafka ; make up (만약 make가 안되면 docker-compose up -d 실행)
2. uv run src/consumer/main.py
3. docker build -t eth-etl-stream -f Dockerfile.etl .
docker run --rm --network host eth-etl-stream

## 트러블슈팅 및 운영 최적화 노트

### 1. `ethereum-etl` stream 구동 시 빈 블록 폭주 이슈
- **원인**: `--start-block`을 지정하지 않고 `last_synced_block.txt`도 없을 경우, `ethereum-etl`은 이더리움 제네시스 블록(0번)부터 수집을 시작함. 0~4.6만 번 블록은 트랜잭션이 하나도 없는 빈 블록이므로, 실행 즉시 `blocks` 토픽에만 수천 개의 빈 블록이 쌓이고 `transactions` 등은 0개가 됨.
- **해결**: `run_stream.py` 실행 시 Alchemy 노드에 `eth_blockNumber` RPC 요청을 보내 **현재 실시간 최신 블록 번호**를 알아낸 뒤, `--start-block` 파라미터로 자동 주입하도록 수정.

### 2. Kafka Topic 이름 복수형(Plural) 자동 변환 이슈
- **원인**: `ethereum-etl` 실행 시 `--entity-types block,transaction...` 과 같이 단수형으로 입력해도, 내부 Kafka Exporter는 자동으로 끝에 `s`를 붙여서 `blocks`, `transactions`, `token_transfers` 등 복수형 토픽으로 퍼블리싱함.
- **해결**: 컨슈머가 구독하는 토픽명과 `Makefile`의 토픽 생성 스크립트를 모두 복수형으로 맞춤. 또한 불필요하게 가공 전 날것의 로그가 `logs` 토픽에 쌓이는 것을 막기 위해 `--entity-types`에서 `log`를 제거 (토큰 전송은 내부적으로 알아서 파싱됨).

### 3. Redis TTL (만료 시간) 최적화
- **이슈**: 블록 타임스탬프를 보관하는 레디스 키의 TTL이 1일(86400초)로 과도하게 설정되어 있어 불필요한 메모리 낭비 발생.
- **해결**: 카프카 파티션 처리 딜레이를 고려하더라도 수 분이면 블록/토큰 전송 처리가 끝나므로, TTL을 10분(600초)으로 대폭 축소하여 메모리 누수 방지.

### 4. TimescaleDB 데이터 압축 (Compression) 정책 적용
- **이슈**: 하루 수백만 건(약 3GB)씩 쌓이는 시계열 데이터를 그대로 방치하면 스토리지 유지 비용이 기하급수적으로 증가함.
- **해결**: 
  - `transactions`와 `token_transfers` 테이블에 대해 `timescaledb.compress` 활성화.
  - 검색 성능을 위해 `segmentby`를 지갑 주소(`from_address`, `token_address`)로, `orderby`를 `timestamp DESC`로 설정.
  - `add_compression_policy`를 통해 **7일이 지난 Cold Data는 백그라운드에서 자동 압축**되도록 설정 (용량 90% 절감 가능).
