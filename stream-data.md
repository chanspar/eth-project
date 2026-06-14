eth-etl stream -> kafka -> consumer -> redis                        -> fastapi -> websocket 
									-> postgres 확장버전(timescaleDB)


init.sql 
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
