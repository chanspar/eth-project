.PHONY: up ps logs down clean shell psql redis

# 백그라운드 실행
up:
	docker compose up -d

# 컨테이너 상태 확인
ps:
	docker compose ps

# 카프카 로그 확인
logs:
	docker compose logs kafka

# 완전 종료 및 정리 (볼륨 유지)
down:
	docker compose down

# 완전 초기화 (컨테이너 종료 및 연결된 볼륨(데이터)까지 싹 다 삭제)
clean:
	docker compose down -v

# 카프카 컨테이너 내부 접속 (CLI 사용을 위해)
shell:
	docker exec -it kafka bash

# Postgres 컨테이너 내부 psql 접속, 유저: user, db: eth_data
psql:
	docker exec -it postgres psql -U user -d eth_data

# Redis 컨테이너 내부 redis-cli 접속
redis:
	docker exec -it redis redis-cli

# Kafka CLI를 사용해 명시적으로 토픽과 파티션 수를 설정하여 생성 (Auto-create보다 권장되는 방식)
create-topics:
	docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic blocks --partitions 3 --replication-factor 1 --if-not-exists
	docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic transactions --partitions 3 --replication-factor 1 --if-not-exists
	docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic token_transfers --partitions 3 --replication-factor 1 --if-not-exists
