# 변수 설정
COMPOSE_FILE := infra/airflow_docker/docker-compose.yaml
ENV_FILE := .env
COMPOSE_CMD := docker-compose -f $(COMPOSE_FILE) --env-file $(ENV_FILE)

.PHONY: init up down build logs restart clean

# 1. 초기화 (DB 마이그레이션 및 이미지 빌드)
init:
	$(COMPOSE_CMD) up airflow-init --build

# 2. 서비스 실행
up:
	$(COMPOSE_CMD) up -d

# 3. 서비스 중지
down:
	$(COMPOSE_CMD) down

# 4. 이미지 다시 빌드
build:
	$(COMPOSE_CMD) build

# 5. 로그 확인
logs:
	$(COMPOSE_CMD) logs -f

# 6. 재시작
restart:
	$(COMPOSE_CMD) down
	$(COMPOSE_CMD) up -d

# 7. 볼륨까지 모두 삭제 (완전 초기화)
clean:
	$(COMPOSE_CMD) down -v
