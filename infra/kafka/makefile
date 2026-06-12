.PHONY: up ps logs down shell

# 백그라운드 실행
up:
	docker compose up -d

# 컨테이너 상태 확인
ps:
	docker compose ps

# 카프카 로그 확인
logs:
	docker compose logs kafka

# 완전 종료 및 정리
down:
	docker compose down

# 카프카 컨테이너 내부 접속 (CLI 사용을 위해)
shell:
	docker exec -it kafka bash
