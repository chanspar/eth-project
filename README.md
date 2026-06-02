# eth-project

## 로컬 전용 (with docker-compose)

- 현재 VirtualBox로 vm3개를 만들어서 쿠버네티스 구축을 진행 중이여서 이 브랜치에 로컬의 원본을 남기기로 결정했다.

- 현재 eth-etl을 쓰고 있다. 이건 3.9에서까지 잘 돌아가는 지 테스트가 완료되었고 3.10에서 에러가 발생하였다.

- 나는 airflow3을 이용해보고싶었는데 파이썬 3.13부터 잘 지원해 주는 거 같았다.

- 그래서 도커파일로 파이썬 3.13으로 만들고 그 안에 venv환경으로 3.9를 만들어주었다.

- 현재 이 코드는 내가 실험과정을 여러번 걸쳐서 스파크의 버전이랑 airflow 버전이랑 뒤죽박죽 섞여있다. 그래서 안될 가능성 매우 높음

<br>
<br>
<br>

---


# 초반 아이디어 



## Ethereum-etl -> GCS

- airflow를 활용하여 하루에 한 번씩 7200개의 이더리움 블록과 관련된 내용을 GCS에 적재

- 불장이 아닌 지금 하루에 10GB 정도 쌓임 확인

- https://www.alchemy.com api 이용

#### GCS 저장 구조:
```
bronze/
    ├── blocks/dt={date}/
    ├── transactions/dt={date}/
    ├── receipts/dt={date}/
    ├── logs/dt={date}/
    ├── token_transfers/dt={date}/
    └── contracts/dt={date}/
```

> https://velog.io/@chanspar/DE-%EC%97%AD%EB%9F%89-%EA%B8%B0%EB%A5%B4%EA%B8%B0-Ethereum-ETL

- 저장되어 있는 것이 어떤 것인지 블로그를 통해 확인 가능 합니다.

---
#### airflow 시도

**권장 사양**
- vCPU: 4
- RAM: 8~16 GB
- 디스크: 20 GB

1. VM 기반의 Docker Compose

- 실험해 보고 만약 단일 VM으로 벅차다면? 
  - 특정 태스크 때문에 Airflow 전체가 뻗으면..?

2. Celery 혹은 Kubernetes 이용 고려
  - 아마 쿠버네티스를 이용 할 듯 (회복력으로 안정적인 운영 가능 + 쿠버네티스 연습)
  - 이유: Celery는 항상 실행되어 있고 블록마다 담긴 트랜잭션 수가 들쭉날쭉한 데이터 특성상 쿠버네티스가 맞다. 
    - 현재 ethereum_etl은 python3.9 기반이고, airflow3는 python 3.13 기반이다. dag코드에서 ExternalPythonOperator 이거 써야 된다고 한다. <- 현재는 이걸 사용 중(05.13일 기준)

---

#### 체크 사항

- 데이터 누락률 체크.

- 가끔 GCS에 0바이트인 파일이 올라감. -> 비정상 파일 트래킹 필요.

- `blocks`에 있는 트랜잭션 개수 총합과 `transactions` 파일의 row 수, 그리고 `receipts` 파일의 row 수가 정확히 1:1:1로 일치하는 지 확인 필요.

- 적재되는데 걸리는 시간 체크
  - airflow의 Deadline Alerts(SLA) 이용

- 클라우드 사용 시 비용 체크

> slack 알림을 이용


---


