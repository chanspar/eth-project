from datetime import datetime
from airflow.sdk import dag, task
from utils.notifications import task_fail_slack_alert

# default_args에 설정하면 모든 태스크 실패 시 즉시 알림이 갑니다.
default_args = {
    "on_failure_callback": task_fail_slack_alert,
    "retries": 0  # 테스트를 위해 재시도 없이 즉시 실패하도록 설정
}

@dag(
    dag_id="test_slack_notification",
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    schedule="@once",
    catchup=False,
    tags=["test"]
)
def test_slack_dag():
    
    @task
    def will_fail_task():
        print("의도적으로 에러를 발생시킵니다...")
        raise ValueError("슬랙 알림 테스트용 에러 발생! 🚀")

    will_fail_task()

test_slack_dag()
