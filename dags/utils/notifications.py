from datetime import datetime, timezone
from airflow.providers.slack.notifications.slack_webhook import SlackWebhookNotifier


SLACK_DAG_CONN_ID = "eth_etl_webhook"

class Airflow3SlackNotifier:
    """
    Airflow 3 SDK의 모순(검증은 async 요구, 실행은 sync 처리)을 해결하기 위한 래퍼 클래스.
    __qualname__을 가져야 하며, 호출 가능해야 하고, 검증기 통과를 위해 awaitable 처럼 보여야 합니다.
    """
    def __init__(self, is_fail=True):
        self.is_fail = is_fail
        self.__qualname__ = "task_fail_slack_alert" if is_fail else "task_succ_slack_alert"
        self.__name__ = self.__qualname__
        self.__module__ = __name__

    def __call__(self, context):
        print(f"DEBUG: Airflow3SlackNotifier triggered (is_fail={self.is_fail})")
        
        # 소요 시간 계산 및 시간대 변환 (UTC -> KST)
        from datetime import timedelta
        dag_run = context.get("dag_run")
        start_date_utc = dag_run.start_date if dag_run else None
        now_utc = datetime.now(timezone.utc)
        
        # KST 변환 함수
        def to_kst(dt):
            return (dt + timedelta(hours=9)).strftime('%Y-%m-%d %H:%M:%S') if dt else 'N/A'

        duration_str = "Unknown"
        if start_date_utc:
            duration = now_utc - start_date_utc
            duration_str = str(duration).split(".")[0]

        if self.is_fail:
            text = f"""
:red_circle: *Task Failed!*
• *Task*: `{context.get('ti').task_id if context.get('ti') else 'N/A'}`
• *Dag*: `{context.get('dag').dag_id}`
• *Execution Date*: {context.get('logical_date').strftime('%Y-%m-%d')}
• *Duration so far*: `{duration_str}`
• *Error*: ```{context.get('exception', 'No exception info')}```
            """
        else:
            text = f"""
:large_green_circle: *DAG Run Succeeded!*
• *DAG*: `{context.get('dag').dag_id}`
• *Execution Date*: {context.get('logical_date').strftime('%Y-%m-%d')}
• *Start Time*: {to_kst(start_date_utc)} (KST)
• *End Time*: {to_kst(now_utc)} (KST)
• *Duration*: `{duration_str}`
            """

        notifier = SlackWebhookNotifier(
            slack_webhook_conn_id=SLACK_DAG_CONN_ID,
            text=text
        )
        # SlackWebhookNotifier 내부적으로 Jinja 렌더링을 시도하므로, 
        # 이미 포맷팅된 문자열을 보낼 때는 템플릿 변수가 섞이지 않게 주의
        return notifier(context)

    def __await__(self):
        """inspect.isawaitable() 우회용"""
        return self.__call__().__await__()

# 객체 생성
task_fail_slack_alert = Airflow3SlackNotifier(is_fail=True)
task_succ_slack_alert = Airflow3SlackNotifier(is_fail=False)
