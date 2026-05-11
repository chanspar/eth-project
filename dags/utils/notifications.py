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
        
        if self.is_fail:
            text = """
:red_circle: *Task Failed!*
• *Task*: `{{ ti.task_id if ti else 'N/A' }}`
• *Dag*: `{{ ti.dag_id if ti else 'N/A' }}`
• *Execution Date*: {{ data_interval_end | ds if (data_interval_end is defined and data_interval_end) else (logical_date | ds if (logical_date is defined and logical_date) else 'N/A') }}
• *Log URL*: <{{ ti.log_url if ti else '#' }}|View Logs>
• *Error Message*: 
```{{ exception if exception else 'No error message provided' }}```
            """
        else:
            text = """
:large_green_circle: *DAG Run Succeeded!*
• *DAG*: `{{ dag.dag_id }}`
• *Execution Date*: {{ data_interval_end | ds if (data_interval_end is defined and data_interval_end) else (logical_date | ds if (logical_date is defined and logical_date) else 'N/A') }}
            """

        notifier = SlackWebhookNotifier(
            slack_webhook_conn_id=SLACK_DAG_CONN_ID,
            text=text
        )
        return notifier(context)

    def __await__(self):
        """inspect.isawaitable() 우회용"""
        return self.__call__().__await__()

# 객체 생성
task_fail_slack_alert = Airflow3SlackNotifier(is_fail=True)
task_succ_slack_alert = Airflow3SlackNotifier(is_fail=False)
