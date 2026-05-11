from airflow.providers.slack.notifications.slack_webhook import SlackWebhookNotifier


SLACK_DAG_CONN_ID = "eth_etl_webhook"

task_fail_slack_alert = SlackWebhookNotifier(
    slack_webhook_conn_id=SLACK_DAG_CONN_ID,
    text="""
:red_circle: *Task Failed!*
• *Task*: `{{ ti.task_id if ti else 'N/A' }}`
• *Dag*: `{{ ti.dag_id if ti else 'N/A' }}`
• *Execution Date*: {{ data_interval_end | ds if (data_interval_end is defined and data_interval_end) else (logical_date | ds if (logical_date is defined and logical_date) else 'N/A') }}
• *Log URL*: <{{ ti.log_url if ti else '#' }}|View Logs>
• *Error Message*: 
```{{ exception if exception else 'No error message provided' }}```
    """
)

# 성공 알림 — DAG 전체 성공(on_success_callback) 시 발동
# send_summary_report task가 상세 리포트를 담당하므로
# 여기선 간단한 완료 신호 역할만 수행
task_succ_slack_alert = SlackWebhookNotifier(
    slack_webhook_conn_id=SLACK_DAG_CONN_ID,
    text="""
:large_green_circle: *DAG Run Succeeded!*
• *DAG*: `{{ dag.dag_id }}`
• *Execution Date*: {{ data_interval_end | ds if (data_interval_end is defined and data_interval_end) else (logical_date | ds if (logical_date is defined and logical_date) else 'N/A') }}
    """
)
