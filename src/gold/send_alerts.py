import argparse
import time
from pyspark.sql import functions as F
from src.silver.spark_config import get_spark_session, get_logger
from src.config import BQ_DATASET
from src.gold.utils import gold_path
from dags.utils.notifications import send_slack_message

def format_alert_message(dt_val, alert_df, top_whales_df):
    """
    슬랙 메시지 포맷팅
    """
    lines = []
    lines.append(f"🐋 *[Ethereum Whale Report] {dt_val}*")
    lines.append("="*40)

    # 1. 주요 알림 이벤트 (Top 5 High Value)
    lines.append("\n🚨 *Major Events (Top 5)*")
    alerts = alert_df.orderBy(F.col("value_eth").desc()).limit(5).collect()
    
    if not alerts:
        lines.append("_No major events detected._")
    else:
        for row in alerts:
            emoji = "🔴" if "CEX_DEPOSIT" in row['event_type'] else "🟢" if "CEX_WITHDRAWAL" in row['event_type'] else "⚠️"
            val = f"{row['value_eth']:,.1f} ETH"
            lines.append(f"{emoji} *{row['event_type']}*: {val}")
            lines.append(f"   > {row['detail']}")

    # 2. 오늘의 TOP 5 고래
    lines.append("\n📊 *Top 5 Individual Whales*")
    whales = top_whales_df.orderBy("rank").limit(5).collect()
    
    for w in whales:
        pos_emoji = "📈" if w['position'] == "ACCUMULATING" else "📉" if w['position'] == "DISTRIBUTING" else "↔️"
        lines.append(f"{w['rank']}. `{w['address'][:10]}...` | {pos_emoji} *{w['position']}*")
        lines.append(f"   (Vol: {w['total_volume_eth']:,.1f} ETH / Net: {w['net_flow_eth']:,.1f})")

    lines.append("\n" + "="*40)
    lines.append(f"📊 _BigQuery Sync Complete: {BQ_DATASET}_")
    lines.append(f"🔗 *[View Tableau Dashboard](https://public.tableau.com/views/YourDashboard)*")
    
    return "\n".join(lines)

def main():
    parser = argparse.ArgumentParser(description="Gold Layer: 분석 결과 알림 전송")
    parser.add_argument("--date", required=True)
    args = parser.parse_args()

    logger = get_logger("GoldAlertSender")
    spark = get_spark_session("GoldAlertSender")
    
    dt_val = args.date if "dt=" in args.date else f"dt={args.date}"
    
    try:
        # 데이터 로드
        alert_df = spark.read.parquet(gold_path("alert_events", dt_val))
        top_whales_df = spark.read.parquet(gold_path("top_whales", dt_val))
        
        # 메시지 생성
        message = format_alert_message(args.date, alert_df, top_whales_df)
        
        # 전송
        send_slack_message(message)
        logger.info(f"✅ Slack 알림 전송 완료 ({args.date})")
        
    except Exception as e:
        logger.error(f"❌ 알림 전송 실패: {str(e)}")
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
