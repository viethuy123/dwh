import logging
import html
from utils.telegram_notification import send_telegram_message

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def send_validation_results(
    table_name: str,
    validation_result: dict,
    telegram_bot_token: str,
    telegram_chat_id: str,
    total_rows: int,
    new_rows_inserted: int
) -> None:
    """Send validation result summary to a Telegram channel via Telegram bot."""

    if validation_result is None:
        print("Error: validation_result is None. Please check the JSON data source.")
        exit(1)
    try:
        from config import get_local_now
        execution_time = get_local_now().format('YYYY-MM-DD HH:mm:ss')

        # Extract key information
        success = validation_result["success"]
        stats = validation_result["statistics"]
        total_expectations = stats["evaluated_expectations"]
        successful = stats["successful_expectations"]
        success_percent = stats["success_percent"]

        # Summarize failed expectations
        failed_results = validation_result.get("failed_expectations") or [
            r for r in validation_result.get("results", []) if not r["success"]
        ]
        failed_summary = ""
        if failed_results:
            failed_summary = "\n<b>Failed Expectations:</b>\n"
            for r in failed_results:
                # trimmed format: {'type', 'column', 'result'}
                # full format:    {'expectation_config': {...}, 'result': {...}}
                if "expectation_config" in r:
                    exp_type = html.escape(str(r["expectation_config"]["type"]))
                    column = html.escape(str(r["expectation_config"]["kwargs"].get("column", "N/A")))
                else:
                    exp_type = html.escape(str(r.get("type", "N/A")))
                    column = html.escape(str(r.get("column", "N/A")))

                observed = html.escape(str(r.get("result", {}).get("unexpected_count", "N/A")))
                failed_summary += f"- {exp_type} (Column: {column}, Unexpected: {observed})\n"
        # Create message (Telegram uses HTML for formatting)
        message = (
            f"🚀 <b>ELT Process Completed</b> 🚀\n"
            f"<b>Table</b>: {html.escape(str(table_name))}\n"
            f"<b>Total Rows</b>: {total_rows}\n"
            f"<b>New Rows Inserted</b>: {new_rows_inserted}\n"
            f"<b>Execution Time</b>: {execution_time}\n\n"
            f"📊 <b>Data Quality Validation Report</b> 📊\n"
            f"<b>Status</b>: {'✅ Passed' if success else '❌ Failed'}\n"
            f"<b>Total Expectations</b>: {total_expectations}\n"
            f"<b>Successful</b>: {successful}\n"
            f"<b>Success Rate</b>: {success_percent}%\n"
            f"{failed_summary if failed_summary else 'All expectations passed!'}"
        )

        # Send message to Telegram using shared helper
        send_telegram_message(
            message=message,
            bot_token=telegram_bot_token,
            chat_id=telegram_chat_id
        )

    except Exception as e:
        logger.error(f"Failed to send Telegram message: {str(e)}")
        raise