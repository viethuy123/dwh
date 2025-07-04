from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def send_validation_results(
    table_name: str,
    validation_result: dict,
    slack_bot_token: str,
    slack_channel_id: str,
    total_rows: int,
    new_rows_inserted: int
) -> None:
    """Send validation result summary to a Slack channel via Slack bot."""

    if validation_result is None:
        print("Error: validation_result is None. Please check the JSON data source.")
        exit(1)
    try:
        # Initialize Slack client
        client = WebClient(token=slack_bot_token)

        # Extract key information
        success = validation_result["success"]
        stats = validation_result["statistics"]
        total_expectations = stats["evaluated_expectations"]
        successful = stats["successful_expectations"]
        success_percent = stats["success_percent"]

        # Summarize failed expectations
        failed_results = [
            r for r in validation_result["results"] if not r["success"]
        ]
        failed_summary = ""
        if failed_results:
            failed_summary = "\n*Failed Expectations:*\n"
            for r in failed_results:
                config = r["expectation_config"]
                expectation_type = config["type"]
                kwargs = config["kwargs"]
                observed = r["result"].get("unexpected_count", "N/A")
                failed_summary += f"- {expectation_type} (Column: {kwargs.get('column', 'N/A')}, Unexpected: {observed})\n"

        # Create message (Slack uses mrkdwn for formatting)
        message = (
            f":rocket: *ELT Process Completed* :rocket:\n"
            f"*Table*: {table_name}\n"
            f"*Total Rows*: {total_rows}\n"
            f"*New Rows Inserted*: {new_rows_inserted}\n\n"
            f":bar_chart: *Data Quality Validation Report* :bar_chart:\n"
            f"*Status*: {'✅ Passed' if success else '❌ Failed'}\n"
            f"*Total Expectations*: {total_expectations}\n"
            f"*Successful*: {successful}\n"
            f"*Success Rate*: {success_percent}%\n"
            f"{failed_summary if failed_summary else 'All expectations passed!'}"
        )

        # Send message to Slack
        client.chat_postMessage(
            channel=slack_channel_id,
            text=message,
            mrkdwn=True
        )
        logger.info("Validation result sent to Slack")

    except SlackApiError as e:
        logger.error(f"Failed to send Slack message: {e.response['error']}")
        raise
    except Exception as e:
        logger.error(f"Failed to send Slack message: {str(e)}")
        raise