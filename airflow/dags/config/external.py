"""External service configurations (Slack, Dropbox, dbt)"""
from airflow.sdk import Variable

# def get_slack_config():
#     """Get Slack configuration (lazy evaluation)"""
#     return {
#         'bot_token': Variable.get('slack-bot_token'),
#         'chat_id': Variable.get('slack-chat_id')
#     }

def get_telegram_config():
    """Get Telegram configuration (lazy evaluation)"""
    bot_token = Variable.get('telegram_bot_token', default=None) or Variable.get('telegram-bot_token')
    chat_id = Variable.get('telegram_chat_id', default=None) or Variable.get('telegram-chat_id')
    return {
        'bot_token': bot_token,
        'chat_id': chat_id
    }


def get_dropbox_config():
    """Get Dropbox configuration (lazy evaluation)"""
    return {
        'app_key': Variable.get("DROPBOX_APP_KEY"),
        'app_secret': Variable.get("DROPBOX_APP_SECRET"),
        'refresh_token': Variable.get("DROPBOX_REFRESH_TOKEN"),
        'backup_local_dir': '/opt/airflow/database_backup/'
    }

