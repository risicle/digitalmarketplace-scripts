from dmapiclient import DataAPIClient as data_api_client
from datetime import datetime, date, timedelta
from dmscripts.helpers import env_helpers


def get_brief_response_emails(data_api_client, brief):
    responses = data_api_client.find_brief_responses(brief_id=brief["id"], status="submitted").get("briefResponses")
    return [response["respondToEmailAddress"] for response in responses]

def create_context_for_brief(stage, brief):
    return {
        'brief_title': brief['title'],
        'brief_link': '{0}/{1}/opportunities/{2}'.format(
            env_helpers.get_web_url_from_stage(stage),
            brief['frameworkFramework'],
            brief['id']
        )
    }

def main(data_api_client, notify_client, notify_template_id, stage, withdrawn_date):
    briefs_withdrawn_on_date = data_api_client.find_briefs(withdrawn_on=withdrawn_date).get("briefs")

    for brief in briefs_withdrawn_on_date:
        email_addresses = get_brief_response_emails(data_api_client, brief)
        if email_addresses:
            brief_email_context = create_context_for_brief(stage, brief)
            for email_address in email_addresses:
                notify_client.send_email(
                    email_address, notify_template_id, brief_email_context, allow_resend=False
                )
    return True
