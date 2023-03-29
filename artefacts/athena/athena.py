import logging
import time
from botocore.exceptions import ClientError


def query_execution(session_client, query, OutputLocation):
    """
    :param session_client:
    :param query:
    :param OutputLocation:
    :return: True if bucket created, else False
    """
    try:
        client = session_client.client('athena')
        client.start_query_execution(
            QueryString=query,
            ResultConfiguration={"OutputLocation": OutputLocation}
        )
    except ClientError as e:
        logging.error(e)
        return False
    print("Executing Query Athena ...")
    time.sleep(5)
    print("Execute Query Athena success")
    return True
