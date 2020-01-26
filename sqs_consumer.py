#!/usr/bin/env python
from service.aws_services import *
from service.message_parser import parse_message


def main():
    aws_service = "sqs"
    sqs_queue = ""
    queue_url = ""
    message = ""

    aws_conn = aws_connect(aws_service)

    while True:
        # Recive messages
        try:
            message_dict = recive_messages(aws_conn, queue_url)
            if message_dict:
                reciptHandler = message_dict.get("message").get("messageKey")
                message = message_dict.get("message").get("body")
                parsed_message = parse_message(message)
                print(parsed_message)
                # parsed_message.get("some_field")
                # TODO: Persist message into database and validate persistence

                # delete_message(aws_conn, queue_url, reciptHandler)
            else:
                print("INFO: This Queue does not have messages")
        except Exception as e:
            print(f"ERROR: {e}")
            continue


if __name__ == "__main__":
    main()
