import pika
import sys
import os
import logging

# Configure logging
logging.basicConfig(filename='message_listener.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# define a main function to run the program
def main():
    try:
        # create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        # use the connection to create a communication channel
        channel = connection.channel()
        # use the channel to declare a queue
        channel.queue_declare(queue="hello")

        # define a callback function to be called when a message is received
        def callback(ch, method, properties, body):
            message = body.decode()
            print(" [x] Received %r" % message)
            # Log the received message
            logging.info("Received message: %s" % message)

        # use the channel to consume messages from the queue
        channel.basic_consume(queue="hello", on_message_callback=callback, auto_ack=True)
        # Log a message for the user
        logging.info("Waiting for messages. To exit press CTRL+C")
        # print a message to the console for the user
        print(" [*] Waiting for messages. To exit press CTRL+C")
        # start consuming messages
        channel.start_consuming()

    except Exception as e:
        # Log any exceptions that occur during execution
        logging.exception("Exception occurred")

# ---------------------------------------------------------------------------
# If this is the script we are running, then call some functions and execute code!
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
