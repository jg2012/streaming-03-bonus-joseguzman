# add imports at the beginning of the file
import pika
import csv
import time
import logging

# Configure logging
logging.basicConfig(filename='message_sender.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# create a blocking connection to the RabbitMQ server
conn = pika.BlockingConnection(pika.ConnectionParameters("LOCALHOST"))

# use the connection to create a communication channel
ch = conn.channel()

# use the channel to declare a queue
ch.queue_declare(queue="hello")

#CSV file variable
atp_tennis = "atp_tennis.csv"

#Opens the CSV file and reads the data
with open(atp_tennis, newline='') as csvfile:
    csvreader = csv.reader(csvfile)
    
    # get the headers (first row)
    headers = next(csvreader)
    
    # iterate over each row in the CSV
    for row in csvreader:
        # construct the message from the row
        message = ', '.join(f"{header}: {value}" for header, value in zip(headers, row))
        
        # use the channel to publish a message to the queue
        ch.basic_publish(exchange="", routing_key="hello", body=message)

         # Log the message
        logging.info(f"Sent message: {message}")

        # print a message to the console for the user
        print(f" [x] Sent '{message}'")

        # wait for 3 seconds before sending the next message
        time.sleep(3)

# close the connection to the server
conn.close()
