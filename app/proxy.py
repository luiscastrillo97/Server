# example_consumer.py
import pika, os, csv
from analitica_modulo import analitica

def save(data, file_name):
  with open(file_name, 'a', newline='') as file:
    writer = csv.writer(file, delimiter=',')
    writer.writerow(data)

def process_function(msg, alalitica_servidor):
  mesage = msg.decode("utf-8")
  alalitica_servidor.update_data(mesage)
  return

while 1:
  url = os.environ.get('CLOUDAMQP_URL', 'amqp://guest:guest@rabbit:5672/%2f')
  params = pika.URLParameters(url)
  connection = pika.BlockingConnection(params)
  channel = connection.channel() # start a channel
  channel.queue_declare(queue='mensajes') # Declare a queue
  channel.queue_bind(exchange= 'amq.topic', queue='mensajes', routing_key='1.datos')
  alalitica_servidor = analitica()
  # create a function which is called on incoming messages
  def callback(ch, method, properties, body):
    process_function(body, alalitica_servidor)

  # set up subscription on the queue
  channel.basic_consume('mensajes',
    callback,
    auto_ack=True)

  # start consuming (blocks)
  channel.start_consuming()
  connection.close()
