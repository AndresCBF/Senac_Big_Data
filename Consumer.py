import pika, sys, os
import json
import pymongo
from pymongo import MongoClient

def main():
  parameters = pika.URLParameters('amqps://iruzrpzv:t6yoW3eJV-fPJK_ITIR5n0R2h5_Q4nR3@woodpecker.rmq.cloudamqp.com/iruzrpzv')
  
  connection = pika.BlockingConnection(parameters)
  channel = connection.channel()

  channel.queue_declare(queue='EP1')

  def callback(ch, method, properties, body):
      r  = json.loads(body)

      client = MongoClient('mongodb+srv://Andres:Aula123@cluster0.pazrr.mongodb.net/ArquiteturaBigData?retryWrites=true&w=majority')
      
      db = client.get_database ('ArquiteturaBigData')
      
      cadastro = db.cadastros 
      cadastro.insert_one (r)

      print (cadastro.count_documents ({})) 
      print("[X] Cadastros recibido %r" %body)
      
  channel.basic_consume(queue='EP1', on_message_callback=callback, auto_ack=True)

  print(' [*] Waiting for messages. To exit press CTRL+C')
  channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print(' [X] Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
