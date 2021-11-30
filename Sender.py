import json
import requests
import pika, sys, os

r=requests.get('https://senac-api.herokuapp.com/')
d = open ("Ep01.txt", "w")
d.write (json.dumps(r.json()))
d.close ()

c = open ("Ep01.txt", "r")
for linha in c :
 cadastro = json.loads (linha)
 i=0
 total = len(cadastro["cadastros"])
 while (i < total) :
    a = cadastro["cadastros"][i]
    b = json.dumps (a)
    
    parameters = pika.URLParameters('amqps://iruzrpzv:t6yoW3eJV-fPJK_ITIR5n0R2h5_Q4nR3@woodpecker.rmq.cloudamqp.com/iruzrpzv')
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue='EP1')
    channel.basic_publish (exchange='', routing_key='EP1', body= b)

    print( str(i) + " [X] Cadastro enviado!'")
    print (b)

    i = i+1

connection.close()