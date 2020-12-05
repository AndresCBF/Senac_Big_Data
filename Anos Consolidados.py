import redis

r = redis.Redis(host='redis-19626.c14.us-east-1-2.ec2.cloud.redislabs.com', port=19626, db=0, password='wsEfJaONueZpkL6Go8C81LhxD7Smjqx8')
#r = redis.Redis(host='url', port=0, db=0, password='pass')

#first_id = r.xinfo_stream("veiculo")["first-entry"][0]
#ultima_entrada = r.xinfo_stream("anos-consolidado")["last-entry"]
#print(last_id[1][b'ultimo_id'])

if r.xlen("teste17") == 0:
    print("não existe")
    soma2 = {}
    first_id = r.xinfo_stream("veiculos")["first-entry"][0]
    next_id = int(first_id.decode("utf-8").split("-")[0])

else:
    ultima_entrada = r.xinfo_stream("teste17")['last-entry'][1]
    #print(ultima_entrada)
    #print(ultima_entrada[b'ultimo_id'])
    last_id = ultima_entrada[b'ultimo_id']
    del ultima_entrada[b'ultimo_id']
    soma = ultima_entrada
    print(soma)
    soma2={}
    for item in soma:
      x=item.decode("utf-8")
      y=soma[item].decode("utf-8")
      soma2[x]=y
    next_id = last_id
    print(next_id)
    #print(soma2)


count = 0
loops = 0 
while(True):
    dados = r.xrange("veiculos", min=next_id, count=2)
    for item in dados:
        ano = item[1][b'veiculo_ano'].decode("utf-8")
        if ano in soma2:
          soma2[ano] = int(soma2[ano]) + 1
        else: 
          soma2[ano] = 1

    if len(dados) == 0:
       break
    count += len(dados)
    next_id = dados[-1][0].decode("utf-8")
    next_id = next_id.split("-")[0]
    next_id = next_id + "-1"

    loops +=1

#print("Total: %s" % count)

soma =  soma2
soma["ultimo_id"] = next_id
print(soma)
r.xadd("teste17", soma)



