from faker import Faker
from faker_vehicle import VehicleProvider
import time
import random
import redis

fake = Faker()
fake.add_provider(VehicleProvider)


r = redis.Redis(host='redis-19626.c14.us-east-1-2.ec2.cloud.redislabs.com', port=19626, db=0, password='wsEfJaONueZpkL6Go8C81LhxD7Smjqx8')
#r = redis.Redis(host='url', port=0, db=0, password='pass')

while(True):
  output = {
    "id": fake.numerify(text="id-%#%#"),
    "nome": fake.name(),
    "telefone": fake.numerify(text="(%%) 9%%%%-%%%%"),
    "email": fake.ascii_safe_email(),
    "endereco": fake.address(),
    "veiculo_placa": fake.license_plate(),
    "veiculo_ano": fake.vehicle_year(),
    "veiculo_fabricante": fake.vehicle_make(),
    "veiculo_modelo": fake.vehicle_model()
  }
  print(output)
  print(r.xadd("veiculos", output))
  time.sleep(random.randint(1, 10))