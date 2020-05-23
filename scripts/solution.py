from __future__ import print_function
import aerospike
import logging
from aerospike import exception as ex


logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)
logging.info('Testig local Aerospike connectivity')

# Configure the client
config = {
  'hosts': [ ('db', 3000) ]
}

# Create a client and connect it to the cluster
try:
  client = aerospike.client(config).connect()
except:
  import sys
  logging.error("failed to connect to the cluster with", config['hosts'])
  sys.exit(1)

# Records are addressable via a tuple of (namespace, set, key)
key = ('test', 'otus', 'de')

try:
  # Write a record
  client.put(key, {
    'provider': 'otus',
    'class': 'data-engineering'
  })
except Exception as e:
  import sys
  print("error: {0}".format(e), file=sys.stderr)

# Read a record
(key, metadata, record) = client.get(key)
logging.debug(record)
assert(record['provider'] == 'otus')
assert(record['class'] == 'data-engineering')



def add_customer(customer_id, phone_number, lifetime_value):
    # простейшая проверка входных данных    
    if (str(customer_id) == '' or str(phone_number) == '' or str(lifetime_value)==''):
        logging.error('Некорректные данные для записи! customer_id = {0},  phone_number = {1}, lifetime_value = {2}'.format(customer_id, phone_number, lifetime_value))              
        sys.exit(1)
                     
    try:
        # запишем данные по ключу customer_id:
        key = ('test', 'customer', customer_id)
        client.put(key, {'phone': phone_number, 'ltv': lifetime_value})
        # а также запишем данные по ключу phone_number:
        key = ('test', 'customer', phone_number)
        client.put(key, {'ltv': lifetime_value})
    except ex.AerospikeError as e:
        print("Error: {0} [{1}]".format(e.msg, e.code))

def get_ltv_by_id(customer_id):
    record = {}
    # простейшая проверка входных данных  
    if (str(customer_id) == ''):
        logging.error('Некорректные данные для запроса! customer_id не должен быть пустым!')
        sys.exit(1)        
    key = ('test', 'customer', customer_id)
    (key, metadata, record) = client.get(key)
    #print(record)
    if (record == {}):
        logging.error('Requested non-existent customer ' + str(customer_id))
    else:
        return record.get('ltv')
    
def get_ltv_by_phone(phone_number):
    try:
        # простейшая проверка входных данных  
        if (str(phone_number) == ''):
            logging.error('Некорректные данные для запроса! phone_number не должен быть пустым!')             
            sys.exit(1)        
        key = ('test', 'customer', phone_number)
        (key, metadata, record) = client.get(key)
    except ex.AerospikeError as e:
        logging.error("Error: {0} [{1}]".format(e.msg, e.code))          
    if (record == {}):
        logging.error('Requested phone number is not found ' + str(phone_number))
    else:
        return record.get('ltv')
    
for i in range(0,1000):
    add_customer(i,-i,i + 1)
       
for i in range(0,1000):
    #print("LTV by ID " + str(i) + " = " + str(get_ltv_by_id(i))) # new
    #print("LTV by phone " + str(-i) + " = " + str(get_ltv_by_phone(-i))) # new
    assert (i + 1 == get_ltv_by_id(i)), "No LTV by ID " + str(i)
    assert (i + 1 == get_ltv_by_phone(-i)), "No LTV by phone " + str(-i) 

    
logging.info('Local testing completed')

# Close the connection to the Aerospike cluster
client.close()
logging.info("Local Aerospike works fine")
