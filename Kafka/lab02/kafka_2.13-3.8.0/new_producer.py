from kafka import KafkaProducer
import json


producer = KafkaProducer(value_serializer=lambda x: json.dumps(x).encode('utf-8'))

trans_id = 102

while True:
    user_input = input('Do you need transaction ?\n')
    if user_input.lower() == 'no':
        print('Stopping transaction, bye!')
        break
    if user_input.lower() == 'yes':
        atm_choice = input('Please choose transaction number: (1 or 2)\n')
        if atm_choice == '1' or atm_choice == '2':
            producer.send("bankbranch", {'atmid': int(atm_choice), 'transid':trans_id})
            producer.flush()
            trans_id +=1
        else:
            print('invalid ATM number')
    else:
        print('invalid ATM number')
        continue

producer.close()