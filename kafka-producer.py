import random
from faker import Faker
from datetime import datetime, timedelta

from confluent_kafka import Producer
import time
import json

def generate_sales_transactions():
    fake = Faker()
    user = fake.simple_profile()
    categories = ['t-shirt', 'shirt', 'shorts', 'pants', 'jacket', 'pullover', 'bag', 'shoes', 'watch', 'hat']
    colours = ['red', 'green', 'blue', 'white', 'black', 'gray', 'orange', 'brown']

    category = random.choice(categories)
    product_name = category + "_" + random.choice(colours)

    return {
        "transactionId": fake.uuid4(),
        "productId": fake.uuid4(),
        "productName": product_name,
        "productCategory": category,
        "productPrice": round(random.uniform(20000, 1000000), -2),
        "productQuantity": random.randint(1, 10),
        "productBrand": random.choice(['erigo', 'thanksinsomnia', 'cotton_ink', '3second', 'eiger', 'compass', 'the_executive']),
        "customerId": user['username'],
        "transactionDate": (datetime(2024, 1, 1) + timedelta(days=random.randint(0, (datetime.now() - datetime(2024, 1, 1)).days))).strftime('%Y-%m-%d %H:%M:%S'),
        "paymentMethod": random.choice(['credit_card', 'debit_card', 'e_wallet', 'bank_transfer', 'cash'])
    }

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f"Message delivered to {msg.topic} [{msg.partition()}]")
        
def main():
    topic = 'financial_transactions'
    producer= Producer({
        'bootstrap.servers': 'localhost:9092'
    })

    curr_time = datetime.now()

    while (datetime.now() - curr_time).seconds < 120:
        try:
            transaction = generate_sales_transactions()
            transaction['totalAmount'] = transaction['productPrice'] * transaction['productQuantity']

            print(transaction)

            producer.produce(topic,
                             key=transaction['transactionId'],
                             value=json.dumps(transaction),
                             on_delivery=delivery_report
                             )
            producer.poll(0)

            #wait for 15 seconds before sending the next transaction
            time.sleep(15)
        except BufferError:
            print("Buffer full! Waiting...")
            time.sleep(1)
        except Exception as e:
            print(e)

if __name__ == "__main__":
    main()