from kafka import KafkaConsumer, TopicPartition
from json import loads
import psycopg2
from psycopg2._psycopg import cursor


class XactionConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer('bank-customer-events',
                                      bootstrap_servers=['localhost:9092'],
                                      # auto_offset_reset='earliest',
                                      value_deserializer=lambda m: loads(m.decode('ascii')))
        ## These are two python dictionarys
        # Ledger is the one where all the transaction get posted
        self.ledger = {}
        # custBalances is the one where the current blance of each customer
        # account is kept.
        self.custBalances = {}
        # THE PROBLEM is every time we re-run the Consumer, ALL our customer
        # data gets lost!
        # add a way to connect to your database here.
        self.con = psycopg2.connect(#insert connection info here)
        print("Database opened successfully")

        self.cur = self.con.cursor()

        self.cur.execute('''DROP TABLE IF EXISTS BANK;
                        CREATE TABLE BANK 
                        (CUSTID INT NOT NULL,
                        TYPE TEXT NOT NULL,
                        DATE INT NOT NULL,
                        AMT INT NOT NULL);''')
        print("Table created successfully")



        self.con.commit()

    def handleMessages(self):
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            self.ledger[message['custid']] = message
            # add message to the transaction table in your SQL usinf SQLalchemy
            if message['custid'] not in self.custBalances:
                self.custBalances[message['custid']] = 0
            if message['type'] == 'dep':
                self.custBalances[message['custid']] += message['amt']
            else:
                self.custBalances[message['custid']] -= message['amt']
            print(self.custBalances)

            cur = self.con.cursor()

            bank_model = (message['custid'], message['type'], message['date'], message['amt'])
            cur.execute("insert into BANK(custid, type, date, amt) VALUES(%s,%s,%s,%s);", bank_model)

            self.con.commit()

if __name__ == "__main__":
    c = XactionConsumer()
    c.handleMessages()