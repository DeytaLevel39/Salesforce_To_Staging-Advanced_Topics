import json, pandas as pd
from create_customer_record import *
from create_order_record import *
from create_product_record import *
from update_customer_record import *
def process_json_message(client, path):
    #Open the JSON message
    with open(path) as json_file:
        #Load the event message into a python dict
        customer_c = json.load(json_file)
        #Extract the relevant fields
        payload = customer_c["data"]["payload"]
        if payload["ChangeEventHeader"]["changeType"]=="CREATE":
            entity = payload["ChangeEventHeader"]["entityName"]
            if entity=="Customer":
                create_customer(payload, client)
            elif entity=="Order":
                create_order(payload, client)
            elif entity=="Product":
                create_product(payload, client)
        elif payload["ChangeEventHeader"]["changeType"]=="UPDATE":
            update_customer(payload,client)
        elif payload["ChangeEventHeader"]["changeType"]=="DELETE":
            print("Let's soft delete",payload["ChangeEventHeader"]["recordIDs"][0])
