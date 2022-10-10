import json, pandas as pd
from create_customer_record import *
from create_order_record import *
from update_order_record import *
from update_customer_record import *
from delete_customer_record import *
from delete_order_record import *
def process_json_message(client, path):
    #Open the JSON message
    with open(path) as json_file:
        #Load the event message into a python dict
        customer_c = json.load(json_file)
        #Extract the relevant fields
        payload = customer_c["data"]["payload"]
        event_type = payload["ChangeEventHeader"]["changeType"]
        entity = payload["ChangeEventHeader"]["entityName"]
        if event_type=="CREATE":
            if entity=="Customer":
                create_customer(payload, client)
            elif entity=="Order":
                create_order(payload, client)
        elif event_type=="UPDATE":
            if entity == "Customer":
                update_customer(payload,client)
            elif entity == "Order":
                update_order(payload,client)
        elif event_type=="DELETE":
            if entity == "Customer":
                delete_customer(payload, client)
            elif entity == "Order":
                delete_order(payload, client)
        print("A %s %s event message has been processed"%(event_type, entity))
