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
                if json_file.name.find("US")>0:
                    country_code = "US"
                else:
                    country_code = "UK"
                create_customer(payload, client, country_code)
            elif entity=="Order":
                create_order(payload, client)
        elif event_type=="UPDATE":
            if json_file.name.find("US") > 0:
                country_code = "US"
            else:
                country_code = "UK"
            if entity == "Customer":
                #First up we need to fetch back the latest version of the customer
                record_id = payload["ChangeEventHeader"]["recordIDs"][0]
                lastmodifieddate = payload["LastModifiedDate"]
                query = """
                select customer_id, customer_number, first_name, last_name, 
                date(lastmodifieddate)||'T'||time(lastmodifieddate) as lastmodifieddate, 
                date(createddate)||'T'||time(createddate) as createddate, 
                cast(current_datetime() as string) as applieddate,
                CRUD_flag
                from (select *,row_number() over(partition by customer_number order by lastmodifieddate desc) as rn 
                      from data_vault.sat_%s_customer s, data_vault.hub_customer h, data_vault.ref_record_source r 
                      where h.customer_hk = s.customer_hk
                      and s.record_source = r.id
                      and r.country_code = '%s'
                      and lastmodifieddate < '%s')
                where rn = 1
                and customer_id = '%s'""" %(country_code, country_code, lastmodifieddate, record_id)
                update_customer(query,payload,client,country_code, True)
                #Then we need to apply the update to all records > lastmodifieddate
                query = """
                                select customer_id, customer_number, first_name, last_name, 
                                date(lastmodifieddate)||'T'||time(lastmodifieddate) as lastmodifieddate, 
                                date(createddate)||'T'||time(createddate) as createddate,
                                cast(current_datetime() as string) as applieddate, 
                                CRUD_flag
                                from data_vault.sat_%s_customer s, data_vault.hub_customer h, data_vault.ref_record_source r 
                                where h.customer_hk = s.customer_hk 
                                and s.record_source = r.id
                                and r.country_code = '%s'
                                and lastmodifieddate > '%s'
                                and customer_id='%s'""" % (country_code, country_code, lastmodifieddate, record_id)
                update_customer(query, payload, client, country_code, False)

            elif entity == "Order":
                update_order(payload,client)
        elif event_type=="DELETE":
            if entity == "Customer":
                delete_customer(payload, client)
            elif entity == "Order":
                delete_order(payload, client)
        print("A %s %s event message has been processed"%(event_type, entity))
