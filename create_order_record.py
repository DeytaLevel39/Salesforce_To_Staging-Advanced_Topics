def create_order(payload, client):
    order_id = payload["ChangeEventHeader"]["recordIDs"][0]
    order_number = payload["OrderNumber"]
    customer_id = payload["CustomerID"]
    createddate = payload["CreatedDate"]
    lastmodifieddate = payload["LastModifiedDate"]
    CRUD_flag = payload["ChangeEventHeader"]["changeType"][0]
    # Construct a JSON in a format that can be inserted into a table
    order_json = [{"order_id": order_id,
                      "order_number": order_number,
                      "customer_id": customer_id,
                      "lastmodifieddate": lastmodifieddate,
                      "createddate": createddate,
                      "CRUD_flag": CRUD_flag
                   }]
    # Do the insertion of the row into the customers table in BigQuery Staging
    errors = client.insert_rows_json("steadfast-task-363413.staging.orders", order_json)
    if errors != []:
        print("Encountered errors while inserting rows: {}".format(errors))
    else:
        print("A new order record has been created")