def create_customer(payload, client):
    customer_id = payload["ChangeEventHeader"]["recordIDs"][0]
    customer_number = payload["CustomerNumber"]
    first_name = payload["First_Name"]
    last_name = payload["Last_Name"]
    createddate = payload["CreatedDate"]
    lastmodifieddate = payload["CreatedDate"]
    # Construct a JSON in a format that can be inserted into a table
    customer_json = [{"customer_id": customer_id, "customer_number": customer_number, "first_name": first_name,
                      "last_name": last_name, "lastmodifieddate": lastmodifieddate, "createddate": createddate}]
    # Do the insertion of the row into the customers table in BigQuery Staging
    errors = client.insert_rows_json("steadfast-task-363413.staging.customers", customer_json)
    if errors != []:
        print("Encountered errors while inserting rows: {}".format(errors))
    else:
        print("A new customer record has been created")