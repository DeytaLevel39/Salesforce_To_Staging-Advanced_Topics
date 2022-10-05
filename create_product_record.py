def create_product(payload, client):
    product_id = payload["ChangeEventHeader"]["recordIDs"][0]
    product_number = payload["ProductNumber"]
    product_name = payload["ProductName"]
    unit_price = payload["UnitPrice"]
    createddate = payload["CreatedDate"]
    lastmodifieddate = payload["LastModifiedDate"]
    CRUD_flag = payload["ChangeEventHeader"]["changeType"][0]
    # Construct a JSON in a format that can be inserted into a table
    product_json = [{"product_id": product_id,
                      "product_number": product_number,
                      "product_name": product_name,
                      "unit_price": unit_price,
                      "lastmodifieddate": lastmodifieddate,
                      "createddate": createddate,
                      "CRUD_flag": CRUD_flag
                      }]
    # Do the insertion of the row into the products table in BigQuery Staging
    errors = client.insert_rows_json("steadfast-task-363413.staging.products", product_json)
    if errors != []:
        print("Encountered errors while inserting rows: {}".format(errors))
    else:
        print("A new product record has been created")