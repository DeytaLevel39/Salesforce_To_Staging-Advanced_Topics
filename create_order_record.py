def create_order(payload, client):
    order_id = payload["ChangeEventHeader"]["recordIDs"][0]
    order_number = payload["Order_Number"]
    order_price = payload["Order_Price"]
    customer_id = payload["CustomerID"]
    createddate = payload["CreatedDate"]
    lastmodifieddate = payload["LastModifiedDate"]
    CRUD_flag = payload["ChangeEventHeader"]["changeType"][0]

    # Insert a new customer record if its not a duplicate
    insert_stmt = """
        merge into staging.repl_orders target
        using (select '%s' as order_id,
            '%s' as order_number,
            cast(%f as decimal) as order_price,
            '%s' as customer_id,
            cast('%s' as datetime) as lastmodifieddate,
            cast('%s' as datetime) as createddate,
            '%s' as CRUD_flag) as source
        on source.order_id = target.order_id and source.lastmodifieddate = target.lastmodifieddate
        when not matched then insert row""" % (
    order_id, order_number, order_price, customer_id, lastmodifieddate, createddate, CRUD_flag)
    # Do the insertion of the row into the customers table in BigQuery
    insert = client.query(insert_stmt)
    # Wait for job to end
    insert.result()

