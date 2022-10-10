def create_customer(payload, client):
    customer_id = payload["ChangeEventHeader"]["recordIDs"][0]
    customer_number = payload["CustomerNumber"]
    first_name = payload["First_Name"]
    last_name = payload["Last_Name"]
    createddate = payload["CreatedDate"]
    lastmodifieddate = payload["LastModifiedDate"]
    CRUD_flag = payload["ChangeEventHeader"]["changeType"][0]
    #Insert a new customer record if its not a duplicate
    insert_stmt = """
    merge into staging.repl_customers target
    using (select '%s' as customer_id,
        '%s' as customer_number ,
        '%s' as first_name,
        '%s' as last_name,
        cast('%s' as datetime) as lastmodifieddate,
        cast('%s' as datetime) as createddate,
        '%s' as CRUD_flag) as source
    on source.customer_id = target.customer_id and source.lastmodifieddate = target.lastmodifieddate
    when not matched then insert row"""%(customer_id,customer_number,first_name,last_name,lastmodifieddate,createddate,CRUD_flag)
    # Do the insertion of the row into the customers table in BigQuery
    insert = client.query(insert_stmt)
    # Wait for job to end
    insert.result()