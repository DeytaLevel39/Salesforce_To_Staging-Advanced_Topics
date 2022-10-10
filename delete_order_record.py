def delete_order(payload, client):
    # Fetch back the latest record from staging that needs to be updated
    query = """
    select order_id, order_number, cast(order_price as string) as order_price, customer_id,
    date(createddate)||'T'||time(createddate) as createddate,
    from (select *,row_number() over(partition by order_number order by lastmodifieddate desc) as rn from staging.repl_orders)
    where rn = 1
    and order_id='%s'""" % payload["ChangeEventHeader"]["recordIDs"][0]
    query_job = client.query(query)
    records = [dict(row) for row in query_job]
    #We will need to indicate that this is a 'Delete'
    #and set the effective_from to the lastmodifieddate in the payload
    update = {"CRUD_flag": "D", "lastmodifieddate":payload["LastModifiedDate"]}
    records[0].update(update)
    errors = client.insert_rows_json("steadfast-task-363413.staging.repl_orders", records)
    if errors != []:
        print("Encountered errors while inserting rows: {}".format(errors))
