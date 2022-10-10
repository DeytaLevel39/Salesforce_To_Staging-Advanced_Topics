def update_order(payload,client):
    # Fetch back the latest record from staging that needs to be updated
    query = """
    select order_id, order_number, order_price, customer_id, 
    date(lastmodifieddate)||'T'||time(lastmodifieddate) as lastmodifieddate, 
    date(createddate)||'T'||time(createddate) as createddate, CRUD_flag
    from (select *,row_number() over(partition by order_number order by lastmodifieddate desc) as rn from staging.repl_orders)
    where rn = 1
    and order_id='%s'""" % payload["ChangeEventHeader"]["recordIDs"][0]
    query_job = client.query(query)
    records = [dict(row) for row in query_job]
    # Find the changed fields identifed in the payload
    changed_fields = payload["ChangeEventHeader"]["changedFields"]
    changed_field_detected = False
    # For every changed field, update the fetched record with the new entry
    for field in changed_fields:
        # We'll only update the payload if it's different from the current record contents
        if payload[field] != records[0][field.lower()]:
            changed_field_detected = True
            update = {field.lower(): payload[field]}
            records[0].update(update)
    # To avoid duplicate updates, we'll only insert a new rows if something changed
    if changed_field_detected == True:
        #As the retrieved crud_flag could be 'C' or even 'D', we'll also need to update it to 'U'
        update = {"CRUD_flag": "U"}
        records[0].update(update)
        errors = client.insert_rows_json("steadfast-task-363413.staging.repl_orders", records)
        if errors != []:
            print("Encountered errors while inserting rows: {}".format(errors))
