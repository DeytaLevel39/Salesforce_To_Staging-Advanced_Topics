def update_customer(payload,client):
    # Fetch back the latest record from staging that needs to be updated
    query = """
    select customer_id, customer_number, first_name, last_name, 
    date(lastmodifieddate)||'T'||time(lastmodifieddate) as lastmodifieddate, 
    date(createddate)||'T'||time(createddate) as createddate, CRUD_flag
    from (select *,row_number() over(partition by customer_number order by lastmodifieddate desc) as rn from staging.customers)
    where rn = 1
    and customer_id='%s'""" % payload["ChangeEventHeader"]["recordIDs"][0]
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
        print("An existing record has been updated")
        errors = client.insert_rows_json("steadfast-task-363413.staging.customers", records)
        if errors != []:
            print("Encountered errors while inserting rows: {}".format(errors))
