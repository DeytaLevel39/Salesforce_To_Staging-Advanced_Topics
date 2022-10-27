from datetime import datetime
def update_customer(query,payload,client,country_code, update_lastmodified):
    query_job = client.query(query)
    records = [dict(row) for row in query_job]
    for record in records:
        # Find the changed fields identifed in the payload
        changed_fields = payload["ChangeEventHeader"]["changedFields"]
        changed_field_detected = False
        # For every changed field, update the fetched record with the new entry
        for field in changed_fields:
            #Ignore update to LastModifiedDate if we are cleaning records after late arriving record
            if (field == "LastModifiedDate" and update_lastmodified == True) or field != "LastModifiedDate":
                # We'll only update the payload if it's different from the current record contents
                if payload[field] != record[field.lower()]:
                    changed_field_detected = True
                    update = {field.lower(): payload[field]}
                    record.update(update)
        # To avoid duplicate updates, we'll only insert a new row if something changed
        if changed_field_detected == True:
            #As the retrieved crud_flag could be 'C', we'll also need to update it to 'U'
            update = {"CRUD_flag": "U"}
            record.update(update)
            table_name = "steadfast-task-363413.staging.repl_%s_customers"%country_code
            errors = client.insert_rows_json(table_name,[record])
            if errors != []:
                print("Encountered errors while inserting rows: {}".format(errors))
        else:
            print("Duplicate UPDATE Event Message detected")

