from watchdog.events import PatternMatchingEventHandler
#The next library will contain the code that will be fired
#when a CDC Event message is received
from pop_staging_tables import *

def create_watchdog(client):
    #This covers what happens when a new event message is detected
    def on_created(event):
        #Shred the json message and put into staging table
        process_json_message(client, event.src_path)

    #This provides a simple event handler
    if __name__ == "create_watchdog":
        patterns = ["*.json"]
        ignore_patterns = None
        ignore_directories = False
        case_sensitive = True
        my_event_handler = PatternMatchingEventHandler(patterns, ignore_patterns, ignore_directories, case_sensitive)
        my_event_handler.on_created = on_created
        return my_event_handler

