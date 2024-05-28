from time import sleep

from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError, ConnectionFailure

import modules.preferences.preferences as pref

fuse_date = "2024-05-26"
results = [
    [
        "Y2lzY29zcGFyazovL3VzL01FU1NBR0UvOGQ4ZTg3OTAtMWJiYS0xMWVmLTk2OTQtY2RkNmY3YjhhOWY5",
        "test@test.com",
        200,
    ],
    [
        "Y2lzY29zcGFyazovL3VzL01FU1NBR0UvOGQ3ZTBjZDAtMWJiYS0xMWVmLTljZjQtNGY2ZWIyMGJhNWFl",
        "test@test.com",
        200,
    ],
]
operations = []
for result in results:
    print(f"Processing result: {result}")
    # Assuming that result[1] is the email
    alias = result[1].replace("@test.com", "") if len(result) > 1 else None
    try:
        # Attempt to unpack the tuple
        message_id, email, status = result
        print(f"Adding {email} message to reminders database")
        # Add the UpdateOne operation to the list
        operations.append(
            UpdateOne(
                {"date": fuse_date, "alias": alias},
                {
                    "$set": {
                        "message_id": message_id,
                        "email": email,
                        "status": status,
                    }
                },
                upsert=True,
            )
        )
    except (TypeError, ValueError) as te:
        # Log an error message if unpacking fails
        print(f"Error unpacking result for record {alias}: {te} - skipping record")
if operations:
    print(f"Operations to execute: {len(operations)}")
else:
    print("No operations to execute")

# Only perform the bulk write if there are operations to execute
if operations:
    print(f"Operations to execute: {len(operations)}")
    print("Updating reminders database")
    for attempt in range(5):
        try:
            reminder_updates = pref.MONGO_URI[pref.MONGODB]["reminders"].bulk_write(
                operations
            )
            if reminder_updates.upserted_ids:
                print(f"MongoDB upserted {len(reminder_updates.upserted_ids)} records.")
            break  # Exit the retry loop if successful
        except BulkWriteError as bwe:
            print("Bulk Write Error: ", bwe.details)
            sleep_duration = pow(2, attempt)
            print(f"*** Sleeping for {sleep_duration} seconds and trying again ***")
            sleep(sleep_duration)  # Exponential backoff
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            print(f"operations: {operations}")
            break  # Exit the retry loop if an unexpected exception occurs
else:
    print("No operations to execute")
print(operations[0])
