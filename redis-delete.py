import redis
import sys
import threading
import time
import json
from datetime import datetime

# Function to read Redis keys from a file in batches
def read_redis_keys(file_path, batch_size=1000):
    with open(file_path, 'r') as file:
        keys = file.read().split(',')
    return [keys[i:i + batch_size] for i in range(0, len(keys), batch_size)]

# Function to delete Redis keys and their derived keys in a separate thread
def delete_redis_keys(keys_batch, redis_host, redis_port, redis_db, sentinel_config, redis_password, output_file_path):
    if sentinel_config:
        sentinel = redis.StrictRedis(
            host=sentinel_config[0],
            port=sentinel_config[1],
            password=redis_password,
            db=redis_db
        )
        master_info = sentinel.sentinel_master(sentinel_config[2])
        master_host, master_port = master_info['ip'], master_info['port']

        r = redis.StrictRedis(
            host=master_host,
            port=master_port,
            password=redis_password,
            db=redis_db
        )
    else:
        r = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db, password=redis_password)

    deleted_keys_data = []

    for key in keys_batch:
        value = r.get(key)
        if value:
            decoded_value = value.decode('utf-8')
            # Append to the backup data
            deleted_keys_data.append({"key": key, "value": decoded_value})
            # Parse the JSON value to find the derived key
            try:
                json_data = json.loads(decoded_value)
                if 'customerId' in json_data and 'brandAccount' in json_data:
                    numeric_customer_id = json_data['customerId'].split(':')[-1]
                    derived_key = f"{json_data['gupshupChannel']}::{numeric_customer_id}::{json_data['brandAccount']}"
                    derived_value = r.get(derived_key)
                    deleted_keys_data.append({"key": derived_key, "value": derived_value.decode('utf-8') if derived_value else None})  # Add derived key and value to backup

                    # Delete the derived key from Redis
                    r.delete(derived_key)
                    print(f"Deleted derived key: {derived_key}")
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON for key {key}: {e}")

            # Delete the original key from Redis
            r.delete(key)
            print(f"Deleted key: {key}")

    # Write backup data to the output file
    with open(output_file_path, 'a') as output_file:
        for entry in deleted_keys_data:
            output_file.write(f"{entry['key']},{entry['value']}\n")

# Function to print progress every 2 seconds
def print_progress(threads):
    while any(t.is_alive() for t in threads):
        print("Progress: {}/{} threads completed".format(sum(not t.is_alive() for t in threads), len(threads)))
        time.sleep(2)

# Main script
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python redis-delete.py <keys_file_path> [redis_host] [redis_port] [redis_db] [sentinel_host] [sentinel_port] [master_name] [redis_password]")
        sys.exit(1)

    # Get file paths and Redis configuration from command line arguments
    keys_file_path = sys.argv[1]

    redis_host = sys.argv[2] if len(sys.argv) > 2 else 'localhost'
    redis_port = int(sys.argv[3]) if len(sys.argv) > 3 else 6379
    redis_db = int(sys.argv[4]) if len(sys.argv) > 4 else 0

    sentinel_config = None
    if len(sys.argv) > 7:
        sentinel_config = [sys.argv[5], int(sys.argv[6]), sys.argv[7]]

    redis_password = sys.argv[8] if len(sys.argv) > 8 else None

    # Generate timestamp for the output file paths
    timestamp = datetime.now().strftime("%d-%m-%Y-%H-%M-%S")
    output_file_path = f"{keys_file_path}-deleted-{timestamp}.txt"

    # Read Redis keys from the file in batches
    key_batches = read_redis_keys(keys_file_path)

    # Start a thread for each key batch
    threads = []
    for batch in key_batches:
        thread = threading.Thread(
            target=delete_redis_keys,
            args=(batch, redis_host, redis_port, redis_db, sentinel_config, redis_password, output_file_path)
        )
        threads.append(thread)
        thread.start()

    # Start a thread to print progress every 2 seconds
    progress_thread = threading.Thread(target=print_progress, args=(threads,))
    progress_thread.start()

    # Wait for all threads to finish
    for thread in threads:
        thread.join()

    # Wait for the progress thread to finish
    progress_thread.join()

    print(f"Deletion completed for keys from {keys_file_path}")
    print(f"Backup data exported to {output_file_path}")
