import redis
import sys
import threading
import time
from datetime import datetime

# Function to read Redis keys from a file in batches
def read_redis_keys(file_path, batch_size=1000):
    with open(file_path, 'r') as file:
        keys = file.read().split(',')
    return [keys[i:i + batch_size] for i in range(0, len(keys), batch_size)]

# Function to fetch Redis values for each key in a separate thread
def fetch_redis_values(keys_batch, redis_host, redis_port, redis_db, sentinel_config, redis_password, csv_file_path):
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

    with open(csv_file_path, 'a') as file:
        for key in keys_batch:
            value = r.get(key)
            # Modify the format without quotes
            formatted_value = value.decode('utf-8') if value else ''
            file.write(f"{key},{formatted_value}\n")

# Function to print progress every 2 seconds
def print_progress(threads):
    while any(t.is_alive() for t in threads):
        print("Progress: {}/{} threads completed".format(sum(not t.is_alive() for t in threads), len(threads)))
        time.sleep(2)

# Main script
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python script_name.py <keys_file_path> [redis_host] [redis_port] [redis_db] [sentinel_host] [sentinel_port] [master_name] [redis_password]")
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

    # Generate timestamp for the CSV file path
    timestamp = datetime.now().strftime("%d-%m-%Y-%H-%M-%S")
    csv_file_path = f"{keys_file_path}-{timestamp}.csv"

    # Read Redis keys from the file in batches
    key_batches = read_redis_keys(keys_file_path)

    # Start a thread for each key batch
    threads = []
    for batch in key_batches:
        thread = threading.Thread(
            target=fetch_redis_values,
            args=(batch, redis_host, redis_port, redis_db, sentinel_config, redis_password, csv_file_path)
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

    print(f"Data export completed to {csv_file_path}")
