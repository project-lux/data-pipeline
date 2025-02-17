import concurrent.futures
import time
import random
# Create dummy data
DATA_SIZE = 1000
dummy_data = {str(i): {'data': random.randint(1000, 10000)} for i in range(DATA_SIZE)}
def is_prime(n):
    """CPU-intensive function to check if a number is prime"""
    if n < 2:
        return False
    for i in range(2, int(n ** 0.5) + 1):
        if n % i == 0:
            return False
    return True
def process_chunk(chunk_data, thread_id):
    """CPU-intensive processing - find all primes up to each number"""
    results = []
    for k, v in chunk_data.items():
        number = v['data']
        # Find all primes up to this number
        primes = sum(1 for i in range(2, number) if is_prime(i))
        results.append({k: primes})
    return results
def run_comparison():
    data_items = list(dummy_data.items())
    
    # Thread-based processing
    print("\nTesting with 4 threads...")
    thread_start = time.time()
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for i in range(4):
            chunk = dict(data_items[i::4])
            futures.append(executor.submit(process_chunk, chunk, i))
        for future in concurrent.futures.as_completed(futures):
            results.extend(future.result())
    thread_time = time.time() - thread_start
    print(f"Threading time: {thread_time:.2f}s")
    # CPU-based processing
    print("\nTesting with 4 CPU processes...")
    cpu_start = time.time()
    results = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=4) as executor:
        futures = []
        for i in range(4):
            chunk = dict(data_items[i::4])
            futures.append(executor.submit(process_chunk, chunk, i))
        for future in concurrent.futures.as_completed(futures):
            results.extend(future.result())
    cpu_time = time.time() - cpu_start
    print(f"CPU time: {cpu_time:.2f}s")
    # Sequential processing
    print("\nTesting with sequential processing...")
    seq_start = time.time()
    results = process_chunk(dummy_data, 0)
    seq_time = time.time() - seq_start
    print(f"Sequential time: {seq_time:.2f}s")
if __name__ == '__main__':
    run_comparison()
