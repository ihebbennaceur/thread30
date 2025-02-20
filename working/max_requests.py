import threading
import time
import requests
import statistics

# Configuration
URL = "https://example.com"  # URL à tester
INCREMENTS = 10  # Incrément du nombre de requêtes simultanées
MAX_THREADS = 50  # Nombre maximum de threads à tester
TEST_RUNS = 3  # Nombre de fois pour exécuter chaque test pour obtenir une moyenne

def make_request():
    try:
        response = requests.get(URL)
        return response.status_code
    except Exception as e:
        print(f"Request failed: {e}")
        return None

def test_concurrent_requests(num_threads):
    threads = []
    start_time = time.time()

    for _ in range(num_threads):
        thread = threading.Thread(target=make_request)
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    end_time = time.time()
    return end_time - start_time

def main():
    results = {}
    for num_threads in range(INCREMENTS, MAX_THREADS + 1, INCREMENTS):
        print(f"Testing with {num_threads} threads")
        times = []
        for _ in range(TEST_RUNS):
            elapsed_time = test_concurrent_requests(num_threads)
            times.append(elapsed_time)
            time.sleep(5)  # Pause entre les tests pour éviter la surcharge
        avg_time = statistics.mean(times)
        results[num_threads] = avg_time
        print(f"Average time for {num_threads} threads: {avg_time:.2f} seconds")

    # Afficher les résultats
    print("\nTest Results:")
    for num_threads, avg_time in results.items():
        print(f"{num_threads} threads: {avg_time:.2f} seconds")

if __name__ == "__main__":
    main()