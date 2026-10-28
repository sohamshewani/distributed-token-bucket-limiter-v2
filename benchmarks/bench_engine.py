import time
import statistics
import threading
from typing import List, Protocol


class RateLimiter(Protocol):
    def acquire(self) -> bool:
        ...


class MockDistributedTokenBucket:
    """A simulated distributed token bucket limiter for benchmarking purposes."""
    def __init__(self, capacity: int = 1000000, refill_rate: float = 1000000.0):
        self.capacity = capacity
        self.tokens = float(capacity)
        self.refill_rate = refill_rate
        self.last_refill = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self) -> bool:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_refill
            self.last_refill = now
            
            # Refill tokens
            self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
            
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return True
            return False


def run_benchmark(limiter: RateLimiter, iterations: int, concurrency: int) -> Tuple[float, float, float]:
    latencies: List[float] = []
    latencies_lock = threading.Lock()

    def worker(n_ops: int):
        local_latencies = []
        for _ in range(n_ops):
            start = time.perf_counter_ns()
            limiter.acquire()
            end = time.perf_counter_ns()
            local_latencies.append((end - start) / 1_000_000.0)  # Convert to ms

        with latencies_lock:
            latencies.extend(local_latencies)

    ops_per_thread = iterations // concurrency
    threads = [
        threading.Thread(target=worker, args=(ops_per_thread,))
        for _ in range(concurrency)
    ]

    # Warmup
    limiter.acquire()

    start_time = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    end_time = time.perf_counter()

    total_time = end_time - start_time
    total_ops = ops_per_thread * concurrency
    ops_per_sec = total_ops / total_time

    # Calculate percentiles
    sorted_latencies = sorted(latencies)
    p50 = statistics.median(sorted_latencies)
    
    p99_index = int(len(sorted_latencies) * 0.99)
    p99 = sorted_latencies[min(p99_index, len(sorted_latencies) - 1)]

    return ops_per_sec, p50, p99


def main():
    iterations = 200_000
    concurrency_levels = [1, 4, 8]

    print("=" * 72)
    print(f"{'Benchmark':<35} | {'Ops/Sec':<12} | {'P50 (ms)':<10} | {'P99 (ms)':<10}")
    print("-" * 72)

    for concurrency in concurrency_levels:
        limiter = MockDistributedTokenBucket()
        ops_sec, p50, p99 = run_benchmark(limiter, iterations, concurrency)
        
        name = f"TokenBucket (Threads: {concurrency})"
        print(f"{name:<35} | {ops_sec:<12.2f} | {p50:<10.4f} | {p99:<10.4f}")

    print("=" * 72)


if __name__ == "__main__":
    from typing import Tuple
    main()