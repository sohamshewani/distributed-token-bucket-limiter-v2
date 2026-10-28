import time
import math
import threading
import pytest
from dataclasses import dataclass
from typing import Dict, Any, Optional

# ==========================================
# IMPLEMENTATION (Distributed Token Bucket)
# ==========================================

@dataclass
class BucketState:
    tokens: float
    last_updated: float
    version: int

class InMemoryStore:
    """Thread-safe simulated distributed store with optional network latency."""
    def __init__(self, latency_sec: float = 0.0):
        self._store: Dict[str, BucketState] = {}
        self._lock = threading.Lock()
        self.latency_sec = latency_sec
        self.failure_mode = False

    def get(self, key: str) -> Optional[BucketState]:
        if self.failure_mode:
            raise ConnectionError("Simulated network failure")
        if self.latency_sec > 0:
            time.sleep(self.latency_sec)
        with self._lock:
            state = self._store.get(key)
            if state is None:
                return None
            return BucketState(tokens=state.tokens, last_updated=state.last_updated, version=state.version)

    def set_if_match(self, key: str, expected_version: int, state: BucketState) -> bool:
        if self.failure_mode:
            raise ConnectionError("Simulated network failure")
        if self.latency_sec > 0:
            time.sleep(self.latency_sec)
        with self._lock:
            current = self._store.get(key)
            if current is None:
                if expected_version != 0:
                    return False
                self._store[key] = BucketState(tokens=state.tokens, last_updated=state.last_updated, version=1)
                return True
            
            if current.version != expected_version:
                return False
            
            self._store[key] = BucketState(tokens=state.tokens, last_updated=state.last_updated, version=current.version + 1)
            return True

    def force_set(self, key: str, state: BucketState):
        with self._lock:
            self._store[key] = state

class DistributedTokenBucketLimiter:
    def __init__(self, store: InMemoryStore, capacity: float, refill_rate_per_sec: float, key: str, max_retries: int = 10):
        if capacity <= 0:
            raise ValueError("Capacity must be positive")
        if refill_rate_per_sec <= 0:
            raise ValueError("Refill rate must be positive")
        
        self.store = store
        self.capacity = float(capacity)
        self.refill_rate = float(refill_rate_per_sec)
        self.key = key
        self.max_retries = max_retries

    def _time(self) -> float:
        return time.time()

    def consume(self, tokens: float = 1.0) -> bool:
        if tokens <= 0:
            return True
        if tokens > self.capacity:
            return False

        for _ in range(self.max_retries):
            now = self._time()
            state = self.store.get(self.key)

            if state is None:
                # Initialize bucket full, minus consumed tokens
                new_tokens = self.capacity - tokens
                if new_tokens < 0:
                    return False
                new_state = BucketState(tokens=new_tokens, last_updated=now, version=0)
                if self.store.set_if_match(self.key, 0, new_state):
                    return True
                continue

            # Calculate token refill
            delta = max(0.0, now - state.last_updated)
            refilled_tokens = delta * self.refill_rate
            current_tokens = min(self.capacity, state.tokens + refilled_tokens)

            if current_tokens < tokens:
                return False  # Insufficient tokens, no need to retry write if strictly limited by current state

            new_tokens = current_tokens - tokens
            new_state = BucketState(tokens=new_tokens, last_updated=now, version=state.version)

            if self.store.set_if_match(self.key, state.version, new_state):
                return True

        raise RuntimeError("Exceeded maximum retries due to high concurrency contention")

    def get_current_tokens(self) -> float:
        now = self._time()
        state = self.store.get(self.key)
        if state is None:
            return self.capacity
        delta = max(0.0, now - state.last_updated)
        return min(self.capacity, state.tokens + (delta * self.refill_rate))


# ==========================================
# TEST SUITE
# ==========================================

@pytest.fixture
def store():
    return InMemoryStore()

@pytest.fixture
def limiter(store):
    return DistributedTokenBucketLimiter(store=store, capacity=10.0, refill_rate_per_sec=5.0, key="test_bucket")

# ------------------------------------------
# 1. UNIT & BOUNDARY TESTS
# ------------------------------------------

def test_initialization_boundaries(store):
    with pytest.raises(ValueError):
        DistributedTokenBucketLimiter(store, capacity=0, refill_rate_per_sec=5, key="b1")
    with pytest.raises(ValueError):
        DistributedTokenBucketLimiter(store, capacity=-10, refill_rate_per_sec=5, key="b2")
    with pytest.raises(ValueError):
        DistributedTokenBucketLimiter(store, capacity=10, refill_rate_per_sec=0, key="b3")
    with pytest.raises(ValueError):
        DistributedTokenBucketLimiter(store, capacity=10, refill_rate_per_sec=-1, key="b4")

def test_zero_and_negative_consumption(limiter):
    assert limiter.consume(0.0) is True
    assert limiter.consume(-5.0) is True
    assert limiter.get_current_tokens() == 10.0

def test_oversized_consumption(limiter):
    # Requesting more than total capacity should immediately fail
    assert limiter.consume(15.0) is False
    assert limiter.get_current_tokens() == 10.0

def test_sequential_consumption_and_refill(limiter, store):
    # Consume all tokens
    for _ in range(10):
        assert limiter.consume(1.0) is True
    
    assert limiter.consume(1.0) is False
    assert limiter.get_current_tokens() < 1.0

    # Simulate time passing by manually shifting last_updated backwards
    state = store.get("test_bucket")
    store.force_set("test_bucket", BucketState(
        tokens=state.tokens,
        last_updated=state.last_updated - 2.0, # 2 seconds passed -> +10 tokens capacity clamped to 10
        version=state.version
    ))

    assert limiter.get_current_tokens() == 10.0
    assert limiter.consume(10.0) is True


# ------------------------------------------
# 2. INVARIANT ASSERTIONS
# ------------------------------------------

def test_invariant_tokens_never_exceed_capacity(limiter, store):
    # Consume some
    limiter.consume(5.0)
    
    # Force time forward by a massive amount (1000 seconds)
    state = store.get("test_bucket")
    store.force_set("test_bucket", BucketState(
        tokens=state.tokens,
        last_updated=state.last_updated - 1000.0,
        version=state.version
    ))

    # Tokens must be capped at capacity
    assert limiter.get_current_tokens() == limiter.capacity
    assert limiter.consume(10.0) is True
    assert limiter.get_current_tokens() == 0.0

def test_invariant_tokens_never_negative(limiter, store):
    for _ in range(20):
        limiter.consume(1.0)
    
    state = store.get("test_bucket")
    if state:
        assert state.tokens >= 0.0


# ------------------------------------------
# 3. CONCURRENT STRESS & RACE-CONDITION TESTS
# ------------------------------------------

def test_concurrent_heavy_contention(store):
    # Capacity 100, Refill 10/s. 50 threads trying to consume 5 tokens each concurrently.
    # Total requested: 250 tokens. Only 100 should succeed.
    limiter = DistributedTokenBucketLimiter(store=store, capacity=100.0, refill_rate_per_sec=10.0, key="stress_bucket", max_retries=100)
    
    num_threads = 50
    tokens_per_thread = 5.0
    results = []

    def worker():
        try:
            res = limiter.consume(tokens_per_thread)
            results.append(res)
        except Exception as e:
            results.append(e)

    threads = [threading.Thread(target=worker) for _ in range(num_threads)]
    
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    successes = sum(1 for r in results if r is True)
    failures = sum(1 for r in results if r is False)
    exceptions = [r for r in results if isinstance(r, Exception)]

    assert not exceptions, f"Encountered unexpected exceptions: {exceptions}"
    # Exactly 20 threads should succeed (20 * 5 = 100 capacity)
    assert successes == 20
    assert failures == 30
    assert limiter.get_current_tokens() == pytest.approx(0.0, abs=1e-2)


# ------------------------------------------
# 4. FAILURE AND RECOVERY VERIFICATION
# ------------------------------------------

def test_network_failure_and_recovery(store):
    limiter = DistributedTokenBucketLimiter(store=store, capacity=10.0, refill_rate_per_sec=1.0, key="fail_bucket")
    
    # Normal operation
    assert limiter.consume(2.0) is True
    
    # Trigger network failure
    store.failure_mode = True
    
    with pytest.raises(ConnectionError):
        limiter.consume(1.0)

    with pytest.raises(ConnectionError):
        limiter.get_current_tokens()

    # Recover network
    store.failure_mode = False

    # Should resume normal operations seamlessly
    assert limiter.consume(3.0) is True
    assert limiter.get_current_tokens() <= 5.0