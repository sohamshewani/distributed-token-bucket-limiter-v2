import ctypes
import mmap
import time
import math
import struct
import threading
from typing import Tuple, Optional, Dict, Any

class TokenBucketState(ctypes.Structure):
    _pack_ = 4
    _fields_ = [
        ("tokens", ctypes.c_double),
        ("last_update_ns", ctypes.c_int64),
        ("capacity", ctypes.c_double),
        ("refill_rate_per_ns", ctypes.c_double),
        ("version", ctypes.c_uint64)
    ]

class SlidingWindowCounters(ctypes.Structure):
    _pack_ = 4
    _fields_ = [
        ("micro_buckets", ctypes.c_uint64 * 16),
        ("window_size_ns", ctypes.c_int64),
        ("current_index", ctypes.c_uint32),
        ("padding", ctypes.c_uint32)
    ]

class SlabAllocator:
    def __init__(self, size_bytes: int):
        self.size_bytes = size_bytes
        self.map_file = mmap.mmap(-1, size_bytes)
        self.lock = threading.Lock()
        self.offset = 0

    def allocate(self, struct_type: ctypes.Structure) -> ctypes.Structure:
        with self.lock:
            size = ctypes.sizeof(struct_type)
            if self.offset + size > self.size_bytes:
                raise MemoryError("Slab allocator out of memory")
            
            instance = struct_type.from_buffer(self.map_file, self.offset)
            self.offset += size
            return instance

    def close(self):
        self.map_file.close()

class RingBufferNode(ctypes.Structure):
    _pack_ = 4
    _fields_ = [
        ("timestamp_ns", ctypes.c_int64),
        ("event_type", ctypes.c_uint32),
        ("value", ctypes.c_uint64)
    ]

class LockFreeRingBuffer:
    def __init__(self, capacity: int = 1024):
        if (capacity & (capacity - 1)) != 0:
            raise ValueError("Capacity must be a power of two")
        self.capacity = capacity
        self.mask = capacity - 1
        self.buffer_size = ctypes.sizeof(RingBufferNode) * capacity
        self.map_file = mmap.mmap(-1, self.buffer_size)
        self.head = ctypes.c_int64(0)
        self.tail = ctypes.c_int64(0)
        self.lock = threading.Lock()

    def push(self, timestamp_ns: int, event_type: int, value: int) -> bool:
        with self.lock:
            h = self.head.value
            t = self.tail.value
            if h - t >= self.capacity:
                return False
            
            idx = h & self.mask
            node_addr = ctypes.addressof(RingBufferNode.from_buffer(self.map_file, idx * ctypes.sizeof(RingBufferNode)))
            node = RingBufferNode.from_address(node_addr)
            node.timestamp_ns = timestamp_ns
            node.event_type = event_type
            node.value = value
            self.head.value = h + 1
            return True

    def pop(self) -> Optional[Tuple[int, int, int]]:
        with self.lock:
            h = self.head.value
            t = self.tail.value
            if t >= h:
                return None
            
            idx = t & self.mask
            node_addr = ctypes.addressof(RingBufferNode.from_buffer(self.map_file, idx * ctypes.sizeof(RingBufferNode)))
            node = RingBufferNode.from_address(node_addr)
            res = (node.timestamp_ns, node.event_type, node.value)
            self.tail.value = t + 1
            return res

    def close(self):
        self.map_file.close()

class DistributedTokenBucketLimiter:
    def __init__(self, capacity: float, refill_rate_per_sec: float, window_size_ms: int = 1000):
        self.capacity = capacity
        self.refill_rate_per_ns = refill_rate_per_sec / 1e9
        self.window_size_ns = window_size_ms * 1_000_000
        self.micro_window_ns = self.window_size_ns // 16

        self.slab = SlabAllocator(ctypes.sizeof(TokenBucketState) + ctypes.sizeof(SlidingWindowCounters) + 4096)
        
        self.bucket = self.slab.allocate(TokenBucketState)
        self.bucket.tokens = capacity
        self.bucket.last_update_ns = time.time_ns()
        self.bucket.capacity = capacity
        self.bucket.refill_rate_per_ns = self.refill_rate_per_ns
        self.bucket.version = 1

        self.windows = self.slab.allocate(SlidingWindowCounters)
        self.windows.window_size_ns = self.window_size_ns
        self.windows.current_index = 0
        for i in range(16):
            self.windows.micro_buckets[i] = 0

        self.ring_buffer = LockFreeRingBuffer(2048)
        self.state_lock = threading.Lock()
        self.clock_skew_ns = 0

    def _adjust_time(self, current_ns: int) -> int:
        return current_ns + self.clock_skew_ns

    def update_clock_skew(self, measured_skew_ns: int):
        with self.state_lock:
            self.clock_skew_ns = int(self.clock_skew_ns * 0.8 + measured_skew_ns * 0.2)

    def acquire(self, tokens_requested: float = 1.0) -> Tuple[bool, Dict[str, Any]]:
        if tokens_requested <= 0:
            raise ValueError("Tokens requested must be greater than zero")

        with self.state_lock:
            now_ns = self._adjust_time(time.time_ns())
            elapsed_ns = now_ns - self.bucket.last_update_ns

            if elapsed_ns > 0:
                generated = elapsed_ns * self.bucket.refill_rate_per_ns
                self.bucket.tokens = min(self.bucket.capacity, self.bucket.tokens + generated)
                self.bucket.last_update_ns = now_ns

            allowed = False
            if self.bucket.tokens >= tokens_requested:
                self.bucket.tokens -= tokens_requested
                self.bucket.version += 1
                allowed = True
                self._record_traffic(now_ns, int(tokens_requested))

            telemetry = {
                "tokens_remaining": self.bucket.tokens,
                "capacity": self.bucket.capacity,
                "version": self.bucket.version,
                "clock_skew_ns": self.clock_skew_ns
            }

            self.ring_buffer.push(now_ns, 1 if allowed else 0, int(tokens_requested))
            return allowed, telemetry

    def _record_traffic(self, now_ns: int, volume: int):
        elapsed_window_ns = now_ns - self.bucket.last_update_ns
        micro_idx = (now_ns // self.micro_window_ns) % 16
        
        if micro_idx != self.windows.current_index:
            diff = (micro_idx - self.windows.current_index) % 16
            if diff >= 16:
                for i in range(16):
                    self.windows.micro_buckets[i] = 0
            else:
                for _ in range(diff):
                    self.windows.current_index = (self.windows.current_index + 1) % 16
                    self.windows.micro_buckets[self.windows.current_index] = 0
            self.windows.current_index = micro_idx

        self.windows.micro_buckets[micro_idx] += volume

    def get_total_traffic_sum(self) -> int:
        with self.state_lock:
            total = 0
            for i in range(16):
                total += self.windows.micro_buckets[i]
            return total

    def reconcile_state(self, remote_bucket: TokenBucketState, remote_windows: SlidingWindowCounters):
        with self.state_lock:
            if remote_bucket.version > self.bucket.version:
                self.bucket.tokens = remote_bucket.tokens
                self.bucket.last_update_ns = remote_bucket.last_update_ns
                self.bucket.capacity = remote_bucket.capacity
                self.bucket.refill_rate_per_ns = remote_bucket.refill_rate_per_ns
                self.bucket.version = remote_bucket.version

            for i in range(16):
                self.windows.micro_buckets[i] = max(self.windows.micro_buckets[i], remote_windows.micro_buckets[i])

    def serialize_rpc_state(self) -> bytes:
        with self.state_lock:
            bucket_bytes = bytes(self.bucket)
            window_bytes = bytes(self.windows)
            return struct.pack(f"=II", len(bucket_bytes), len(window_bytes)) + bucket_bytes + window_bytes

    def deserialize_rpc_state(self, payload: bytes):
        b_len, w_len = struct.unpack_from("=II", payload, 0)
        offset = 8
        remote_bucket_data = payload[offset:offset+b_len]
        offset += b_len
        remote_window_data = payload[offset:offset+w_len]

        remote_bucket = TokenBucketState.from_buffer_copy(remote_bucket_data)
        remote_windows = SlidingWindowCounters.from_buffer_copy(remote_window_data)
        self.reconcile_state(remote_bucket, remote_windows)

    def close(self):
        self.ring_buffer.close()
        self.slab.close()