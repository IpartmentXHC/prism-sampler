#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ctypes
import os
import threading
import time
from pathlib import Path


PR_SET_NAME = 15


def set_comm(value: str) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(PR_SET_NAME, value.encode(), 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "prctl(PR_SET_NAME) failed")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=float, default=45.0)
    parser.add_argument("--file", type=Path, default=Path("/tmp/prism-live-smoke.dat"))
    args = parser.parse_args()

    args.file.write_bytes(b"0" * 4096)
    descriptor = os.open(args.file, os.O_RDWR)
    condition = threading.Condition()
    stop = threading.Event()
    generation = 0

    def producer(index: int) -> None:
        nonlocal generation
        set_comm(f"LiveProducer{index}")
        while not stop.is_set():
            os.pwrite(descriptor, bytes([48 + index]) * 64, index * 64)
            with condition:
                generation += 1
                condition.notify_all()
            time.sleep(0.001)

    def consumer(index: int) -> None:
        seen = 0
        set_comm(f"LiveConsumer{index}")
        while not stop.is_set():
            with condition:
                condition.wait_for(lambda: generation != seen or stop.is_set(), timeout=0.1)
                seen = generation
            os.pread(descriptor, 64, index * 64)

    threads = [
        threading.Thread(target=producer, args=(index,)) for index in range(2)
    ] + [threading.Thread(target=consumer, args=(index,)) for index in range(2)]
    for thread in threads:
        thread.start()
    try:
        time.sleep(args.duration)
    finally:
        stop.set()
        with condition:
            condition.notify_all()
        for thread in threads:
            thread.join()
        os.close(descriptor)


if __name__ == "__main__":
    main()
