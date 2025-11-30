# nbd-s3-backend

A cloud-backed Network Block Device (NBD) backend that exposes a virtual block device over NBD and persists each block to S3-compatible object storage (or local filesystem). Designed to demonstrate block-device fundamentals, durable flush semantics, and clean separation between protocol handling and storage layers.

---

## 🚀 Overview

`nbd-s3-backend` implements the storage backend portion of an NBD server.  
The NBD protocol is handled externally by `nbdkit`, while this backend handles:

- Block-level reads, writes, and flushes  
- Addressing via `offset` and `length`  
- Read-through caching and write-back buffering  
- Durable persistence to S3 or local disk  
- Arbitrary export namespaces  
- Configurable block size (default: 4096 bytes)

This makes it possible to format the virtual device with `ext4`, mount it, write files, unmount, and reconnect — with all data preserved across restarts.

---

## 🧩 Architecture

```
ext4 filesystem (inside VM)
        ↓
Linux NBD kernel client
        ↓  (TCP, NBD protocol)
nbdkit NBD server
        ↓  (Python plugin callbacks)
nbd-s3-backend
    ├── Handle.cache    (per-connection read/write cache)
    ├── FileStorage     (local persistent blocks)
    └── S3Storage       (durable object-backed blocks)
```

The backend exposes a **flat block address space**; it has no awareness of files, directories, or artifacts.  
Filesystems like ext4 translate file operations into block offsets.

---

## 📁 Storage Layout

Each export has its own namespace:

```
/data/exports/<export_name>/blocks/<block_id>        # local mode

s3://<bucket>/exports/<export_name>/blocks/<block_id> # S3 mode
```

Each `<block_id>` stores exactly one block of size `BLOCK_SIZE`.

---

## 🛠️ Running Locally (Docker)

Build:

```
docker build -t nbd-s3-backend -f docker/Dockerfile .
```

Run:

```
docker run --privileged -it \
    -p 10809:10809 \
    -v $(pwd)/data:/data \
    nbd-s3-backend
```

Attach a Linux NBD client:

```
sudo nbd-client localhost 10809 -N test_device
sudo mkfs.ext4 /dev/nbd0
sudo mount /dev/nbd0 /mnt
```

Write test data:

```
echo "hello" | sudo tee /mnt/hello.txt
sudo umount /mnt
sudo nbd-client -d /dev/nbd0
```

After restart, reconnect and verify persistence.

---

## 📦 Code Structure

```
plugin/
    nbdkit_replit.py    # main NBD backend (pread, pwrite, flush)
    s3_backend.py       # S3 and local storage implementations
    cache.py            # in-memory read/write cache (LRU)

docker/
    Dockerfile
    run_local.sh

tests/
    basic_write_read.py
```

---

## 🧪 Local Testing Example

```python
from plugin.s3_backend import FileStorage

BLOCK_SIZE = 4096
storage = FileStorage("/tmp/blockstore")

storage.write_block("dev1", 42, b"hello".ljust(BLOCK_SIZE, b"\x00"))
data = storage.read_block("dev1", 42, BLOCK_SIZE)
print(data[:10])  # prints: b'hello\x00\x00...'
```

---

## 📜 License

MIT
