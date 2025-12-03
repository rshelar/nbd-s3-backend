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
                        ┌───────────────────────────┐
                        │        ext4 filesystem    │
                        │   (running inside VM)     │
                        └───────────────┬───────────┘
                                        │
                                   block I/O
                                        │
                        ┌───────────────▼──────────────┐
                        │   Linux NBD kernel client     │
                        └───────────────┬──────────────┘
                                        │  NBD protocol
                                        ▼
                        ┌────────────────────────────────┐
                        │        nbdkit (C daemon)        │
                        │   loads Python plugin below     │
                        └────────────────┬────────────────┘
                                         │ plugin callbacks
                                         ▼
                   ┌────────────────────────────────────────────┐
                   │                nbdkit_plugin.py            │
                   │  • config()                               │
                   │  • open() / close()                        │
                   │  • pread() / pwrite() / flush()            │
                   └────────────────┬────────────────────────────┘
                                    │
                           delegates all I/O
                                    ▼
                     ┌─────────────────────────────────┐
                     │           NbdServer              │
                     │  • read(offset, length)          │
                     │  • write(offset, bytes)          │
                     │  • flush()                       │
                     │  • block ID math                 │
                     │  • dirty block tracking          │
                     └──────────────┬───────────────────┘
                                    │
                ┌───────────────────┴─────────────────────┐
                │                                         │
 ┌────────────────────────────┐             ┌──────────────────────────┐
 │   FileStorage (volatile)   │             │   S3Storage (durable)    │
 │  /data/exports/...         │             │  s3://bucket/exports/... │
 │  • immediate writes        │             │  • updated on flush()    │
 │  • read-through            │             │  • durable persistence   │
 └────────────────────────────┘             └──────────────────────────┘
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

Added in main.py

---

## 📜 License

MIT

## S3 Storage (MinIO)

This project supports an S3-compatible backend using **MinIO**, which runs locally via Docker.

### Start MinIO

Run the container:

```bash
docker run -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  quay.io/minio/minio server /data --console-address ":9001"
```
- API Endpoint: http://localhost:9000
- Web Console: http://localhost:9001
- Username: minioadmin
- Password: minioadmin

### Run S3 Storage Tests

- Start the docker container either in Docker Desktop or ```docker start <container name>```
- ```pytest test_s3_storage.py```

## Running the Block Device via nbdkit

1. Install nbdkit
   On macOs:
   ```commandline
   brew install nbdkit
   ``` 
   On Linux:
   ```commandline
   sudo apt-get install nbdkit nbdkit-python
   ```
2. Run MinIO (if using S3 backend)
   ```commandline
   docker run -p 9000:9000 -p 9001:9001 \
    -e MINIO_ROOT_USER=minioadmin \
    -e MINIO_ROOT_PASSWORD=minioadmin \
    quay.io/minio/minio server /data --console-address ":9001"
   ``` 
3. Run nbdkit with the Python plugin
   ```commandline 
   nbdkit python nbd_server/nbdkit_plugin.py \
    export=testdev \
    size=10485760 \
    volatile_path=data/exports \
    bucket=nbdbucket \
    s3_endpoint=http://localhost:9000 \
    s3_access_key=minioadmin \
    s3_secret_key=minioadmin
   ```
   This starts an NBD server on port 10809 (default).

4. Attach a Linux NBD client (inside VM)
   ```commandline
   sudo nbd-client localhost 10809 /dev/nbd0
   ```
   Format it:
   ```commandline
   sudo mkfs.ext4 /dev/nbd0
   ``` 
   Mount it:
   ```commandline
   sudo mount /dev/nbd0 /mnt
   ```
5. Test writing and flush behavior
   ```commandline
    echo "hello world" | sudo tee /mnt/hello.txt
    sync
   ``` 
   The sync command triggers a flush:
   •	nbdkit sends NBD_CMD_FLUSH
   •	plugin calls NbdServer.flush()
   •	dirty blocks are written to S3Storage

    You should see new block objects appear in MinIO under:
    ```commandline
    exports/testdev/blocks/
    ```
   
6. Unmount and detach
   ```commandline
   sudo umount /mnt
   sudo nbd-client -d /dev/nbd0
   ```
