import minio
import boto3
from nbd_server.nbd_server import NbdServer
from nbd_server.util import BLOCK_SIZE

if __name__ == "__main__":
    backend = NbdServer(export_name="testdev", total_size_bytes=BLOCK_SIZE * 10)
    backend.write(0, b"hello world")
    print(backend.read(0, 11))  # should print: b'hello world'