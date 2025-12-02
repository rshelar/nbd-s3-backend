from nbd_server.nbd_server import NbdServer
from nbd_server.s3_storage import S3Storage
from nbd_server.util import BLOCK_SIZE

def test_file_storage():
    backend = NbdServer(export_name="testdev", total_size_bytes=BLOCK_SIZE * 10)
    backend.write(0, b"hello world")
    print(backend.read(0, 11))

def test_s3_bucket_storage():
    storage = S3Storage(
        bucket="nbdbuket",
        export_name="testdev",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )

    data = b"hello" + bytes(BLOCK_SIZE - 5)
    storage.write_block("testdev", 0, data)
    out = storage.read_block("testdev", 0)
    print(out[:10])  # b'hello\x00\x00\x00\x00\x00'
    print(len(out))  # 4096

if __name__ == "__main__":
    test_s3_bucket_storage()
