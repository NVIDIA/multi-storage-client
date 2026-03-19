import multistorageclient as msc
from multistorageclient.providers.s3 import S3StorageProvider

def main():
    for url in ["msc://s3-my-bucket/my_file.txt", "s3://my-bucket/my_file.txt"]:
        print(f"Resolving MSC client for {url}...")
        client, _ = msc.resolve_storage_client(url)
        assert isinstance(client._storage_provider, S3StorageProvider)
        print(client._storage_provider._provider_name)
        print(client._storage_provider._endpoint_url)
        print(client._storage_provider._base_path)
        assert client._storage_provider._endpoint_url == "https://my-endpoint.s3.com"
        print()

if __name__ == "__main__":
    main()
