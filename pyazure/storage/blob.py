
import os
import tempfile
import time
from io import BytesIO

from azure.storage.blob import BlobPrefix, ContainerClient, generate_blob_sas, BlobSasPermissions


class BlobStorageHelper:
    """
    A helper class for interacting with Azure Blob Storage.
    Provides utility methods to list directories, read blobs,
    search for blobs by name, and manipulate blob data.

    Attributes:
        container_client (ContainerClient): The Azure Blob container client.
    """

    def __init__(
        self,
        conn_str: str | None = None,
        container: str | None = None,
        sas_url: str | None = None
    ) -> None:
        """
        Initialize the helper with either a connection string and container name, or a SAS token URL.

        Args:
            conn_str (str, optional): Azure storage account connection string.
            container (str, optional): Name of the container to interact with.
            sas_url (str, optional): SAS token URL for the container.
        """
        self.created_with_connection_string = None
        self.created_with_sas_token = None
        if sas_url:
            self.created_with_sas_token = True
            self.sas_url = sas_url
            self.container_client = ContainerClient.from_container_url(sas_url)
        elif conn_str and container:
            self.created_with_connection_string = True  # some functions can only work with container created with connection string
            self.conn_str = conn_str
            self.container_name = container
            self.container_client = ContainerClient.from_connection_string(
                conn_str, container_name=container
            )
        else:
            raise ValueError("Must provide either sas_url or both conn_str and container.")
    def list_blobs(self, prefix: str = ""):
        """
        List all blobs in the container recursively, optionally filtered by a prefix.

        Args:
            prefix (str, optional): Path prefix within the blob container. Defaults to "".
        Returns:
            List[str]: A list of blob names.
        """
        return [blob.name for blob in self.container_client.list_blobs(name_starts_with=prefix)]

    def list_subdirectories(self, folder: str = "."):
        """
        List subdirectories under the given folder.
        
        Args:
            folder (str): Path prefix within the blob container. 
                        Use "." to list folders at the container root.

        Returns:
            List[str]: A list of folder-like blob prefixes (ending in '/').
        """

        # Interpret "." or empty string as root
        if folder in (".", "", None):
            prefix = ""      # no prefix → search from base
        else:
            prefix = folder if folder.endswith("/") else folder + "/"

        return [
            item.name
            for item in self.container_client.walk_blobs(
                name_starts_with=prefix, delimiter="/"
            )
            if isinstance(item, BlobPrefix)
        ]

    def get_blob_client(self, path: str):
        """Return a blob client for the given blob path."""
        return self.container_client.get_blob_client(path)

    def download_blob_to_local(self, blob_path: str, local_file_path: str, binary: bool = True):
        """
        Download a blob to a local file.

        Args:
            blob_path (str): Path of the blob to download.
            local_file_path (str): Local file path to save the downloaded blob.
            binary (bool): If True, download the blob as binary. If False, download as text.
        """
        blob = self.get_blob_client(blob_path)
        if blob.exists():
            with open(local_file_path, "wb" if binary else "w") as file:
                try:
                    file.write(blob.download_blob().readall())
                except Exception as e:
                    print(f"Error downloading blob: {e}")
        else:
            print("Provided blob path doesn't exist.")

    def read_data(self, path: str, as_text=False):
        """
        Read blob content from the given path.

        Args:
            path (str): Path of the blob to read.
            as_text (bool): If True, decode as UTF-8 text. Otherwise, return raw bytes.

        Returns:
            bytes or str or None: Blob content or None if failed or not found.
        """
        blob = self.get_blob_client(path)
        if blob.exists():
            try:
                return (
                    blob.download_blob(encoding="utf-8").readall()
                    if as_text
                    else blob.download_blob().readall()
                )
            except Exception as e:
                print(f"Error reading blob: {e}")
        else:
            print("Provided path doesn't exist.")

    def read_data_to_memory(self, path: str):
        """
        Read blob content into an in-memory BytesIO stream.

        Args:
            path (str): Path of the blob.

        Returns:
            BytesIO or None: In-memory stream of blob content.
        """
        blob = self.get_blob_client(path)
        if blob.exists():
            stream = BytesIO()
            try:
                blob.download_blob().readinto(stream)
                stream.seek(0)
                return stream
            except Exception as e:
                print(f"Error reading blob to memory: {e}")
        else:
            print("Provided path doesn't exist.")

    def read_vtk_data(self, path: str):
        """
        Read VTK-compatible file using PyVista from blob storage.

        Args:
            path (str): Path to the VTK-compatible file in blob storage.

        Returns:
            pyvista.DataSet or None: PyVista dataset if successful, otherwise None.
        """
        _vtk_extensions = {
            ".vtk",
            ".vtu",
            ".vtp",
            ".vti",
            ".vtr",
            ".vts",
            ".pvtu",
            ".pvtp",
            ".pvti",
            ".pvtr",
            ".pvts",
            ".ply",
            ".stl",
            ".obj",
            ".glb",
            ".gltf",
            ".3ds",
            ".xmf",
            ".xdmf",
            ".mesh",
            ".case",
            ".cas",
            ".ensight",
            ".exo",
            ".mhd",
            ".dem",
        }
        ext = os.path.splitext(path)[-1].lower()
        if ext not in _vtk_extensions:
            print("File extension not supported by PyVista.")
            return None

        try:
            import pyvista as pv # type: ignore
        except ImportError:
            print("PyVista is not installed.")
            return None

        blob = self.get_blob_client(path)
        if blob.exists():
            try:
                data = blob.download_blob().readall()
                with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as tmp:
                    tmp.write(data)
                    tmp_path = tmp.name
                mesh = pv.read(tmp_path)
                os.remove(tmp_path)
                return mesh
            except Exception as e:
                print(f"Error reading VTK data: {e}")
        else:
            print("Provided path doesn't exist.")

    def rename_blob(self, source: str, target: str):
        """
        Rename a blob by copying it to a new path and deleting the original.

        Args:
            source (str): Source blob path.
            target (str): Target blob path.
        """
        source_blob = self.get_blob_client(source)
        target_blob = self.get_blob_client(target)

        target_blob.start_copy_from_url(source_blob.url)
        while target_blob.get_blob_properties().copy.status == "pending":
            time.sleep(0.5)

        if target_blob.get_blob_properties().copy.status == "success":
            source_blob.delete_blob()

    def search_path_by_name(self, keyword: str, path: str = ""):
        """
        Search for blobs containing a keyword in their name.

        Args:
            keyword (str): Substring to search for in blob names.
            path (str, optional): If provided, limits search to blobs under this prefix.

        Returns:
            List[str]: List of matching blob paths.
        """
        prefix = path.rstrip("/") + "/" if path else ""
        return [
            blob.name
            for blob in self.container_client.list_blobs(name_starts_with=prefix)
            if keyword in blob.name
        ]
    
    def upload_local_file_to_blob(self, local_file_path: str, blob_file_path: str, overwrite: bool = True):
        """
        Uploads a file from the local filesystem to the specified blob path in the container.
        """
        blob_client = self.get_blob_client(blob_file_path)
        with open(local_file_path, "rb") as f:
            blob_client.upload_blob(f, overwrite=overwrite)

    def upload_stream_to_blob(self, file_data, blob_file_path, overwrite: bool = True):
        """
        Uploads a file-like object to the specified blob path in the base container.
        Args:
            file_data: A file-like object (e.g., BytesIO, file handle).
            blob_file_path (str): The destination path for the blob in the container.
            overwrite (bool): Whether to overwrite the blob if it already exists. Default is True.
        Example:
            with open("local_file.txt", "rb") as f:
                upload_stream_to_blob(f, "path/in/container/blob.txt")
        """
        blob_client = self.get_blob_client(blob_file_path)
        blob_client.upload_blob(file_data, overwrite=overwrite)
        
    def copy_blob_to_path(self, source_blob_client, target_blob_path):
        """
        Copies a blob to a new path in the container using a server-side copy.

        Args:
            source_blob_client: The BlobClient instance of the source blob.
            target_blob_path (str): The destination path for the new blob.
        """
        target_blob_client = self.get_blob_client(target_blob_path)
        target_blob_client.start_copy_from_url(source_blob_client.url)

    def generate_blob_sas_url(self, blob_path, expiry_hours=24):
        """
        Generate a SAS token for a blob given its path in the container, only if the blob exists.

        Args:
            blob_path (str): Path of the blob in the container.
            expiry_hours (int): Expiry time in hours.

        Returns:
            str: SAS URL for the blob, or None if blob does not exist.
        """
        from datetime import datetime, timedelta
        if self.created_with_connection_string:
            blob_client = self.get_blob_client(blob_path)
            if not blob_client.exists():
                print(f"Blob '{blob_path}' does not exist.")
                return None
            
            if self.container_client.account_name is None or self.container_client.credential.account_key is None:
                print("Account name or key is not set, cannot generate SAS token.")
                return None

            sas_token = generate_blob_sas(
                account_name=self.container_client.account_name,
                container_name=self.container_client.container_name,
                blob_name=blob_path,
                account_key=self.container_client.credential.account_key,
                permission=BlobSasPermissions(read=True, write=True, delete=True),
                expiry=datetime.utcnow() + timedelta(hours=expiry_hours),
            )
            sas_url = f"https://{self.container_client.account_name}.blob.core.windows.net/{self.container_client.container_name}/{blob_path}?{sas_token}"
        elif self.created_with_sas_token:
            base_url, _, sas_token = self.sas_url.partition('?')
            base_url = base_url.rstrip('/')
            sas_url = f"{base_url}/{blob_path}?{sas_token}"
        else:
            print("Container was not created with a connection string or SAS token, cannot generate SAS token.")
            return None

        return sas_url

    def delete_blob(self, blob_path, force=False):
        """
        Delete a blob from the container, with optional confirmation prompt.

        Args:
            blob_path (str): Path of the blob in the container.
            force (bool): If True, delete without prompt. If False, prompt user for confirmation. Default is False.

        Returns:
            bool: True if deleted, False if blob does not exist or user cancels.
        """
        blob_client = self.get_blob_client(blob_path)
        if not blob_client.exists():
            print(f"Blob '{blob_path}' does not exist.")
            return False

        if not force:
            confirm = input(f"Are you sure you want to delete blob '{blob_path}'? (y/N): ").strip().lower()
            if confirm != 'y':
                print("Deletion cancelled.")
                return False

        blob_client.delete_blob()
        print(f"Blob '{blob_path}' deleted.")
        return True

    def delete_directory(self, directory_path, verbose=False):
        container_client = self.container_client

        # Normalize path forms
        if directory_path.endswith('/'):
            dir_prefix = directory_path
            dir_name_no_slash = directory_path.rstrip('/')
        else:
            dir_prefix = directory_path + '/'
            dir_name_no_slash = directory_path

        # Delete all blobs under the directory
        if verbose:
            print(f"Deleting all blobs under '{dir_prefix}'...")

        for blob_ in container_client.list_blobs(name_starts_with=dir_prefix):
            if len(list(container_client.list_blobs(name_starts_with=blob_.name))) > 1:
                if verbose:
                    print(f"Skipping non-empty directory: {blob_.name}")
                continue
            # delete real blobs and empty directories
            try:
                container_client.delete_blob(blob_.name)
                if verbose:
                    print(f"Deleted: {blob_.name}")
            except Exception as e:
                if verbose:
                    print(f"Could not delete {blob_.name}: {e}")

        if directory_path.endswith('/'):
            dir_prefix = directory_path
            dir_name_no_slash = directory_path.rstrip('/')
        else:
            dir_prefix = directory_path + '/'
            dir_name_no_slash = directory_path

        if len(list(container_client.list_blobs(name_starts_with=dir_prefix))) > 1:
            self.delete_directory(dir_prefix)
        
        # Delete the directory marker blob (if any)
        if verbose:
            print(f"Checking for directory marker for '{directory_path}'...")
        marker_candidates = list(container_client.list_blobs(name_starts_with=dir_name_no_slash))
        for blob in marker_candidates:
            # The marker blob is usually the directory name itself, optionally with trailing slash
            if blob.name.rstrip('/') == dir_name_no_slash:
                try:
                    container_client.delete_blob(blob.name)
                    if verbose:
                        print(f"Deleted directory marker: '{blob.name}'")
                except Exception as e:
                    if verbose:
                        print(f"Could not delete directory marker {blob.name}: {e}")
                break
        else:
            if verbose:
                print(f"No directory marker found for: {directory_path}")