import os
import tempfile
import time
from io import BytesIO

from azure.storage.blob import BlobPrefix, ContainerClient


class BlobStorageHelper:
    """
    A helper class for interacting with Azure Blob Storage.
    Provides utility methods to list directories, read blobs,
    search for blobs by name, and manipulate blob data.

    Attributes:
        container_client (ContainerClient): The Azure Blob container client.
    """

    def __init__(self, conn_str: str, container: str):
        """
        Initialize the helper with a connection string and container name.

        Args:
            conn_str (str): Azure storage account connection string.
            container (str): Name of the container to interact with.
        """
        self.container_client = ContainerClient.from_connection_string(
            conn_str, container_name=container
        )

    def list_subdirectories(self, folder: str):
        """
        List subdirectories under the given folder.

        Args:
            folder (str): Path prefix within the blob container.

        Returns:
            List[str]: A list of folder-like blob prefixes (ending in '/').
        """
        folder = folder if folder.endswith("/") else folder + "/"
        return [
            item.name
            for item in self.container_client.walk_blobs(
                name_starts_with=folder, delimiter="/"
            )
            if isinstance(item, BlobPrefix)
        ]

    def _get_blob_client(self, path: str):
        """Return a blob client for the given blob path."""
        return self.container_client.get_blob_client(path)

    def read_data(self, path: str, as_text=False):
        """
        Read blob content from the given path.

        Args:
            path (str): Path of the blob to read.
            as_text (bool): If True, decode as UTF-8 text. Otherwise, return raw bytes.

        Returns:
            bytes or str or None: Blob content or None if failed or not found.
        """
        blob = self._get_blob_client(path)
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
        blob = self._get_blob_client(path)
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
            import pyvista as pv
        except ImportError:
            print("PyVista is not installed.")
            return None

        blob = self._get_blob_client(path)
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
        source_blob = self._get_blob_client(source)
        target_blob = self._get_blob_client(target)

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
    def upload_local_file_to_blob(self, local_file_path: str, blob_file_path: str):
        """
        Uploads a file from the local filesystem to the specified blob path in the base container.
        """
        blob_client = self.base_container.get_blob_client(blob_file_path)
        with open(local_file_path, "rb") as f:
            blob_client.upload_blob(f, overwrite=True)
            
    def upload_base_file(self, file_data, blob_file_path):
        """
        Upload file data stream to blob storage
        """
        blob_client = self.base_container.get_blob_client(blob_file_path)
        blob_client.upload_blob(file_data, overwrite=True)

