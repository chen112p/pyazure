# pyazure
Version 0.1.0
need to add more useful stuff


# Install
## pip
pip install git+ssh://git@github.com/chen112p/pyazure.git
## uv
uv pip install git+ssh://git@github.com/chen112p/pyazure.git
### add to pyproject.toml
dependencies = [
    "azure-storage-blob>=12.26.0",
    "pyazure @ git+ssh://git@github.com/chen112p/pyazure.git",
]
