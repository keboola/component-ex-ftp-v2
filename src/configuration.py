import logging
from enum import Enum

from keboola.component.exceptions import UserException
from pydantic import BaseModel, Field, ValidationError, field_validator, computed_field


class Protocol(str, Enum):
    FTP = "ftp"
    EX_FTPS = "ex-ftps"
    IM_FTPS = "im-ftps"
    SFTP = "sftp"


class Mode(str, Enum):
    FILE = "file"
    TABLE = "table"


class LoadType(str, Enum):
    FULL_LOAD = "full_load"
    INCREMENTAL_LOAD = "incremental_load"


class DataSelectionMode(str, Enum):
    ALL_DATA = "all_data"
    SELECTED_COLUMNS = "selected_columns"


class Source(BaseModel):
    namespace: str = ""
    table_name: str = ""
    snapshot_id: int | None = None


class DataSelection(BaseModel):
    mode: DataSelectionMode = Field(default=DataSelectionMode.ALL_DATA)
    columns: list[str] = Field(default_factory=list)


class Destination(BaseModel):
    preserve_insertion_order: bool = True
    parquet_output: bool = False
    file_name: str = ""
    table_name: str = ""
    load_type: LoadType = Field(default=LoadType.INCREMENTAL_LOAD)
    primary_key: list[str] = Field(default_factory=list)
    columns: list[str] = Field(default_factory=list)

    @computed_field
    @property
    def incremental(self) -> bool:
        return self.load_type == LoadType.INCREMENTAL_LOAD


class SSH(BaseModel):
    keys: dict = Field(default_factory=dict)
    disabled_algorithms: dict = Field(default_factory=dict)
    banner_timeout: int = 120

    @property
    def private_key(self) -> str:
        return self.keys.get("#private", "")


class Connection(BaseModel):
    protocol: Protocol = Protocol.SFTP
    hostname: str
    port: int = 22
    user: str
    password: str = Field(default="", alias="#pass")
    passphrase: str = Field(default="", alias="#passphrase")
    ssh: SSH = Field(default_factory=SSH)
    passive_mode: bool = True
    connection_timeout: int = 30
    max_retries: int = 2
    base_path: str = ""  # Optional base directory to change to after connecting (SFTP only)

    @field_validator("port")
    @classmethod
    def set_default_port(cls, v, info):
        if v is None or v == 0:
            protocol = info.data.get("protocol", Protocol.SFTP)
            if protocol == Protocol.SFTP:
                return 22
            elif protocol in [Protocol.FTP, Protocol.EX_FTPS]:
                return 21
            elif protocol == Protocol.IM_FTPS:
                return 990
        return v


class Configuration(BaseModel):
    connection: Connection
    mode: Mode = Field(default=Mode.FILE)
    files: list[str] = Field(default_factory=list)
    table_file: str = ""
    include_path_in_filename: bool = False
    append_timestamp: bool = False
    incremental_mode: bool = False
    tags: list[str] = Field(default_factory=list)
    debug: bool = False
    has_header: bool = True
    destination: Destination = Field(default_factory=Destination)

    def __init__(self, **data):
        try:
            super().__init__(**data)
        except ValidationError as e:
            error_messages = [f"{err['loc'][0]}: {err['msg']}" for err in e.errors()]
            raise UserException(f"Validation Error: {', '.join(error_messages)}")

        if self.debug:
            logging.debug("Component will run in Debug mode")

        # Validate table mode requirements
        if self.mode == Mode.TABLE:
            # In table mode, use table_file field or fallback to files[0]
            file_path = self.table_file or (self.files[0] if self.files else None)

            if not file_path:
                raise UserException("Table mode requires exactly one file path")

            # Check for wildcards in the file path
            if any(char in file_path for char in ["*", "?"]):
                raise UserException("Wildcards are not allowed in table mode")

            # Validate columns requirement when has_header is False
            if not self.has_header and not self.destination.columns:
                raise UserException("When has_header is false, columns must be defined")
