import logging
from enum import Enum

from keboola.component.exceptions import UserException
from pydantic import BaseModel, Field, ValidationError, field_validator


class Protocol(str, Enum):
    FTP = "ftp"
    EX_FTPS = "ex-ftps"
    IM_FTPS = "im-ftps"
    SFTP = "sftp"


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
    files: list[str] = Field(default_factory=list)
    include_path_in_filename: bool = False
    append_timestamp: bool = False
    incremental_mode: bool = False
    tags: list[str] = Field(default_factory=list)
    debug: bool = False

    def __init__(self, **data):
        try:
            super().__init__(**data)
        except ValidationError as e:
            error_messages = [f"{err['loc'][0]}: {err['msg']}" for err in e.errors()]
            raise UserException(f"Validation Error: {', '.join(error_messages)}")

        if self.debug:
            logging.debug("Component will run in Debug mode")
