# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import atexit
import os
import shutil
import tempfile

import oracledb

from common.logger.logger import Logger
from common.ocihelpers.vault import AdwSecrets


class AdwConnection:
    logger = Logger(__name__).get_logger()
    __connection = None
    __cursor = None
    __wallet_dir = None

    @classmethod
    def get_connection(cls, username: str | None = None):
        if cls.__connection is None:
            cls.logger.info("Initializing ADW connection (loading wallet and secrets)")

            secrets_mgr = AdwSecrets()
            if cls.__wallet_dir is None:
                cls.__wallet_dir = tempfile.mkdtemp(prefix="dfa_wallet_")
                os.chmod(cls.__wallet_dir, 0o700)
                # Write wallet files into the temp directory
                with open(os.path.join(cls.__wallet_dir, "cwallet.sso"), "wb") as f:
                    f.write(secrets_mgr.get_wallet())
                with open(
                    os.path.join(cls.__wallet_dir, "ewallet.pem"), "w", encoding="utf-8"
                ) as f:
                    f.write(secrets_mgr.get_ewallet_pem())
                # Cleanup temp wallet directory on process exit
                atexit.register(shutil.rmtree, cls.__wallet_dir, ignore_errors=True)

            wallet_directory = cls.__wallet_dir
            username = os.environ["DFA_ADW_DFA_SCHEMA"] if username is None else username
            password = secrets_mgr.get_dfa_user_password()
            wallet_password = secrets_mgr.get_wallet_password()

            params = {
                "retry_count": os.environ["DFA_CONN_RETRY_COUNT"],
                "retry_delay": os.environ["DFA_CONN_RETRY_DELAY"],
            }
            query = "&".join([f"{k}={v}" for k, v in params.items()])
            dsn = (
                f'{os.environ["DFA_CONN_PROTOCOL"]}://'
                f'{os.environ["DFA_CONN_HOST"]}:{os.environ["DFA_CONN_PORT"]}/'
                f'{os.environ["DFA_CONN_SERVICE_NAME"]}?{query}'
            )

            cls.__connection = oracledb.connect(
                user=username,
                password=password,
                dsn=dsn,
                wallet_password=wallet_password,
                wallet_location=wallet_directory,
            )
            atexit.register(cls._close_all)

            cls.logger.info("ADW connection established")

        return cls.__connection

    @classmethod
    def get_cursor(cls, username: str | None = None):
        if cls.__cursor is None:
            cls.__cursor = cls.get_connection(username).cursor()

        return cls.__cursor

    @classmethod
    def _close_all(cls):
        if cls.__cursor:
            try:
                cls.__cursor.close()
                cls.__cursor = None
            except Exception as e:
                cls.logger.warning("Failed to close cursor: %s", e)
                cls.__cursor = None
        else:
            cls.__cursor = None

        if cls.__connection:
            try:
                cls.__connection.close()
                cls.__connection = None
            except Exception as e:
                cls.logger.warning("Failed to close connection: %s", e)
                cls.__connection = None
        else:
            cls.__connection = None

    @classmethod
    def commit(cls):
        cls.__connection.commit()

        if cls.__cursor is not None:
            cls.__cursor.close()

        cls.__cursor = None

    @classmethod
    def close(cls):
        cls._close_all()
