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
    __username = None
    __wallet_dir = None
    MAX_CONN_RETRY_COUNT = 3
    MAX_CONN_RETRY_DELAY = 3
    MAX_CONN_TCP_CONNECT_TIMEOUT = 10

    @classmethod
    def _reset_connection(cls):
        if cls.__cursor is not None:
            try:
                cls.__cursor.close()
            except Exception as e:
                cls.logger.warning("Failed to close stale cursor: %s", e)
            finally:
                cls.__cursor = None

        if cls.__connection is not None:
            try:
                cls.__connection.close()
            except Exception as e:
                cls.logger.warning("Failed to close stale connection: %s", e)
            finally:
                cls.__connection = None
                cls.__username = None

    @classmethod
    def _reset_cursor(cls):
        if cls.__cursor is not None:
            try:
                cls.__cursor.close()
            except Exception as e:
                cls.logger.warning("Failed to close stale cursor: %s", e)
            finally:
                cls.__cursor = None

    @classmethod
    def _ensure_connection_is_usable(cls):
        if cls.__connection is None:
            return

        try:
            cls.__connection.ping()
        except Exception as e:
            cls.logger.warning("ADW connection is no longer usable; reconnecting: %s", e)
            cls._reset_connection()

    @classmethod
    def _ensure_cursor_is_usable(cls):
        if cls.__cursor is None:
            return

        try:
            cursor_connection = cls.__cursor.connection
        except Exception as e:
            cls.logger.warning("ADW cursor is no longer usable; recreating: %s", e)
            cls._reset_cursor()
            return

        if cursor_connection is not cls.__connection:
            cls.logger.info("ADW cursor belongs to a stale connection; recreating")
            cls._reset_cursor()

    @staticmethod
    def _get_bounded_int_env(name: str, default: int, maximum: int) -> int:
        try:
            value = int(os.environ.get(name, str(default)))
        except ValueError:
            value = default
        return max(0, min(value, maximum))

    @staticmethod
    def _get_password(secrets_mgr, username: str):
        if username.upper() == "ADMIN":
            return secrets_mgr.get_password()
        return secrets_mgr.get_dfa_user_password()

    @classmethod
    def get_connection(cls, username: str | None = None):
        username = os.environ["DFA_ADW_DFA_SCHEMA"] if username is None else username
        if cls.__connection is not None and cls.__username != username:
            cls.logger.info("ADW username changed from %s to %s; reconnecting", cls.__username, username)
            cls._reset_connection()

        cls._ensure_connection_is_usable()
        if cls.__connection is None:
            cls.logger.info("Initializing ADW connection (loading wallet and secrets)")

            secrets_mgr = AdwSecrets()
            if cls.__wallet_dir is None:
                cls.__wallet_dir = tempfile.mkdtemp(prefix="dfa_wallet_")
                os.chmod(cls.__wallet_dir, 0o700)
                # Write wallet files into the temp directory
                with open(os.path.join(cls.__wallet_dir, "cwallet.sso"), "wb") as f:
                    f.write(secrets_mgr.get_wallet())
                with open(os.path.join(cls.__wallet_dir, "ewallet.pem"), "w", encoding="utf-8") as f:
                    f.write(secrets_mgr.get_ewallet_pem())
                # Cleanup temp wallet directory on process exit
                atexit.register(shutil.rmtree, cls.__wallet_dir, ignore_errors=True)

            wallet_directory = cls.__wallet_dir
            password = cls._get_password(secrets_mgr, username)
            wallet_password = secrets_mgr.get_wallet_password()

            params = {
                "retry_count": cls._get_bounded_int_env(
                    "DFA_CONN_RETRY_COUNT", cls.MAX_CONN_RETRY_COUNT, cls.MAX_CONN_RETRY_COUNT
                ),
                "retry_delay": cls._get_bounded_int_env(
                    "DFA_CONN_RETRY_DELAY", cls.MAX_CONN_RETRY_DELAY, cls.MAX_CONN_RETRY_DELAY
                ),
                "tcp_connect_timeout": cls._get_bounded_int_env(
                    "DFA_CONN_TCP_CONNECT_TIMEOUT",
                    cls.MAX_CONN_TCP_CONNECT_TIMEOUT,
                    cls.MAX_CONN_TCP_CONNECT_TIMEOUT,
                ),
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
            cls.__username = username

            cls.logger.info("ADW connection established")

        return cls.__connection

    @classmethod
    def get_cursor(cls, username: str | None = None):
        connection = cls.get_connection(username)
        cls._ensure_cursor_is_usable()
        if cls.__cursor is None:
            cls.__cursor = connection.cursor()

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
            finally:
                cls.__username = None
        else:
            cls.__connection = None
            cls.__username = None

    @classmethod
    def commit(cls):
        if cls.__connection is not None:
            try:
                cls.__connection.commit()
            except Exception as e:
                cls.logger.warning("Failed to commit: %s", e)
                raise

    @classmethod
    def rollback(cls, suppress_errors: bool = False):
        if cls.__connection is not None:
            try:
                cls.__connection.rollback()
            except Exception as e:
                cls.logger.warning("Failed to roll back: %s", e)
                if not suppress_errors:
                    raise

    @classmethod
    def rollback_and_close(cls):
        cls.rollback(suppress_errors=True)
        cls._close_all()

    @classmethod
    def close(cls):
        cls._close_all()
