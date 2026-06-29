import os
from unittest.mock import MagicMock, patch

import pytest

from dfa.adw.connection import AdwConnection


def _reset_adw_connection_state():
    AdwConnection._AdwConnection__connection = None
    AdwConnection._AdwConnection__cursor = None
    AdwConnection._AdwConnection__username = None
    AdwConnection._AdwConnection__wallet_dir = None


def _adw_env(**overrides):
    env = {
        "DFA_ADW_DFA_SCHEMA": "DFA",
        "DFA_CONN_PROTOCOL": "tcps",
        "DFA_CONN_HOST": "db.example.com",
        "DFA_CONN_PORT": "1522",
        "DFA_CONN_SERVICE_NAME": "dbsvc",
    }
    env.update(overrides)
    return env


def test_commit_commits_cached_connection():
    connection = MagicMock()
    AdwConnection._AdwConnection__connection = connection

    try:
        AdwConnection.commit()

        connection.commit.assert_called_once()
    finally:
        _reset_adw_connection_state()


def test_commit_raises_when_cached_connection_commit_fails():
    connection = MagicMock()
    connection.commit.side_effect = RuntimeError("commit failed")
    AdwConnection._AdwConnection__connection = connection

    try:
        with pytest.raises(RuntimeError, match="commit failed"):
            AdwConnection.commit()

        connection.commit.assert_called_once()
    finally:
        _reset_adw_connection_state()


def test_rollback_and_close_rolls_back_then_closes_connection():
    connection = MagicMock()
    cursor = MagicMock()
    AdwConnection._AdwConnection__connection = connection
    AdwConnection._AdwConnection__cursor = cursor

    try:
        AdwConnection.rollback_and_close()

        connection.rollback.assert_called_once()
        cursor.close.assert_called_once()
        connection.close.assert_called_once()
        assert AdwConnection._AdwConnection__connection is None
        assert AdwConnection._AdwConnection__cursor is None
    finally:
        _reset_adw_connection_state()


def test_rollback_raises_when_cached_connection_rollback_fails():
    connection = MagicMock()
    connection.rollback.side_effect = RuntimeError("rollback failed")
    AdwConnection._AdwConnection__connection = connection

    try:
        with pytest.raises(RuntimeError, match="rollback failed"):
            AdwConnection.rollback()

        connection.rollback.assert_called_once()
    finally:
        _reset_adw_connection_state()


def test_rollback_and_close_closes_connection_when_rollback_fails():
    connection = MagicMock()
    cursor = MagicMock()
    connection.rollback.side_effect = RuntimeError("rollback failed")
    AdwConnection._AdwConnection__connection = connection
    AdwConnection._AdwConnection__cursor = cursor

    try:
        AdwConnection.rollback_and_close()

        connection.rollback.assert_called_once()
        cursor.close.assert_called_once()
        connection.close.assert_called_once()
        assert AdwConnection._AdwConnection__connection is None
        assert AdwConnection._AdwConnection__cursor is None
    finally:
        _reset_adw_connection_state()


@patch("dfa.adw.connection.oracledb.connect")
@patch("dfa.adw.connection.AdwSecrets")
def test_get_cursor_reconnects_when_cached_connection_ping_fails(mock_secrets_cls, mock_connect):
    mock_secrets = mock_secrets_cls.return_value
    mock_secrets.get_wallet.return_value = b"wallet"
    mock_secrets.get_ewallet_pem.return_value = "pem"
    mock_secrets.get_dfa_user_password.return_value = "password"
    mock_secrets.get_wallet_password.return_value = "wallet-password"
    stale_connection = MagicMock()
    stale_connection.ping.side_effect = Exception("DPY-4011")
    stale_cursor = MagicMock()
    new_cursor = MagicMock()
    new_connection = MagicMock()
    new_connection.cursor.return_value = new_cursor
    mock_connect.return_value = new_connection

    AdwConnection._AdwConnection__connection = stale_connection
    AdwConnection._AdwConnection__cursor = stale_cursor
    AdwConnection._AdwConnection__username = "DFA"

    try:
        with patch.dict(os.environ, _adw_env(), clear=False):
            assert AdwConnection.get_cursor() is new_cursor

        stale_cursor.close.assert_called_once()
        stale_connection.close.assert_called_once()
        mock_connect.assert_called_once()
        assert AdwConnection._AdwConnection__connection is new_connection
        assert AdwConnection._AdwConnection__cursor is new_cursor
    finally:
        _reset_adw_connection_state()


def test_get_cursor_reuses_cached_cursor_when_connection_ping_succeeds():
    connection = MagicMock()
    cursor = MagicMock()
    cursor.connection = connection

    AdwConnection._AdwConnection__connection = connection
    AdwConnection._AdwConnection__cursor = cursor
    AdwConnection._AdwConnection__username = "DFA"

    try:
        with patch.dict(os.environ, _adw_env(), clear=False):
            assert AdwConnection.get_cursor() is cursor

        connection.ping.assert_called_once()
        cursor.close.assert_not_called()
        connection.close.assert_not_called()
    finally:
        _reset_adw_connection_state()


def test_get_cursor_recreates_cached_cursor_when_cursor_is_invalid():
    connection = MagicMock()
    stale_cursor = MagicMock()
    new_cursor = MagicMock()
    connection.cursor.return_value = new_cursor
    type(stale_cursor).connection = property(lambda self: (_ for _ in ()).throw(RuntimeError("cursor closed")))

    AdwConnection._AdwConnection__connection = connection
    AdwConnection._AdwConnection__cursor = stale_cursor
    AdwConnection._AdwConnection__username = "DFA"

    try:
        with patch.dict(os.environ, _adw_env(), clear=False):
            assert AdwConnection.get_cursor() is new_cursor

        connection.ping.assert_called_once()
        stale_cursor.close.assert_called_once()
        connection.cursor.assert_called_once()
        assert AdwConnection._AdwConnection__cursor is new_cursor
    finally:
        del type(stale_cursor).connection
        _reset_adw_connection_state()


@patch("dfa.adw.connection.oracledb.connect")
@patch("dfa.adw.connection.AdwSecrets")
def test_get_connection_reconnects_when_username_changes(mock_secrets_cls, mock_connect):
    _reset_adw_connection_state()
    mock_secrets = mock_secrets_cls.return_value
    mock_secrets.get_wallet.return_value = b"wallet"
    mock_secrets.get_ewallet_pem.return_value = "pem"
    mock_secrets.get_dfa_user_password.return_value = "dfa-password"
    mock_secrets.get_password.return_value = "admin-password"
    mock_secrets.get_wallet_password.return_value = "wallet-password"
    dfa_connection = MagicMock()
    admin_connection = MagicMock()
    mock_connect.side_effect = [dfa_connection, admin_connection]

    try:
        with patch.dict(os.environ, _adw_env(), clear=False):
            assert AdwConnection.get_connection() is dfa_connection
            assert AdwConnection.get_connection(username="ADMIN") is admin_connection

        dfa_connection.close.assert_called_once()
        assert mock_connect.call_args_list[0].kwargs["user"] == "DFA"
        assert mock_connect.call_args_list[0].kwargs["password"] == "dfa-password"
        assert mock_connect.call_args_list[1].kwargs["user"] == "ADMIN"
        assert mock_connect.call_args_list[1].kwargs["password"] == "admin-password"
        assert AdwConnection._AdwConnection__username == "ADMIN"
    finally:
        _reset_adw_connection_state()


@patch("dfa.adw.connection.oracledb.connect")
@patch("dfa.adw.connection.AdwSecrets")
def test_get_cursor_reconnects_when_username_changes(mock_secrets_cls, mock_connect):
    _reset_adw_connection_state()
    mock_secrets = mock_secrets_cls.return_value
    mock_secrets.get_wallet.return_value = b"wallet"
    mock_secrets.get_ewallet_pem.return_value = "pem"
    mock_secrets.get_dfa_user_password.return_value = "password"
    mock_secrets.get_password.return_value = "admin-password"
    mock_secrets.get_wallet_password.return_value = "wallet-password"
    stale_connection = MagicMock()
    stale_cursor = MagicMock()
    admin_cursor = MagicMock()
    admin_connection = MagicMock()
    admin_connection.cursor.return_value = admin_cursor
    mock_connect.return_value = admin_connection

    AdwConnection._AdwConnection__connection = stale_connection
    AdwConnection._AdwConnection__cursor = stale_cursor
    AdwConnection._AdwConnection__username = "DFA"

    try:
        with patch.dict(os.environ, _adw_env(), clear=False):
            assert AdwConnection.get_cursor(username="ADMIN") is admin_cursor

        stale_cursor.close.assert_called_once()
        stale_connection.close.assert_called_once()
        mock_connect.assert_called_once()
        assert mock_connect.call_args.kwargs["user"] == "ADMIN"
        assert mock_connect.call_args.kwargs["password"] == "admin-password"
    finally:
        _reset_adw_connection_state()


@patch("dfa.adw.connection.oracledb.connect")
@patch("dfa.adw.connection.AdwSecrets")
def test_get_connection_includes_bounded_connect_parameters(mock_secrets_cls, mock_connect, tmp_path):
    _reset_adw_connection_state()
    mock_secrets = mock_secrets_cls.return_value
    mock_secrets.get_wallet.return_value = b"wallet"
    mock_secrets.get_ewallet_pem.return_value = "pem"
    mock_secrets.get_dfa_user_password.return_value = "password"
    mock_secrets.get_wallet_password.return_value = "wallet-password"
    connection = MagicMock()
    mock_connect.return_value = connection

    env = _adw_env(
        **{
            "DFA_CONN_RETRY_COUNT": "2",
            "DFA_CONN_RETRY_DELAY": "1",
            "DFA_CONN_TCP_CONNECT_TIMEOUT": "5",
        }
    )

    try:
        with patch.dict(os.environ, env, clear=False):
            assert AdwConnection.get_connection() is connection

        dsn = mock_connect.call_args.kwargs["dsn"]
        assert "retry_count=2" in dsn
        assert "retry_delay=1" in dsn
        assert "tcp_connect_timeout=5" in dsn
    finally:
        _reset_adw_connection_state()


@patch("dfa.adw.connection.oracledb.connect")
@patch("dfa.adw.connection.AdwSecrets")
def test_get_connection_caps_large_connect_retry_parameters(mock_secrets_cls, mock_connect):
    _reset_adw_connection_state()
    mock_secrets = mock_secrets_cls.return_value
    mock_secrets.get_wallet.return_value = b"wallet"
    mock_secrets.get_ewallet_pem.return_value = "pem"
    mock_secrets.get_dfa_user_password.return_value = "password"
    mock_secrets.get_wallet_password.return_value = "wallet-password"
    mock_connect.return_value = MagicMock()

    env = _adw_env(
        **{
            "DFA_CONN_RETRY_COUNT": "20",
            "DFA_CONN_RETRY_DELAY": "30",
            "DFA_CONN_TCP_CONNECT_TIMEOUT": "60",
        }
    )

    try:
        with patch.dict(os.environ, env, clear=False):
            AdwConnection.get_connection()

        dsn = mock_connect.call_args.kwargs["dsn"]
        assert "retry_count=3" in dsn
        assert "retry_delay=3" in dsn
        assert "tcp_connect_timeout=10" in dsn
    finally:
        _reset_adw_connection_state()
