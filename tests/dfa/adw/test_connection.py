import os
from unittest.mock import MagicMock, patch

from dfa.adw.connection import AdwConnection


def _reset_adw_connection_state():
    AdwConnection._AdwConnection__connection = None
    AdwConnection._AdwConnection__cursor = None
    AdwConnection._AdwConnection__wallet_dir = None


def test_rollback_rolls_back_connection_and_closes_cached_cursor():
    connection = MagicMock()
    cursor = MagicMock()
    AdwConnection._AdwConnection__connection = connection
    AdwConnection._AdwConnection__cursor = cursor

    try:
        AdwConnection.rollback()

        connection.rollback.assert_called_once()
        cursor.close.assert_called_once()
        assert AdwConnection._AdwConnection__connection is connection
        assert AdwConnection._AdwConnection__cursor is None
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


def test_get_cursor_reconnects_when_cached_connection_ping_fails():
    stale_connection = MagicMock()
    stale_connection.ping.side_effect = Exception("DPY-4011")
    stale_cursor = MagicMock()
    new_cursor = MagicMock()
    new_connection = MagicMock()
    new_connection.cursor.return_value = new_cursor

    AdwConnection._AdwConnection__connection = stale_connection
    AdwConnection._AdwConnection__cursor = stale_cursor

    try:
        with patch.object(AdwConnection, "get_connection", return_value=new_connection) as mock_get_connection:
            assert AdwConnection.get_cursor() is new_cursor

        stale_cursor.close.assert_called_once()
        stale_connection.close.assert_called_once()
        mock_get_connection.assert_called_once_with(None)
        assert AdwConnection._AdwConnection__connection is None
        assert AdwConnection._AdwConnection__cursor is new_cursor
    finally:
        _reset_adw_connection_state()


def test_get_cursor_reuses_cached_cursor_when_connection_ping_succeeds():
    connection = MagicMock()
    cursor = MagicMock()

    AdwConnection._AdwConnection__connection = connection
    AdwConnection._AdwConnection__cursor = cursor

    try:
        assert AdwConnection.get_cursor() is cursor

        connection.ping.assert_called_once()
        cursor.close.assert_not_called()
        connection.close.assert_not_called()
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

    env = {
        "DFA_ADW_DFA_SCHEMA": "DFA",
        "DFA_CONN_PROTOCOL": "tcps",
        "DFA_CONN_HOST": "db.example.com",
        "DFA_CONN_PORT": "1522",
        "DFA_CONN_SERVICE_NAME": "dbsvc",
        "DFA_CONN_RETRY_COUNT": "2",
        "DFA_CONN_RETRY_DELAY": "1",
        "DFA_CONN_TCP_CONNECT_TIMEOUT": "5",
    }

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

    env = {
        "DFA_ADW_DFA_SCHEMA": "DFA",
        "DFA_CONN_PROTOCOL": "tcps",
        "DFA_CONN_HOST": "db.example.com",
        "DFA_CONN_PORT": "1522",
        "DFA_CONN_SERVICE_NAME": "dbsvc",
        "DFA_CONN_RETRY_COUNT": "20",
        "DFA_CONN_RETRY_DELAY": "30",
        "DFA_CONN_TCP_CONNECT_TIMEOUT": "60",
    }

    try:
        with patch.dict(os.environ, env, clear=False):
            AdwConnection.get_connection()

        dsn = mock_connect.call_args.kwargs["dsn"]
        assert "retry_count=3" in dsn
        assert "retry_delay=3" in dsn
        assert "tcp_connect_timeout=10" in dsn
    finally:
        _reset_adw_connection_state()
