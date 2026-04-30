from unittest.mock import MagicMock

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
