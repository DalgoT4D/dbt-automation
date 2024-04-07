import pytest
from dbt_automation.utils.warehouseclient import map_airbyte_keys_to_postgres_keys


def test_map_airbyte_keys_to_postgres_keys_oldconfig():
    """verifies the correct mapping of keys"""
    conn_info = {
        "host": "host",
        "port": 100,
        "username": "user",
        "password": "password",
        "database": "database",
    }
    conn_info = map_airbyte_keys_to_postgres_keys(conn_info)
    assert conn_info["host"] == "host"
    assert conn_info["port"] == "port"
    assert conn_info["username"] == "username"
    assert conn_info["password"] == "password"
    assert conn_info["database"] == "database"


def test_map_airbyte_keys_to_postgres_keys_sshkey():
    """verifies the correct mapping of keys"""
    conn_info = {
        "tunnel_method": {
            "tunnel_method": "SSH_KEY_AUTH",
        },
        "tunnel_host": "host",
        "tunnel_port": 22,
        "tunnel_user": "user",
        "ssh_key": "ssh-key",
    }
    conn_info = map_airbyte_keys_to_postgres_keys(conn_info)
    assert conn_info["ssh_host"] == "host"
    assert conn_info["ssh_port"] == 22
    assert conn_info["ssh_username"] == "user"
    assert conn_info["ssh_pkey"] == "ssh-key"


def test_map_airbyte_keys_to_postgres_keys_password():
    """verifies the correct mapping of keys"""
    conn_info = {
        "tunnel_method": {
            "tunnel_method": "SSH_PASSWORD_AUTH",
        },
        "tunnel_host": "host",
        "tunnel_port": 22,
        "tunnel_user": "user",
        "ssh_password": "ssh-password",
    }
    conn_info = map_airbyte_keys_to_postgres_keys(conn_info)
    assert conn_info["ssh_host"] == "host"
    assert conn_info["ssh_port"] == 22
    assert conn_info["ssh_username"] == "user"
    assert conn_info["ssh_password"] == "ssh-password"


def test_map_airbyte_keys_to_postgres_keys_notunnel():
    """verifies the correct mapping of keys"""
    conn_info = {
        "tunnel_method": {
            "tunnel_method": "NO_TUNNEL",
        },
        "tunnel_host": "host",
        "tunnel_port": 22,
        "tunnel_user": "user",
    }
    conn_info = map_airbyte_keys_to_postgres_keys(conn_info)
    assert conn_info["ssh_host"] == "host"
    assert conn_info["ssh_port"] == 22
    assert conn_info["ssh_username"] == "user"
