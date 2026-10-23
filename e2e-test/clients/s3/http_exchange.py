# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

"""Read an early HTTP rejection even when its peer has stopped accepting body bytes."""


def request_response(connection, method, path, body, headers):
    try:
        connection.request(method, path, body=body, headers=headers)
    except (BrokenPipeError, ConnectionResetError):
        response = connection.getresponse()
        if response.status < 400:
            raise
        return response
    return connection.getresponse()


def exchange(connection, method, path, body, headers):
    response = request_response(connection, method, path, body, headers)
    return response.status, dict(response.getheaders()), response.read()
