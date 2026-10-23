# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import importlib.util
import unittest
from pathlib import Path
from unittest.mock import Mock

spec = importlib.util.spec_from_file_location('s3_http_exchange',
    Path(__file__).resolve().parents[1] / 'clients/s3/http_exchange.py')
exchange = importlib.util.module_from_spec(spec)
spec.loader.exec_module(exchange)


class S3HttpExchangeTest(unittest.TestCase):
    def test_early_rejection_is_read_after_peer_stops_accepting_body(self):
        for failure in [BrokenPipeError('body rejected'), ConnectionResetError('body rejected')]:
            connection = Mock()
            connection.request.side_effect = failure
            connection.getresponse.return_value.status = 403
            response = exchange.request_response(connection, 'GET', '/key', b'body', {})
            self.assertEqual(response.status, 403)
            self.assertIs(response, connection.getresponse.return_value)

    def test_incomplete_send_cannot_be_reported_as_success(self):
        connection = Mock()
        connection.request.side_effect = BrokenPipeError('incomplete body')
        connection.getresponse.return_value.status = 200
        with self.assertRaisesRegex(BrokenPipeError, 'incomplete body'):
            exchange.request_response(connection, 'PUT', '/key', b'body', {})

    def test_disconnect_without_an_http_response_still_fails(self):
        connection = Mock()
        connection.request.side_effect = BrokenPipeError('incomplete body')
        connection.getresponse.side_effect = ConnectionResetError('no response')
        with self.assertRaisesRegex(ConnectionResetError, 'no response'):
            exchange.request_response(connection, 'PUT', '/key', b'body', {})


from http.client import BadStatusLine, HTTPConnection, RemoteDisconnected
from io import BytesIO
from pathlib import Path
import unittest

SPEC = importlib.util.spec_from_file_location(
    "s3_http_exchange", Path(__file__).parents[1] / "clients/s3/http_exchange.py")
HTTP = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(HTTP)


class TestEarlyResponseSocket:
    def __init__(self, response):
        self.response = response
        self.sent = []

    def sendall(self, data):
        self.sent.append(data)
        if len(self.sent) > 1:
            raise BrokenPipeError("peer rejected the body")

    def makefile(self, _mode):
        return BytesIO(self.response)

    def close(self):
        pass


class S3HttpSocketExchangeTest(unittest.TestCase):
    def exchange(self, response):
        connection = HTTPConnection("test.invalid")
        sock = TestEarlyResponseSocket(response)
        connection.sock = sock
        try:
            return HTTP.exchange(connection, "GET", "/bucket/key", b"body", {})
        finally:
            connection.close()
            self.assertEqual(len(sock.sent), 2)
            self.assertTrue(sock.sent[0].startswith(b"GET /bucket/key HTTP/1.1\r\n"))

    def test_rejection_status_and_complete_body_survive_failed_upload(self):
        response = b"HTTP/1.1 403 Forbidden\r\nContent-Length: 6\r\nConnection: close\r\n\r\ndenied"
        status, _, body = self.exchange(response)
        self.assertEqual((status, body), (403, b"denied"))

    def test_missing_response_still_fails(self):
        with self.assertRaises(RemoteDisconnected):
            self.exchange(b"")

    def test_malformed_response_still_fails(self):
        with self.assertRaises(BadStatusLine):
            self.exchange(b"invalid response\r\n")

    def test_success_cannot_hide_an_incomplete_upload(self):
        with self.assertRaises(BrokenPipeError):
            self.exchange(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
