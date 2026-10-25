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
