# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import errno
import struct
import unittest
from unittest.mock import patch
from profiles.supplied_media import Media, LOOP_CLR_FD, LOOP_GET_STATUS64


class SuppliedMediaTest(unittest.TestCase):
    def medium(self):
        medium = Media.__new__(Media)
        medium.descriptor = 123
        medium.bound = True
        medium.identity = (10, 20)
        medium.raw = "/owned/private-device"
        return medium

    def status(self, identity=(10, 20), raw=0, offset=0, limit=0):
        def ioctl(fd, command, buffer=None, mutate=None):
            self.assertEqual(fd, 123)
            if command == LOOP_GET_STATUS64:
                struct.pack_into("QQQQQ", buffer, 0, *identity, raw, offset, limit)
            else:
                self.assertEqual(command, LOOP_CLR_FD)
            return 0
        return ioctl

    def test_non_owned_host_rejects_before_creating_any_path(self):
        with patch("profiles.supplied_media.os.geteuid", return_value=1000), patch("profiles.supplied_media.Path.mkdir") as mkdir:
            with self.assertRaisesRegex(RuntimeError, "owned privileged host"):
                Media("/unrelated")
            mkdir.assert_not_called()

    def test_whole_mapping_checks_identity_and_cleanup_only_targets_its_recorded_descriptor(self):
        medium = self.medium()
        with patch("profiles.supplied_media.fcntl.ioctl", side_effect=self.status()) as ioctl, patch("profiles.supplied_media.os.close") as close:
            medium.close()
            self.assertEqual([call.args[1] for call in ioctl.call_args_list], [LOOP_GET_STATUS64, LOOP_CLR_FD])
            close.assert_called_once_with(123)
            self.assertIsNone(medium.descriptor)

    def test_changed_backing_or_slice_never_dispatches_detach(self):
        for kwargs in ({"identity": (10, 21)}, {"raw": 2}, {"offset": 512}, {"limit": 4096}):
            medium = self.medium()
            with patch("profiles.supplied_media.fcntl.ioctl", side_effect=self.status(**kwargs)) as ioctl, patch("profiles.supplied_media.os.close"):
                with self.assertRaisesRegex(RuntimeError, "identity changed"):
                    medium.close()
                self.assertEqual([call.args[1] for call in ioctl.call_args_list], [LOOP_GET_STATUS64])

    def test_busy_is_a_claim_observation_but_permission_errors_are_not(self):
        medium = self.medium()
        with patch.object(medium, "status"), patch("profiles.supplied_media.os.open", side_effect=OSError(errno.EBUSY, "claimed")):
            self.assertFalse(medium.exclusive_available())
        with patch.object(medium, "status"), patch("profiles.supplied_media.os.open", side_effect=OSError(errno.EACCES, "denied")):
            with self.assertRaises(OSError):
                medium.exclusive_available()
