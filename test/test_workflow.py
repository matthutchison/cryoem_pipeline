import workflow.utilities as util
import asyncio
import pathlib
import unittest
import tempfile

named_temp = tempfile.NamedTemporaryFile


class UtilitiesTests(unittest.TestCase):

    fake_path = '/path/to/a/fake/file'

    def future_has_stderr_content(self, fut):
        self.assertTrue(fut.result()[1])

    def future_has_no_stderr_content(self, fut):
        self.assertFalse(fut.result()[1])

    def setUp(self):
        self.loop = asyncio.get_event_loop()

    def tearDown(self):
        pass

    def test_hash_errors_on_fake_path(self):
        func = util.file_hash(self.fake_path)
        task = self.loop.create_task(func)
        task.add_done_callback(self.future_has_stderr_content)
        self.loop.run_until_complete(task)

    def test_hash_succeeds_on_real_path(self):
        with named_temp() as f:
            func = util.file_hash(f.name)
            task = self.loop.create_task(func)
            task.add_done_callback(self.future_has_no_stderr_content)
            self.loop.run_until_complete(task)

    def test_compare_hash_succeeds_on_identical_content(self):
        with named_temp() as f1, named_temp() as f2:
            func = util.compare_hashes(f1.name, f2.name)
            task = self.loop.create_task(func)
            task.add_done_callback(
                lambda fut: self.assertTrue(fut.result()))
            self.loop.run_until_complete(task)

    def test_compare_hash_fails_on_different_content(self):
        with named_temp() as f1, named_temp() as f2:
            f1.write(b'content1')
            f1.flush()
            func = util.compare_hashes(f1.name, f2.name)
            task = self.loop.create_task(func)
            task.add_done_callback(
                lambda fut: self.assertFalse(fut.result()))
            self.loop.run_until_complete(task)

    def test_compare_hash_raises_error_on_fake_path(self):
        with named_temp() as f1:
            func = util.compare_hashes(f1.name, self.fake_path)
            task = self.loop.create_task(func)
            with self.assertRaises(FileNotFoundError):
                self.loop.run_until_complete(task)

    def test_safe_copy_fails_when_dest_exists(self):
        with named_temp() as f1, named_temp() as f2:
            with self.assertRaises(FileExistsError):
                self.loop.run_until_complete(
                    self.loop.create_task(
                        util.safe_copy_file(f1.name, f2.name)))

    def test_safe_copy_succeeds_when_dest_does_not_exists(self):
        with named_temp() as f1:
            newpath = f1.name+'testloc'
            self.loop.run_until_complete(
                self.loop.create_task(
                    util.safe_copy_file(f1.name, newpath))
            )
            self.assertTrue(pathlib.Path(newpath).exists())
