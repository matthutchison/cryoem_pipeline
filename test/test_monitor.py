import workflow.monitor as mon
import unittest
import asyncio
import time


class MonitorTest(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.get_event_loop()

    def tearDown(self):
        pass

    async def next_monitor_result(self, monitor):
        return await monitor

    def test_await_after_walltime_raises_StopAsyncIteration(self):
        fpm = mon.FilePatternMonitor('./*', walltime=0)
        fpm.base_time = time.time() - 10
        with self.assertRaises(StopAsyncIteration):
            self.loop.run_until_complete(self.next_monitor_result(fpm))

    def test_await_before_walltime_doesnt_raise_StopAsyncIteration(self):
        fpm = mon.FilePatternMonitor('./*', walltime=1)
        self.loop.run_until_complete(self.next_monitor_result(fpm))

    def test_monitor_returns_nonzero_files(self):
        fpm = mon.FilePatternMonitor('./*', walltime=1)

        async def has_files(monitor):
            res = await monitor
            self.assertTrue(len(res) > 0)
        self.loop.run_until_complete(has_files(fpm))
