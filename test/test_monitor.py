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
