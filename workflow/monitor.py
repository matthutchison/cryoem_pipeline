import glob
import time


class FilePatternMonitor():
    """Async infinite generator, `await` returns new files matching pattern

    Strictly monitors only the *addition* of files. Each await returns the
        files added since the previous await. Anything removed from the
        directory is not reported. Anything removed and then re-added will be
        reported again.

    Keyword arguments:
    recursive -- as the "recursive" argument from glob.glob
    walltime -- time in seconds that the directory may remain unchanged before
        the monitor raises StopAsyncIteration and ends.

    Usage example:

    loop = asyncio.get_event_loop()
    test = FilePatternMonitor('./*', walltime=25)
    async def tmp():
        while True:
            print(await test)
            await asyncio.sleep(5)
    loop.run_until_complete(tmp())

    """

    def __init__(self, pattern, recursive=False, walltime=43200):
        self.pattern = pattern
        self.recursive = recursive
        self.walltime = walltime
        self.old = set()
        self.base_time = time.time()

    def __await__(self):
        if self.base_time + self.walltime < time.time():
            raise StopAsyncIteration
        else:
            return self._get_new_files().__await__()

    async def _get_new_files(self):
        new = set(glob.glob(self.pattern, recursive=self.recursive))
        temp = new.difference(self.old)
        self.old = new
        if temp:
            self.base_time = time.time()
        return sorted(temp)
