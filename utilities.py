import shutil
import pathlib

def safe_copy_file(src, dest):
    try:
        pathlib.Path(dest).mkdir(parents=True, exist_ok=True)
    except FileExistsError as e:
        pass
    shutil.copy2(src, dest)

async def compress_file(path):
    '''Compress the file using lbzip2. Returns only after compression complete.

    Defaults are set for the ATC linux box to not saturate. Adjust if necessary.
    '''
    cmd = ['lbzip2', '-k', '-n 8', '-z', path.expanduser().resolve()]
    process = await asyncio.create_subprocess_exec(*cmd)
    return await process.wait()

async def stack_files(paths):
    '''Stack the files using imod. Return only after stacking complete.
    '''
    cmd = ['newstack', *paths]
    process = await asyncio.create_subprocess_exec(*cmd)
    return await process.wait()
