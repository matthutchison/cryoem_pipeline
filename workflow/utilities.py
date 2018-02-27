import asyncio
import hashlib
import shutil
import pathlib


def safe_copy_file(src, dest):
    try:
        pathlib.Path(dest).mkdir(parents=True, exist_ok=True)
    except FileExistsError as e:
        pass
    shutil.copy2(str(src), str(dest))


def file_hash(path):
    h = hashlib.sha1()
    with open(str(path), 'rb', buffering=0) as f:
        for b in iter(lambda: f.read(128*1024), b''):
            h.update(b)
    return h.hexdigest()


async def compress_file(path, force=False):
    '''Compress the file using lbzip2. Returns only after compression complete.

    Defaults are set for the ATC linux box to not saturate. Adjust if necessary
    '''
    cmd = ['lbzip2', '-k', '-n 8', '-z', str(path)]
    cmd.insert(1, '-f') if force else None
    process = await asyncio.create_subprocess_exec(*cmd)
    return await process.wait()


async def uncompress_file(path, force=False):
    '''Uncompress the file using lbzip2. Returns only after uncompress complete.

    Defaults are set for the ATC linux box to not saturate.
    '''
    cmd = ['lbzip2', '-k', '-n 4', '-d', str(path)]
    cmd.insert(1, '-f') if force else None
    process = await asyncio.create_subprocess_exec(*cmd)
    return await process.wait()


async def stack_files(in_paths, out_path):
    '''Stack the files using imod. Return only after stacking complete.
    '''
    cmd = ['newstack', '-bytes 0', *[str(p) for p in in_paths], str(out_path)]
    process = await asyncio.create_subprocess_exec(*cmd)
    return await process.wait()
