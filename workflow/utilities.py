import asyncio


async def safe_copy_file(src, dest):
    '''Copy the file from source to dest.
    '''
    cmd = ['cp', str(src), str(dest)]
    return await _wait_subprocess_exec(cmd)


async def file_hash(path):
    '''Get the sha1 hash of the given file.
    '''
    cmd = ['shasum', str(path)]
    return await _communicate_subprocess_exec(cmd)


async def compare_hashes(path_a, path_b):
    '''Compare the hashes of two files
    '''
    a = await file_hash(path_a)
    b = await file_hash(path_b)
    if a[0] and b[0]:
        return a[0].split()[0] == b[0].split()[0]
    else:
        raise FileNotFoundError


async def compress_file(path, force=False):
    '''Compress the file using lbzip2. Returns only after compression complete.

    Defaults are set for the ATC linux box to not saturate. Adjust if necessary
    '''
    cmd = ['lbzip2', '-k', '-n 8', '-z', str(path)]
    cmd.insert(1, '-f') if force else None
    return await _wait_subprocess_exec(cmd)


async def uncompress_file(path, force=False):
    '''Uncompress the file using lbzip2. Returns only after uncompress complete.

    Defaults are set for the ATC linux box to not saturate.
    '''
    cmd = ['lbzip2', '-k', '-n 4', '-d', str(path)]
    cmd.insert(1, '-f') if force else None
    return await _wait_subprocess_exec(cmd)


async def stack_files(in_paths, out_path):
    '''Stack the files using imod. Return only after stacking complete.
    '''
    cmd = ['newstack', '-bytes 0', *[str(p) for p in in_paths], str(out_path)]
    return await _wait_subprocess_exec(cmd)


async def _wait_subprocess_exec(cmd):
    process = await asyncio.create_subprocess_exec(*cmd)
    return await process.wait()


async def _communicate_subprocess_exec(cmd):
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE)
    return await process.communicate(None)
