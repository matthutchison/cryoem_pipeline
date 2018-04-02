import asyncio
import logging


logger = logging.getLogger(__name__)


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


async def globus_transfer(src_endpoint_spec, dest_endpoint_spec, *args):
    '''Tranfer file via globus.

    Tranfer file from the src_endpoint_spec to the dest_endpoint_spec using
    the python globus cli (the "new" cli from 2017).

    The endpoint spec should be of the form endpoint_uuid:path as in:
        "00c368d6-8cb0-48cf-8896-f31426d5eab2:/path/to/file(s)"

    Additional options to the globus command can be passed via *args.
    Each option and its arguments must be separate list items.  For instance:
        'globus transfer -s mtime src dest' must be passed as
        globus_transfer(src, dest, '-s', 'mtime')
    The transfer command will error if options are combined with arguments
    '''
    cmd = ['globus', 'transfer', src_endpoint_spec, dest_endpoint_spec, *args]
    return await _wait_subprocess_exec(cmd)


async def _wait_subprocess_exec(cmd):
    logger.debug('_wait_subprocess_exec starting {0}'.format(cmd))
    process = await asyncio.create_subprocess_exec(*cmd)
    ret = await process.wait()
    if ret:
        logger.warning('_wait_subprocess_exec error {0} with cmd {1}'
                       .format(ret, cmd))
    else:
        logger.debug('_wait_subprocess_exec completed {0}'
                     .format(cmd))
    return ret


async def _communicate_subprocess_exec(cmd):
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE)
    ret = await process.communicate(None)
    if ret[1]:
        logger.warning('_communicate_subprocess_exec error {0} with cmd {1}'
                       .format(ret, cmd))
    else:
        logger.debug('_communicate_subprocess_exec completed {0} from cmd {1}'
                     .format(ret, cmd))
    return ret
