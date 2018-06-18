import asyncio
import errno
import logging
import os
import pathlib


logger = logging.getLogger(__name__)


async def safe_copy_file(src, dest):
    '''Async copy the file from src to dest using system cp.

    Parameters:
    src (string or pathlike object): path to the source file
    dest (string or pathlike object): path to the destination

    Creates a cp subprocess. Primarily useful for copying extremely large
    files without blocking the main thread. Fails if file already exists.
    '''
    if pathlib.Path(dest).exists():
        raise FileExistsError(
            errno.EEXIST,
            os.strerror(errno.EEXIST),
            dest)
    cmd = ['cp', str(src), str(dest)]
    return await _wait_subprocess_exec(cmd)


async def file_hash(path):
    '''Async get the sha1 hash of the given file using system shasum.

    Parameters:
    path (string or pathlike object): the path to the file to hash

    Creates a shasum subprocess. Returns the output of shasum as a 2-tuple
    of bytes.
    (b'af9fce487c7d1e5d6101d5317b9884481a874442  /path/to/file', b'')
    First element is the stdout result of shasum, second is the stderr.
    '''
    cmd = ['shasum', str(path)]
    return await _communicate_subprocess_exec(cmd)


async def compare_hashes(path_a, path_b):
    '''Compare the hashes of two files.

    Parameters:
    path_a, path_b (string or pathlike object): the paths to the files to hash
        and compare.
    '''
    a = await file_hash(path_a)
    b = await file_hash(path_b)
    if a[0] and b[0]:
        return a[0].split()[0] == b[0].split()[0]
    else:
        raise FileNotFoundError(
            errno.ENOENT,
            os.strerror(errno.ENOENT),
            (a, b)
        )


async def compress_file(path, force=False):
    '''Compress the file using lbzip2. Returns only after compression complete.

    Parameters:
    path (string or pathlib.Path): path of file to compress
    force (bool): overwrite existing files (default False)

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


async def convert_to_mrc(src, dest):
    '''Convert DM4 to MRC using newstack.

    The -bytes 0 flag is to force the unsigned integer convention that
    motioncor2 expects.
    '''
    cmd = ['newstack', '-bytes', '0', str(src), str(dest)]
    return await _communicate_subprocess_exec(cmd)


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


async def create_scipion_project(project_name, config_path):
    '''Start an instance of scipion using the provided project name and config
    '''
    cmd = ['scipion',
           'python', '/usr/local/scipion/scripts/create_project.py',
           project_name, config_path]
    return await _communicate_subprocess_exec(cmd)


async def start_scipion_project(project_name):
    cmd = ['scipion',
           'python', '/usr/local/scipion/scripts/schedule_project.py',
           project_name]
    return await _communicate_subprocess_exec(cmd)


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
    logger.debug('_communicate_subprocess_exec starting {0}'.format(cmd))
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
