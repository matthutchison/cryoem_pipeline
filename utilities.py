import shutil
import pathlib

def safe_copy_file(src, dest):
    try:
        pathlib.Path(dest).mkdir(parents=True, exist_ok=True)
    except FileExistsError as e:
        pass
    shutil.copy2(src, dest)
