import os

def try_create_directory(path):
    try:
        # Unfortunately this block is subject to a race condition
        # so we need to catch the exception for the case where
        # another node created it after we looked for it.
        if not os.path.exists(path):
            os.mkdir(path)
    except FileExistsError:
        # As long as /somebody/ creates it, we don't care.
        pass

def remove_flag(path):
    # Remove in_progress flag
    try:
        os.remove(path)
    except Exception as e:
        # print(f"Couldn't remove file at {path} (got {e})")
        pass

def create_flag(path):
    # Touch complete flag
    with open(path,'a') as _:
        pass
