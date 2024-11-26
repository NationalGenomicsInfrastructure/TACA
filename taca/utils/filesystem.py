"""Filesystem utilities."""

import contextlib
import os
import shutil

RUN_RE_ILLUMINA = r"^\d{6,8}_[a-zA-Z\d\-]+_\d{2,}_[AB0][A-Z\d\-]+$"
RUN_RE_ONT = r"^(\d{8})_(\d{4})_([0-9a-zA-Z]+)_([0-9a-zA-Z]+)_([0-9a-zA-Z]+)$"
RUN_RE_ELEMENT = r"^\d{8}_AV\d{6}_[AB]\d{10}$"


@contextlib.contextmanager
def chdir(new_dir):
    """Context manager to temporarily change to a new directory."""
    cur_dir = os.getcwd()
    os.chdir(new_dir)
    try:
        yield
    finally:
        os.chdir(cur_dir)


def create_folder(target_folder):
    """Ensure that a folder exists and create it if it doesn't, including any
    parent folders, as necessary.

    :param target_folder: the target folder
    :returns: True if the folder exists or was created, False if the folder
    does not exists and could not be created
    """
    try:
        os.makedirs(target_folder)
    except OSError:
        pass
    return os.path.exists(target_folder)


def touch(file):
    open(file, "w").close()


def do_symlink(src_file, dst_file):
    link_f = os.symlink
    if not os.path.isfile(dst_file):
        link_f(os.path.realpath(src_file), dst_file)


def do_copy(src_path, dst_path):
    # copies folder structure and files (recursively)
    # if symlinks, will copy content, not the links
    # dst_path will be created, it must NOT exist
    shutil.copytree(src_path, dst_path)
