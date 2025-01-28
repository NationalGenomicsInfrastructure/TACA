"""Helper classes for handling file trasfers."""

import logging
import os
import shutil
import subprocess

from taca.utils.filesystem import create_folder
from taca.utils.misc import call_external_command, hashfile

logger = logging.getLogger(__name__)


class TransferAgent:
    """
    (Abstract) superclass representing an Agent that performs file transfers.
    Agents implementing specific methods for transferring files should extend
    this and implement the transfer() method.
    """

    def __init__(self, src_path=None, dest_path=None, opts={}, **kwargs):
        """Creates an agent instance
        :param string src_path: the file or folder that should be transferred
        :param string dest_path: the destination file or folder
        :param bool validate: whether to validate the transferred files
        :param opts: options that will be passed to the transfer command
        """
        self.src_path = src_path
        self.dest_path = dest_path
        self.validate = kwargs.get("validate", False)
        self.cmdopts = opts

    def __str__(self):
        return type(self).__name__

    def format_options(self):
        """Format the options dictionary stored in this instance's cmdopts
        attribute and return the formatted options as a list of strings.
        A key in the dictionary represents the option name. If
        the corresponding value is None, the option will be assumed to
        represent a flag. If the value is a list, the option will be given
        multiple times.

        For example:

            opts = {'opt1': None, 'opt2': 'val1', 'opt3': ['val2','val3']}

        will be expanded to:

            ['--opt1','--opt2=val1','--opt3=val2','--opt3=val3']

        :returns: List of formatted options as strings
        """
        cmdopts = []
        for param, val in self.cmdopts.items():
            if val is None:
                cmdopts.append(param)
            elif isinstance(val, list):
                for v in val:
                    cmdopts.append(f"{param}={v}")
            else:
                cmdopts.append(f"{param}={val}")
        return cmdopts

    def transfer(self):
        """Abstract method, should be implemented by subclasses."""
        raise NotImplementedError("This method should be implemented by subclass")

    def validate_src_path(self):
        """Validates that the src_path attribute of the Agent instance.
        :raises transfer.TransferError: if src_path is not valid
        """
        if self.src_path is None:
            raise TransferError(
                msg="src_path cannot be None",
                src_path=self.src_path,
                dest_path=self.dest_path,
            )
        if not os.path.exists(self.src_path):
            raise TransferError(
                msg=f'src_path "{self.src_path}" does not exist',
                src_path=self.src_path,
                dest_path=self.dest_path,
            )

    def validate_dest_path(self):
        """Validates that the dest_path attribute of the Agent instance.
        :raises transfer.TransferError: if dest_path is not valid
        """
        if self.dest_path is None:
            raise TransferError(
                msg="dest_path cannot be None",
                src_path=self.src_path,
                dest_path=self.dest_path,
            )

    def validate_transfer(self):
        """Abstract method, should be implemented by subclasses."""
        raise NotImplementedError("This method should be implemented by subclass")


class RsyncAgent(TransferAgent):
    """An agent that knows how to perform an rsync transfer locally or
    between hosts. If supplied with a checksum file, the transfer can
    be validated on the receiving side.
    """

    CMD = "rsync"
    DEFAULT_OPTS = {
        "-a": None,
    }

    def __init__(
        self,
        src_path,
        dest_path=None,
        remote_host=None,
        remote_user=None,
        validate=True,
        digestfile=None,
        opts=None,
        **kwargs,
    ):
        """Creates an RsyncAgent instance
        :param string src_path: the file or folder that should be transferred
        :param string dest_path: the destination file or folder
        :param string remote_host: the remote host to transfer to.
            If None, the transfer will be on the local filesystem
        :param string remote_user: the remote user to connect with.
             If None, the local user will be used
        :param bool validate: whether to validate the transferred files
            using a supplied file with checksums
        :param string digestfile: a file with checksums for the files to be
            transferred. Must be specified if validate is True. The checksum
            algorithm will be inferred from the extension of the digest file
        :param opts: options that will be passed to the rsync command
        """
        super().__init__(
            src_path=src_path,
            dest_path=dest_path,
            opts=opts or self.DEFAULT_OPTS,
            validate=validate,
            **kwargs,
        )
        self.remote_host = remote_host
        self.remote_user = remote_user
        self.digestfile = digestfile

    def transfer(self, transfer_log=None):
        """Execute the transfer as set up by this instance and, if requested,
        validate the transfer.

        :param string transfer_log: path prefix to log files where stderr
            and stdout streams will be directed if this option is specified
        :returns True on success, False if the validation failed
        :raises transfer.TransferError: if src_path or dest_path were not valid
        :raises transfer.RsyncError: if the rsync command did not exit successfully
        """
        self.validate_src_path()
        self.validate_dest_path()
        command = (
            [self.CMD] + self.format_options() + [self.src_path, self.remote_path()]
        )
        try:
            call_external_command(
                command, with_log_files=(transfer_log is not None), prefix=transfer_log
            )
        except subprocess.CalledProcessError as e:
            raise RsyncError(e)
        return (not self.validate) or self.validate_transfer()

    def remote_path(self):
        """Construct the remote path according to what has been specified.
        :returns: the remote path string on the form
            [remote_user]@[remote_host]:[dest_path]
        """
        return "{}{}{}".format(
            f"{self.remote_user}@" if self.remote_user is not None else "",
            f"{self.remote_host}:" if self.remote_host is not None else "",
            self.dest_path if self.dest_path is not None else "",
        )

    def validate_dest_path(self):
        """Validates the dest_path, remote_user and remote_host attributes
        of this Agent instance.

        :raises transfer.TransferError:
            if the combination of attributes is not valid
        """
        if self.dest_path is None and self.remote_host is None:
            raise TransferError(
                msg="dest_path and remote_host cannot both be None",
                src_path=self.src_path,
            )
        if self.remote_user is not None and self.remote_host is None:
            raise TransferError(
                msg="dest_path cannot be None if remote_user is not None",
                src_path=self.src_path,
            )

    def validate_transfer(self):
        """Validate the transferred files by computing checksums and comparing
        to the pre-computed checksums, supplied in the digestfile attribute
        of this Agent instance. The hash algorithm is inferred from the file
        extension of the digestfile. The paths of the files to check are
        assumed to be relative to the location of the digestfile.

        Currently not implemented for remote transfers.

        :returns: False if any checksum does not match, or if a file does
            not exist. True otherwise.
        :raises transfer.RsyncValidationError: if the digestfile was not
            supplied
        """
        if self.remote_host is not None:
            raise NotImplementedError("Validation on remote host not implemented")
        try:
            with open(self.digestfile) as fh:
                hasher = self.digestfile.split(".")[-1]
                dpath = os.path.dirname(self.digestfile)
                for line in fh:
                    digest, fpath = line.split()
                    tfile = os.path.join(dpath, fpath)
                    if not os.path.exists(tfile) or digest != hashfile(
                        tfile, hasher=hasher
                    ):
                        return False
        except TypeError:
            raise RsyncValidationError(
                "no digest file specified", self.src_path, self.dest_path
            )
        return True


class SymlinkAgent(TransferAgent):
    def __init__(self, src_path, dest_path, overwrite=True, relative=True, **kwargs):
        """Creates an SymlinkAgent instance for creating symlinks.

        :param string src_path: the file or folder that should be symlinked
        :param string dest_path: the destination symlink
        :param bool overwrite: if true, the destination file or folder will
            be overwritten if it already exists
        :param bool relative: if true, the destination symlink will be relative
        """
        super().__init__(src_path=src_path, dest_path=dest_path, **kwargs)
        self.overwrite = overwrite
        self.relative = relative

    def transfer(self):
        """Create the symlink as specified by this SymlinkAgent instance.

        :returns: True if the symlink was created successfully, False otherwise
        :raises transfer.TransferError:
            if src_path or dest_path were not valid
        :raises transfer.SymlinkError:
            if an error occurred when creating the symlink
        """
        self.validate_src_path()
        self.validate_dest_path()
        if os.path.exists(self.dest_path):
            # If the existing target is a symlink that points to the
            # source, we're all good
            if self.validate_transfer():
                logger.debug(
                    "target exists and points to the correct "
                    f'source path: "{self.src_path}"'
                )
                return True
            # If we are not overwriting, return False
            if not self.overwrite:
                logger.debug(
                    f'target "{self.dest_path}" exists and will not be overwritten'
                )
                return False
            # If the target is a mount, let's not mess with it
            if os.path.ismount(self.dest_path):
                raise SymlinkError("target exists and is a mount")
            # If the target is a link or a file, we remove it
            if os.path.islink(self.dest_path) or os.path.isfile(self.dest_path):
                logger.debug(f'removing existing target file "{self.dest_path}"')
                try:
                    os.unlink(self.dest_path)
                except OSError as e:
                    raise SymlinkError(e)
            # If the target is a directory, we remove it and
            # everything underneath
            elif os.path.isdir(self.dest_path):
                logger.debug(f'removing existing target folder "{self.dest_path}"')
                try:
                    shutil.rmtree(self.dest_path)
                except OSError as e:
                    raise SymlinkError(e)
            # If it's something else, let's bail out
            else:
                raise SymlinkError("target exists and will not be overwritten")
        if not create_folder(os.path.dirname(self.dest_path)):
            raise SymlinkError("failed to create target folder hierarchy")
        try:
            # If we should create a relative symlink, determine the relative path
            os.symlink(
                os.path.relpath(self.src_path, os.path.dirname(self.dest_path))
                if self.relative
                else self.src_path,
                self.dest_path,
            )
        except OSError as e:
            raise SymlinkError(e)
        return (not self.validate) or self.validate_transfer()

    def validate_transfer(self):
        """Validates the symlinked files by verifying that the dest_path was
        created, is a link and resolves to the same file as src_path.

        :returns: True if link is valid, False otherwise
        """
        return (
            os.path.exists(self.dest_path)
            and os.path.islink(self.dest_path)
            and os.path.samefile(self.src_path, self.dest_path)
        )


class TransferError(Exception):
    def __init__(self, msg, src_path=None, dest_path=None):
        super().__init__(msg)
        self.src_path = src_path
        self.dest_path = dest_path


class SymlinkError(TransferError):
    pass


class RsyncError(TransferError):
    pass


class RsyncValidationError(TransferError):
    pass
