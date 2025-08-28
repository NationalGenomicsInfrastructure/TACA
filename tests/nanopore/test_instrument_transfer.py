import os
import subprocess
import tempfile
from textwrap import dedent
from unittest.mock import Mock, mock_open, patch

import pytest

from taca.nanopore import instrument_transfer

DUMMY_RUN_NAME = "20240112_2342_1A_TEST12345_randomhash"


@pytest.fixture
def setup_test_fixture():
    """Set up tempdir to mimic an ONT instrument file system"""

    tmp = tempfile.TemporaryDirectory()

    # Set up args
    args = Mock()
    args.local_runs = tmp.name + "/data"
    args.exclude_dirs = ["nosync", "keep_data", "cg_data"]
    args.nas_runs = tmp.name + "/preproc"
    args.miarka_runs = tmp.name + "/hpc/promethion/"
    args.miarka_settings = ["--chown=:group", "--chmod=Dg+s,g+rw"]
    args.local_archive = tmp.name + "/data/nosync"
    args.minknow_logs = tmp.name + "/minknow_logs"
    args.rsync_log = tmp.name + "/data/rsync_log.txt"
    args.log = tmp.name + "/data/instrument_transfer.log"

    # Create dirs
    for dir in [
        args.local_runs,
        args.nas_runs,
        args.nas_runs + "/nosync",
        args.nas_runs + "/nosync/archived",
        args.miarka_runs,
        args.local_archive,
        args.minknow_logs,
    ]:
        os.makedirs(dir)

    # Create files
    for file_path in [args.log, args.rsync_log]:
        open(file_path, "w").close()

    # Build log dirs
    for position_dir_n, position_dir in enumerate(["1A", "MN19414"]):
        os.makedirs(f"{args.minknow_logs}/{position_dir}")

        # Build log files
        for log_file_n, log_file in enumerate(
            ["control_server_log-1.txt", "control_server_log-2.txt"]
        ):
            # For each flowcell
            for flowcell_n, flowcell in enumerate(["PAM12345", "TEST12345"]):
                # Build log entries
                for log_entry_n, log_entry in enumerate(
                    ["mux_scan_result", "platform_qc.report", "something.else"]
                ):
                    # Sneak build metadata into log entries to retain traceability
                    lines = [
                        f"2024-01-01 0{position_dir_n}:0{log_file_n}:0{flowcell_n}.0{log_entry_n}    INFO: {log_entry} (user_messages)",
                        f"    flow_cell_id: {flowcell}",
                        f"    num_pores: {position_dir_n}{log_file_n}{flowcell_n}{log_entry_n}",
                        f"    total_pores: {position_dir_n}{log_file_n}{flowcell_n}{log_entry_n}",
                    ]

                    with open(
                        args.minknow_logs + f"/{position_dir}/{log_file}", "a"
                    ) as file:
                        file.write("\n".join(lines) + "\n")

    yield args, tmp

    tmp.cleanup()


def test_main_ignore_CTC(setup_test_fixture):
    """Check so that runs on configuration test cells are not picked up."""

    # Run fixture
    args, tmp = setup_test_fixture

    # Setup run
    run_path = (
        f"{args.local_runs}/experiment/sample/{DUMMY_RUN_NAME.replace('TEST', 'CTC')}"
    )
    os.makedirs(run_path)

    with patch("taca.nanopore.instrument_transfer.dump_path") as mock_dump_path:
        # Start testing
        instrument_transfer.main(args)

        # Check dump_path was not called
        mock_dump_path.assert_not_called()


@patch("subprocess.run")
@patch("subprocess.check_output")
@patch("subprocess.Popen")
def test_main_ongoing_run(mock_popen, mock_check_output, mock_run, setup_test_fixture):
    # Run fixture
    args, tmp = setup_test_fixture

    # Configure mock behaviors
    mock_run.return_value.returncode = 0
    mock_popen.return_value.pid = 1234
    mock_check_output.side_effect = subprocess.CalledProcessError(1, "noRsyncRunning")

    # Set up ONT run
    run_path = f"{args.local_runs}/experiment/sample/{DUMMY_RUN_NAME}"
    os.makedirs(run_path)

    # Start testing
    instrument_transfer.main(args)

    # Check rsync was called
    assert mock_popen.call_args_list[0].args[0] == [
        "run-one",
        "rsync",
        "-au",
        f"--log-file={args.rsync_log}",
        run_path,
        args.nas_runs,
    ]
    assert mock_popen.call_args_list[1].args[0] == [
        "run-one",
        "rsync",
        "-au",
        f"--log-file={args.rsync_log}",
        "--chown=:group",
        "--chmod=Dg+s,g+rw",
        run_path,
        args.miarka_runs,
    ]

    # Check path was dumped
    assert os.path.exists(run_path + "/run_path.txt")
    assert open(run_path + "/run_path.txt").read() == "/".join(run_path.split("/")[-3:])

    # Check pore count history was dumped
    assert os.path.exists(run_path + "/pore_count_history.csv")
    assert open(run_path + "/pore_count_history.csv").read() == (
        dedent("""
            flow_cell_id,timestamp,position,type,num_pores,total_pores
            TEST12345,2024-01-01 01:01:01.01,MN19414,qc,1111,1111
            TEST12345,2024-01-01 01:01:01.00,MN19414,mux,1110,1110
            TEST12345,2024-01-01 01:00:01.01,MN19414,qc,1011,1011
            TEST12345,2024-01-01 01:00:01.00,MN19414,mux,1010,1010
            TEST12345,2024-01-01 00:01:01.01,1A,qc,0111,0111
            TEST12345,2024-01-01 00:01:01.00,1A,mux,0110,0110
            TEST12345,2024-01-01 00:00:01.01,1A,qc,0011,0011
            TEST12345,2024-01-01 00:00:01.00,1A,mux,0010,0010
        """).lstrip()
    )


def test_sequencing_finished():
    with patch("os.listdir") as mock_listdir:
        mock_listdir.return_value = ["file1", "file2", "final_summary"]
        assert instrument_transfer.sequencing_finished("path") is True

        mock_listdir.return_value = ["file1", "file2"]
        assert instrument_transfer.sequencing_finished("path") is False


def test_dump_path():
    with patch("builtins.open", new_callable=mock_open) as mock_file:
        instrument_transfer.dump_path("path/to/run")
        mock_file.assert_called_once_with("path/to/run/run_path.txt", "w")


def test_write_finished_indicator():
    with tempfile.TemporaryDirectory() as temp_dir:
        # Call the function with the temporary directory
        result = instrument_transfer.write_finished_indicator(temp_dir)
        # Assert the file was created correctly
        assert os.path.exists(result)
        assert os.path.basename(result) == ".sync_finished"


def test_sync_to_storage():
    with (
        patch("subprocess.Popen") as mock_Popen,
        patch("subprocess.check_output") as mock_check_output,
    ):
        # Configure check_output to simulate no running rsync process
        mock_check_output.side_effect = subprocess.CalledProcessError(1, ["pgrep"])

        instrument_transfer.sync_to_storage(
            run_path="/path/to/run",
            destination="/path/to/destination",
            rsync_log="/path/to/rsync_log",
            background=True,
            settings=[],
        )
        mock_Popen.assert_called_once_with(
            [
                "run-one",
                "rsync",
                "-au",
                "--log-file=/path/to/rsync_log",
                "/path/to/run",
                "/path/to/destination",
            ]
        )


def test_archive_finished_run():
    # Set up tmp dir
    tmp = tempfile.TemporaryDirectory()
    tmp_path = tmp.name

    # Create run dir
    run_path = tmp_path + "/experiment" + "/sample" + f"/{DUMMY_RUN_NAME}"
    os.makedirs(run_path)

    # Create archive dir
    archive_path = tmp_path + "/data/nosync"
    os.makedirs(archive_path)

    # Execute code
    instrument_transfer.archive_finished_run(run_path, archive_path)

    # Assert run is moved to archive dir
    assert os.path.exists(archive_path + f"/{DUMMY_RUN_NAME}")

    # Assert run is removed from original location
    assert not os.path.exists(run_path)

    # Assert experiment and sample dirs are removed if empty
    assert not os.path.exists(tmp_path + "/experiment/sample")
    assert not os.path.exists(tmp_path + "/experiment")

    tmp.cleanup()
