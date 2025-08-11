import datetime
import logging
import subprocess

from taca.utils import statusdb
from taca.utils.config import CONFIG
from taca.utils.misc import send_mail


def get_nases_disk_space():
    result = {}
    config = CONFIG["server_status"]
    servers = config.get("servers", dict())

    for server_name, path_vars in servers.items():
        # Get command
        command = config["command"]
        path = path_vars["path"]
        if path_vars["url"] == "localhost":
            command = f"{command} {path}".split()
        else:
            command = f"{command} {path}"
            if "promethion" in server_name:
                user = "prom"
            else:
                user = config["user"]
            # Connect via ssh to server and execute the command
            command = ["ssh", "-t", f"{user}@{path_vars['url']}", command]

        result[server_name] = _run_cmd(command)

    # Storage systems are mouted locally, e.g. ngi-nas
    for storage_system, path in config.get("storage_systems", {}).items():
        # Get command
        command = "{command} {path}".format(command=config["command"], path=path)
        result[storage_system] = _run_cmd(command.split())

    return result


def _run_cmd(command):
    proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output = proc.stdout.read().decode("utf-8")
    return _parse_output(output)


def _parse_output(output):  # for nases
    # command = df -h /home
    # output = Filesystem            Size  Used Avail Use% Mounted on
    # /dev/mapper/VGStor-lv_illumina
    #                   24T   12T   13T  49% /srv/illumina

    output = output.strip()
    output = output.split()
    try:
        mounted_on = output[-1]
        used_percentage = output[-2]
        space_available = output[-3]
        space_used = output[-4]
        disk_size = output[-5]
        filesystem = output[-6]

        available_percentage = str(100 - int(used_percentage.replace("%", ""))) + "%"

        result = {
            "disk_size": disk_size,
            "space_used": space_used,
            "space_available": space_available,
            "used_percentage": used_percentage,
            "available_percentage": available_percentage,
            "mounted_on": mounted_on,
            "filesystem": filesystem,
        }
    except:
        # Sometimes it fails for whatever reason as Popen returns not what it is supposed to
        result = {
            "disk_size": "NaN",
            "space_used": "NaN",
            "space_available": "NaN",
            "used_percentage": "NaN",
            "available_percentage": "NaN",
            "mounted_on": "NaN",
            "filesystem": "NaN",
        }
        logging.error(f"Can not parse the output: {output}")

    return result


def update_status_db(data, server_type=None):
    """Pushed the data to status db.

    data can be from nases
    server_type should be 'nas'.
    """
    db_config = CONFIG.get("statusdb")
    if db_config is None:
        logging.error('"statusdb" must be present in the config file!')
        raise RuntimeError('"statusdb" must be present in the config file!')
    try:
        couch_connection = statusdb.StatusdbSession(db_config)
    except Exception as e:
        logging.error(e.message)
        raise

    for key in data.keys():  # data is dict of dicts
        server = data[key]  # data[key] is dictionary (the command output)
        server["name"] = key  # key is nas url
        # datetime.datetime(2015, 11, 18, 9, 54, 33, 473189) is not JSON serializable
        server["time"] = datetime.datetime.now().isoformat()
        server["server_type"] = server_type or "unknown"

        try:
            logging.info(f"Updating server status for {key}")
            couch_connection.save_db_doc(doc=server, db="server_status")
        except Exception as e:
            logging.error(e.message)
            raise
        else:
            logging.info(f"{key}: Server status has been updated")


def check_promethion_status():
    config = CONFIG.get("promethion_status")
    server = config.get("server")
    path = config.get("path")
    command = config.get("command")
    command_to_run = f"{command} {path}"
    user = config.get("user")

    try:
        subprocess.run(["ssh", "-t", f"{user}@{server}", command_to_run], check=True)
    except subprocess.CalledProcessError:
        _send_promethion_warning_email()
        return False
    return True


def _send_promethion_warning_email():
    email_recipients = CONFIG.get("mail").get("recipients")
    email_subject = "An issue with the PromethION has been detected."
    email_message = (
        "An issue with the PromethION has been detected. "
        "Please investigate and consider pausing the transfer cronjob on preproc1"
    )
    send_mail(email_subject, email_message, email_recipients)
