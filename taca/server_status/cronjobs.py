import datetime
import getpass
import logging
import platform

from crontab import CronTab

from taca.utils import statusdb
from taca.utils.config import CONFIG


def _parse_crontab():
    result = {}
    user = getpass.getuser()
    logging.info(f"Getting crontab for user {user}")
    try:
        crontab = CronTab(user=user)
    except Exception as e:
        logging.error(f"Cannot get a crontab for user: {user}")
        logging.error(e.message)
    else:
        result[user] = []
        for job in crontab.crons:
            # this is for special syntax like @monthly or @reboot
            special_syntax = str(job).split()[0] if str(job).startswith("@") else ""
            result[user].append(
                {
                    "Command": job.command,
                    "Comment": job.comment,
                    "Enabled": job.enabled,
                    "Minute": str(job.minutes),
                    "Hour": str(job.hours),
                    "Day of month": str(job.dom),
                    "Month": str(job.month),
                    "Day of week": str(job.dow),
                    "Special syntax": special_syntax,
                }
            )
    return result


def update_cronjob_db():
    server = platform.node().split(".")[0]
    timestamp = datetime.datetime.now()
    # parse results
    result = _parse_crontab()
    # connect to db
    statusdb_conf = CONFIG.get("statusdb")
    logging.info(
        "Connecting to database: {}".format(CONFIG.get("statusdb", {}).get("url"))
    )
    try:
        couch_connection = statusdb.StatusdbSession(statusdb_conf)
    except Exception as e:
        logging.error(e.message)
    else:
        # update document
        view = couch_connection.connection.post_view(
            db="cronjobs",
            ddoc="server",
            view="alias",
            key=server,
            include_docs=True,
        ).get_result()
        # to be safe
        doc = {}
        # create doc if not exist
        if not view["rows"]:
            logging.info("Creating a document")
            doc = {
                "users": {user: cronjobs for user, cronjobs in result.items()},
                "Last updated": str(timestamp),
                "server": server,
            }
        # else: get existing doc
        else:
            logging.info("Updating the document")
            doc = view["rows"][0]["doc"]
            doc["users"].update(result)
            doc["Last updated"] = str(timestamp)
        if doc:
            try:
                couch_connection.save_db_doc(doc=doc, db="cronjobs")
            except Exception as e:
                logging.error(e.message)
            else:
                logging.info(f"{server} has been successfully updated")
        else:
            logging.warning("Document has not been created/updated")
