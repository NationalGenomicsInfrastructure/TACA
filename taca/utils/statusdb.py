"""Classes for handling connection to StatusDB."""

import csv
import logging
from datetime import datetime

from ibmcloudant import CouchDbSessionAuthenticator, cloudant_v1

logger = logging.getLogger(__name__)


class StatusdbSession:
    """Wrapper class for couchdb."""

    def __init__(self, config, db=None):
        user = config.get("username")
        password = config.get("password")
        url = config.get("url")
        display_url_string = f"https://{user}:********@{url}"
        self.connection = cloudant_v1.CloudantV1(
            authenticator=CouchDbSessionAuthenticator(user, password)
        )
        self.connection.set_service_url(f"https://{url}")
        try:
            server_info = self.connection.get_server_information().get_result()
            if not server_info:
                raise Exception(f"Connection failed for URL {display_url_string}")
        except Exception as e:
            raise Exception(
                f"Couchdb connection failed for URL {display_url_string} with error: {e}"
            )
        if db:
            self.dbname = db

    def get_entry(self, name, use_id_view=False, db=None):
        """Retrieve entry from a given db for a given name.

        :param name: unique name identifier (primary key, not the uuid)
        """
        dbname = db or self.dbname
        if use_id_view:
            view = self.id_view
        else:
            view = self.name_view
        if not view.get(name, None):
            return None
        return self.connection.get_document(
            db=dbname, doc_id=view.get(name)
        ).get_result()

    def save_db_doc(self, doc, db=None):
        try:
            dbname = db or self.dbname
            self.connection.post_document(db=dbname, document=doc).get_result()
        except Exception as e:
            raise Exception(f"Failed saving document due to {e}")

    def update_doc(self, dbname, obj, over_write_db_entry=False):
        view = self.connection.post_view(
            db=dbname,
            ddoc="info",
            view="name",
            key=obj["name"],
            include_docs=True,
        ).get_result()
        if len(view["rows"]) == 1:
            remote_doc = view["rows"][0]["doc"]
            doc_id = remote_doc.pop("_id")
            doc_rev = remote_doc.pop("_rev")
            if remote_doc != obj:
                if not over_write_db_entry:
                    obj = merge_dicts(obj, remote_doc)
                obj["_id"] = doc_id
                obj["_rev"] = doc_rev
                response = self.connection.put_document(
                    db=dbname, doc_id=doc_id, document=obj
                ).get_result()
                if not response.get("ok"):
                    raise Exception(
                        f"Failed to update document in {dbname} with response: {response}"
                    )
                logger.info(f"Updating {obj['name']}")
        elif len(view["rows"]) == 0:
            response = self.connection.post_document(
                db=dbname, document=obj
            ).get_result()
            if not response.get("ok"):
                raise Exception(
                    f"Failed to create new document in {dbname} with response: {response}"
                )
            logger.info(f"Saving {obj['name']}")
        else:
            logger.warning(f"More than one row with name {obj['name']} found")


class ProjectSummaryConnection(StatusdbSession):
    def __init__(self, config, dbname="projects"):
        super().__init__(config)
        self.dbname = dbname
        self.name_view = {
            row["key"]: row["id"]
            for row in self.connection.post_view(
                db=self.dbname, ddoc="project", view="project_name", reduce=False
            ).get_result()["rows"]
        }
        self.id_view = {
            row["key"]: row["value"]
            for row in self.connection.post_view(
                db=self.dbname, ddoc="project", view="project_id", reduce=False
            ).get_result()["rows"]
        }


class GenericFlowcellRunConnection(StatusdbSession):
    def __init__(self, config, dbname=None):
        super().__init__(config)
        self.dbname = dbname
        self.proj_list = {}

    def get_project_flowcell(
        self, project_id, open_date="2015-01-01", date_format="%Y-%m-%d", dbname=None
    ):
        """From information available in flowcell db connection,
        collect the flowcell this project was sequenced.

        :param project_id: NGI project ID to get the flowcells
        :param open_date: Open date of project to skip the check for all flowcells
        :param date_format: The format of specified open_date
        """
        proj_list = {}
        if not self.proj_list and dbname:
            proj_list = {
                row["key"]: row["value"]
                for row in self.connection.post_view(
                    db=dbname, ddoc="names", view="project_ids_list", reduce=False
                ).get_result()["rows"]
                if row["key"]
            }
        else:
            proj_list = self.proj_list
        try:
            open_date = datetime.strptime(open_date, date_format)
        except:
            open_date = datetime.strptime("2015-01-01", "%Y-%m-%d")

        project_flowcells = {}
        time_format = (
            "%y%m%d"
            if type(self)
            in (X_FlowcellRunMetricsConnection, FlowcellRunMetricsConnection)
            or dbname in ("x_flowcells", "flowcells")
            else "%Y%m%d"
        )
        date_sorted_fcs = sorted(
            list(proj_list.keys()),
            key=lambda k: datetime.strptime(k.split("_")[0], time_format),
            reverse=True,
        )
        for fc in date_sorted_fcs:
            if type(self) in (
                X_FlowcellRunMetricsConnection,
                FlowcellRunMetricsConnection,
            ) or dbname in ("x_flowcells", "flowcells"):
                fc_date, fc_name = fc.split("_")
            elif type(self) is NanoporeRunsConnection or dbname == "nanopore_runs":
                fc_date, fc_time, position, fc_name, fc_hash = fc.split(
                    "_"
                )  # 20220721_1216_1G_PAM62368_3ae8de85
            elif type(self) is ElementRunsConnection or dbname == "element_runs":
                fc_date, run_on, fc_name = fc.split("_")

            if datetime.strptime(fc_date, time_format) < open_date:
                break

            if project_id in proj_list[fc] and fc_name not in project_flowcells.keys():
                project_flowcells[fc_name] = {
                    "name": fc_name,
                    "run_name": fc,
                    "date": fc_date,
                    "db": dbname or self.dbname,
                }
        return project_flowcells


class FlowcellRunMetricsConnection(GenericFlowcellRunConnection):
    def __init__(self, config, dbname="flowcells"):
        super().__init__(config)
        self.dbname = dbname
        self.name_view = {
            row["key"]: row["id"]
            for row in self.connection.post_view(
                db=self.dbname, ddoc="names", view="name", reduce=False
            ).get_result()["rows"]
        }
        self.proj_list = {
            row["key"]: row["value"]
            for row in self.connection.post_view(
                db=self.dbname, ddoc="names", view="project_ids_list", reduce=False
            ).get_result()["rows"]
            if row["key"]
        }


class X_FlowcellRunMetricsConnection(GenericFlowcellRunConnection):
    def __init__(self, config, dbname="x_flowcells"):
        super().__init__(config)
        self.dbname = dbname
        self.name_view = {
            row["key"]: row["id"]
            for row in self.connection.post_view(
                db=self.dbname, ddoc="names", view="name", reduce=False
            ).get_result()["rows"]
        }
        self.proj_list = {
            row["key"]: row["value"]
            for row in self.connection.post_view(
                db=self.dbname, ddoc="names", view="project_ids_list", reduce=False
            ).get_result()["rows"]
            if row["key"]
        }


class NanoporeRunsConnection(GenericFlowcellRunConnection):
    def __init__(self, config, dbname="nanopore_runs"):
        super().__init__(config)
        self.dbname = dbname

    def check_run_exists(self, ont_run) -> bool:
        ont_run_row = self.connection.post_view(
            db=self.dbname,
            ddoc="names",
            view="name",
            key=ont_run.run_name,
        ).get_result()["rows"]
        if len(ont_run_row) > 0:
            return True
        else:
            return False

    def check_run_status(self, ont_run) -> str:
        ont_run_doc = self.connection.post_view(
            db=self.dbname,
            ddoc="names",
            view="name",
            key=ont_run.run_name,
            include_docs=True,
        ).get_result()["rows"][0]["doc"]
        return ont_run_doc["run_status"]

    def create_ongoing_run(
        self, ont_run, run_path_file: str, pore_count_history_file: str
    ):
        run_path = open(run_path_file).read().strip()

        pore_counts = []
        with open(pore_count_history_file) as stream:
            for line in csv.DictReader(stream):
                pore_counts.append(line)

        new_doc = {
            "run_path": run_path,
            "run_status": "ongoing",
            "pore_count_history": pore_counts,
        }

        response = self.connection.post_document(
            db=self.dbname, document=new_doc
        ).get_result()
        if not response.get("ok"):
            raise Exception(
                f"Failed to create new document in {self.dbname} with response: {response}"
            )
        logger.info(
            f"New database entry created: {ont_run.run_name}, id {response['id']}, rev {response['rev']}"
        )

    def finish_ongoing_run(self, ont_run, dict_json: dict):
        doc = self.connection.post_view(
            db=self.dbname,
            ddoc="names",
            view="name",
            key=ont_run.run_name,
            include_docs=True,
        ).get_result()["rows"][0]["doc"]

        doc.update(dict_json)
        doc["run_status"] = "finished"
        response = self.connection.put_document(
            db=self.dbname,
            doc_id=doc["_id"],
            document=doc,
        ).get_result()
        if not response.get("ok"):
            raise Exception(
                f"Failed to update document in {self.dbname} with response: {response}"
            )


class ElementRunsConnection(GenericFlowcellRunConnection):
    def __init__(self, config, dbname="element_runs"):
        super().__init__(config)
        self.dbname = dbname

    def get_db_entry(self, run_id, get_doc=False):
        query_result = self.connection.post_view(
            db=self.dbname,
            ddoc="info",
            view="id",
            key=run_id,
            include_docs=get_doc,
        ).get_result()
        if query_result["rows"]:
            return query_result["rows"][0]
        else:
            return None

    def check_if_run_exists(self, run_id) -> bool:
        return self.get_db_entry(run_id) is not None

    def check_db_run_status(self, run_name) -> str:
        query_result = self.connection.post_view(
            db=self.dbname,
            ddoc="info",
            view="status",
            key=run_name,
        ).get_result()

        status = "Unknown"
        if query_result["rows"]:
            status = query_result["rows"][0]["value"]

        return status

    def upload_to_statusdb(self, run_obj: dict):
        self.update_doc(self.dbname, run_obj)


def merge_dicts(d1, d2):
    """Merge dictionary d2 into dictionary d1.
    If the same key is found, the one in d1 will be used.
    """
    for key in d2:
        if key in d1:
            if isinstance(d1[key], dict) and isinstance(d2[key], dict):
                merge_dicts(d1[key], d2[key])
            elif d1[key] == d2[key]:
                pass  # same leaf value
            else:
                logger.debug(
                    f"Values for key {key} in d1 and d2 differ, using the value of d1"
                )
        else:
            d1[key] = d2[key]
    return d1
