"""Storage methods and utilities"""

import logging
import os
import re
from collections import defaultdict
from datetime import datetime
from glob import glob

from taca.utils import filesystem, misc, statusdb
from taca.utils.config import CONFIG, load_config

logger = logging.getLogger(__name__)


def cleanup_miarka(
    days_fastq,
    days_analysis,
    only_fastq,
    only_analysis,
    clean_undetermined,
    status_db_config,
    exclude_projects,
    list_only,
    date,
    dry_run=False,
):
    """Remove fastq/analysis data for projects that have been closed more than given
    days (as days_fastq/days_analysis) from the given 'miarka' cluster.

    :param int days_fastq: Days to consider to remove fastq files for project
    :param int days_analysis: Days to consider to remove analysis data for project
    :param bool only_fastq: Remove only fastq files for closed projects
    :param bool only_analysis: Remove only analysis data for closed projects
    :param bool dry_run: Will summarize what is going to be done without really doing it

    Example format for entry in the taca config file
    cleanup:
        miarka:
            flowcell:
                ##this path is nothing but incoming directory, can given multiple paths
                root:
                    - path/to/flowcells_dir
                relative_project_source: Demultiplexing
                undet_file_pattern: "Undetermined_*.fastq.gz"

            ##this is path where projects are organized
            data_dir: path/to/data_dir
            analysis:
                ##directory where analysis are perfoemed for projects
                root: path/to/analysis_dir
                #should be exactly same as the qc folder name and files wished to be removed
                files_to_remove:
                    piper_ngi:
                        - "*.bam"
    """
    try:
        config = CONFIG["cleanup"]["miarka"]
        flowcell_dir_root = config["flowcell"]["root"]
        flowcell_project_source = config["flowcell"]["relative_project_source"]
        flowcell_undet_files = config["flowcell"]["undet_file_pattern"]
        data_dir = config["data_dir"]
        analysis_dir = config["analysis"]["root"]
        analysis_data_to_remove = config["analysis"]["files_to_remove"]
        if date:
            date = datetime.strptime(date, "%Y-%m-%d")
    except KeyError as e:
        logger.error(
            f"Config file is missing the key {str(e)}, make sure it has all required information"
        )
        raise SystemExit
    except ValueError:
        logger.error(
            'Date given with "--date" option is not in required format, see help for more info'
        )
        raise SystemExit

    # make a connection for project db
    db_config = load_config(status_db_config)
    pcon = statusdb.ProjectSummaryConnection(db_config.get("statusdb"))
    assert pcon, "Could not connect to project database in StatusDB"

    # make exclude project list if provided
    exclude_list = []
    if exclude_projects:
        if os.path.isfile(exclude_projects):
            with open(exclude_projects) as in_file:
                exclude_list.extend([p.strip() for p in in_file.readlines()])
        else:
            exclude_list.extend(exclude_projects.split(","))
        # sanity check for mentioned project to exculde or valid
        invalid_projects = [
            p
            for p in exclude_list
            if p not in pcon.id_view.keys() and p not in pcon.name_view.keys()
        ]
        if invalid_projects:
            logger.error(
                '"--exclude_projects" was called with some invalid projects "{}", '
                "provide valid project name/id".format(",".join(invalid_projects))
            )
            raise SystemExit

    # compile list for project to delete
    project_clean_list, project_processed_list = ({}, [])
    if not list_only and not clean_undetermined:
        logger.info("Building initial project list for removing data...")
    if only_fastq:
        logger.info(
            'Option "--only_fastq" is given, so will not look for analysis data'
        )
    elif only_analysis:
        logger.info(
            'Option "--only_analysis" is given, so will not look for fastq data'
        )

    if clean_undetermined:
        all_undet_files = []
        for flowcell_dir in flowcell_dir_root:
            for fc in [
                d
                for d in os.listdir(flowcell_dir)
                if re.match(filesystem.RUN_RE_ILLUMINA, d)
            ]:
                fc_abs_path = os.path.join(flowcell_dir, fc)
                with filesystem.chdir(fc_abs_path):
                    if not os.path.exists(flowcell_project_source):
                        logger.warn(
                            f'Flowcell {fc} does not contain a "{flowcell_project_source}" directory'
                        )
                        continue
                    projects_in_fc = [
                        d
                        for d in os.listdir(flowcell_project_source)
                        if re.match(r"^[A-Z]+[_\.]+[A-Za-z]+_\d\d_\d\d$", d)
                        and not os.path.exists(
                            os.path.join(flowcell_project_source, d, "cleaned")
                        )
                    ]
                    # the above check looked for project directories and also that are not cleaned
                    # so if it could not find any project, means there is no project diretory at all
                    # or all the project directory is already cleaned. Then we can remove the undet
                    if len(projects_in_fc) > 0:
                        continue
                    fc_undet_files = glob(
                        os.path.join(flowcell_project_source, flowcell_undet_files)
                    )
                    if fc_undet_files:
                        logger.info(
                            f"All projects was cleaned for FC {fc}, found {len(fc_undet_files)} undeterminded files"
                        )
                        all_undet_files.extend(
                            list(map(os.path.abspath, fc_undet_files))
                        )
        if all_undet_files:
            undet_size = _def_get_size_unit(sum(map(os.path.getsize, all_undet_files)))
            if misc.query_yes_no(
                f"In total found {len(all_undet_files)} undetermined files which are {undet_size} in size, delete now ?",
                default="no",
            ):
                _remove_files(all_undet_files)
        return
    elif only_analysis:
        for pid in [
            d
            for d in os.listdir(analysis_dir)
            if re.match(r"^P\d+$", d)
            and not os.path.exists(os.path.join(analysis_dir, d, "cleaned"))
        ]:
            os.path.join(analysis_dir, pid)
            proj_info = get_closed_proj_info(
                pid, pcon.get_entry(pid, use_id_view=True), date
            )
            if proj_info and proj_info["closed_days"] >= days_analysis:
                # move on if this project has to be excluded
                if (
                    proj_info["name"] in exclude_list
                    or proj_info["pid"] in exclude_list
                ):
                    continue
                analysis_data, analysis_size = collect_analysis_data_miarka(
                    pid, analysis_dir, analysis_data_to_remove
                )
                proj_info["analysis_to_remove"] = analysis_data
                proj_info["analysis_size"] = analysis_size
                proj_info["fastq_to_remove"] = "not_selected"
                proj_info["fastq_size"] = 0
                project_clean_list[proj_info["name"]] = proj_info
    else:
        for flowcell_dir in flowcell_dir_root:
            for fc in [
                d
                for d in os.listdir(flowcell_dir)
                if re.match(filesystem.RUN_RE_ILLUMINA, d)
            ]:
                fc_abs_path = os.path.join(flowcell_dir, fc)
                with filesystem.chdir(fc_abs_path):
                    if not os.path.exists(flowcell_project_source):
                        logger.warn(
                            f'Flowcell {fc} do not contain a "{flowcell_project_source}" direcotry'
                        )
                        continue
                    projects_in_fc = [
                        d
                        for d in os.listdir(flowcell_project_source)
                        if re.match(r"^[A-Z]+[_\.]+[A-Za-z0-9]+_\d\d_\d\d$", d)
                        and not os.path.exists(
                            os.path.join(flowcell_project_source, d, "cleaned")
                        )
                    ]
                    for _proj in projects_in_fc:
                        proj = re.sub(r"_+", ".", _proj, 1)
                        # if a project is already processed no need of fetching it again from status db
                        if proj in project_processed_list:
                            # if the project is closed more than threshold days collect the fastq files from FC
                            # no need of looking for analysis data as they would have been collected in the first time
                            if (
                                proj in project_clean_list
                                and project_clean_list[proj]["closed_days"]
                                >= days_fastq
                            ):
                                fc_fq_files, fq_size = collect_fastq_data_miarka(
                                    fc_abs_path,
                                    os.path.join(flowcell_project_source, _proj),
                                )
                                project_clean_list[proj]["fastq_to_remove"][
                                    "flowcells"
                                ][fc] = fc_fq_files["flowcells"][fc]
                                project_clean_list[proj]["fastq_size"] += fq_size
                            continue
                        project_processed_list.append(proj)
                        # by default assume all projects are not old enough for delete
                        fastq_data, analysis_data = ("young", "young")
                        fastq_size, analysis_size = (0, 0)
                        proj_info = get_closed_proj_info(
                            proj, pcon.get_entry(proj), date
                        )
                        if proj_info:
                            # move on if this project has to be excluded
                            if (
                                proj_info["name"] in exclude_list
                                or proj_info["pid"] in exclude_list
                            ):
                                continue
                            # if project not old enough for fastq files and only fastq files selected move on to next project
                            if proj_info["closed_days"] >= days_fastq:
                                fastq_data, fastq_size = collect_fastq_data_miarka(
                                    fc_abs_path,
                                    os.path.join(flowcell_project_source, _proj),
                                    data_dir,
                                    proj_info["pid"],
                                )
                            if not only_fastq:
                                # if project is old enough for fastq files and not 'only_fastq' try collect analysis files
                                if proj_info["closed_days"] >= days_analysis:
                                    (
                                        analysis_data,
                                        analysis_size,
                                    ) = collect_analysis_data_miarka(
                                        proj_info["pid"],
                                        analysis_dir,
                                        analysis_data_to_remove,
                                    )
                                # if both fastq and analysis files are not old enough move on
                                if (analysis_data == fastq_data) or (
                                    (not analysis_data or analysis_data == "cleaned")
                                    and fastq_data == "young"
                                ):
                                    continue
                            elif fastq_data == "young":
                                continue
                            else:
                                analysis_data = "not_selected"
                            proj_info["fastq_to_remove"] = fastq_data
                            proj_info["fastq_size"] = fastq_size
                            proj_info["analysis_to_remove"] = analysis_data
                            proj_info["analysis_size"] = analysis_size
                            project_clean_list[proj] = proj_info

    if not project_clean_list:
        logger.info("There are no projects to clean")
        return

    # list only the project and exit if 'list_only' option is selected
    if list_only:
        print(
            "Project ID\tProject Name\tBioinfo resp.\tClosed Days\tClosed Date\tFastq size\tAnalysis size"
        )
        for p_info in sorted(
            list(project_clean_list.values()),
            key=lambda d: d["closed_days"],
            reverse=True,
        ):
            print(
                "\t".join(
                    [
                        p_info["name"],
                        p_info["pid"],
                        p_info["bioinfo_responsible"],
                        str(p_info["closed_days"]),
                        p_info["closed_date"],
                        _def_get_size_unit(p_info["fastq_size"]),
                        _def_get_size_unit(p_info["analysis_size"]),
                    ]
                )
            )
        raise SystemExit

    logger.info(
        f"Initial list is built with {len(project_clean_list)} projects {get_files_size_text(project_clean_list)}"
    )
    if misc.query_yes_no("Interactively filter projects for cleanup ?", default="yes"):
        filtered_project, proj_count = ([], 0)
        # go through complied project list and remove files
        for proj, info in project_clean_list.items():
            proj_count += 1
            if not misc.query_yes_no(
                f"{get_proj_meta_info(info, days_fastq)}Delete files for this project ({proj_count}/{len(project_clean_list)})",
                default="no",
            ):
                logger.info(f"Will not remove files for project {proj}")
                filtered_project.append(proj)
        # remove projects that were decided not to delete
        map(project_clean_list.pop, filtered_project)
        logger.info(
            f"Removed {len(filtered_project)}/{proj_count} projects from initial list"
        )
        if not project_clean_list:
            logger.info("There are no projects to clean after filtering")
            return
        logger.info(
            f"Final list is created with {len(project_clean_list)} projects {get_files_size_text(project_clean_list)}"
        )
        if not misc.query_yes_no("Proceed with cleanup ?", default="no"):
            logger.info("Aborting cleanup")
            return
    logger.info("Will start cleaning up project now")

    for proj, info in project_clean_list.items():
        fastq_info = info.get("fastq_to_remove")
        if fastq_info and isinstance(fastq_info, dict):
            logger.info(f"Cleaning fastq files for project {proj}")
            fastq_fc = fastq_info.get("flowcells", {})
            removed_fc = []
            for fc, fc_info in fastq_fc.items():
                proj_fc_root = fc_info["proj_root"]
                logger.info(f"Removing fastq files from {proj_fc_root}")
                if not dry_run:
                    if _remove_files(fc_info["fq_files"]):
                        logger.info(
                            f"Removed fastq files from FC {fc} for project {proj}, marking it as cleaned"
                        )
                        _touch_cleaned(proj_fc_root)
                        removed_fc.append(fc)
            if len(fastq_fc) == len(removed_fc):
                try:
                    proj_data_root = fastq_info["proj_data"]["proj_data_root"]
                    logger.info(
                        f"All flowcells cleaned for this project, marking it as cleaned in {proj_data_root}"
                    )
                    _touch_cleaned(proj_data_root)
                except:
                    pass

        analysis_info = info.get("analysis_to_remove")
        if analysis_info and isinstance(analysis_info, dict):
            proj_analysis_root = analysis_info["proj_analysis_root"]
            logger.info(f"cleaning analysis data for project {proj}")
            removed_qc = []
            for qc, files in analysis_info["analysis_files"].items():
                logger.info(f'Removing files of "{qc}" from {proj_analysis_root}')
                if not dry_run:
                    if _remove_files(files):
                        removed_qc.append(qc)
                    else:
                        logger.warn(
                            f'Could not remove some files in qc directory "{qc}"'
                        )
            map(analysis_info["analysis_files"].pop, removed_qc)
            if len(analysis_info["analysis_files"]) == 0:
                logger.info(
                    f"Removed analysis data for project {proj}, marking it cleaned"
                )
                _touch_cleaned(proj_analysis_root)


#############################################################
# Class helper methods, not exposed as commands/subcommands #
#############################################################


def get_closed_proj_info(prj, pdoc, tdate=None):
    """Check and return a dict if project is closed."""
    pdict = None
    if not tdate:
        tdate = datetime.today()
    if not pdoc:
        logger.warn(
            f"Seems like project {prj} does not have a proper statusdb document, skipping it"
        )
    elif "close_date" in pdoc:
        closed_date = pdoc["close_date"]
        try:
            closed_days = tdate - datetime.strptime(closed_date, "%Y-%m-%d")
            pdict = {
                "name": pdoc.get("project_name"),
                "pid": pdoc.get("project_id"),
                "closed_date": closed_date,
                "closed_days": closed_days.days,
                "bioinfo_responsible": pdoc.get("project_summary", {}).get(
                    "bioinfo_responsible", ""
                ),
            }
        except:
            logger.warn(
                "Problem calculating closed days for project {} with close date {}. Skipping it".format(
                    pdoc.get("project_name"), closed_date
                )
            )
    return pdict


def collect_analysis_data_miarka(pid, analysis_root, files_ext_to_remove={}):
    """Collect the analysis files that have to be removed from Miarka
    return a tuple with files and total size of collected files."""
    size = 0
    proj_abs_path = os.path.join(analysis_root, pid)
    if not os.path.exists(proj_abs_path):
        file_list = None
    elif os.path.exists(os.path.join(proj_abs_path, "cleaned")):
        file_list = "cleaned"
    else:
        file_list = {
            "proj_analysis_root": proj_abs_path,
            "analysis_files": defaultdict(list),
        }
        for qc_type, ext in files_ext_to_remove.items():
            qc_path = os.path.join(proj_abs_path, qc_type)
            if os.path.exists(qc_path):
                file_list["analysis_files"][qc_type].extend(
                    collect_files_by_ext(qc_path, ext)
                )
    try:
        size += sum(
            [
                sum(map(os.path.getsize, fls))
                for fls in file_list["analysis_files"].values()
            ]
        )
    except:
        pass
    return (file_list, size)


def collect_fastq_data_miarka(fc_root, fc_proj_src, proj_root=None, pid=None):
    """Collect the fastq files that have to be removed from Miarka.
    Return a tuple with files and total size of collected files."""
    size = 0
    file_list = {"flowcells": defaultdict(dict)}
    fc_proj_path = os.path.join(fc_root, fc_proj_src)
    fc_id = os.path.basename(fc_root)
    file_list["flowcells"][fc_id] = {
        "proj_root": fc_proj_path,
        "fq_files": collect_files_by_ext(fc_proj_path, "*.fastq.gz"),
    }
    if proj_root and pid:
        proj_abs_path = os.path.join(proj_root, pid)
        if not os.path.exists(proj_abs_path):
            file_list["proj_data"] = None
        elif os.path.exists(os.path.join(proj_abs_path, "cleaned")):
            file_list["proj_data"] = "cleaned"
        else:
            file_list["proj_data"] = {
                "proj_data_root": proj_abs_path,
                "fastq_files": collect_files_by_ext(proj_abs_path, "*.fastq.gz"),
            }
    size += sum(map(os.path.getsize, file_list["flowcells"][fc_id]["fq_files"]))
    return (file_list, size)


def collect_files_by_ext(path, ext=[]):
    """Collect files with a given extension from a given path."""
    if isinstance(ext, str):
        ext = [ext]
    collected_files = []
    for root, dirs, files in os.walk(path):
        for e in ext:
            collected_files.extend(glob(os.path.join(root, e)))
        for d in dirs:
            collected_files.extend(collect_files_by_ext(d, ext))
    return collected_files


def get_proj_meta_info(info, days_fastq):
    """From given info collect meta info for a project."""
    template = "\n"

    def _get_template_string(h, v):
        try:
            v = f"{h}: {v}\n"
        except:
            v = f"{h}: Problem getting this"
        return v

    template += _get_template_string("Project overview", info.get("name"))
    template += _get_template_string("Project ID", info.get("pid"))
    template += _get_template_string(
        "Bioinfo Responsible", info.get("bioinfo_responsible", "")
    )
    template += _get_template_string("Closed for (days)", info.get("closed_days"))
    template += _get_template_string("Closed from (date)", info.get("closed_date"))

    # set analysis info based upon what we have
    analysis_info = info.get("analysis_to_remove")
    if not analysis_info:
        template += "Project analysis: No analysis directory\n"
    elif isinstance(analysis_info, str) and analysis_info == "cleaned":
        template += "Project analysis: Analysis directory already cleaned\n"
    elif isinstance(analysis_info, dict):
        f_stat = []
        for qc_type, files in analysis_info["analysis_files"].items():
            f_stat.append(f"{qc_type} ({len(files)} files)")
        template += "Project analyzed: {}\n".format(", ".join(f_stat))

    # set fastq info based upon what we have
    fq_info = info.get("fastq_to_remove")
    if isinstance(fq_info, str) and fq_info == "young":
        template += f"Project been closed less than {days_fastq} days, so will not remove any fastq files\n"
    elif isinstance(fq_info, dict):
        proj_fq_info = fq_info.get("proj_data")
        if not proj_fq_info:
            template += "Project organized: No organized directory for project\n"
        elif isinstance(proj_fq_info, str) and proj_fq_info == "cleaned":
            template += "Project organized: Project directory is already cleaned\n"
        elif isinstance(proj_fq_info, dict):
            template += (
                "Project organized: Project is organized with {} fastq files\n".format(
                    len(proj_fq_info["fastq_files"])
                )
            )
        fc_fq_info = fq_info.get("flowcells", {})
        fc_num = len(fc_fq_info.keys())
        fc_files = sum(
            map(len, [fc_info.get("fq_files", []) for fc_info in fc_fq_info.values()])
        )
        template += (
            f"Flowcells: There are {fc_num} FC with total {fc_files} fastq files\n"
        )
    template += "Estimated data size: {}\n".format(
        _def_get_size_unit(info.get("fastq_size", 0) + info.get("fastq_size", 0))
    )

    return template


def get_files_size_text(plist):
    """Get project list dict and give back string with overll sizes."""
    fsize = _def_get_size_unit(sum([i.get("fastq_size", 0) for i in plist.values()]))
    asize = _def_get_size_unit(sum([i.get("analysis_size", 0) for i in plist.values()]))
    return "({f}{s}{a}) ".format(
        f=f"~{fsize} fastq data" if fsize else "",
        a=f"~{asize} analysis data" if asize else "",
        s=" and " if fsize and asize else "",
    )


def _def_get_size_unit(s):
    """Change the given size to appropriate unit measurement for better readability."""
    kb = 1000
    mb = kb * 1000
    gb = mb * 1000
    tb = gb * 1000
    if s > tb:
        s = f"~{int(s/tb)}tb"
    elif s > gb:
        s = f"~{int(s/gb)}gb"
    elif s > mb:
        s = f"~{int(s/mb)}mb"
    elif s > kb:
        s = f"~{int(s/kb)}kb"
    elif s > 0:
        s = f"~{int(s)}b"
    return str(s)


def _remove_files(files):
    """Remove files from given list."""
    status = True
    for fl in files:
        try:
            os.remove(fl)
        except Exception as e:
            logger.warn(f'Could not remove file {fl} due to "{e.message}"')
            status = False
    return status


def _touch_cleaned(path):
    """Touch a 'cleaned' file in a given path."""
    try:
        open(os.path.join(path, "cleaned"), "w").close()
    except Exception as e:
        logger.warn(
            f'Could not create "cleaned" file in path {path} due to "{e.message}"'
        )
