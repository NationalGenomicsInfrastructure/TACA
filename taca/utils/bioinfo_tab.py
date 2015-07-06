
import socket
import os
import couchdb
import glob

from csv import DictReader
from taca.utils.config import CONFIG
from flowcell_parser.classes import XTenSampleSheetParser


def setupServer(conf):
    db_conf = conf['statusdb']
    url="http://{0}:{1}@{2}:{3}".format(db_conf['username'], db_conf['password'], db_conf['url'], db_conf['port'])
    return couchdb.Server(url)

def merge(d1, d2):
    """ Will merge dictionary d2 into dictionary d1.
    On the case of finding the same key, the one in d1 will be used.
    :param d1: Dictionary object
    :param s2: Dictionary object
    """
    for key in d2:
        if key in d1:
            if isinstance(d1[key], dict) and isinstance(d2[key], dict):
                merge(d1[key], d2[key])
            elif d1[key] == d2[key]:
                pass # same leaf value
        else:
            d1[key] = d2[key]
    return d1

def update_statusdb(run_dir):
    project_ids=get_ss_projects(run_dir)
    run_name = os.path.basename(os.path.abspath(run_dir))
    status=get_status(run_dir)
    couch=setupServer(CONFIG)
    db=couch['bioinfo_analysis']
    view = db.view('full_doc/pj_run_to_doc')
    for p in project_ids:
        obj={'run_id':run_name, 'project_id':p, 'status':status}
        if len(view[[p, run_name]].rows) == 1:
            remote_doc= view[[p, run_name]].rows[0].value
            remote_status=remote_doc["status"]
            if remote_status in ['Incoming', 'Sequencing Done', 'Demultiplexing', 'Demultiplexed', 'Transferring']:
                final_obj=merge(obj, remote_doc)
                db.save(final_obj)
        else:
            db.save(obj)



def get_status(run_dir):
    status='Incoming'

    run_name = os.path.basename(os.path.abspath(run_dir))
    xten_dmux_folder=os.path.join(run_dir, 'Demultiplexing')
    xten_dmux_stats=os.path.join(xten_dmux_folder, 'Stats', 'DemultiplexingStats.xml')
    unaligned_folder=glob.glob(os.path.join(run_dir, 'Unaligned_*'))
    unaligned_dmux_stats=glob.glob(os.path.join(run_dir, 'Unaligned_*', 'Basecall_Stats_*', 'Demultiplexing_Stats.htm'))
    taca_transfer=os.path.join(CONFIG['analysis']['status_dir'], 'transfer.tsv')
    old_transfer=CONFIG['bioinfo_tab']['b5_transfer']

    


    if os.path.exists(os.path.join(run_dir, 'RTAComplete.txt')):
        status='Sequencing Done'
    if os.path.exists(xten_dmux_folder) or unaligned_folder:
        status="Demultiplexing"
    if os.path.exists(xten_dmux_stats) or unaligned_dmux_stats:
        status='Demultiplexed'
    if os.path.exists(os.path.join(run_dir, 'transferring')):
        status='Transferring'
    if os.path.exists(taca_transfer):
        with open(taca_transfer) as t_file:
            for line in t_file:
                if run_name in line:
                    status='Ongoing'

    if os.path.exists(old_transfer):
        with open(old_transfer) as t_file:
            for line in t_file:
                if run_name in line:
                    elements=line.split("\s")
                    if len(elements)==2:
                        status='Transferring'
                    else:
                        status='Ongoing'

    return status




def get_ss_projects(run_dir):
    project_ids=set()
    run_name = os.path.basename(os.path.abspath(run_dir))
    current_year = '20' + run_name[0:2]
    run_name_components = run_name.split("_")
    FCID = run_name_components[3][1:]

    xten_samplesheets_dir = os.path.join(CONFIG['bioinfo_tab']['xten_samplesheets'],
                                    current_year)
    hiseq_samplesheets_dir = os.path.join(CONFIG['bioinfo_tab']['hiseq_samplesheets'],
                                    current_year)
    FCID_samplesheet_origin = os.path.join(hiseq_samplesheets_dir, '{}.csv'.format(FCID))
    #if it is not hiseq
    if not os.path.exists(FCID_samplesheet_origin):
        FCID_samplesheet_origin = os.path.join(xten_samplesheets_dir, '{}.csv'.format(FCID))
        #if it is not xten
        if not os.path.exists(FCID_samplesheet_origin):
            #if it is miseq
            FCID_samplesheet_origin = os.path.join(run_dir,'Data','Intensities','BaseCalls', 'SampleSheet.csv')

        ss_reader=XTenSampleSheetParser(FCID_samplesheet_origin)
        data=ss_reader.data

    else:
        csvf=open(FCID_samplesheet_origin, 'rU')
        data=DictReader(csvf)
    



    for d in data:
        project_ids.add(d['SampleID'].split('_')[0])

    
    return project_ids




    

    
