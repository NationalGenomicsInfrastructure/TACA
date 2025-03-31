import importlib
import os
import shutil
from tempfile import TemporaryDirectory
from unittest.mock import patch

import yaml

from taca.analysis import analysis
from taca.log import init_logger_file


def make_illumina_test_config(tmp):
    test_config_yaml_string = f"""mail: 
    recipients: mock
statusdb: mock
log:
    file: {tmp.name}/log/taca.log
    log_level: DEBUG
analysis:
    MiSeq:
        QC:
            max_number_undetermined_reads_simple_lane: 5000000
            max_percentage_undetermined_indexes_simple_lane: 5
            max_percentage_undetermined_indexes_NoIndex_lane: 30
            max_frequency_most_represented_und_index_NoIndex_lane: 40
        bclconvert:
            Version: bcl-convert Version 00.000.000.4.2.4
            bin: mock
            tenX_index_path: {tmp.name}/config/Chromium_10X_indexes.txt
            smartseq_index_path: {tmp.name}/config/Smart-seq3_v1.5.csv
            index_path: {tmp.name}/config/Chromium_10X_indexes.txt
            options:
                common:
                    - bcl-sampleproject-subdirectories: true
                    - fastq-gzip-compression-level: 4
                    - output-directory: Demultiplexing
                    - output-legacy-stats: true
                    - sample-name-column-enabled: true
                    - shared-thread-odirect-output: true
                    - strict-mode: false
                    - force
                SMARTSEQ:
                    - no-lane-splitting: true
            settings:
                common:
                    - MinimumTrimmedReadLength: 0
                    - MaskShortReads: 0
                10X_SINGLE:
                    - CreateFastqForIndexReads: 1
                IDT_UMI:
                    - CreateFastqForIndexReads: 1
                    - TrimUMI: 0
                SMARTSEQ:
                    - CreateFastqForIndexReads: 1
                NOINDEX:
                    - CreateFastqForIndexReads: 1
                    - BarcodeMismatchesIndex1: 0
                    - BarcodeMismatchesIndex2: 0
                short_single_index:
                    - BarcodeMismatchesIndex1: 0
                    - BarcodeMismatchesIndex2: 0
        bcl2fastq:
            Version: bcl2fastq_v2.20.0.422
            bin: mock
            index_path: {tmp.name}/config/Chromium_10X_indexes.txt
            tenX_index_path: {tmp.name}/config/Chromium_10X_indexes.txt
            smartseq_index_path: {tmp.name}/config/Smart-seq3_v1.5.csv
            options:
                common:
                    - output-dir: Demultiplexing
                    - loading-threads: 1
                    - processing-threads: 8
                    - writing-threads: 1
                    - minimum-trimmed-read-length: 0
                    - mask-short-adapter-reads: 0
                    - ignore-missing-positions
                    - ignore-missing-controls
                    - ignore-missing-filter
                    - ignore-missing-bcls
                10X_SINGLE:
                    - create-fastq-for-index-reads
                SMARTSEQ:
                    - create-fastq-for-index-reads
                    - no-lane-splitting
                NOINDEX:
                    - create-fastq-for-index-reads
                    - barcode-mismatches: 0
                short_single_index:
                    - barcode-mismatches: 0
        samplesheets_dir: ''
        analysis_server:
            host: miarka1.uppmax.uu.se
            port:
            user: funk_903
            sync:
                data_archive: ""
                include:
                    - "*.xml"
                    - "*.htm"
                    - "*.html"
                    - "*.csv"
                    - "*.fastq.gz"
    NovaSeq:
        bclconvert:
            Version: bcl-convert Version 00.000.000.4.2.4
            bin: mock
            tenX_index_path: {tmp.name}/config/Chromium_10X_indexes.txt
            smartseq_index_path: {tmp.name}/config/Smart-seq3_v1.5.csv
            index_path: {tmp.name}/config/Chromium_10X_indexes.txt
            options:
                common:
                    - bcl-sampleproject-subdirectories: true
                    - fastq-gzip-compression-level: 4
                    - output-directory: Demultiplexing
                    - output-legacy-stats: true
                    - sample-name-column-enabled: true
                    - shared-thread-odirect-output: true
                    - strict-mode: false
                    - force
                SMARTSEQ:
                    - no-lane-splitting: true
            settings:
                common:
                    - MinimumTrimmedReadLength: 0
                    - MaskShortReads: 0
                10X_SINGLE:
                    - CreateFastqForIndexReads: 1
                IDT_UMI:
                    - CreateFastqForIndexReads: 1
                    - TrimUMI: 0
                SMARTSEQ:
                    - CreateFastqForIndexReads: 1
                NOINDEX:
                    - CreateFastqForIndexReads: 1
                    - BarcodeMismatchesIndex1: 0
                    - BarcodeMismatchesIndex2: 0
                short_single_index:
                    - BarcodeMismatchesIndex1: 0
                    - BarcodeMismatchesIndex2: 0
        bcl2fastq:
            Version: bcl2fastq_v2.20.0.422
            bin: mock
            index_path: {tmp.name}/config/Chromium_10X_indexes.txt
            tenX_index_path: {tmp.name}/config/Chromium_10X_indexes.txt
            smartseq_index_path: {tmp.name}/config/Smart-seq3_v1.5.csv
            options:
                common:
                    - output-dir: Demultiplexing
                    - loading-threads: 1
                    - processing-threads: 8
                    - writing-threads: 1
                    - minimum-trimmed-read-length: 0
                    - mask-short-adapter-reads: 0
                    - ignore-missing-positions
                    - ignore-missing-controls
                    - ignore-missing-filter
                    - ignore-missing-bcls
                10X_SINGLE:
                    - create-fastq-for-index-reads
                SMARTSEQ:
                    - create-fastq-for-index-reads
                    - no-lane-splitting
                NOINDEX:
                    - create-fastq-for-index-reads
                    - barcode-mismatches: 0
                short_single_index:
                    - barcode-mismatches: 0
        samplesheets_dir: {tmp.name}/ngi-nas-ns/samplesheets/novaseq
        analysis_server:
            host: miarka1.uppmax.uu.se
            port:
            user: funk_903
            sync:
                data_archive: ""
                include:
                    - "*.xml"
                    - "*.htm"
                    - "*.html"
                    - "*.fastq.gz"
                    - "*.csv"
    NovaSeqXPlus:
        bclconvert:
            Version: bcl-convert Version 00.000.000.4.2.4
            bin: mock
            tenX_index_path: {tmp.name}/config/Chromium_10X_indexes.txt
            smartseq_index_path: {tmp.name}/config/Smart-seq3_v1.5.csv
            index_path: {tmp.name}/config/Chromium_10X_indexes.txt
            options:
                common:
                    - bcl-sampleproject-subdirectories: true
                    - fastq-gzip-compression-level: 4
                    - output-directory: Demultiplexing
                    - output-legacy-stats: true
                    - sample-name-column-enabled: true
                    - shared-thread-odirect-output: true
                    - strict-mode: false
                    - force
                SMARTSEQ:
                    - no-lane-splitting: true
            settings:
                common:
                    - MinimumTrimmedReadLength: 0
                    - MaskShortReads: 0
                10X_SINGLE:
                    - CreateFastqForIndexReads: 1
                IDT_UMI:
                    - CreateFastqForIndexReads: 1
                    - TrimUMI: 0
                SMARTSEQ:
                    - CreateFastqForIndexReads: 1
                NOINDEX:
                    - CreateFastqForIndexReads: 1
                    - BarcodeMismatchesIndex1: 0
                    - BarcodeMismatchesIndex2: 0
                short_single_index:
                    - BarcodeMismatchesIndex1: 0
                    - BarcodeMismatchesIndex2: 0
        bcl2fastq:
            Version: bcl2fastq_v2.20.0.422
            bin: mock
            tenX_index_path: {tmp.name}/config/Chromium_10X_indexes.txt
            smartseq_index_path: {tmp.name}/config/Smart-seq3_v1.5.csv
            index_path: {tmp.name}/config/Chromium_10X_indexes.txt
            options:
                common:
                    - output-dir: Demultiplexing
                    - loading-threads: 1
                    - processing-threads: 8
                    - writing-threads: 1
                    - minimum-trimmed-read-length: 0
                    - mask-short-adapter-reads: 0
                    - ignore-missing-positions
                    - ignore-missing-controls
                    - ignore-missing-filter
                    - ignore-missing-bcls
                10X_SINGLE:
                    - create-fastq-for-index-reads
                SMARTSEQ:
                    - create-fastq-for-index-reads
                    - no-lane-splitting
                NOINDEX:
                    - create-fastq-for-index-reads
                    - barcode-mismatches: 0
                short_single_index:
                    - barcode-mismatches: 0
        samplesheets_dir: {tmp.name}/ngi-nas-ns/samplesheets/NovaSeqXPlus
        analysis_server:
            host: miarka1.uppmax.uu.se
            port:
            user: funk_903
            sync:
                data_archive: ""
                include:
                    - "*.xml"
                    - "*.htm"
                    - "*.html"
                    - "*.fastq.gz"
                    - "*.csv"
    NextSeq:
        bclconvert:
            Version: bcl-convert Version 00.000.000.4.2.4
            bin: mock
            tenX_index_path: {tmp.name}/config/Chromium_10X_indexes.txt
            smartseq_index_path: {tmp.name}/config/Smart-seq3_v1.5.csv
            index_path: {tmp.name}/config/Chromium_10X_indexes.txt
            options:
                common:
                    - bcl-sampleproject-subdirectories: true
                    - fastq-gzip-compression-level: 4
                    - output-directory: Demultiplexing
                    - output-legacy-stats: true
                    - sample-name-column-enabled: true
                    - shared-thread-odirect-output: true
                    - strict-mode: false
                    - force
                SMARTSEQ:
                    - no-lane-splitting: true
            settings:
                common:
                    - MinimumTrimmedReadLength: 0
                    - MaskShortReads: 0
                10X_SINGLE:
                    - CreateFastqForIndexReads: 1
                IDT_UMI:
                    - CreateFastqForIndexReads: 1
                    - TrimUMI: 0
                SMARTSEQ:
                    - CreateFastqForIndexReads: 1
                NOINDEX:
                    - CreateFastqForIndexReads: 1
                    - BarcodeMismatchesIndex1: 0
                    - BarcodeMismatchesIndex2: 0
                short_single_index:
                    - BarcodeMismatchesIndex1: 0
                    - BarcodeMismatchesIndex2: 0
        bcl2fastq:
            Version: bcl2fastq_v2.20.0.422
            bin: {tmp.name}/src/bcl2fastq_v2.20.0.422/bin/bcl2fastq
            index_path: {tmp.name}/config/Chromium_10X_indexes.txt
            tenX_index_path: {tmp.name}/config/Chromium_10X_indexes.txt
            smartseq_index_path: {tmp.name}/config/Smart-seq3.csv
            options:
                common:
                    - output-dir: Demultiplexing
                    - loading-threads: 1
                    - processing-threads: 8
                    - writing-threads: 1
                    - minimum-trimmed-read-length: 0
                    - mask-short-adapter-reads: 0
                    - ignore-missing-positions
                    - ignore-missing-controls
                    - ignore-missing-filter
                    - ignore-missing-bcls
                10X_SINGLE:
                    - create-fastq-for-index-reads
                SMARTSEQ:
                    - create-fastq-for-index-reads
                    - no-lane-splitting
                NOINDEX:
                    - create-fastq-for-index-reads
                    - barcode-mismatches: 0
                short_single_index:
                    - barcode-mismatches: 0
        samplesheets_dir: {tmp.name}/ngi-nas-ns/samplesheets/nextseq
        analysis_server:
            host: miarka1.uppmax.uu.se
            port:
            user: funk_903
            sync:
                data_archive: ""
                include:
                    - "*.xml"
                    - "*.htm"
                    - "*.html"
                    - "*.fastq.gz"
                    - "*.csv"

   # Directories where TACA will look for new data to be processed
    data_dirs:
        - {tmp.name}/ngi_data/sequencing/MiSeq
        - {tmp.name}/ngi_data/sequencing/NovaSeq
        - {tmp.name}/ngi_data/sequencing/NovaSeqXPlus
        - {tmp.name}/ngi_data/sequencing/NextSeq/Runs
    # finished_dir is the direcory suffix that TACA will use to store flowcells after being
    # processed and transferred. The end finished_dir directories will be
    # data_dir/finished_dir for data_dir in data_dirs
    finished_dir: nosync
    # status_dir indicates the directory where TACA will store the CSV file used
    # to keep track of which flowcells have already been processed and transferred
    status_dir: {tmp.name}/log/
    shared_filesystem_path:
        miseq: {tmp.name}/ngi-nas-ns/miseq_data
        novaseq: {tmp.name}/ngi-nas-ns/NovaSeq_data
        novaseqxplus: {tmp.name}/ngi-nas-ns/NovaSeqXPlus_data
        nextseq: {tmp.name}/ngi-nas-ns/NextSeq_data
    deliver_runfolder:
        analysis_server:
            host: miarka1.uppmax.uu.se
            port:
            user: funk_903
        destination: /"""

    test_config_yaml = yaml.safe_load(test_config_yaml_string)

    return test_config_yaml


def create_illumina_run_dir(
    tmp: TemporaryDirectory,
    instrument: str = "NovaSeqXPlus",
    run_name: str = "20240202_LH00217_0044_A2255J2LT3",
    completed: bool = True,
):
    """Create a run directory according to specifications.

    e.g.

        tmp
        └── ngi_data
            └── sequencing
                └── NovaSeqXPlus
                    └── 20240202_LH00217_0044_A2255J2LT3
                        ├── CopyComplete.txt
                        ├── Demultiplexing
                        ├── RTAComplete.txt
                        ├── RunInfo.xml
                        ├── RunParameters.xml
                        └── SampleSheet.csv

    Return it's path.
    """

    # Get run path
    run_path = f"{tmp.name}/ngi_data/sequencing/{instrument}/{run_name}"

    # Create runs directory structure
    if os.path.exists(run_path):
        shutil.rmtree(run_path)
    os.makedirs(run_path)

    # Set up files
    if completed:
        open(os.path.join(run_path, "CopyComplete.txt"), "w").close()
        open(os.path.join(run_path, "RTAComplete.txt"), "w").close()
        # Run parameters
        with open(os.path.join(run_path, "RunParameters.xml"), "w") as f:
            xml_str = r"""<?xml version="1.0" encoding="utf-8"?>
<RunParameters xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
  <Side>A</Side>
  <Application>NovaSeqXSeries Control Software</Application>
  <SystemSuiteVersion>1.2.0.28691</SystemSuiteVersion>
  <OutputFolder>//ngi-nas.scilifelab.se/ngi_data/sequencing/NovaSeqXPlus/20240202_LH00217_0044_A2255J2LT3</OutputFolder>
  <CloudUploadMode>InstrumentPerformance</CloudUploadMode>
  <RunSetupMode>Manual</RunSetupMode>
  <SecondaryAnalysisMode>None</SecondaryAnalysisMode>
  <InstrumentType>NovaSeqXPlus</InstrumentType>
  <InstrumentSerialNumber>LH00217</InstrumentSerialNumber>
  <RunId>20240202_LH00217_0044_A2255J2LT3</RunId>
  <RunCounter>44</RunCounter>
  <RecipeName>10B Sequencing</RecipeName>
  <RecipeVersion>10B-01.02.02</RecipeVersion>
  <ExperimentName>2255J2LT3</ExperimentName>
  <FlowCellName>NovaSeqXSeries B3</FlowCellName>
  <FlowCellType>NovaSeqXSeriesB3</FlowCellType>
  <ConsumableInfo>
    <ConsumableInfo>
      <SerialNumber>LC2019066-LC1</SerialNumber>
      <LotNumber>18026054</LotNumber>
      <PartNumber>20081650</PartNumber>
      <ExpirationDate>2025-02-04T00:00:00+01:00</ExpirationDate>
      <Type>Lyo</Type>
      <Mode>3</Mode>
      <Version>1.0</Version>
      <Name>Mid</Name>
    </ConsumableInfo>
    <ConsumableInfo>
      <SerialNumber>LC2309060519-1</SerialNumber>
      <LotNumber>23090501</LotNumber>
      <PartNumber>20089853</PartNumber>
      <ExpirationDate>2025-03-05T00:00:00+01:00</ExpirationDate>
      <Type>Buffer</Type>
      <Mode>3</Mode>
      <Version>1.0</Version>
      <Name>Universal</Name>
    </ConsumableInfo>
    <ConsumableInfo>
      <SerialNumber>LC4064650-LC3</SerialNumber>
      <LotNumber>20799103</LotNumber>
      <PartNumber>20066614</PartNumber>
      <ExpirationDate>2025-04-12T00:00:00+02:00</ExpirationDate>
      <Type>Reagent</Type>
      <Mode>3</Mode>
      <Version>1.0</Version>
      <Name>10B 300c</Name>
    </ConsumableInfo>
    <ConsumableInfo>
      <SerialNumber>2255J2LT3</SerialNumber>
      <LotNumber>20742703</LotNumber>
      <PartNumber>20080370</PartNumber>
      <ExpirationDate>2024-03-12T00:00:00+01:00</ExpirationDate>
      <Type>FlowCell</Type>
      <Mode>3</Mode>
      <Version>1.0</Version>
      <Name>10B</Name>
    </ConsumableInfo>
    <ConsumableInfo>
      <SerialNumber>LC1030409-LC1</SerialNumber>
      <LotNumber>1000019104</LotNumber>
      <PartNumber>20072271</PartNumber>
      <ExpirationDate>2025-09-18T00:00:00+02:00</ExpirationDate>
      <Type>SampleTube</Type>
      <Mode>3</Mode>
      <Version>1.0</Version>
      <Name>8 Lane</Name>
    </ConsumableInfo>
  </ConsumableInfo>
  <PlannedReads>
    <Read ReadName="Read1" Cycles="151" />
    <Read ReadName="Index1" Cycles="19" />
    <Read ReadName="Index2" Cycles="10" />
    <Read ReadName="Read2" Cycles="151" />
  </PlannedReads>
  <SecondaryAnalysisInfo />
  <DisableBclCopy>false</DisableBclCopy>
  </RunParameters>"""
            f.write(xml_str)
        # Run info
        with open(os.path.join(run_path, "RunInfo.xml"), "w") as f:
            xml_str = r"""<?xml version="1.0"?>
<RunInfo Version="6">
	<Run Id="20240202_LH00217_0044_A2255J2LT3" Number="44">
		<Flowcell>2255J2LT3</Flowcell>
		<Instrument>LH00217</Instrument>
		<Date>2024-02-02T15:36:21Z</Date>
		<Reads>
			<Read Number="1" NumCycles="151" IsIndexedRead="N" IsReverseComplement="N" />
			<Read Number="2" NumCycles="19" IsIndexedRead="Y" IsReverseComplement="N" />
			<Read Number="3" NumCycles="10" IsIndexedRead="Y" IsReverseComplement="Y" />
			<Read Number="4" NumCycles="151" IsIndexedRead="N" IsReverseComplement="N" />
		</Reads>
		<FlowcellLayout LaneCount="8" SurfaceCount="2" SwathCount="2" TileCount="98">
			<TileSet TileNamingConvention="FourDigit">
				<Tiles>
				</Tiles>
			</TileSet>
		</FlowcellLayout>
		<ImageDimensions Width="5120" Height="2879" />
		<ImageChannels>
			<Name>blue</Name>
			<Name>green</Name>
		</ImageChannels>
	</Run>
</RunInfo>"""
            f.write(xml_str)
        # Samplesheet
        samplesheet_name = f"{run_name.split('_')[-1][1:]}.csv"
        year = run_name[0:4]
        if not os.path.exists(
            f"{tmp.name}/ngi-nas-ns/samplesheets/{instrument}/{year}"
        ):
            os.makedirs(f"{tmp.name}/ngi-nas-ns/samplesheets/{instrument}/{year}")
        samplesheet_path = (
            f"{tmp.name}/ngi-nas-ns/samplesheets/{instrument}/{year}/{samplesheet_name}"
        )
        with open(samplesheet_path, "w") as f:
            csv_str = r"""[Header]
[Data]
FCID,Lane,Sample_ID,Sample_Name,Sample_Ref,index,index2,Description,Control,Recipe,Operator,Sample_Project
2255J2LT3,1,Sample_P00001_101,P00001_101,Multiple (- -),TAACTTGGTC,GTCGTGAATC,M__Snakytown_23_01,N,151-151,Operator_Lastname,M__Snakytown_23_01
2255J2LT3,1,Sample_P00001_102,P00001_102,Multiple (- -),TCGAACACGA,GCGAGGTTCT,M__Snakytown_23_01,N,151-151,Operator_Lastname,M__Snakytown_23_01
2255J2LT3,1,Sample_P00001_103,P00001_103,Multiple (- -),TTACTTCGTG,AAGATCGGAT,M__Snakytown_23_01,N,151-151,Operator_Lastname,M__Snakytown_23_01
2255J2LT3,1,Sample_P00001_105,P00001_105,Multiple (- -),ACGAGACGTC,CGGTCTCTCT,M__Snakytown_23_01,N,151-151,Operator_Lastname,M__Snakytown_23_01
2255J2LT3,1,Sample_P00001_106,P00001_106,Multiple (- -),CATAAGCGAC,CCTCTTCAAG,M__Snakytown_23_01,N,151-151,Operator_Lastname,M__Snakytown_23_01
2255J2LT3,1,Sample_P00001_107,P00001_107,Multiple (- -),AACTCTCTAC,ACGGCTTCTC,M__Snakytown_23_01,N,151-151,Operator_Lastname,M__Snakytown_23_01
2255J2LT3,1,Sample_P00001_109,P00001_109,Multiple (- -),GTATGCCACA,CCGAGTCTTC,M__Snakytown_23_01,N,151-151,Operator_Lastname,M__Snakytown_23_01
2255J2LT3,1,Sample_P00001_110,P00001_110,Multiple (- -),GCCAACAGAC,CGTTCACGTT,M__Snakytown_23_01,N,151-151,Operator_Lastname,M__Snakytown_23_01
2255J2LT3,1,Sample_P00001_111,P00001_111,Multiple (- -),ATAAGCGGAG,CTGTTCTACG,M__Snakytown_23_01,N,151-151,Operator_Lastname,M__Snakytown_23_01
2255J2LT3,1,Sample_P00001_112,P00001_112,Multiple (- -),TCCAACTGAA,TGTCCGTAGG,M__Snakytown_23_01,N,151-151,Operator_Lastname,M__Snakytown_23_01
2255J2LT3,1,Sample_P00001_113,P00001_113,Multiple (- -),ATTCGGCTTA,TTAACGTCCG,M__Snakytown_23_01,N,151-151,Operator_Lastname,M__Snakytown_23_01
2255J2LT3,1,Sample_P00001_114,P00001_114,Multiple (- -),TATGGCAAGC,AACAGCAATG,M__Snakytown_23_01,N,151-151,Operator_Lastname,M__Snakytown_23_01
2255J2LT3,1,Sample_P00001_115,P00001_115,Multiple (- -),CCTAAGCTGA,CGTCGCATCT,M__Snakytown_23_01,N,151-151,Operator_Lastname,M__Snakytown_23_01
2255J2LT3,1,Sample_P00001_116,P00001_116,Multiple (- -),GCCACCTAGT,TGCGATGTAT,M__Snakytown_23_01,N,151-151,Operator_Lastname,M__Snakytown_23_01
2255J2LT3,1,Sample_P00001_117,P00001_117,Multiple (- -),AGTATCCGAT,TATTGCGCTA,M__Snakytown_23_01,N,151-151,Operator_Lastname,M__Snakytown_23_01
2255J2LT3,1,Sample_P00001_118,P00001_118,Multiple (- -),CCTCGTTGGA,GACTGCGTTA,M__Snakytown_23_01,N,151-151,Operator_Lastname,M__Snakytown_23_01
2255J2LT3,1,Sample_P00001_119,P00001_119,Multiple (- -),CGACCTTAAT,GCTCTTAGGT,M__Snakytown_23_01,N,151-151,Operator_Lastname,M__Snakytown_23_01
2255J2LT3,1,Sample_P00001_120,P00001_120,Multiple (- -),GAACCGCATT,CATTATGCCT,M__Snakytown_23_01,N,151-151,Operator_Lastname,M__Snakytown_23_01"""
            f.write(csv_str)

    return run_path


def test_run_preprocessing(create_dirs):
    tmp = create_dirs

    # Mock CONFIG
    test_config_yaml = make_illumina_test_config(tmp)
    mock_config = patch("taca.utils.config.CONFIG", new=test_config_yaml)
    mock_config.start()

    # Create run dir
    run_path = create_illumina_run_dir(tmp)

    # Reload module to add mocks
    importlib.reload(analysis)

    software = "bcl2fastq"

    _run_obj = analysis.get_runObj(run_path, software)

    _mock_upload_to_db = patch("taca.analysis.analysis._upload_to_statusdb").start()

    with patch("subprocess.Popen") as mock_Popen:
        mock_Popen.start()

        log_file = test_config_yaml.get("log", {}).get("file", None)
        if log_file:
            level = test_config_yaml.get("log").get("log_level", "INFO")
            init_logger_file(log_file, level)

        # Start demux, all runs
        analysis.run_preprocessing(None, software)
        # Demux in progress, specified run
        analysis.run_preprocessing(run_path, software)
