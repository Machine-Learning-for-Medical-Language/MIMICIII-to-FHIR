import numpy as np
import pandas as pd
import os
import gc
import sys
from pathlib import Path
import time
import gc

'''
MIMIC-III tables to FHIR resource mapping in this notebook
Original format	FHIR resource	Progress	Final Check
8	prescriptions	medicationRequest	C,L,A	Done
9	chartevents	observation	C,L,A	Done
10	datetimeevents	observation	C,L,A	Done
11	labevents	observation	C,L,A	Done

'''
# This contains the functions and steps from notebook 3 in the original repo.
data_path = '/home/tmill/mnt/r/resources/mimic/mimic3-v1.4/physionet.org/files/mimiciii/1.4/'
output_path = './data/fhir_out/'
file_ext = '.csv'
compression = None
data_files = os.listdir(data_path)

'''
fhir.medicationRequest table
MAPPING:
Original format	FHIR resource format
1	mimic.prescriptions.ROW_ID	fhir.medicationRequest.identifier
2	mimic.prescriptions.SUBJECT_ID	fhir.medicationRequest.subject
3	mimic.prescriptions.HADM_ID	fhir.medicationRequest.encounter
4	mimic.prescriptions.ICUSTAY_ID	fhir.medicationRequest.partOf
5	mimic.prescriptions.STARTDATE	fhir.medicationRequest.dispenseRequest_start
6	mimic.prescriptions.ENDDATE	fhir.medicationRequest.dispenseRequest_end
7	mimic.prescriptions.DRUG_TYPE	fhir.medicationRequest.category
8	mimic.prescriptions.DRUG	fhir.medicationRequest.medication_name
9	mimic.prescriptions.DRUG_NAME_GENERIC	fhir.medicationRequest.medication_genericName
10	mimic.prescriptions.FORMULARY_DRUG_CD	fhir.medicationRequest.medication_code_CD
11	mimic.prescriptions.GSN	fhir.medicationRequest.medication_code_GSN
12	mimic.prescriptions.NDC	fhir.medicationRequest.medication_code_NDC
13	mimic.prescriptions.DOSE_VAL_RX	fhir.medicationRequest.dosageInstruction_value
14	mimic.prescriptions.DOSE_UNIT_RX	fhir.medicationRequest.dosageInstruction_unit
15	mimic.prescriptions.FORM_VAL_DISP	fhir.medicationRequest.dispenseRequest_value
16	mimic.prescriptions.FORM_UNIT_DISP	fhir.medicationRequest.dispenseRequest_unit
17	mimic.prescriptions.ROUTE	fhir.medicationRequest.courseOfTherapyType
'''
def transform_prescriptions(data_path, output_path):
    prescriptions = pd.read_csv(data_path+'PRESCRIPTIONS'+file_ext, compression=compression,
                                usecols=lambda x: x not in ['DRUG_NAME_POE', 'PROD_STRENGTH'],
                                dtype={'ROW_ID': int, 'SUBJECT_ID': int, 'HADM_ID': int, 'ICUSTAY_ID': float,
                                       'DRUG_TYPE': str, 'DRUG': str, 'DRUG_NAME_GENERIC': str,
                                       'FORMULARY_DRUG_CD': str, 'GSN': str, 'NDC': str,
                                       'DOSE_VAL_RX': str, 'DOSE_UNIT_RX': str, 'FORM_VAL_DISP': str,
                                       'FORM_UNIT_DISP': str, 'ROUTE': str},
                                parse_dates=['STARTDATE', 'ENDDATE'])

#     prescriptions.STARTDATE = pd.to_datetime(prescriptions.STARTDATE, format = '%Y-%m-%d', errors = 'coerce')
#     prescriptions.ENDDATE = pd.to_datetime(prescriptions.ENDDATE, format = '%Y-%m-%d', errors = 'coerce')
#     
#     Drop extra columns
#     prescriptions.drop(['DRUG_NAME_POE', 'PROD_STRENGTH'], axis=1, inplace=True)

    prescriptions.rename(columns={'ROW_ID':'identifier',
                                  'SUBJECT_ID':'subject', 
                                  'HADM_ID':'encounter', 
                                  'ICUSTAY_ID':'partOf',
                                  'STARTDATE':'dispenseRequest_start', 
                                  'ENDDATE':'dispenseRequest_end',
                                  'DRUG_TYPE':'category', 
                                  'DRUG':'medication_name', 
                                  'DRUG_NAME_GENERIC':'medication_genericName',
                                  'FORMULARY_DRUG_CD':'medication_code_CD', 
                                  'GSN':'medication_code_GSN', 
                                  'NDC':'medication_code_NDC', 
                                  'DOSE_VAL_RX':'dosageInstruction_value',
                                  'DOSE_UNIT_RX':'dosageInstruction_unit', 
                                  'FORM_VAL_DISP':'dispenseRequest_value', 
                                  'FORM_UNIT_DISP':'dispenseRequest_unit', 
                                  'ROUTE':'courseOfTherapyType'}, inplace=True)

#     prescriptions.to_csv(output_path+'medicationRequest.csv.gz', compression='gzip', index=False)
    return prescriptions

'''
CHARTEVENTS MAPPING:
Original format	FHIR resource format
mimic.chartevents.ROW_ID	fhir.observation.identifier
mimic.chartevents.SUBJECT_ID	fhir.observation.subject
mimic.chartevents.HADM_ID	fhir.observation.encounter
mimic.chartevents.ICUSTAY_ID	fhir.observation.partOf
mimic.chartevents.ITEMID	fhir.observation.code
mimic.chartevents.CHARTTIME	fhir.observation.effectiveDateTime
mimic.chartevents.CGID	fhir.observation.performer
mimic.chartevents.VALUE	fhir.observation.value
mimic.chartevents.VALUENUM	fhir.observation.value_quantity
mimic.chartevents.VALUEUOM	fhir.observation.unit
mimic.chartevents.WARNING	fhir.observation.interpretation
mimic.chartevents.RESULTSTATUS	fhir.observation.status
mimic.d_items.(LABEL+DBSOURCE+PARAM_TYPE)	fhir.observation.note
mimic.d_items.CATEGORY	fhir.observation.category_sub
'chartevents'	fhir.observation.category
'''
def transform_chartevents(data_path, output_path, chunksize=10**7):
    """ ~6GB RAM in peak consumption with default chunksize
    """
    # delete outputfile if exists
    output_filename = output_path+'observation_ce.csv.gz'
    Path(output_filename).unlink(missing_ok=True)
    
    d_items = pd.read_csv(data_path+'D_ITEMS'+file_ext, compression=compression, index_col=0,
                      # dropped 'ABBREVIATION', 'LINKSTO', 'CONCEPTID', 'UNITNAME'
                      usecols=['ROW_ID', 'ITEMID', 'LABEL', 'DBSOURCE', 'CATEGORY', 'PARAM_TYPE'],
                      dtype={'ROW_ID': int, 'ITEMID': int, 'LABEL': str, 'DBSOURCE': 'category',
                             'CATEGORY': 'category', 'PARAM_TYPE': str})
    
    # it is the biggest file ~4GB gzipped, 33GB unpacked, 330M strings
    # looks like CareVue and Metavision data should be processed separately
    chunk_container =  pd.read_csv(data_path+'CHARTEVENTS'+file_ext, compression=compression,
                                   # STORETIME
                                   usecols=['ROW_ID', 'SUBJECT_ID', 'HADM_ID', 'ICUSTAY_ID', 'ITEMID', 'CHARTTIME',
                                            'STORETIME', 'CGID', 'VALUE', 'VALUENUM', 'VALUEUOM', 'WARNING', 'ERROR',
                                            'RESULTSTATUS', 'STOPPED'],
                                   dtype={'ROW_ID': int, 'SUBJECT_ID': int, 'HADM_ID': int, 'ICUSTAY_ID': float,
                                          'ITEMID': int, 'CGID': float, 'VALUE': str, 'VALUENUM': float, 
                                          'VALUEUOM': str, 'WARNING': float, 'ERROR': float,
                                          'RESULTSTATUS': str, 'STOPPED': str},
                                   parse_dates=['CHARTTIME'],
                                   chunksize=chunksize)  # 2.67GB for 10**7
    for i, chartevents in enumerate(chunk_container):
        # Show progress (~330M strings)
        print(f'{i + 1}/{330*10**6 / chunksize}', flush=True, end =" ")
        start_time = time.time()

        observation_ce = pd.merge(chartevents, d_items, on='ITEMID')

        observation_ce['note'] = observation_ce['LABEL'].str.cat(observation_ce['DBSOURCE'], sep=' ', na_rep='NA')
        observation_ce['note'] = observation_ce['note'].str.cat(observation_ce['PARAM_TYPE'], sep=' ', na_rep='')

        observation_ce.loc[observation_ce['STOPPED'] == "D/C'd", 'RESULTSTATUS'] = 'discharged'
        observation_ce.loc[observation_ce['ERROR'] == 1, 'RESULTSTATUS'] = 'Error'
        # New columns to adapt to Chartevents observations
        observation_ce['category'] = 'chartevents'  # ????

        observation_ce.drop(['LABEL', 'PARAM_TYPE', 'ERROR', 'DBSOURCE', 'STOPPED'], axis=1, inplace=True)

        observation_ce.rename(columns={'ROW_ID':'identifier',
                                       'SUBJECT_ID':'subject',
                                       'HADM_ID':'encounter',                               
                                       'ICUSTAY_ID':'partOf',
                                       'ITEMID':'code',
                                       'CGID':'performer',
                                       'CHARTTIME':'effectiveDateTime',
                                       'VALUE':'value',
                                       'VALUENUM':'value_quantity',
                                       'VALUEUOM':'unit',
                                       'WARNING':'interpretation',
                                       'RESULTSTATUS':'status',
                                       'CATEGORY':'category_sub'}, inplace=True)

        observation_ce = observation_ce.reindex(columns=['identifier',
                                                         'subject', 
                                                         'encounter', 
                                                         'partOf', 
                                                         'code',
                                                         'effectiveDateTime',
                                                         'performer',
                                                         'value',
                                                         'value_quantity',
                                                         'unit', 
                                                         'interpretation',
                                                         'status',
                                                         'note',
                                                         'category_sub',
                                                         'category'], copy=False)

        observation_ce.to_csv(output_filename, compression={'method': 'gzip', 'compresslevel': 1},
                              index=False, mode='a')
        # force free mem, for some reasons without it, RAM ends pretty quick
        gc.collect()
        # show execution time per chunk
        print(f"--- {time.time() - start_time} seconds ---", flush=True)

'''
DATETIMEEVENTS MAPPING:
Original format	FHIR resource format
mimic.datetimeevents.ROW_ID	fhir.observation.identifier
mimic.datetimeevents.SUBJECT_ID	fhir.observation.subject
mimic.datetimeevents.HADM_ID	fhir.observation.encounter
mimic.datetimeevents.ICUSTAY_ID	fhir.observation.partOf
mimic.datetimeevents.ITEMID	fhir.observation.code
mimic.datetimeevents.CHARTTIME	fhir.observation.effectiveDateTime
mimic.datetimeevents.CGID	fhir.observation.performer
mimic.datetimeevents.VALUE	fhir.observation.value
mimic.datetimeevents.VALUEUOM	fhir.observation.unit
mimic.datetimeevents.WARNING	fhir.observation.interpretation
mimic.datetimeevents.RESULTSTATUS	fhir.observation.status
mimic.d_items.(LABEL+DBSOURCE+PARAM_TYPE)	fhir.observation.note
mimic.d_items.CATEGORY	fhir.observation.category_sub
'datetimeevents'	fhir.observation.category
'''
def transform_datetimeevents(data_path, output_path):
    # 'STORETIME'
    datetimeevents = pd.read_csv(data_path+'DATETIMEEVENTS'+file_ext, compression=compression,
                                 usecols=['ROW_ID', 'SUBJECT_ID', 'HADM_ID', 'ICUSTAY_ID',
                                          'ITEMID', 'CHARTTIME', 'CGID', 'VALUE',
                                          'VALUEUOM', 'WARNING', 'ERROR', 'RESULTSTATUS',
                                          'STOPPED'],
                                 dtype={'ROW_ID': int, 'SUBJECT_ID': int, 'HADM_ID': float,
                                        'ICUSTAY_ID': float, 'ITEMID': int, 'CGID': float,
                                        'VALUEUOM': str, 'WARNING': float, 'ERROR': float,
                                        'RESULTSTATUS': str, 'STOPPED': str},
                                 parse_dates=['CHARTTIME', 'VALUE']
                                )

    
    d_items = pd.read_csv(data_path+'D_ITEMS'+file_ext, compression=compression, index_col=0,
                      # dropped 'ABBREVIATION', 'LINKSTO', 'CONCEPTID', 'UNITNAME'
                      usecols=['ROW_ID', 'ITEMID', 'LABEL', 'DBSOURCE', 'CATEGORY', 'PARAM_TYPE'],
                      dtype={'ROW_ID': int, 'ITEMID': int, 'LABEL': str, 'DBSOURCE': 'category',
                             'CATEGORY': 'category', 'PARAM_TYPE': str})
    
    observation_dte = pd.merge(datetimeevents, d_items, on='ITEMID')
    
    observation_dte['note'] = observation_dte['LABEL'].str.cat(observation_dte['DBSOURCE'], sep=' ', na_rep='NA')
    observation_dte['note'] = observation_dte['note'].str.cat(observation_dte['PARAM_TYPE'], sep=' ', na_rep='')
        
    observation_dte.loc[observation_dte['STOPPED'] == "D/C'd", 'RESULTSTATUS'] = 'discharged'
    observation_dte.loc[observation_dte['ERROR'] == 1, 'RESULTSTATUS'] = 'Error'
    # New columns to adapt to DateTimeEvents observations
    observation_dte['category'] = 'datetimeevents'  # ???

    observation_dte.drop(['LABEL', 'PARAM_TYPE', 'ERROR', 'DBSOURCE', 'STOPPED'], axis=1, inplace=True)

    observation_dte.rename(columns={'ROW_ID':'identifier',
                                    'SUBJECT_ID':'subject',
                                    'HADM_ID':'encounter',
                                    'ICUSTAY_ID':'partOf',
                                    'ITEMID':'code',
                                    'CGID':'performer',
                                    'CHARTTIME':'effectiveDateTime',
                                    'VALUE':'value',
                                    'VALUEUOM':'unit',
                                    'WARNING':'interpretation',
                                    'RESULTSTATUS':'status',
                                    'CATEGORY':'category_sub'}, inplace=True)

    observation_dte = observation_dte.reindex(columns=['identifier',
                                                       'subject', 
                                                       'encounter', 
                                                       'partOf',
                                                       'code',
                                                       'effectiveDateTime', 
                                                       'performer',
                                                       'value',
                                                       'unit', 
                                                       'interpretation',
                                                       'status',
                                                       'note',
                                                       'category_sub',
                                                       'category'], copy=False)

#     observation_dte.to_csv(output_path+'observation_dte.csv.gz', compression='gzip', index=False)
    return observation_dte

'''
LABEVENTS MAPPING:
Consider assigning loinc_code to code not to method. LOINC_CODE would first need to be assigned, which isn't straightforward.
Original format	FHIR resource format
mimic.labevents.ROW_ID	fhir.observation.identifier
mimic.labevents.SUBJECT_ID	fhir.observation.subject
mimic.labevents.HADM_ID	fhir.observation.encounter
mimic.labevents.CHARTTIME	fhir.observation.effectiveDateTime
mimic.labevents.ITEMID	fhir.observation.code
mimic.d_labitems.LOINC_CODE	fhir.observation.code_loinc
mimic.labevents.VALUE	fhir.observation.value
mimic.labevents.VALUENUM	fhir.observation.value_quantity
mimic.labevents.VALUEUOM	fhir.observation.unit
mimic.labevents.FLAG	fhir.observation.interpretation
mimic.d_labitems.(LABEL+FLUID)	fhir.observation.note
mimic.d_labitems.CATEGORY	fhir.observation.category_sub
'labevents'	fhir.observation.category
'''
def transform_labevents(data_path, output_path, chunksize=10**6):
    """ Surprisingly, it is memory greed thing """
    # delete outputfile if exists
    output_filename = output_path + 'observation_le.csv.gz'
    Path(output_filename).unlink(missing_ok=True)
    
    d_labitems = pd.read_csv(data_path+'D_LABITEMS'+file_ext, compression=compression, index_col=0,
                             usecols=['ROW_ID', 'ITEMID', 'LABEL', 'FLUID', 'CATEGORY', 'LOINC_CODE'],
                             dtype={'ROW_ID': int, 'ITEMID': int, 'LABEL': str,
                                    'FLUID': str, 'CATEGORY': str, 'LOINC_CODE': str}
                            )

    # 5.5GB
    chunk_container = pd.read_csv(data_path+'LABEVENTS'+file_ext, compression=compression,
                                  usecols=['ROW_ID', 'SUBJECT_ID', 'HADM_ID', 'ITEMID',
                                           'CHARTTIME', 'VALUE', 'VALUENUM','VALUEUOM', 'FLAG'],
                                  dtype={'ROW_ID': int, 'SUBJECT_ID': int, 'HADM_ID': float, 'ITEMID': int,
                                         'VALUE': str, 'VALUENUM': float, 'VALUEUOM': str, 'FLAG': str},
                                  parse_dates=['CHARTTIME'],
                                  chunksize=chunksize)
    for i, labevents in enumerate(chunk_container):
        # Show progress (~28M strings)
        print(f'{i + 1}/{28*10**6 / chunksize}', flush=True, end =" ")
        start_time = time.time()

        observation_le = pd.merge(labevents, d_labitems, on='ITEMID')

        observation_le['note'] = observation_le['LABEL'].str.cat(observation_le['FLUID'], sep=' ', na_rep='NA')
        observation_le.drop(['LABEL', 'FLUID'], axis=1, inplace=True)

        # Add observation type
        observation_le['category'] = 'labevents'  # ???

        # SUBJECT_ID will be underfilled
        observation_le.rename(columns={'ROW_ID':'identifier',
                                       'SUBJECT_ID':'subject',
                                       'HADM_ID':'encounter',
                                       'ITEMID':'code',
                                       'LOINC_CODE':'code_loinc',
                                       'CHARTTIME':'effectiveDateTime',
                                       'VALUE':'value',
                                       'VALUENUM':'value_quantity',
                                       'VALUEUOM':'unit',
                                       'FLAG':'interpretation',
                                       'CATEGORY':'category_sub'}, inplace=True)

        observation_le = observation_le.reindex(columns=['identifier',
                                                         'subject', 
                                                         'encounter',
                                                         'effectiveDateTime',
                                                         'code',
                                                         'code_loinc',
                                                         'value',
                                                         'value_quantity',
                                                         'unit', 
                                                         'interpretation',
                                                         'note',
                                                         'category_sub',
                                                         'category'], copy=False)

        observation_le.to_csv(output_filename, compression={'method': 'gzip', 'compresslevel': 1},
                              index=False, mode='a')
        # force free mem, for some reasons without it, RAM ends pretty quick
        gc.collect()
        # show execution time per chunk
        print(f"--- {time.time() - start_time} seconds ---", flush=True)

        

def main(args);
    medicationRequest = transform_prescriptions(data_path, output_path)
    medicationRequest.to_csv(output_path+'medicationRequest.csv.gz',
                         compression={'method': 'gzip', 'compresslevel': 1}, index=False)
    del medicationRequest
    gc.collect()

    transform_chartevents(data_path, output_path)
    observation_dte = transform_datetimeevents(data_path, output_path)
    observation_dte.to_csv(output_path+'observation_dte.csv.gz',
                       compression={'method': 'gzip', 'compresslevel': 1}, index=False)
    
    del observation_dte
    gc.collect()

    transform_labevents(data_path, output_path)

if __name__ == '__main__':
    main(sys.argv[1:])