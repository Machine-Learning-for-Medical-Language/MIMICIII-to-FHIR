import sys
import numpy as np
import pandas as pd
import os
import gc

# This contains the functions and steps from notebook 2 in the original repo.
'''
fhir.medicationDispense table
INPUTEVENTS_CV MAPPING:
Original format	FHIR resource format
1	mimic.inputevents_cv.ROW_ID	fhir.medicationDispense.identifier
2	mimic.inputevents_cv.SUBJECT_ID	fhir.medicationDispense.subject
3	mimic.inputevents_cv.HADM_ID	fhir.medicationDispense.encounter
4	mimic.inputevents_cv.ICUSTAY_ID	fhir.medicationDispense.partOf
5	mimic.inputevents_cv.ITEMID	fhir.medicationDispense.medicationCodeableConcept
6	mimic.inputevents_cv.CHARTTIME	fhir.medicationDispense.whenHandedOver
7	mimic.inputevents_cv.AMOUNT	fhir.medicationDispense.valueQuantity
8	mimic.inputevents_cv.AMOUNTUOM	fhir.medicationDispense.unit
9	mimic.inputevents_cv.RATE	fhir.medicationDispense.dosageRate
10	mimic.inputevents_cv.RATEUOM	fhir.medicationDispense.dosageRate_unit
11	mimic.inputevents_cv.CGID	fhir.medicationDispense.performer
12	mimic.inputevents_cv.ORDERID	fhir.medicationDispense.type
13	mimic.inputevents_cv.LINKORDERID	fhir.medicationDispense.type_sub
14	mimic.inputevents_cv.STOPPED	fhir.medicationDispense.status
15	mimic.inputevents_cv.ORIGINALAMOUNT	fhir.medicationDispense.dosageOriginal_amount
16	mimic.inputevents_cv.ORIGINALAMOUNTUOM	fhir.medicationDispense.dosageOriginal_amountUnit
17	mimic.inputevents_cv.ORIGNALROUTE	fhir.medicationDispense.dosageOriginal_route
18	mimic.inputevents_cv.ORIGINALRATE	fhir.medicationDispense.dosageOriginal_rate
19	mimic.inputevents_cv.ORIGINALRATEUOM	fhir.medicationDispense.dosageOriginal_rateUnit
20	mimic.inputevents_cv.ORIGINALSITE	fhir.medicationDispense.dosageOriginal_site
21	mimic.inputevents_cv.NEWBOTTLE	fhir.medicationDispense.note
22	mimic.d_items.(LABEL+DBSOURCE+PARAM_TYPE)	fhir.medicationDispense.note
23	mimic.d_items.CATEGORY	fhir.medicationDispense.category


INPUTEVENTS_MV MAPPING:
Original format	FHIR resource format
1	mimic.inputevents_mv.ROW_ID	fhir.medicationDispense.identifier
2	mimic.inputevents_mv.SUBJECT_ID	fhir.medicationDispense.subject
3	mimic.inputevents_mv.HADM_ID	fhir.medicationDispense.encounter
4	mimic.inputevents_mv.ICUSTAY_ID	fhir.medicationDispense.partOf
5	mimic.inputevents_mv.ITEMID	fhir.medicationDispense.medicationCodeableConcept
6	mimic.inputevents_mv.STARTTIME	fhir.medicationDispense.whenHandedOver_start
7	mimic.inputevents_mv.ENDTIME	fhir.medicationDispense.whenHandedOver_end
8	mimic.inputevents_mv.AMOUNT	fhir.medicationDispense.valueQuantity
9	mimic.inputevents_mv.AMOUNTUOM	fhir.medicationDispense.unit
10	mimic.inputevents_mv.RATE	fhir.medicationDispense.dosageRate
11	mimic.inputevents_mv.RATEUOM	fhir.medicationDispense.dosageRate_unit
12	mimic.inputevents_mv.CGID	fhir.medicationDispense.performer
13	mimic.inputevents_mv.ORDERID	fhir.medicationDispense.type
14	mimic.inputevents_mv.LINKORDERID	fhir.medicationDispense.type_sub
15	mimic.inputevents_mv.ORDERCATEGORYNAME	fhir.medicationDispense.supportingInformation_order_catName
16	mimic.inputevents_mv.SECONDARYORDERCATEGORYNAME	fhir.medicationDispense.supportingInformation_order_secCatName
17	mimic.inputevents_mv.ORDERCOMPONENTTYPEDESCRIPTION	fhir.medicationDispense.supportingInformation_order_desc_componentTyped
18	mimic.inputevents_mv.ORDERCATEGORYDESCRIPTION	fhir.medicationDispense.supportingInformation_order_desc_cat
19	mimic.inputevents_mv.PATIENTWEIGHT	fhir.medicationDispense.supportingInformation_patientWeight
20	mimic.inputevents_mv.ORIGINALAMOUNT	fhir.medicationDispense.dosageInstruction_original_amount
21	mimic.inputevents_mv.ORIGINALRATE	fhir.medicationDispense.dosageInstruction_original_rate
22	mimic.inputevents_mv.TOTALAMOUNT	fhir.medicationDispense.dosageInstruction_total_amount
23	mimic.inputevents_mv.TOTALAMOUNTUOM	fhir.medicationDispense.dosageInstruction_total_unit
24	mimic.inputevents_mv.ISOPENBAG	fhir.medicationDispense.dosageInstruction_openBag
25	mimic.inputevents_mv.CONTINUEINNEXTDEPT	fhir.medicationDispense.eventHistory_contExtDep
26	mimic.inputevents_mv.CANCELREASON	fhir.medicationDispense.detectedIssue_code
27	mimic.inputevents_mv.STATUSDESCRIPTION	fhir.medicationDispense.status
28	mimic.inputevents_mv.COMMENTS_EDITEDBY	fhir.medicationDispense.performer_comment_edit
29	mimic.inputevents_mv.COMMENTS_CANCELEDBY	fhir.medicationDispense.performer_comment_cancel
30	mimic.inputevents_mv.COMMENTS_DATE	fhir.medicationDispense.detectedIssue_date
31	mimic.d_items.(LABEL+DBSOURCE+PARAM_TYPE)	fhir.medicationDispense.note

'''

data_path = '/home/tmill/mnt/r/resources/mimic/mimic3-v1.4/physionet.org/files/mimiciii/1.4/'
output_path = './data/fhir_out/'
file_ext = '.csv'
compression = None
data_files = os.listdir(data_path)

def transform_inputevents_cv(data_path, output_path):
    inputevents_cv = pd.read_csv(data_path+'INPUTEVENTS_CV'+file_ext, compression=compression,
                                 # dropped 'STORETIME'
                                 usecols=['ROW_ID', 'SUBJECT_ID', 'HADM_ID', 'ICUSTAY_ID', 'CHARTTIME',
                                          'ITEMID', 'AMOUNT', 'AMOUNTUOM', 'RATE', 'RATEUOM',
                                          'CGID', 'ORDERID', 'LINKORDERID', 'STOPPED', 'NEWBOTTLE', 'ORIGINALAMOUNT',
                                          'ORIGINALAMOUNTUOM', 'ORIGINALROUTE', 'ORIGINALRATE', 'ORIGINALRATEUOM',
                                          'ORIGINALSITE'],
                                 dtype={'ROW_ID': int, 'SUBJECT_ID': int, 'HADM_ID': np.float32, 'ICUSTAY_ID': np.float32,
                                        'ITEMID': int, 'AMOUNT': np.float16, 'AMOUNTUOM': 'category', 'RATE': np.float64, 
                                        'RATEUOM': 'category', 'CGID': float, 'ORDERID': int, 'LINKORDERID': int,
                                        'STOPPED': 'category', 'NEWBOTTLE': np.float16, 'ORIGINALAMOUNT': np.float64,
                                        'ORIGINALAMOUNTUOM': 'category', 'ORIGINALROUTE': 'category', 'ORIGINALRATE': float,
                                        'ORIGINALRATEUOM': 'category', 'ORIGINALSITE': 'category'},
                                 parse_dates=['CHARTTIME'])
                                
    d_items = pd.read_csv(data_path+'D_ITEMS'+file_ext, compression=compression, index_col=0,
                          # dropped 'ABBREVIATION', 'LINKSTO', 'CONCEPTID', 'UNITNAME'
                          usecols=['ROW_ID', 'ITEMID', 'LABEL', 'DBSOURCE', 'CATEGORY', 'PARAM_TYPE'],
                          dtype={'ROW_ID': int, 'ITEMID': int, 'LABEL': str, 'DBSOURCE': 'category',
                                 'CATEGORY': 'category', 'PARAM_TYPE': str})
    
    inputevents_cv['NEWBOTTLE'].replace(np.NaN, 0, inplace=True)
    medicationDispense = pd.merge(inputevents_cv, d_items, on='ITEMID')
#     medicationDispense.CHARTTIME = pd.to_datetime(medicationDispense.CHARTTIME, format = '%Y-%m-%d %H:%M:%S', errors = 'coerce')
# 
#     medicationDispense['NEWBOTTLE'].replace(np.NaN, 0, inplace=True)
#     medicationDispense['PARAM_TYPE'].replace(np.NaN, '', regex=True, inplace=True)
#     medicationDispense['note'] = medicationDispense['LABEL'] + ' ' + medicationDispense['DBSOURCE'] + ' ' + medicationDispense['PARAM_TYPE'] + ' ' + medicationDispense['NEWBOTTLE'].astype(str) +' new bottle'

    # x2.5 fater and more readable
    medicationDispense['note'] = medicationDispense['LABEL'].str.cat(medicationDispense['DBSOURCE'], sep=' ', na_rep='NA')
    medicationDispense['note'] = medicationDispense['note'].str.cat(medicationDispense['PARAM_TYPE'], sep=' ', na_rep='')
    medicationDispense['note'] = medicationDispense['note'].str.cat(medicationDispense['NEWBOTTLE'].astype(str), sep=' ')
    medicationDispense['note'] = medicationDispense['note'] + ' new bottle'
    
    
    medicationDispense.rename(columns={'ROW_ID':'identifier',
                                       'SUBJECT_ID':'subject',
                                       'HADM_ID':'encounter',
                                       'ICUSTAY_ID':'partOf',
                                       'CHARTTIME':'whenHandedOver',
                                       'ITEMID':'medicationCodeableConcept',
                                       'AMOUNT':'valueQuantity',
                                       'AMOUNTUOM':'unit',
                                       'RATE':'dosageRate',
                                       'RATEUOM':'dosageRate_unit',
                                       'CGID':'performer',
                                       'ORDERID':'type',
                                       'LINKORDERID':'type_sub',
                                       'STOPPED':'status',
                                       'ORIGINALAMOUNT':'dosageOriginal_amount',
                                       'ORIGINALAMOUNTUOM':'dosageOriginal_amountUnit',
                                       'ORIGINALROUTE':'dosageOriginal_route',
                                       'ORIGINALRATE':'dosageOriginal_rate',
                                       'ORIGINALRATEUOM':'dosageOriginal_rateUnit',
                                       'ORIGINALSITE':'dosageOriginal_site',
                                       'CATEGORY':'category'}, inplace=True)

    # 'STORETIME', 'ABBREVIATION', 'LINKSTO', 'CONCEPTID', 'UNITNAME'
    medicationDispense.drop(['LABEL', 'PARAM_TYPE', 'DBSOURCE','NEWBOTTLE'], axis=1, inplace=True)
#     medicationDispense.to_csv(output_path + 'medicationDispense.csv.gz', compression='gzip', index=False)

    return medicationDispense

def transform_inputevents_mv(data_path, output_path):
    inputevents_mv = pd.read_csv(data_path+'INPUTEVENTS_MV'+file_ext, compression=compression,
                                 # STORETIME, 'COMMENTS_STATUS', 'COMMENTS_TITLE'
                                 usecols=['ROW_ID', 'SUBJECT_ID', 'HADM_ID', 'ICUSTAY_ID', 'STARTTIME', 'ENDTIME',
                                          'ITEMID', 'AMOUNT', 'AMOUNTUOM', 'RATE', 'RATEUOM', 'CGID',
                                          'ORDERID', 'LINKORDERID', 'ORDERCATEGORYNAME', 'SECONDARYORDERCATEGORYNAME',
                                          'ORDERCOMPONENTTYPEDESCRIPTION', 'ORDERCATEGORYDESCRIPTION', 'PATIENTWEIGHT',
                                          'TOTALAMOUNT', 'TOTALAMOUNTUOM', 'ISOPENBAG', 'CONTINUEINNEXTDEPT',
                                          'CANCELREASON', 'STATUSDESCRIPTION',
                                          'COMMENTS_DATE', 'ORIGINALAMOUNT', 'ORIGINALRATE'],
                                 dtype={'ROW_ID': int, 'SUBJECT_ID': int, 'HADM_ID': int, 'ICUSTAY_ID': float,
                                        'ITEMID': int, 'AMOUNT': np.float64, 'AMOUNTUOM': str, 'RATE': np.float64,
                                        'RATEUOM': str, 'CGID': np.float64, 'ORDERID': np.float64,
                                        'LINKORDERID': np.float64, 'ORDERCATEGORYNAME': str,
                                        'SECONDARYORDERCATEGORYNAME': str, 'ORDERCOMPONENTTYPEDESCRIPTION': str,
                                        'ORDERCATEGORYDESCRIPTION': str, 'PATIENTWEIGHT': np.float64,
                                        'TOTALAMOUNT': np.float64, 'TOTALAMOUNTUOM': str, 'ISOPENBAG': int,
                                        'CONTINUEINNEXTDEPT': int, 'CANCELREASON': int, 'STATUSDESCRIPTION': str,
                                        'ORIGINALAMOUNT': np.float64, 'ORIGINALRATE': np.float64},
                                 parse_dates=['STARTTIME', 'ENDTIME', 'COMMENTS_DATE']
                                )
    
    d_items = pd.read_csv(data_path+'D_ITEMS'+file_ext, compression=compression, index_col=0,
                      # dropped 'ABBREVIATION', 'LINKSTO', 'CONCEPTID', 'UNITNAME'
                      usecols=['ROW_ID', 'ITEMID', 'LABEL', 'DBSOURCE', 'CATEGORY', 'PARAM_TYPE'],
                      dtype={'ROW_ID': int, 'ITEMID': int, 'LABEL': str, 'DBSOURCE': 'category',
                             'CATEGORY': 'category', 'PARAM_TYPE': str}) 
    
    medicationDispense = pd.merge(inputevents_mv, d_items, on='ITEMID')
    
#     medicationDispense.STARTTIME = pd.to_datetime(medicationDispense.STARTTIME, format = '%Y-%m-%d %H:%M:%S', errors = 'coerce')
#     medicationDispense.ENDTIME = pd.to_datetime(medicationDispense.ENDTIME, format = '%Y-%m-%d %H:%M:%S', errors = 'coerce')
#     medicationDispense.COMMENTS_DATE = pd.to_datetime(medicationDispense.COMMENTS_DATE, format = '%Y-%m-%d', errors = 'coerce')

#     medicationDispense['PARAM_TYPE'].replace(np.NaN, '', regex=True, inplace=True)
#     medicationDispense['note'] = medicationDispense['LABEL'] + ' ' + medicationDispense['DBSOURCE'] + ' ' + medicationDispense['PARAM_TYPE']
    
    medicationDispense['note'] = medicationDispense['LABEL'].str.cat(medicationDispense['DBSOURCE'], sep=' ', na_rep='NA')
    medicationDispense['note'] = medicationDispense['note'].str.cat(medicationDispense['PARAM_TYPE'], sep=' ', na_rep='')
    
    medicationDispense.drop(['LABEL', 'PARAM_TYPE', 'DBSOURCE'], axis=1, inplace=True)

    medicationDispense.rename(columns={'ROW_ID':'identifier',
                                       'SUBJECT_ID':'subject',
                                       'HADM_ID':'encounter',
                                       'ICUSTAY_ID':'partOf',
                                       'STARTTIME':'whenHandedOver_start',
                                       'ENDTIME':'whenHandedOver_end',
                                       'ITEMID':'medicationCodeableConcept',
                                       'AMOUNT':'valueQuantity',
                                       'AMOUNTUOM':'unit',
                                       'RATE':'dosageRate',
                                       'RATEUOM':'dosageRate_unit',
                                       'CGID':'performer',
                                       'ORDERID':'type',
                                       'LINKORDERID':'type_sub',
                                       'ORDERCATEGORYNAME':'supportingInformation_order_catName',
                                       'SECONDARYORDERCATEGORYNAME':'supportingInformation_order_secCatName', 
                                       'ORDERCOMPONENTTYPEDESCRIPTION':'supportingInformation_order_desc_componentTyped',
                                       'ORDERCATEGORYDESCRIPTION':'supportingInformation_order_desc_cat', 
                                       'PATIENTWEIGHT':'supportingInformation_patientWeight', 
                                       'TOTALAMOUNT':'dosageInstruction_total_amount',
                                       'TOTALAMOUNTUOM':'dosageInstruction_total_unit', 
                                       'ISOPENBAG':'dosageInstruction_openBag', 
                                       'CONTINUEINNEXTDEPT':'eventHistory_contExtDep', 
                                       'CANCELREASON':'detectedIssue_code',
                                       'STATUSDESCRIPTION':'status', 
# We do not have such keys and FHIR does not have such resources
#                                        'COMMENTS_EDITEDBY':'performer_comment_edit', 
#                                        'COMMENTS_CANCELEDBY':'performer_comment_cancel',
                                       'COMMENTS_DATE':'detectedIssue_date',
                                       'ORIGINALAMOUNT':'dosageInstruction_original_amount',
                                       'ORIGINALRATE':'dosageInstruction_original_rate',
                                       'CATEGORY':'category'}, inplace=True)
    
#     medicationDispense.to_csv(output_path + 'medicationDispense_mv.csv.gz', compression='gzip', index=False)
    return medicationDispense

def main(args):
    medicationDispense_cv = transform_inputevents_cv(data_path, output_path)
    medicationDispense_cv.to_csv(output_path+'medicationDispense.csv.gz',
                             compression={'method': 'gzip', 'compresslevel': 1}, index=False)
    del medicationDispense_cv
    gc.collect()

    medicationDispense_mv = transform_inputevents_mv(data_path, output_path)
    medicationDispense_mv.to_csv(output_path+'medicationDispense_mv.csv.gz',
                             compression={'method': 'gzip', 'compresslevel': 1}, index=False)

    
if __name__ == '__main__':
    main(sys.argv[1:])