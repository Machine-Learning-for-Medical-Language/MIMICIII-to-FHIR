# MIMIC-III to FHIR conversion pipeline

This code should have been accompanying [the journal article](https://arxiv.org/pdf/2006.16926.pdf).

The original code and realization are awful, incomplete, and nonworkable (
[1](https://github.com/leopold-franz/MIMIC-III_FHIR_Transformation/issues/1),
[2](https://github.com/leopold-franz/MIMIC-III_FHIR_Transformation/issues/2),
[3](https://github.com/leopold-franz/MIMIC-III_FHIR_Transformation/issues/3)
).

Also, authors use some keys that are absent in MIMIC-III tables and resources that absent in FHIR documentation (see `transform_inputevents_mv()` in the 002-kba notebook).

## TODO

- [ ] add dtypes for all `pd.read_csv()` to [avoid dtype guessing](https://stackoverflow.com/questions/24251219/pandas-read-csv-low-memory-and-dtype-options)
- [x] make it run on the laptop with 16GB RAM
- [x] ~~make `mimic_fhir_transformation.py` runnable and actually working code or~~ screw it and use jupyter notebook instead
- [ ] convert tables left behind
- [ ] refactoring and documentation

## ETL Instructions (from original repo)

To Transform MIMIC-III dataset to a dataframe/CSV FHIR format please run the
whole mimic_ETL_FHIR.ipynb Jupyter Notebook.

**Note:** the goal of this thesis is to use the mimic data to train a deep learning
model, therefore the data was kept in a CSV format and not transformed to
JSON/XML.

## Conversion scheme

More details in the `mappings` folder

| MIMIC-III tables             | FHIR resource         |
| ---------------------------- | --------------------- |
| patients + admissions        | patient               |
| admissions + diagnoses_icd   | encounter             |
| icustays                     | encounter_icustays    |
| cptevents + cptevents        | claim                 |
| noteevents                   | diagnosticReport      |
| inputevents_cv + d_items     | medicationDispense    |
| inputevents_mv + d_items     | medicationDispense_mv |
| prescriptions                | medicationRequest     |
| chartevents + d_items        | observation_ce        |
| datetimeevents + d_items     | observation_dte       |
| labevents + d_labitems       | observation_le        |
| caregivers                   | practitioner          |
| procedures_icd               | procedure_icd9        |
| procedureevents_mv + d_items | procedure_mv          |
| outputevents + d_items       | specimen_oe           |
| microbiologyevents + d_items | specimen_mbe          |
| services                     | services              |

**Following tables were not transformed or even mentioned in the code:**

- CALLOUT
- DRGCODES
- TRANSFERS
- D_CPT
- D_ICD_DIAGNOSES
- D_ICD_PROCEDURES
