# [MIMIC-III](https://mimic.physionet.org/about/mimic/) to [FHIR](https://www.hl7.org/fhir/) conversion pipeline

This repo is based on [this code](https://github.com/leopold-franz/MIMIC-III_FHIR_Transformation),
 which should have been accompanying [this journal article](https://arxiv.org/pdf/2006.16926.pdf).

The original code and realization are awful, incomplete, and nonworkable (
[1](https://github.com/leopold-franz/MIMIC-III_FHIR_Transformation/issues/1),
[2](https://github.com/leopold-franz/MIMIC-III_FHIR_Transformation/issues/2),
[3](https://github.com/leopold-franz/MIMIC-III_FHIR_Transformation/issues/3)
).

Code from this repo actually runs without crashes, even on a 16GB RAM laptop.

But all mappings are from the original repo, and I am perfectly sure, that they have errors:
+ authors used some keys that are absent in MIMIC-III tables in `transform_inputevents_mv()` (002-kba-... notebook)
+ some resources absent in FHIR documentation
+ CareVue and Metavision data should be kept in a single resource
+ In the `transform_procedures_icd()` (004-kba-... notebook) authors messed up with the "followUp" feature
+ etc

I've fixed some, but I did not check every mapping due to lack of time.

## Usage

+ Download MIMIC-III dataset ad put it into the data folder
+ Run jupyter notebooks in ascending order
+ collect CSV files from `data/out/` folder

## TODO

- [x] add dtypes for all `pd.read_csv()` to [avoid dtype guessing](https://stackoverflow.com/questions/24251219/pandas-read-csv-low-memory-and-dtype-options)
- [x] make it run on the laptop with 16GB RAM
- [x] ~~make `mimic_fhir_transformation.py` runnable and actually working code or~~ screw it and use jupyter notebooks instead
- [ ] check mappings
- [ ] convert TRANSFERS table (should be part of encounter)?
- [ ] JSON output?
- [ ] refactoring and documentation

## Conversion scheme (original)

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

- [CALLOUT](https://mimic.physionet.org/mimictables/callout/) looks like something unimportant
- [DRGCODES](https://mimic.physionet.org/mimictables/drgcodes/) feels like [it is not part of FHIR](https://www.hl7.org/fhir/codesystem-ex-diagnosisrelatedgroup.html), [the full registry of code systems defined as part of FHIR](https://www.hl7.org/fhir/terminologies-systems.html)
- **[TRANSFERS](https://mimic.physionet.org/mimictables/transfers/) should be in [encounters](https://www.hl7.org/fhir/encounter.html)?**
- [D_CPT](https://mimic.physionet.org/mimictables/d_cpt/) [not normative yet](https://www.hl7.org/fhir/cpt.html)
- [D_ICD_DIAGNOSES](https://mimic.physionet.org/mimictables/d_icd_diagnoses/) just ICD9 codes reference table
- [D_ICD_PROCEDURES](https://mimic.physionet.org/mimictables/d_icd_procedures/) just ICD9 codes reference table
