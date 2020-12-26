// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

CREATE TABLE IF NOT EXISTS SUBJECT_AREA_DB.DQ_METADATA_WATERMARK_TABLE_NAME(
  dqtable               STRING
 ,source                STRING
 ,entity                STRING
 ,watermarkendvalue     STRING
 ,omidqcreateddate      STRING
 ,omidqmodifieddate     STRING
 )
  USING DELTA
    LOCATION 'ADLS_PATH_GEN2/SUBJECT_AREA/DQ_METADATA_WATERMARK_TABLE_NAME'