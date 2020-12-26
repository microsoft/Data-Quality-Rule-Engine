// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

CREATE TABLE IF NOT EXISTS SUBJECT_AREA_DB.DQ_ORPHANEDGEMETADATA_TABLE_NAME(
 source                 STRING
 ,entity                STRING
 ,fromcolumnname        STRING
 ,fromlookupentity      STRING
 ,fromlookupcolumnname  STRING
 ,tocolumnname          STRING
 ,tolookupentity        STRING
 ,tolookupcolumnname    STRING
 ,filtercondition       STRING
 )
  USING DELTA
    LOCATION 'ADLS_PATH_GEN2/SUBJECT_AREA/DQ_ORPHANEDGEMETADATA_TABLE_NAME'