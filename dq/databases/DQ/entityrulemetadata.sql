// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

CREATE TABLE IF NOT EXISTS SUBJECT_AREA_DB.DQ_ENTITYRULEMETADATA_TABLE_NAME(
  source        STRING
 ,entity        STRING
 ,rulename      STRING
 ,parameters    STRING
 )
  USING DELTA
    LOCATION 'ADLS_PATH_GEN2/SUBJECT_AREA/DQ_ENTITYRULEMETADATA_TABLE_NAME'