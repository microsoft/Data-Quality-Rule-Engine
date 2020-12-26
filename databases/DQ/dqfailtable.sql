// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

CREATE TABLE IF NOT EXISTS SUBJECT_AREA_DB.DQ_FAIL_TABLE_NAME(
Source                  STRING,
Entity                  STRING,
ColumnName              STRING,
Rule                    STRING,
Record                  STRING,
PlatformModifiedDate    TIMESTAMP,
PlatformModifiedDateInt BIGINT,
DqAggTableKey           STRING
)
USING delta
location 'ADLS_PATH_GEN2/SUBJECT_AREA/DQ_FAIL_TABLE_NAME'