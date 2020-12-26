// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

CREATE TABLE IF NOT EXISTS SUBJECT_AREA_DB.DQ_RUNDETAILS_TABLE_NAME(
DqAggTableKey           STRING,
Source                  STRING,
Entity                  STRING,
ColumnName              STRING,
Rule                    STRING,
RecordCount             BIGINT,
PipelineId              STRING,
PlatformModifiedDate    TIMESTAMP,
PlatformModifiedDateInt BIGINT
)
USING delta
location 'ADLS_PATH_GEN2/SUBJECT_AREA/DQ_RUNDETAILS_TABLE_NAME'