// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

CREATE TABLE IF NOT EXISTS SUBJECT_AREA_DB.DQ_AGGREGATE_TABLE_NAME(
DqAggTableKey           STRING,
Source                  STRING,
Entity                  STRING,
ColumnName              STRING,
Rule                    STRING,
DQModifiedDate          TIMESTAMP,
DQModifiedDateInt       BIGINT,
RecordCount             BIGINT,
FailCount               BIGINT,
SuccessCount            BIGINT,
PlatformModifiedDate    TIMESTAMP,
PlatformModifiedDateInt BIGINT
)
USING delta
location 'ADLS_PATH_GEN2/SUBJECT_AREA/DQ_AGGREGATE_TABLE_NAME'