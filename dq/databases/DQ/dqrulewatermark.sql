// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

CREATE TABLE IF NOT EXISTS SUBJECT_AREA_DB.DQ_RULE_WATERMARK_TABLE_NAME
(
    SubjectArea               STRING      NOT NULL
    ,SourceEntityName         STRING      NOT NULL
    ,RuleName                 STRING      NOT NULL
    ,WaterMarkStartValue      STRING      NOT NULL
    ,WaterMarkEndValue        STRING      NOT NULL
    ,PlatformModifiedDate     TIMESTAMP
    ,PlatformModifiedDateInt  BIGINT
)
USING DELTA
LOCATION 'ADLS_PATH_GEN2/SUBJECT_AREA/DQ_RULE_WATERMARK_TABLE_NAME'
