USE SCHEMA CRICKET.RAW;

-- 1. Create Raw Table to hold full JSON record from the stage
CREATE TABLE IF NOT EXISTS MATCH_RAW_TBL (
    META            OBJECT,     -- Stores the root 'meta' element (Object data type)
    INFO            VARIANT,    -- Stores the root 'info' element (Variant data type)
    INNINGS         ARRAY,      -- Stores the root 'innings' element (Array data type)
    STG_FILE_NAME   TEXT,
    STG_ROW_NUMBER  NUMBER,
    STG_FILE_HASH   TEXT,
    MODIFIED_TS     TIMESTAMP_NTZ
);

-- 2. Load data from stage into Raw table
COPY INTO CRICKET.RAW.MATCH_RAW_TBL
FROM (
    SELECT
        $1:meta::OBJECT,   -- Extract 'meta' element as OBJECT
        $1:info::VARIANT,  -- Extract 'info' element as VARIANT
        $1:innings::ARRAY, -- Extract 'innings' element as ARRAY
        METADATA$FILENAME, -- Audit column: Source file name
        METADATA$FILE_ROW_NUMBER, -- Audit column: Row number within the file
        SHA2(METADATA$FILENAME, 256), -- Audit column: File hash key
        CURRENT_TIMESTAMP() -- Audit column: Load timestamp
    FROM @CRICKET.LAND.MY_STAGE
)
FILE_FORMAT = (FORMAT_NAME = CRICKET.LAND.MY_JSON_FORMAT)
ON_ERROR = 'CONTINUE'; -- Continue loading even if some files fail
