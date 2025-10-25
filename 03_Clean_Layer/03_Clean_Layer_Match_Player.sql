USE SCHEMA CRICKET.CLEAN;

-- A. Match Detail Clean Table (Event/Match Level)
CREATE OR REPLACE TABLE MATCH_DETAIL_CLEAN AS
SELECT
    RAW.INFO:match_type_number::NUMBER AS MATCH_TYPE_NUMBER, -- Primary Key Candidate
    RAW.INFO:event.name::TEXT AS EVENT_NAME,
    COALESCE(RAW.INFO:event.match_number::TEXT, RAW.INFO:event.stage::TEXT, 'N/A') AS MATCH_STAGE,
    RAW.INFO:dates[0]::DATE AS EVENT_DATE, -- Takes the first date (Index 0)
    YEAR(RAW.INFO:dates[0]::DATE) AS EVENT_YEAR,
    RAW.INFO:match_type::TEXT AS MATCH_TYPE,
    RAW.INFO:season::TEXT AS SEASON,
    RAW.INFO:team_type::TEXT AS TEAM_TYPE,
    RAW.INFO:overs::NUMBER AS TOTAL_OVERS,
    COALESCE(RAW.INFO:city::TEXT, 'N/A') AS CITY,
    RAW.INFO:venue::TEXT AS VENUE,
    RAW.INFO:teams[0]::TEXT AS TEAM_A,
    RAW.INFO:teams[1]::TEXT AS TEAM_B,
    CASE
        WHEN RAW.INFO:outcome.winner IS NOT NULL THEN RAW.INFO:outcome.winner::TEXT
        WHEN RAW.INFO:outcome.result::TEXT = 'tie' THEN 'TIE'
        WHEN RAW.INFO:outcome.result::TEXT = 'no result' THEN 'NO RESULT'
        ELSE 'RESULT UNDECLARED'
    END AS MATCH_RESULT_STATUS,
    RAW.INFO:toss.winner::TEXT AS TOSS_WINNER_TEAM,
    INITCAP(RAW.INFO:toss.decision::TEXT) AS TOSS_DECISION,
    RAW.STG_FILE_NAME,
    RAW.STG_ROW_NUMBER
FROM CRICKET.RAW.MATCH_RAW_TBL RAW;

-- B. Player Clean Table
CREATE OR REPLACE TABLE PLAYER_CLEAN AS
SELECT
    RAW.INFO:match_type_number::NUMBER AS MATCH_TYPE_NUMBER,
    P.KEY::TEXT AS COUNTRY, -- Team name (Country) from the key
    T.VALUE::TEXT AS PLAYER_NAME,
    RAW.STG_FILE_NAME
FROM CRICKET.RAW.MATCH_RAW_TBL RAW,
LATERAL FLATTEN(INPUT => RAW.INFO:players) P -- Flatten the players object (Key=Team, Value=Array of players)
, LATERAL FLATTEN(INPUT => P.VALUE) T; -- Flatten the array of players for each team
