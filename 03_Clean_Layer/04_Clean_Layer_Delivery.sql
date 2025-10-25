USE SCHEMA CRICKET.CLEAN;

CREATE OR REPLACE TABLE DELIVERY_CLEAN AS
SELECT
    RAW.INFO:match_type_number::NUMBER AS MATCH_TYPE_NUMBER,
    I.VALUE:team::TEXT AS INNINGS_TEAM_NAME, -- Team playing the inning
    I.INDEX + 1 AS INNINGS_NUMBER, -- 1st or 2nd Inning
    O.INDEX + 1 AS OVER_NUMBER, -- The over number (0-50)
    D.KEY::NUMBER AS BALL_NUMBER, -- The delivery sequence within the over (0.1 to 0.6, etc.)
    D.VALUE:runs.batter::NUMBER AS RUNS_SCORED_BATTER,
    D.VALUE:runs.extra::NUMBER AS RUNS_SCORED_EXTRA,
    D.VALUE:runs.total::NUMBER AS RUNS_SCORED_TOTAL,
    D.VALUE:batter::TEXT AS BATTER_NAME,
    D.VALUE:bowler::TEXT AS BOWLER_NAME,
    D.VALUE:non_striker::TEXT AS NON_STRIKER_NAME,
    E.KEY::TEXT AS EXTRA_TYPE, -- Type of extra (e.g., 'wides', 'noballs')
    W.VALUE:kind::TEXT AS WICKET_KIND_OF_OUT,
    W.VALUE:player_out::TEXT AS WICKET_PLAYER_OUT,
    W.VALUE:fielder::TEXT AS WICKET_FIELDER,
    RAW.STG_FILE_NAME
FROM CRICKET.RAW.MATCH_RAW_TBL RAW,
LATERAL FLATTEN(INPUT => RAW.INNINGS) I, -- 1. Flatten the Innings Array (I)
LATERAL FLATTEN(INPUT => I.VALUE:overs) O, -- 2. Flatten the Overs Array within the Innings (O)
LATERAL FLATTEN(INPUT => O.VALUE:deliveries) D -- 3. Flatten the Deliveries Array within the Over (D)
, LATERAL FLATTEN(INPUT => D.VALUE:extras, OUTER => TRUE) E -- Outer Join for optional 'extras' element
, LATERAL FLATTEN(INPUT => D.VALUE:wickets, OUTER => TRUE) W; -- Outer Join for optional 'wickets' element
