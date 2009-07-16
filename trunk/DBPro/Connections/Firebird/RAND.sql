﻿CREATE PROCEDURE RAND
RETURNS ( RET Bigint )
AS
BEGIN
  SELECT MAX(CAST(REPLACE(CAST(CURRENT_DATE AS VARCHAR(20)),'-','') AS BIGINT)+
        (CAST(REPLACE(REPLACE(CAST(CURRENT_TIME AS VARCHAR(20)),':',''),'.','') AS BIGINT)/1000))
        FROM RDB$DATABASE INTO :ret;
  SELECT (MAX(CAST(REPLACE(CAST(CURRENT_DATE AS VARCHAR(20)),'-','') AS BIGINT)+
        (CAST(REPLACE(REPLACE(CAST(CURRENT_TIME AS VARCHAR(20)),':',''),'.','') AS BIGINT)/1000))*
        :ret)
        FROM RDB$DATABASE INTO :ret;
END