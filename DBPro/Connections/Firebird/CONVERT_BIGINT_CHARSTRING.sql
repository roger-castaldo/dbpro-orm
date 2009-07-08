SET TERM ^ ;
CREATE PROCEDURE CONVERT_BIGINT_CHARSTRING (
    INTVAL Bigint )
RETURNS (
    RET Varchar(50) )
AS
DECLARE VARIABLE counter INT; 
DECLARE VARIABLE stringToHash VARCHAR(4000);
DECLARE VARIABLE numVal INT;
BEGIN
  stringToHash = CAST(intVal AS VARCHAR(4000));
  counter = 1;
  ret = '';
  WHILE (counter < CHAR_LENGTH(stringToHash)) DO
  BEGIN
    IF ((counter+1)<CHAR_LENGTH(stringToHash)) THEN
        numVal = CAST(SUBSTRING(stringToHash FROM counter FOR 2) AS INT);
    ELSE
        numVal = CAST(SUBSTRING(stringToHash FROM counter FOR 1) AS INT);
    numVal = numVal+20;
    IF (numVal<48) THEN
        numVal = numVal + 28;
    IF (numVal BETWEEN 58 AND 64) THEN
        numVal = numVal + 12;
    IF (numVal BETWEEN 91 AND 97) THEN
        numVal = numVal + 12;
    IF (numVal>122) THEN
        numVal = numVal - 20;
    IF (numVal BETWEEN 58 AND 64) THEN
        numVal = numVal + 12;
    IF (numVal BETWEEN 91 AND 97) THEN
        numVal = numVal + 12;
    ret=ret;
    ret = ret || ASCII_CHAR(numVal);
    counter = counter+1;
  END
END^
SET TERM ; ^
