CREATE PROCEDURE CALCULATE_HASH (
    STRINGTOHASH Varchar(4000) )
RETURNS (
    RET Bigint )
AS
DECLARE VARIABLE counter INT; 
DECLARE VARIABLE switch INT;
DECLARE VARIABLE curChar CHAR;
DECLARE VARIABLE lastChar CHAR;
BEGIN
  lastChar = SUBSTRING(stringToHash FROM 1 FOR 1);
  counter = 2;
  ret = 0;
  switch = 0;
  WHILE (counter < CHAR_LENGTH(stringToHash)) DO
  BEGIN
    curChar = SUBSTRING(stringToHash FROM counter FOR 1);
    IF (switch < 8 ) THEN
        BEGIN
            ret = ret + (ASCII_VAL(curChar)*
                ASCII_VAL(lastChar));
            switch=switch+1;
        END
    ELSE
        BEGIN
            ret = ret - (ASCII_VAL(curChar)*
                ASCII_VAL(lastChar));
            switch=0;
        END
    lastChar=curChar;
    counter = counter+1;
  END
END