CREATE FUNCTION Org_Reddragonit_Dbpro_Connections_MsSql_ConvertBigintToCharstring 
(
	@IntVal BIGINT
)
RETURNS VARCHAR(50)
AS
BEGIN
	DECLARE @ret VARCHAR(50),
	@StringToHash VARCHAR(4000),
	@numVal INT,
	@counter INT;
	SET @counter = 1;
	SET @ret = '';
	SET @StringToHash = CAST(@IntVal as VARCHAR(4000));
	WHILE (@counter < LEN(@StringToHash))
	BEGIN
		IF ((@counter+1)<LEN(@StringToHash))
		BEGIN
			SET @numVal = CAST(SUBSTRING(@StringToHash,@Counter,2) AS INT);
		END
		ELSE
		BEGIN
			SET @numVal = CAST(SUBSTRING(@StringToHash,@Counter,1) AS INT);
		END
		SET @numVal = @numVal + 20;
		IF (@numVal<48)
		BEGIN
			SET @numVal = @numVal + 28;
		END
		IF (@numVal BETWEEN 58 AND 64)
		BEGIN
			SET @numVal = @numVal + 12;
		END
		IF (@numVal BETWEEN 91 AND 97)
		BEGIN
			SET @numVal = @numVal + 12;
		END
		IF (@numVal>122)
		BEGIN
			SET @numVal = @numVal - 20;
		END
		IF (@numVal BETWEEN 58 AND 64)
		BEGIN
			SET @numVal = @numVal + 12;
		END
		IF (@numVal BETWEEN 91 AND 97)
		BEGIN
			SET @numVal = @numVal + 12;
		END
		SET @ret = @ret + CHAR(@numVal);
		SET @counter = @counter+1;
	END
	RETURN @ret;
END