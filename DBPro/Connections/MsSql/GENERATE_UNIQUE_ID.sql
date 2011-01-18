﻿CREATE FUNCTION Org_Reddragonit_Dbpro_Connections_MsSql_GeneateUniqueID
(
	@R1 BIGINT,
	@R2 BIGINT,
	@R3 BIGINT,
	@R4 BIGINT,
	@R5 BIGINT
)
RETURNS VARCHAR(4000)
AS
BEGIN
	DECLARE @ret VARCHAR(4000),
	@SECT VARCHAR(50),
	@RAND_NUM BIGINT,
	@TMP BIGINT;
	
	SET @RAND_NUM = @R1;
	SET @SECT = (SELECT [dbo].[Org_Reddragonit_Dbpro_Connections_MsSql_ConvertBigintToCharstring](@RAND_NUM));
	SET @ret = SUBSTRING(@SECT,3,5)+'-';
	
	SET @TMP = @RAND_NUM;
	SET	@RAND_NUM = @R2;
	SET @RAND_NUM = @TMP + (@RAND_NUM/20);
	SET @TMP = @RAND_NUM;
	SET @SECT = (SELECT [dbo].[Org_Reddragonit_Dbpro_Connections_MsSql_ConvertBigintToCharstring](@RAND_NUM));
	SET @ret = @ret+SUBSTRING(@SECT,1,8)+'-';

	SET @TMP = @RAND_NUM;
	SET	@RAND_NUM = @R3;
	SET @RAND_NUM = @TMP - (@RAND_NUM/40);
	SET @TMP = @RAND_NUM;
	SET @SECT = (SELECT [dbo].[Org_Reddragonit_Dbpro_Connections_MsSql_ConvertBigintToCharstring](@RAND_NUM));
	SET @ret = @ret+SUBSTRING(@SECT,1,8)+'-';

	SET @TMP = @RAND_NUM;
	SET	@RAND_NUM = @R4;
	SET @RAND_NUM = @TMP + (@RAND_NUM/60);
	SET @TMP = @RAND_NUM;
	SET @SECT = (SELECT [dbo].[Org_Reddragonit_Dbpro_Connections_MsSql_ConvertBigintToCharstring](@RAND_NUM));
	SET @ret = @ret+SUBSTRING(@SECT,1,8)+'-';

	SET @TMP = @RAND_NUM;
	SET	@RAND_NUM = @R5;
	SET @RAND_NUM = @TMP - (@RAND_NUM/80);
	SET @TMP = @RAND_NUM;
	SET @SECT = (SELECT [dbo].[Org_Reddragonit_Dbpro_Connections_MsSql_ConvertBigintToCharstring](@RAND_NUM));
	SET @ret = @ret+SUBSTRING(@SECT,3,5);

	RETURN @ret;
END