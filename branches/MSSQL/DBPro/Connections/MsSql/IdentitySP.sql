SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		Roger Castaldo
-- Create date: March 8, 2009
-- Description:	This procedure is designed to add or remove identity from a column
-- Version: 1.0
-- =============================================
CREATE PROCEDURE Org_Reddragonit_DbPro_Create_Remove_Identity
	@table VARCHAR(250),
	@field VARCHAR(250),
	@createIdent bit,
	@version bit
AS
BEGIN
	SET NOCOUNT ON;

	IF (@version=1)
		BEGIN
			SELECT '1.0';
		END
	ELSE
		BEGIN
			DECLARE @createTableQuery varchar(MAX);
			DECLARE @createField varchar(MAX);
			DECLARE @primaryField varchar(MAX);
			DECLARE @primaryKeys varchar(MAX);
			DECLARE @curValue BIGINT;
			DECLARE @fields varchar(MAX);
			DECLARE @tmpField varchar(MAX);


			--extrract current id value
			create table #return (code bigint null)
			EXEC('INSERT INTO #return SELECT (CASE WHEN MAX('+@field+') IS NULL THEN 1 ELSE MAX('+@field+')+1 END) FROM '+@table);
			set @curValue = (SELECT code from #return);

			--extract query to create temp table
			DECLARE tblSelect CURSOR FOR 
				SELECT  '['+c.column_name+'] '+UPPER(c.data_type)+  
					(CASE WHEN c.character_maximum_length is not null then  '('+cast(c.character_maximum_length as varchar(MAX))+')' ELSE '' END)+' '+
					(CASE WHEN c.column_name = @field AND @createIdent=1 THEN 'IDENTITY('+CAST(@curValue as VARCHAR(MAX))+',1)' ELSE '' END)+
					(CASE WHEN c.is_nullable='NO' THEN ' NOT NULL ' ELSE ' NULL ' END),
					(CASE WHEN primarys.IsPrimary is null THEN NULL ELSE '['+c.column_name+']' END),
					'['+c.column_name+']'
					FROM INFORMATION_SCHEMA.COLUMNS c  
						LEFT JOIN (SELECT k.column_name,1 as IsPrimary FROM   
							INFORMATION_SCHEMA.KEY_COLUMN_USAGE k, 
							INFORMATION_SCHEMA.TABLE_CONSTRAINTS c  WHERE   
							k.table_name = c.table_name  AND 
							c.table_name = @table  AND 
							k.table_schema = c.table_schema  AND 
							k.table_catalog = c.table_catalog  AND 
							k.constraint_catalog = c.constraint_catalog  AND 
							k.constraint_name = c.constraint_name   AND 
							c.constraint_type = 'PRIMARY KEY') primarys ON   
					c.column_name = primarys.column_name  WHERE c.table_name = @table; 

			SET @createTableQuery='CREATE TABLE TEMP_'+@table+'(';
			SET @primaryKeys='';
			SET @fields='';

			OPEN tblSelect;

			FETCH NEXT FROM tblSelect
			INTO @createField,@primaryField,@tmpField;

			WHILE @@FETCH_STATUS = 0
			BEGIN

				SET @createTableQuery=@createTableQuery+@createField+',';
				SET @fields=@fields+@tmpField+',';
				IF (@primaryField is not null)
					BEGIN
						SET @primaryKeys = @primaryKeys+@primaryField+',';
					END

				FETCH NEXT FROM tblSelect
				INTO @createField,@primaryField,@tmpField;

			END

			CLOSE tblSelect;

			IF (LEN(@primaryKeys)>0)
				BEGIN
					SET @primaryKeys = SUBSTRING(@primaryKeys,0,LEN(@primaryKeys));
					SET @primaryKeys = ',PRIMARY KEY ('+@primaryKeys+')';
				END

			SET @fields=SUBSTRING(@fields,0,LEN(@fields));

			SET @createTableQuery=SUBSTRING(@createTableQuery,0,LEN(@createTableQuery)-1)+@primaryKeys+')';

			--create temporary table
			EXEC(@createTableQuery);

			--dump data into temporary table
			EXEC('SET IDENTITY_INSERT TEMP_'+@table+' ON; '+
			'INSERT INTO TEMP_'+@table+'('+@fields+') SELECT * FROM '+@table+'; '+
			'SET IDENTITY_INSERT TEMP_'+@table+' OFF;');

			--creating list of queries for relationship transfers
			CREATE TABLE #updates(val varchar(MAX));
			DECLARE @externalFields varchar(MAX);
			DECLARE @internalFields varchar(MAX);
			DECLARE @tableName varchar(MAX);
			DECLARE @internalField varchar(MAX);
			DECLARE @externalField varchar(MAX);
			DECLARE @update varchar(MAX);
			DECLARE @delete varchar(MAX);
			DECLARE @lastUpdate varchar(MAX);
			DECLARE @lastDelete varchar(MAX);
			DECLARE @lastTable varchar(MAX);
			DECLARE curKeys CURSOR FOR 
				SELECT k.table_name,k.column_name, 
				ccu.column_name 'references_field', 
								rc.update_rule 'on_update', 
								rc.delete_rule 'on_delete' 
								FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE k 
								LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS c 
								ON k.table_name = c.table_name 
								AND k.table_schema = c.table_schema 
								AND k.table_catalog = c.table_catalog 
								AND k.constraint_catalog = c.constraint_catalog 
								AND k.constraint_name = c.constraint_name 
								LEFT JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc 
								ON rc.constraint_schema = c.constraint_schema 
								AND rc.constraint_catalog = c.constraint_catalog 
								AND rc.constraint_name = c.constraint_name 
								LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu 
								ON rc.unique_constraint_schema = ccu.constraint_schema 
								AND rc.unique_constraint_catalog = ccu.constraint_catalog 
								AND rc.unique_constraint_name = ccu.constraint_name 
								WHERE k.constraint_catalog = DB_NAME() 
								AND ccu.table_name = @table 
								AND c.constraint_type = 'FOREIGN KEY' ORDER BY k.table_name;

			SET @externalFields='';
			SET @internalFields='';

			OPEN curKeys;

			FETCH NEXT FROM curKeys
			INTO @tableName,@internalField,@externalField,@update,@delete;

			SET @lastUpdate=@update;
			SET @lastDelete=@delete;
			SET @lastTable=@tableName;

			WHILE @@FETCH_STATUS = 0
			BEGIN

				SET @internalFields=@internalFields+@internalField+',';
				SET @externalFields=@externalFields+@externalField+',';

				FETCH NEXT FROM curKeys
				INTO @tableName,@internalField,@externalField,@update,@delete;

				if (@lastTable<>@tableName)
					BEGIN
						INSERT INTO #updates select 'ALTER TABLE '+@lastTable+' DROP CONSTRAINT '+ cast(f.name  as varchar(255))+';' 
								from sysobjects f 
								inner join sysobjects c on  f.parent_obj = c.id 
								inner join sysreferences r on f.id =  r.constid 
								inner join sysobjects p on r.rkeyid = p.id 
								inner  join syscolumns rc on r.rkeyid = rc.id and r.rkey1 = rc.colid 
								inner  join syscolumns fc on r.fkeyid = fc.id and r.fkey1 = fc.colid 
								left join  syscolumns rc2 on r.rkeyid = rc2.id and r.rkey2 = rc.colid 
								left join  syscolumns fc2 on r.fkeyid = fc2.id and r.fkey2 = fc.colid 
								where f.type =  'F' AND cast(c.name as  varchar(255))=@lastTable
								AND cast(p.name as varchar(255)) = @table;
						INSERT INTO #updates VALUES('ALTER TABLE '+@lastTable+' ADD FOREIGN KEY('+
							SUBSTRING(@internalFields,0,LEN(@internalFields))+') REFERENCES '+
							'TEMP_'+@table+'('+SUBSTRING(@externalFields,0,LEN(@externalFields))+')'+
							'ON UPDATE '+@lastUpdate+' ON DELETE '+@lastDelete);
						SET @lastTable=@tableName;
						SET @lastUpdate=@update;
						SET @lastDelete=@delete;
						SET @internalFields='';
						SET @externalFields='';
					END
			END

			CLOSE curKeys;

			INSERT INTO #updates select 'ALTER TABLE '+@lastTable+' DROP CONSTRAINT '+ cast(f.name  as varchar(255))+';' 
								from sysobjects f 
								inner join sysobjects c on  f.parent_obj = c.id 
								inner join sysreferences r on f.id =  r.constid 
								inner join sysobjects p on r.rkeyid = p.id 
								inner  join syscolumns rc on r.rkeyid = rc.id and r.rkey1 = rc.colid 
								inner  join syscolumns fc on r.fkeyid = fc.id and r.fkey1 = fc.colid 
								left join  syscolumns rc2 on r.rkeyid = rc2.id and r.rkey2 = rc.colid 
								left join  syscolumns fc2 on r.fkeyid = fc2.id and r.fkey2 = fc.colid 
								where f.type =  'F' AND cast(c.name as  varchar(255))=@lastTable
								AND cast(p.name as varchar(255)) = @table;
			INSERT INTO #updates select 'ALTER TABLE '+@tableName+' DROP CONSTRAINT '+ cast(f.name  as varchar(255))+';' 
								from sysobjects f 
								inner join sysobjects c on  f.parent_obj = c.id 
								inner join sysreferences r on f.id =  r.constid 
								inner join sysobjects p on r.rkeyid = p.id 
								inner  join syscolumns rc on r.rkeyid = rc.id and r.rkey1 = rc.colid 
								inner  join syscolumns fc on r.fkeyid = fc.id and r.fkey1 = fc.colid 
								left join  syscolumns rc2 on r.rkeyid = rc2.id and r.rkey2 = rc.colid 
								left join  syscolumns fc2 on r.fkeyid = fc2.id and r.fkey2 = fc.colid 
								where f.type =  'F' AND cast(c.name as  varchar(255))=@tableName
								AND cast(p.name as varchar(255)) = @internalField;
			INSERT INTO #updates VALUES('ALTER TABLE '+@lastTable+' ADD FOREIGN KEY('+
							SUBSTRING(@internalFields,0,LEN(@internalFields))+') REFERENCES '+
							'TEMP_'+@table+'('+SUBSTRING(@externalFields,0,LEN(@externalFields))+')'+
							'ON UPDATE '+@lastUpdate+' ON DELETE '+@lastDelete);

			--executing relationship updates
			DECLARE @updateString varchar(MAX);
			DECLARE curUpdates CURSOR FOR 
				SELECT * FROM #updates;

			OPEN curUpdates;

			FETCH NEXT FROM curUpdates
			INTO @updateString;

			WHILE @@FETCH_STATUS = 0
			BEGIN

				EXEC(@updateString);	

				FETCH NEXT FROM curUpdates
					INTO @updateString;
			END

			CLOSE curUpdates;

			--delete existing table and then move temp table to replace it
			EXEC('DROP TABLE '+@table+';');
			EXEC('EXEC sp_rename ''TEMP_'+@table+''', '''+@table+''';');

			--cleanup
			DROP TABLE #updates;
			DROP TABLE #return;
			DEALLOCATE tblSelect;
			DEALLOCATE curKeys;
			DEALLOCATE curUpdates;
		END
END
GO
