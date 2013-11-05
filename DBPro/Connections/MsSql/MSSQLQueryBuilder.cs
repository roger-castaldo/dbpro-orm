using Org.Reddragonit.Dbpro.Connections.Parameters;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using Org.Reddragonit.Dbpro.Connections.PoolComponents;
using Org.Reddragonit.Dbpro.Structure.Attributes;

namespace Org.Reddragonit.Dbpro.Connections.MsSql
{
	internal class MSSQLQueryBuilder : QueryBuilder
	{
		public MSSQLQueryBuilder(ConnectionPool pool) : base(pool)
		{}
		
		protected override string SelectTableNamesString
		{
			get
			{
				return "SELECT LTRIM(RTRIM(name)) FROM sysobjects WHERE xtype = 'U';";
			}
		}

		protected override string SelectTableFieldsString
		{
			get
			{
                return @"SELECT  c.column_name 'name',
					(CASE WHEN UPPER(c.data_type) = 'DECIMAL' THEN 'DECIMAL('+CAST(c.NUMERIC_PRECISION AS VARCHAR(4))+','+CAST(c.NUMERIC_SCALE AS VARCHAR(4))+')' ELSE UPPER(c.DATA_TYPE) END) 'type',
					(CASE WHEN c.character_maximum_length is null then 
					(CASE WHEN UPPER(c.data_type) = 'BIT' THEN 1 
					WHEN UPPER(c.data_type) = 'DATETIME' OR UPPER(c.data_type) = 'DECIMAL' OR UPPER(c.data_type) = 'FLOAT' OR UPPER(c.data_type) = 'BIGINT' OR UPPER(c.data_type) = 'MONEY' THEN 8  
					WHEN UPPER(c.data_type) = 'INT' THEN 4 
					WHEN UPPER(c.data_type) = 'SMALLINT' THEN 2 
					END) 
					ELSE  
					(CASE WHEN UPPER(c.data_type)='IMAGE' OR UPPER(c.data_type) = 'TEXT' THEN -1 ELSE c.character_maximum_length END) END) 'length',+
					(CASE WHEN primarys.IsPrimary is null THEN 'false' ELSE 'true' END) as IsPrimary,
					(CASE WHEN c.is_nullable='NO' THEN 'false' ELSE 'true' END) as IsNullable,
					(CASE WHEN COLUMNPROPERTY( OBJECT_ID('{0}'),c.column_name,'IsIdentity') = 0 THEN 'false' else 'true' END) as IsIdentity,
					(CASE 
					WHEN SUBSTRING(comCalls.COMPUTED_CODE,1,25) = '(CONVERT([decimal](18,9),' THEN SUBSTRING(comCalls.COMPUTED_CODE,26,LEN(comCalls.COMPUTED_CODE)-29)
					WHEN comCalls.COMPUTED_CODE LIKE '(CONVERT(%' THEN SUBSTRING(comCalls.COMPUTED_CODE,CHARINDEX(',',comCalls.COMPUTED_CODE)+1,LEN(comCalls.COMPUTED_CODE)-CHARINDEX(',',comCalls.COMPUTED_CODE)-4)
					ELSE comCalls.COMPUTED_CODE END) AS COMPUTED_CODE
					FROM INFORMATION_SCHEMA.COLUMNS c
					LEFT JOIN (SELECT k.column_name,1 as IsPrimary FROM 
					INFORMATION_SCHEMA.KEY_COLUMN_USAGE k, INFORMATION_SCHEMA.TABLE_CONSTRAINTS c
					WHERE 
					k.table_name = c.table_name
					AND c.table_name = '{0}'
					AND k.table_schema = c.table_schema
					AND k.table_catalog = c.table_catalog
					AND k.constraint_catalog = c.constraint_catalog
					AND k.constraint_name = c.constraint_name 
					AND c.constraint_type = 'PRIMARY KEY') primarys ON 
					c.column_name = primarys.column_name
					LEFT JOIN (
						SELECT 
							sysobjects.name AS TableName, 
							syscolumns.name AS ColumnName,
							com_cols.definition AS COMPUTED_CODE
						FROM syscolumns
							INNER JOIN sysobjects
							ON syscolumns.id = sysobjects.id
							AND sysobjects.xtype = 'U',
							sys.computed_columns com_cols
						WHERE syscolumns.iscomputed = 1
						AND syscolumns.id=com_cols.object_id
						AND syscolumns.name=com_cols.name
					) comCalls ON c.TABLE_NAME=comCalls.TableName
					AND c.COLUMN_NAME = comCalls.ColumnName
					WHERE c.table_name = '{0}'";
			}
		}

		protected override string SelectForeignKeysString
		{
            get
            {
                return @"select DISTINCT 
                 convert(sysname,COL2.name) as field_name, 
                 OBJECT_NAME(FKEYS.referenced_object_id) as references_table, 
                 convert(sysname,col1.name) as references_field, 
                 FKEYS.update_referential_action_desc as on_update, 
                 FKEYS.delete_referential_action_desc as on_delete, 
                 CAST(FKEYS.object_id as VARCHAR(MAX)) as unique_id 
                 from 
                 sys.columns COL1, 
                 sys.columns COL2, 
                 sys.foreign_keys FKEYS 
                 inner join sys.foreign_key_columns KEY_COLUMN on (KEY_COLUMN.constraint_object_id = FKEYS.object_id) 
                 where 
                 COL1.object_id = FKEYS.referenced_object_id 
                 AND COL2.object_id = FKEYS.parent_object_id 
                 AND COL1.column_id = KEY_COLUMN.referenced_column_id 
                 AND COL2.column_id = KEY_COLUMN.parent_column_id 
                 AND OBJECT_NAME(FKEYS.PARENT_OBJECT_ID)='{0}'";
            }
		}
		
		protected override string SelectTriggersString {
			get
			{
                return @"SELECT tbl.trigger_name,
tbl.comm_string,
RIGHT(tbl.code,LEN(tbl.code)-LEN('CREATER TRIGGER '+tbl.trigger_name+tbl.comm_string)) code
FROM (SELECT sys1.name trigger_name, 
					 'ON '+sys2.name+' '+ 
					 (CASE WHEN OBJECTPROPERTY(sys1.id, 'ExecIsInsteadOfTrigger') = 1 
					 THEN 'INSTEAD OF' ELSE 'AFTER' 
					 END 
					 )+' '+ 
					 ( 
					 CASE 
					 WHEN OBJECTPROPERTY(sys1.id, 'ExecIsInsertTrigger') = 1 THEN 
					 (CASE WHEN OBJECTPROPERTY(sys1.id, 'ExecIsUpdateTrigger') = 1 THEN 
						(CASE WHEN OBJECTPROPERTY(sys1.id, 'ExecIsDeleteTrigger')=1 THEN 'INSERT, UPDATE, DELETE'
						ELSE 'INSERT, UPDATE' END)
					  ELSE 
						(CASE WHEN OBJECTPROPERTY(sys1.id, 'ExecIsDeleteTrigger')=1 THEN 'INSERT, DELETE'
					  	ELSE 'INSERT' END)
					 END)
					 WHEN OBJECTPROPERTY(sys1.id, 'ExecIsUpdateTrigger') = 1 THEN 
					 (CASE WHEN OBJECTPROPERTY(sys1.id, 'ExecIsDeleteTrigger') = 1 THEN 'UPDATE, DELETE' 
					 ELSE 'UPDATE' END) 
					 WHEN OBJECTPROPERTY(sys1.id, 'ExecIsDeleteTrigger') = 1 THEN 'DELETE' 
					 END 
					 )	comm_string, 
					 c.text as code
					 FROM sysobjects sys1 
					 JOIN sysobjects sys2 ON sys1.parent_obj = sys2.id 
					 JOIN syscomments c ON sys1.id = c.id 
					 WHERE sys1.xtype = 'TR') tbl";
			}
		}
		
		protected override string SelectCurrentIdentities {
			get {
				return @"SELECT DISTINCT c.table_name 'tableName', 
					c.column_name 'fieldName', 
					UPPER(c.data_type) 'type', 
					CAST(IDENT_CURRENT(c.table_name) as varchar(MAX)) 'curValue' 
					FROM INFORMATION_SCHEMA.COLUMNS c 
					LEFT JOIN (SELECT k.column_name,1 as IsPrimary FROM  
					INFORMATION_SCHEMA.KEY_COLUMN_USAGE k, INFORMATION_SCHEMA.TABLE_CONSTRAINTS c 
					WHERE  
					k.table_name = c.table_name 
					AND k.table_schema = c.table_schema 
					AND k.table_catalog = c.table_catalog 
					AND k.constraint_catalog = c.constraint_catalog 
					AND k.constraint_name = c.constraint_name  
					AND c.constraint_type = 'PRIMARY KEY') primarys ON  
					c.column_name = primarys.column_name 
					WHERE COLUMNPROPERTY( OBJECT_ID(c.table_name),c.column_name,'IsIdentity') = 1";
			}
		}
		
		protected override string CreateIdentityString {
			get { return "EXEC Org_Reddragonit_DbPro_Create_Remove_Identity '{0}','{1}',1,0"; }
		}
		
		protected override string DropIdentityFieldString {
			get { return "EXEC Org_Reddragonit_DbPro_Create_Remove_Identity '{0}','{1}',0,0"; }
		}
		
		protected override string SetIdentityFieldValueString {
			get { return "DBCC CHECKIDENT('{0}', RESEED, {3})"; }
		}
		
		protected override string DropNotNullString {
			get { return "ALTER TABLE [{0}] ALTER COLUMN [{1}] {2} NULL"; }
		}

		protected override string CreateNullConstraintString
		{
			get{return "ALTER TABLE [{0}] ALTER COLUMN [{1}] {2} NOT NULL";}
		}

        protected override string AlterFieldTypeString
        {
            get { return "ALTER TABLE [{0}] ALTER COLUMN [{1}] {2}"; }
        }
		
		protected override string DropPrimaryKeyString {
        	get { return "SELECT 'ALTER TABLE ['+OBJECT_NAME(parent_object_id)+'] DROP CONSTRAINT ['+OBJECT_NAME(OBJECT_ID)+']' AS DROP_STRING FROM sys.objects WHERE type_desc = 'PRIMARY_KEY_CONSTRAINT' AND OBJECT_NAME(parent_object_id)="+CreateParameterName("TableName"); }
		}

        internal override string CreateColumn(string table, ExtractedFieldMap field)
        {
            if (field.ComputedCode != null)
                return string.Format("ALTER TABLE[{0}] ADD [{1}] AS CONVERT({2},{3}) PERSISTED", table, field.FieldName, field.FullFieldType, field.ComputedCode);
            return base.CreateColumn(table, field);
        }

		internal override string DropPrimaryKey(PrimaryKey key)
		{
			string ret = "";
            Connection conn = pool.GetConnection();
			conn.ExecuteQuery(DropPrimaryKeyString, new IDbDataParameter[]{conn.CreateParameter(CreateParameterName("TableName"),key.Name)});
			if (conn.Read())
				ret = conn[0].ToString();
            conn.CloseConnection();
			return ret;
		}
		
		protected override string DropForeignKeyString {
			get { return "select 'ALTER TABLE [{0}] DROP CONSTRAINT '+ cast(f.name  as varchar(255))+';' "+
					" from sysobjects f "+
					" inner join sysobjects c on  f.parent_obj = c.id "+
					" inner join sysreferences r on f.id =  r.constid "+
					" inner join sysobjects p on r.rkeyid = p.id "+
					" inner  join syscolumns rc on r.rkeyid = rc.id and r.rkey1 = rc.colid "+
					" inner  join syscolumns fc on r.fkeyid = fc.id and r.fkey1 = fc.colid "+
					" left join  syscolumns rc2 on r.rkeyid = rc2.id and r.rkey2 = rc.colid "+
					" left join  syscolumns fc2 on r.fkeyid = fc2.id and r.fkey2 = fc.colid "+
					" where f.type =  'F' AND cast(c.name as  varchar(255))='{0}'"+
					" AND cast(p.name as varchar(255)) = '{1}'"+
                    " AND cast(rc.name as varchar(255)) = '{2}'"+
					" AND cast(fc.name as varchar(255)) = '{3}'"; }
		}

        internal override List<Index> ExtractTableIndexes(string tableName, Connection conn)
        {
            List<Index> ret = new List<Index>();
            Dictionary<string, string> objIds = new Dictionary<string, string>();
            List<string> indexIds = new List<string>();
            conn.ExecuteQuery(@"select ind.object_id,ind.index_id,ind.name,ind.is_unique from sys.indexes ind, sysobjects tbl 
                WHERE ind.object_id = tbl.id AND tbl.xtype='U' AND ind.name IS NOT NULL AND ind.is_primary_key=0 AND tbl.name = '"+tableName+"'");
            while (conn.Read())
            {
                ret.Add(new Index(conn[2].ToString(), null, conn[3].ToString() == "1", false));
                objIds.Add(conn[2].ToString(), conn[0].ToString());
                indexIds.Add(conn[1].ToString());
            }
            conn.Close();
            for (int x = 0; x < ret.Count; x++)
            {
                List<string> fields = new List<string>();
                bool asc = false;
                conn.ExecuteQuery("SELECT c.COLUMN_NAME,indcol.is_descending_key FROM sys.index_columns indcol,INFORMATION_SCHEMA.COLUMNS c " +
                    "WHERE indcol.object_id = '"+objIds[ret[x].Name]+"' AND indcol.index_id = "+indexIds[x]+" AND c.table_name = '"+tableName+"' AND c.ORDINAL_POSITION = indcol.column_id ORDER BY index_id,key_ordinal");
                while (conn.Read())
                {
                    fields.Add(conn[0].ToString());
                    asc = conn[1].ToString() == "0";
                }
                conn.Close();
                Index ind = ret[x];
                ret.RemoveAt(x);
                ret.Insert(x, new Index(ind.Name,fields.ToArray(),ind.Unique,asc));
            }
            return ret;
        }

        internal override string DropForeignKey(string table, string externalTable, string primaryField, string relatedField)
		{
			string ret="";
            Connection conn = pool.GetConnection();
			conn.ExecuteQuery(string.Format(DropForeignKeyString,new object[]{table,externalTable,primaryField,relatedField}));
			while (conn.Read())
				ret+=conn[0].ToString()+"\n";
            conn.CloseConnection();
			return ret;
		}

        protected override string SelectWithPagingIncludeOffset
        {
			get {
                return "SELECT * FROM (SELECT *,ROW_NUMBER() OVER (ORDER BY {3}) RowNum" +
					" FROM ({0}) internalTbl) cntTbl WHERE RowNum BETWEEN {1} AND {1}+{2}";
			}
		}

        internal override string SelectPaged(System.Type type, SelectParameter[] parameters, out List<IDbDataParameter> queryParameters, ulong? start, ulong? recordCount, string[] OrderByFields)
        {
            if (!start.HasValue)
                start = 0;
            if (!recordCount.HasValue)
                recordCount = 0;
            string baseQuery = Select(type, parameters, out queryParameters, OrderByFields);
            queryParameters.Add(pool.CreateParameter(CreateParameterName("startIndex"), start.Value));
            queryParameters.Add(pool.CreateParameter(CreateParameterName("rowCount"), recordCount.Value));
            string primarys = "";
            if ((OrderByFields == null) || (OrderByFields.Length == 0))
            {
                foreach (string str in pool.Mapping[type].PrimaryKeyFields)
                    primarys += "," + str;
            }
            else if (baseQuery.Contains("ORDER BY"))
                primarys = baseQuery.Substring(baseQuery.IndexOf("ORDER BY") + "ORDER BY".Length);
            else
            {
                foreach (string str in OrderByFields)
                {
                    if (str.EndsWith(" ASC") || str.EndsWith(" DESC"))
                    {
                        string[] tmp = str.Split(new char[] { ' ' });
                        foreach (sTableField fld in pool.Mapping[type][tmp[0]])
                            primarys += "," + fld.Name+" "+tmp[1];
                    }
                    else
                    {
                        foreach (sTableField fld in pool.Mapping[type][str])
                            primarys += "," + fld.Name;
                    }
                }
            }
            if (primarys.StartsWith(","))
                primarys = primarys.Substring(1);
            return String.Format(SelectWithPagingIncludeOffset, baseQuery, CreateParameterName("startIndex"), CreateParameterName("rowCount"), primarys);
        }

        internal override string SelectPaged(string baseQuery, ref List<IDbDataParameter> queryParameters, ulong? start, ulong? recordCount, string[] OrderByFields)
        {
            if (!start.HasValue)
                start = 0;
            if (!recordCount.HasValue)
                recordCount = 0;
            queryParameters.Add(pool.CreateParameter(CreateParameterName("startIndex"), start.Value));
            queryParameters.Add(pool.CreateParameter(CreateParameterName("rowCount"), recordCount.Value));
            string primarys = "";
            if (baseQuery.Contains("ORDER BY"))
                primarys = baseQuery.Substring(baseQuery.IndexOf("ORDER BY") + "ORDER BY".Length);
            else if (OrderByFields != null && OrderByFields.Length > 0)
            {
                foreach (string str in OrderByFields)
                    primarys += "," + str;
            }
            else
                throw new Exception("Unable to selected Paged query without order by fields.");
            if (primarys.StartsWith(","))
                primarys = primarys.Substring(1);
            return String.Format(SelectWithPagingIncludeOffset, baseQuery, CreateParameterName("startIndex"), CreateParameterName("rowCount"), primarys);
        }

        protected override string DropTableIndexString
        {
            get
            {
                return "DROP INDEX {1} ON {0}";
            }
        }

        internal override string CreateTableIndex(string table, string[] fields, string indexName, bool unique, bool ascending)
        {
            string sfields = "";
            foreach (string str in fields)
                sfields += str + " "+(ascending ? "ASC" : "DESC")+",";
            sfields = sfields.Substring(0, sfields.Length - 1);
            return string.Format(CreateTableIndexString, new object[]{
                table,
                sfields,
                indexName,
                (unique ? "UNIQUE" : "")
            });
        }

        protected override string CreateTableIndexString
        {
            get
            {
                return "CREATE {3} INDEX {2} ON {0} ({1})";
            }
        }

        protected override string SelectProceduresString
        {
            get
            {
                return @"select pro.ROUTINE_NAME,
ISNULL((SELECT STUFF( (SELECT ',' + PARAMETER_NAME + ' '+UPPER(DATA_TYPE+(CASE WHEN CHARACTER_MAXIMUM_LENGTH = -1 THEN '(MAX)' WHEN CHARACTER_MAXIMUM_LENGTH IS NOT NULL THEN '('+CAST(CHARACTER_MAXIMUM_LENGTH AS VARCHAR(MAX))+')' ELSE '' END))
                             FROM information_schema.PARAMETERS
                             where SPECIFIC_NAME = pro.SPECIFIC_NAME
							 AND PARAMETER_MODE = 'IN'
                             ORDER BY ORDINAL_POSITION
                             FOR XML PATH('')), 
                            1, 1, '')),'') as pars,
                            (CASE pro.ROUTINE_TYPE WHEN 'FUNCTION' THEN 
ISNULL((SELECT STUFF( (SELECT ',' +UPPER(DATA_TYPE+(CASE WHEN CHARACTER_MAXIMUM_LENGTH = -1 THEN '(MAX)' WHEN CHARACTER_MAXIMUM_LENGTH IS NOT NULL THEN '('+CAST(CHARACTER_MAXIMUM_LENGTH AS VARCHAR(MAX))+')' ELSE '' END))
                             FROM information_schema.PARAMETERS
                             where SPECIFIC_NAME = pro.SPECIFIC_NAME
							 AND PARAMETER_MODE = 'OUT'
                             ORDER BY ORDINAL_POSITION
                             FOR XML PATH('')), 
                            1, 1, '')),'')
                            ELSE null END) as returnCode,
'',
LTRIM(RTRIM(SUBSTRING(pro.ROUTINE_DEFINITION,CHARINDEX('BEGIN',pro.ROUTINE_DEFINITION)+6,
LEN(pro.ROUTINE_DEFINITION)-(CHARINDEX('BEGIN',pro.ROUTINE_DEFINITION)+6)-(CHARINDEX('DNE',REVERSE(pro.ROUTINE_DEFINITION))+3)))) as code
from information_schema.routines pro where 
pro.routine_type IN ('FUNCTION','PROCEDURE') AND
pro.SPECIFIC_CATALOG = '{0}'";
            }
        }

        internal override string SelectProcedures()
        {
            return string.Format(SelectProceduresString, ((MsSqlConnectionPool)this.pool).Catalog);
        }

        protected override string CreateProcedureStringWithReturn
        {
            get
            {
                return "CREATE FUNCTION {0} ({1}) RETURNS {2} AS BEGIN {3} {4} END";
            }
        }

        protected override string UpdateProcedureStringWithReturn
        {
            get
            {
                return "ALTER FUNCTION {0} ({1}) RETURNS {2} AS BEGIN {3} {4} END";
            }
        }

        protected override string CreateProcedureString
        {
            get
            {
                return "CREATE PROCEDURE {0} ({1}) AS BEGIN {2} {3} END";
            }
        }

        protected override string UpdateProcedureString
        {
            get
            {
                return "ALTER PROCEDURE {0} ({1}) AS BEGIN {2} {3} END";
            }
        }

        protected override string DropProcedureString
        {
            get
            {
                return "IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE type = 'FN' AND name = '{0}') BEGIN DROP FUNCTION {0}; END IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE type='P' AND name='{0}') BEGIN DROP PROCEDURE {0}; END";
            }
        }

        protected override string SelectViewsString
        {
            get
            {
                return @"select o.name,
LTRIM(RTRIM(SUBSTRING(m.[definition],CHARINDEX(' AS ',m.[definition])+3,LEN(m.[definition])-CHARINDEX(' AS ',m.[definition]))))
from sys.objects     o
JOIN sys.sql_modules m on m.object_id = o.object_id
where o.type      = 'V'";
            }
        }

        protected override string DropColumnString
        {
            get { return "ALTER TABLE {0} DROP COLUMN {1}"; }
        }

        #region Description
        internal override string GetAllObjectDescriptions()
        {
            return @"SELECT CONVERT(VARCHAR(MAX),p.value),CONVERT(VARCHAR(MAX),t.name) FROM sys.extended_properties p inner join sys.tables t on p.major_id = t.object_id where p.minor_id = 0 AND p.name='Description' And t.type = 'U' AND t.type_desc = 'USER_TABLE' AND p.class_desc = 'OBJECT_OR_COLUMN'
                                        UNION
                    SELECT CONVERT(VARCHAR(MAX),p.value),CONVERT(VARCHAR(MAX),c.name) FROM sys.extended_properties p inner join sys.columns c on p.major_id = c.object_id AND p.minor_id = c.column_id inner join sys.tables t on p.major_id = t.object_id AND c.object_id = t.object_id where p.name='Description' AND t.type_desc = 'USER_TABLE' AND t.type = 'U' AND p.class_desc = 'OBJECT_OR_COLUMN'
                                        UNION
                    SELECT CONVERT(VARCHAR(MAX),p.value),CONVERT(VARCHAR(MAX),t.name) FROM sys.extended_properties p inner join sys.triggers t on p.major_id = t.object_id where p.name='Description' AND t.type = 'TR'
                    UNION
                    SELECT CONVERT(VARCHAR(MAX),p.value),CONVERT(VARCHAR(MAX),v.name) FROM sys.extended_properties p inner join sys.views v on p.major_id = v.object_id where p.name='Description' AND v.type = 'V'
                    UNION
                    SELECT CONVERT(VARCHAR(MAX),p.value),CONVERT(VARCHAR(MAX),i.name) FROM sys.extended_properties p inner join sys.indexes i on p.major_id = i.object_id where p.name='Description' AND i.is_primary_key=0 AND p.class_desc='INDEX'"; 
        }

        internal override string SetTableDescription(string tableName, string description)
        {
            return string.Format("EXEC sys.sp_addextendedproperty @name = N'DESCRIPTION', @value = N'{1}', @level0type='SCHEMA', @level0name='dbo',@level1type = N'TABLE', @level1name = '{0}'", tableName, description.Replace("'", "''"));
        }

        internal override string SetFieldDescription(string tableName, string fieldName, string description)
        {
            return string.Format("EXEC sys.sp_addextendedproperty @name = N'DESCRIPTION', @value = N'{2}', @level0type='SCHEMA', @level0name='dbo',@level1type = N'TABLE', @level1name = '{0}', @level2type = N'COLUMN', @level2name = N'{1}'", new object[] { tableName, fieldName, description.Replace("'", "''") });
        }

        internal override string SetTriggerDescription(string triggerName, string description)
        {
            return string.Format("DECLARE @tblname VARCHAR(MAX); select @tblName=tbl.name from sys.triggers trig, sysobjects tbl WHERE trig.parent_id = tbl.id AND tbl.xtype='U' AND trig.name IS NOT NULL AND trig.name = '{0}';EXEC sys.sp_addextendedproperty @name = N'DESCRIPTION', @value = N'{1}', @level0type='SCHEMA', @level0name='dbo',@level1type = 'TABLE',@level1name=@tblname,@level2type = N'TRIGGER', @level2name = '{0}'", triggerName, description.Replace("'", "''"));
        }

        internal override string SetViewDescription(string viewName, string description)
        {
            return string.Format("EXEC sys.sp_addextendedproperty @name = N'DESCRIPTION', @value = N'{1}', @level0type='SCHEMA', @level0name='dbo',@level1type = N'VIEW', @level1name = '{0}'", viewName, description.Replace("'", "''"));
        }

        internal override string SetIndexDescription(string indexName, string description)
        {
            return string.Format("DECLARE @tblname VARCHAR(MAX); select @tblname = tbl.name from sys.indexes ind, sysobjects tbl WHERE ind.object_id = tbl.id AND tbl.xtype='U' AND ind.name IS NOT NULL AND ind.is_primary_key=0 AND ind.name = '{0}'; EXEC sys.sp_addextendedproperty @name = N'DESCRIPTION', @value = N'{1}', @level0type='SCHEMA', @level0name='dbo',@level1type = 'TABLE',@level1name=@tblname,@level2type = N'INDEX', @level2name = '{0}'", indexName, description.Replace("'", "''"));
        }
        #endregion

        protected override string _GenerateAutogenIDQuery(sTable tbl, ref List<IDbDataParameter> parameters)
        {
            parameters[parameters.Count - 1].Direction = ParameterDirection.Output;
            if (((MsSqlConnectionPool)pool).Version >= 2005 && tbl.PrimaryKeyFields.Length==1
                && tbl[tbl.AutoGenProperty][0].Type != FieldType.DATETIME
                && tbl[tbl.AutoGenProperty][0].Type != FieldType.TIME
                && tbl[tbl.AutoGenProperty][0].Type != FieldType.STRING)
                return "OUTPUT INSERTED." + tbl.AutoGenField;
            else
            {
                string select = "; SET "+CreateParameterName(tbl.AutoGenField)+" = (SELECT MAX("+tbl.AutoGenField+") FROM "+tbl.Name;
                if (tbl.PrimaryKeyFields.Length > 1)
                {
                    select+=" WHERE ";
                    foreach (string prop in tbl.PrimaryKeyProperties)
                    {
                        foreach (sTableField fld in tbl[prop]){
                            if (!Utility.StringsEqual(tbl.AutoGenField, fld.Name))
                                select += fld.Name + " = " + CreateParameterName(fld.Name) + " AND ";
                        }
                    }
                    select = select.Substring(0, select.Length - 4);
                    if (select.EndsWith("WHERE "))
                        select = select.Substring(0, select.Length - 6);
                }
                return select+")";
            }
        }
    }
}
