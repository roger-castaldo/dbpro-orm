using Org.Reddragonit.Dbpro.Connections.Parameters;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using Org.Reddragonit.Dbpro.Structure.Mapping;

namespace Org.Reddragonit.Dbpro.Connections.MsSql
{
	internal class MSSQLQueryBuilder : QueryBuilder
	{
		public MSSQLQueryBuilder(ConnectionPool pool,Connection conn) : base(pool,conn)
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
				return "SELECT  c.column_name 'name', " +
					" UPPER(c.data_type) 'type', " +
					" (CASE WHEN c.character_maximum_length is null then "+
					" (CASE WHEN UPPER(c.data_type) = 'BIT' THEN 1 "+
					" WHEN UPPER(c.data_type) = 'DATETIME' OR UPPER(c.data_type) = 'DECIMAL' OR UPPER(c.data_type) = 'FLOAT' OR UPPER(c.data_type) = 'BIGINT' OR UPPER(c.data_type) = 'MONEY' THEN 8  "+
					" WHEN UPPER(c.data_type) = 'INT' THEN 4 "+
					" WHEN UPPER(c.data_type) = 'SMALLINT' THEN 2 "+
					" END) "+
					" ELSE  "+
					" (CASE WHEN UPPER(c.data_type)='IMAGE' OR UPPER(c.data_type) = 'TEXT' THEN -1 ELSE c.character_maximum_length END) END) 'length'," +
					" (CASE WHEN primarys.IsPrimary is null THEN 'false' ELSE 'true' END) as IsPrimary, " +
					" (CASE WHEN c.is_nullable='NO' THEN 'false' ELSE 'true' END) as IsNullable, " +
					" (CASE WHEN COLUMNPROPERTY( OBJECT_ID('{0}'),c.column_name,'IsIdentity') = 0 THEN 'false' else 'true' END) as IsIdentity " +
					" FROM INFORMATION_SCHEMA.COLUMNS c " +
					" LEFT JOIN (SELECT k.column_name,1 as IsPrimary FROM  " +
					" INFORMATION_SCHEMA.KEY_COLUMN_USAGE k, INFORMATION_SCHEMA.TABLE_CONSTRAINTS c " +
					" WHERE  " +
					" k.table_name = c.table_name " +
					" AND c.table_name = '{0}' " +
					" AND k.table_schema = c.table_schema " +
					" AND k.table_catalog = c.table_catalog " +
					" AND k.constraint_catalog = c.constraint_catalog " +
					" AND k.constraint_name = c.constraint_name  " +
					" AND c.constraint_type = 'PRIMARY KEY') primarys ON  " +
					" c.column_name = primarys.column_name " +
					" WHERE c.table_name = '{0}'";
			}
		}

		protected override string SelectForeignKeysString
		{
			get
			{
				return "SELECT    ccu.column_name 'references_field', " +
					" ccu.table_name 'references_table', " +
					" k.column_name field_name, " +
					" rc.update_rule 'on_update', " +
					" rc.delete_rule 'on_delete' " +
					" FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE k " +
					" LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS c " +
					" ON k.table_name = c.table_name " +
					" AND k.table_schema = c.table_schema " +
					" AND k.table_catalog = c.table_catalog " +
					" AND k.constraint_catalog = c.constraint_catalog " +
					" AND k.constraint_name = c.constraint_name " +
					" LEFT JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc " +
					" ON rc.constraint_schema = c.constraint_schema " +
					" AND rc.constraint_catalog = c.constraint_catalog " +
					" AND rc.constraint_name = c.constraint_name " +
					" LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu " +
					" ON rc.unique_constraint_schema = ccu.constraint_schema " +
					" AND rc.unique_constraint_catalog = ccu.constraint_catalog " +
					" AND rc.unique_constraint_name = ccu.constraint_name " +
					" WHERE k.constraint_catalog = DB_NAME() " +
					" AND k.table_name = '{0}' " +
					" AND c.constraint_type = 'FOREIGN KEY';";
			}
		}
		
		protected override string SelectTriggersString {
			get
			{
				return "SELECT sys1.name trigger_name, " +
					" 'ON '+sys2.name+' '+ " +
					" (CASE WHEN OBJECTPROPERTY(sys1.id, 'ExecIsInsteadOfTrigger') = 1 " +
					" THEN 'INSTEAD OF' ELSE 'AFTER' " +
					" END " +
					" )+' '+ " +
					" ( " +
					" CASE " +
					" WHEN OBJECTPROPERTY(sys1.id, 'ExecIsInsertTrigger') = 1 THEN "+
					" (CASE WHEN OBJECTPROPERTY(sys1.id, 'ExecIsUpdateTrigger') = 1 THEN " +
					"	(CASE WHEN OBJECTPROPERTY(sys1.id, 'ExecIsDeleteTrigger')=1 THEN 'INSERT, UPDATE, DELETE'" +
					"	ELSE 'INSERT, UPDATE' END)" +
					"  ELSE " +
					"	(CASE WHEN OBJECTPROPERTY(sys1.id, 'ExecIsDeleteTrigger')=1 THEN 'INSERT, DELETE'" +
					"  	ELSE 'INSERT' END)" +
					" END)" +
					" WHEN OBJECTPROPERTY(sys1.id, 'ExecIsUpdateTrigger') = 1 THEN " +
					" (CASE WHEN OBJECTPROPERTY(sys1.id, 'ExecIsDeleteTrigger') = 1 THEN 'UPDATE, DELETE' " +
					" ELSE 'UPDATE' END) " +
					" WHEN OBJECTPROPERTY(sys1.id, 'ExecIsDeleteTrigger') = 1 THEN 'DELETE' " +
					" END " +
					" )	comm_string, " +
					" RIGHT(c.text,LEN(c.text)-PATINDEX('%AS%BEGIN%',c.text)+2) as code " +
					" FROM sysobjects sys1 " +
					" JOIN sysobjects sys2 ON sys1.parent_obj = sys2.id " +
					" JOIN syscomments c ON sys1.id = c.id " +
					" WHERE sys1.xtype = 'TR'";
			}
		}
		
		protected override string SelectCurrentIdentities {
			get {
				return "SELECT DISTINCT c.table_name 'tableName', " +
					" c.column_name 'fieldName', " +
					" UPPER(c.data_type) 'type', " +
					" CAST(IDENT_CURRENT(c.table_name) as varchar(MAX)) 'curValue' " +
					" FROM INFORMATION_SCHEMA.COLUMNS c " +
					" LEFT JOIN (SELECT k.column_name,1 as IsPrimary FROM  " +
					" INFORMATION_SCHEMA.KEY_COLUMN_USAGE k, INFORMATION_SCHEMA.TABLE_CONSTRAINTS c " +
					" WHERE  " +
					" k.table_name = c.table_name " +
					" AND k.table_schema = c.table_schema " +
					" AND k.table_catalog = c.table_catalog " +
					" AND k.constraint_catalog = c.constraint_catalog " +
					" AND k.constraint_name = c.constraint_name  " +
					" AND c.constraint_type = 'PRIMARY KEY') primarys ON  " +
					" c.column_name = primarys.column_name " +
					" WHERE COLUMNPROPERTY( OBJECT_ID(c.table_name),c.column_name,'IsIdentity') = 1";
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

		internal override string DropPrimaryKey(PrimaryKey key)
		{
			string ret = "";
			conn.ExecuteQuery(DropPrimaryKeyString, new IDbDataParameter[]{conn.CreateParameter(CreateParameterName("TableName"),key.Name)});
			if (conn.Read())
				ret = conn[0].ToString();
			conn.Close();
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
					" AND cast(p.name as varchar(255)) = '{1}'"; }
		}
		
		internal override string DropForeignKey(string table, string externalTable)
		{
			string ret="";
			conn.ExecuteQuery(string.Format(DropForeignKeyString,table,externalTable));
			while (conn.Read())
				ret+=conn[0].ToString()+"\n";
			return ret;
		}
		
		protected override string SelectCountString {
			get { 
				return "SELECT * FROM (SELECT ROW_NUMBER() OVER (ORDER BY {3}) RowNum,"+
					"* FROM ({0}) internalTbl) cntTbl WHERE RowNum BETWEEN {1} AND {1}+{2}";
			}
		}
		
		internal override string SelectPaged(Type type, SelectParameter[] parameters, out List<IDbDataParameter> queryParameters, Nullable<ulong> start, Nullable<ulong> recordCount)
		{
			string query = Select(type,parameters,out queryParameters);
			if (queryParameters==null)
				queryParameters = new List<IDbDataParameter>();
			if (!start.HasValue)
				start=0;
			if (!recordCount.HasValue)
				recordCount=0;
			queryParameters.Add(conn.CreateParameter(CreateParameterName("startIndex"),start.Value));
			queryParameters.Add(conn.CreateParameter(CreateParameterName("rowCount"),recordCount.Value));
			string primarys = "";
			TableMap map = ClassMapper.GetTableMap(type);
			foreach (InternalFieldMap ifm in map.PrimaryKeys)
			{
				primarys+=","+ifm.FieldName;
			}
			primarys=primarys.Substring(1);
			return String.Format(SelectWithPagingIncludeOffset,query,CreateParameterName("startIndex"),CreateParameterName("rowCount"),primarys);
		}
	}
}
