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
                return "select DISTINCT " +
                " convert(sysname,COL2.name) as field_name, " +
                " OBJECT_NAME(FKEYS.referenced_object_id) as references_table, " +
                " convert(sysname,col1.name) as references_field, " +
                " FKEYS.update_referential_action_desc as on_update, " +
                " FKEYS.delete_referential_action_desc as on_delete, " +
                " CAST(FKEYS.object_id as VARCHAR(MAX)) as unique_id " +
                " from " +
                " sys.columns COL1, " +
                " sys.columns COL2, " +
                " sys.foreign_keys FKEYS " +
                " inner join sys.foreign_key_columns KEY_COLUMN on (KEY_COLUMN.constraint_object_id = FKEYS.object_id) " +
                " where " +
                " COL1.object_id = FKEYS.referenced_object_id " +
                " AND COL2.object_id = FKEYS.parent_object_id " +
                " AND COL1.column_id = KEY_COLUMN.referenced_column_id " +
                " AND COL2.column_id = KEY_COLUMN.parent_column_id " +
                " AND OBJECT_NAME(FKEYS.PARENT_OBJECT_ID)='{0}'";
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
					" AND cast(p.name as varchar(255)) = '{1}'"+
                    " AND cast(rc.name as varchar(255)) = '{2}'"+
					" AND cast(fc.name as varchar(255)) = '{3}'"; }
		}

        internal override List<Index> ExtractTableIndexes(string tableName, Connection conn)
        {
            List<Index> ret = new List<Index>();
            Dictionary<string, string> objIds = new Dictionary<string, string>();
            List<string> indexIds = new List<string>();
            conn.ExecuteQuery("select ind.object_id,ind.index_id,ind.name,ind.is_unique from sys.indexes ind, sysobjects tbl "+
                "WHERE ind.object_id = tbl.id AND tbl.xtype='U' AND ind.is_primary_key=0 AND tbl.name = '"+tableName+"'");
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
			conn.ExecuteQuery(string.Format(DropForeignKeyString,new object[]{table,externalTable,primaryField,relatedField}));
			while (conn.Read())
				ret+=conn[0].ToString()+"\n";
            conn.CloseConnection();
			return ret;
		}
		
		protected override string SelectCountString {
			get {
                return "SELECT * FROM (SELECT *,ROW_NUMBER() OVER (ORDER BY {3}) RowNum" +
					" FROM ({0}) internalTbl) cntTbl WHERE RowNum BETWEEN {1} AND {1}+{2}";
			}
		}
		
		internal override string SelectPaged(Type type, SelectParameter[] parameters, out List<IDbDataParameter> queryParameters, Nullable<ulong> start, Nullable<ulong> recordCount,string[] OrderByFields)
		{
			string query = Select(type,parameters,out queryParameters,null);
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
            if ((OrderByFields == null) || (OrderByFields.Length == 0))
            {
                foreach (InternalFieldMap ifm in map.PrimaryKeys)
                {
                    primarys += "," + ifm.FieldName;
                }
            }
            else
            {
                foreach (string str in OrderByFields)
                {
                    primarys += "," + ((InternalFieldMap)map[str]).FieldName;
                }
            }
			primarys=primarys.Substring(1);
			return String.Format(SelectWithPagingIncludeOffset,query,CreateParameterName("startIndex"),CreateParameterName("rowCount"),primarys);
		}

        internal override string SelectPaged(string baseQuery, TableMap mainMap, ref List<IDbDataParameter> queryParameters, ulong? start, ulong? recordCount,string[] OrderByFields)
        {
            if (!start.HasValue)
                start = 0;
            if (!recordCount.HasValue)
                recordCount = 0;
            queryParameters.Add(conn.CreateParameter(CreateParameterName("startIndex"), start.Value));
            queryParameters.Add(conn.CreateParameter(CreateParameterName("rowCount"), recordCount.Value));
            string primarys = "";
            if ((OrderByFields == null) || (OrderByFields.Length == 0))
            {
                foreach (InternalFieldMap ifm in mainMap.PrimaryKeys)
                {
                    primarys += "," + ifm.FieldName;
                }
                primarys = primarys.Substring(1);
            }
            else if (baseQuery.Contains("ORDER BY"))
            {
                primarys = baseQuery.Substring(baseQuery.IndexOf("ORDER BY") + "ORDER BY".Length);
            }
            else
            {
                foreach (string str in OrderByFields)
                {
                    primarys += "," + ((InternalFieldMap)mainMap[str]).FieldName;
                }
            }
            return String.Format(SelectWithPagingIncludeOffset, baseQuery, CreateParameterName("startIndex"), CreateParameterName("rowCount"), primarys);
        }

        protected override string DropTableIndexString
        {
            get
            {
                return "DROP INDEX {1}";
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
		protected override string SelectViewsString
        {
            get
            {
                return @"SELECT vws.TABLE_NAME,
SUBSTRING(vws.VIEW_DEFINITION,CHARINDEX('AS',vws.VIEW_DEFINITION)+3,LEN(vws.VIEW_DEFINITION)-CHARINDEX('AS',vws.VIEW_DEFINITION))
FROM INFORMATION_SCHEMA.VIEWS vws";
            }
        }
	}
}
