/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 16/03/2009
 * Time: 8:47 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using System;
using System.Collections.Generic;

namespace Org.Reddragonit.Dbpro.Connections.MySql
{
	/// <summary>
	/// Description of MySqlQueryBuilder.
	/// </summary>
	internal class MySqlQueryBuilder : QueryBuilder
	{
		public MySqlQueryBuilder(ConnectionPool pool,Connection conn) : base(pool,conn)
		{
		}
		
		protected override string SelectTableNamesString
		{
			get
			{
				return "SELECT DISTINCT(UPPER(TABLE_NAME)) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='"+((MySqlConnectionPool)pool).DbName+"'";
			}
		}

		protected override string SelectTableFieldsString
		{
			get
			{
				return "SELECT UPPER(COLUMN_NAME), "+
					" UPPER(DATA_TYPE) dtype, " +
					" (CASE " +
					"   WHEN CHARACTER_MAXIMUM_LENGTH IS NOT NULL THEN CHARACTER_MAXIMUM_LENGTH " +
					"   ELSE " +
					"     (CASE UPPER(DATA_TYPE) " +
					"       WHEN 'INT' THEN 4 " +
					"       WHEN 'BIGINT' THEN 8 " +
					"       WHEN 'TINYINT' THEN 1 " +
					"       WHEN 'TEXT' THEN -1 " +
					"       WHEN 'BLOB' THEN -1 " +
					"       WHEN 'BIT' THEN 1 " +
					"       WHEN 'FLOAT' THEN 8 " +
					"       WHEN 'DOUBLE' THEN 8 " +
					"       WHEN 'DECIMAL' THEN 8 " +
					"       WHEN 'SMALLINT' THEN 2 " +
					"     END) " +
					" END) dataLength, " +
					" (CASE WHEN COLUMN_KEY = 'PRI' THEN 'true' ELSE 'false' END) isPrimary, " +
					" (CASE WHEN IS_NULLABLE = 'NO' THEN 'false' ELSE 'true' END) isNullable, " +
					" (CASE WHEN EXTRA LIKE '%auto_increment%' THEN 'true' ELSE 'false' END) isIndentity " +
					"  from information_schema.COLUMNS WHERE TABLE_SCHEMA='"+((MySqlConnectionPool)pool).DbName+"' AND TABLE_NAME='{0}'";
			}
		}

		protected override string SelectForeignKeysString
		{
			get
			{
				return "SELECT "+
                    " UPPER(cols.COLUMN_NAME) field_name, " +
                    " UPPER(cols.REFERENCED_TABLE_NAME) references_table, " +
					" UPPER(cols.REFERENCED_COLUMN_NAME) references_field, "+
					" UPPER(ref.UPDATE_RULE) 'on Update', "+
					" UPPER(ref.DELETE_RULE) 'on Delete' "+
                    " ref.CONSTRAINT_NAME "+
					" FROM information_schema.KEY_COLUMN_USAGE cols, information_schema.REFERENTIAL_CONSTRAINTS ref "+
					" WHERE ref.CONSTRAINT_SCHEMA = '"+((MySqlConnectionPool)pool).DbName+"' "+
					" AND cols.CONSTRAINT_NAME<>'PRIMARY' "+
					" AND cols.CONSTRAINT_SCHEMA=ref.CONSTRAINT_SCHEMA "+
					" AND cols.CONSTRAINT_NAME=ref.CONSTRAINT_NAME " +
					" AND cols.TABLE_NAME = '{0}'";
			}
		}
		
		protected override string SelectTriggersString {
			get
			{
				return "SELECT TRIGGER_NAME, "+
					" UPPER(CONCAT(ACTION_TIMING,' ',EVENT_MANIPULATION,' ON ',EVENT_OBJECT_TABLE)) comm_string, "+
					" CONCAT('FOR EACH ROW ',ACTION_STATEMENT) code "+
					" from information_schema.TRIGGERS "+
					" WHERE TRIGGER_SCHEMA='"+((MySqlConnectionPool)pool).DbName+"'";
			}
		}
		
		protected override string SelectCurrentIdentities {
			get {
				return "SELECT "+
					" UPPER(TABLE_NAME)," +
					" UPPER(COLUMN_NAME), "+
					" UPPER(DATA_TYPE) " +
					" from information_schema.COLUMNS "+
					" WHERE TABLE_SCHEMA='"+((MySqlConnectionPool)pool).DbName+"' "+
					" AND EXTRA LIKE '%auto_increment%'";
			}
		}
		
		internal override string SelectIdentities()
		{
			string ret="";
			conn.ExecuteQuery(SelectCurrentIdentities);
			while(conn.Read())
			{
				ret+=String.Format("SELECT '{0}','{1}','{2}', "+
				                   " (CASE ISNULL(MAX({1})) "+
				                   " WHEN 1 THEN '1' "+
				                   " ELSE CONVERT(MAX({1}),CHAR(250)) END) VAL FROM {0} GROUP BY '{0}','{1}','{2}' \nUNION\n",
				                   conn[0].ToString(),
				                   conn[1].ToString(),
				                   conn[2].ToString());
			}
			conn.Close();
			if (ret.Length>0)
				ret=ret.Substring(0,ret.Length-7);
			return ret;
		}
		
		protected override string CreateIdentityString {
			get { return "ALTER TABLE {0} MODIFY COLUMN {1} {2} NOT NULL AUTO_INCREMENT;\nALTER TABLE {0} AUTO_INCREMENT = {3};"; }
		}
		
		protected override string SetIdentityFieldValueString {
			get { return "ALTER TABLE {0} AUTO_INCREMENT = {3}"; }
		}
		
		protected override string DropIdentityFieldString {
			get { return "ALTER TABLE {0} MODIFY COLUMN {1} {2} NOT NULL;"; }
		}
		
		protected override string DropNotNullString {
			get { return "ALTER TABLE {0} MODIFY COLUMN {1} {2} NULL"; }
		}

		protected override string CreateNullConstraintString
		{
			get{return "ALTER TABLE {0} MODIFY COLUMN {1} {2} NOT NULL";}
		}
		
		protected override string DropForeignKeyString {
            get
            {
                return "SELECT DISTINCT " +
                    " CONCAT('ALTER TABLE ',ref.TABLE_NAME, " +
                    " ' DROP FOREIGN KEY ',tb.CONSTRAINT_NAME) qry " +
                    " from information_schema.TABLE_CONSTRAINTS tb, " +
                    " information_schema.REFERENTIAL_CONSTRAINTS ref, " +
                    " information_schema.KEY_COLUMN_USAGE cols " +
                    " WHERE tb.TABLE_SCHEMA=ref.CONSTRAINT_SCHEMA " +
                    " AND tb.CONSTRAINT_TYPE='FOREIGN KEY' " +
                    " AND ref.CONSTRAINT_NAME=tb.CONSTRAINT_NAME " +
                    " AND ref.TABLE_NAME='{0}' " +
                    " AND ref.REFERENCED_TABLE_NAME='{1}' " +
                    " AND ref.CONSTRAINT_SCHEMA='" + ((MySqlConnectionPool)pool).DbName + "'" +
                    " AND cols.CONSTRAINT_NAME<>'PRIMARY' " +
                    " AND cols.CONSTRAINT_SCHEMA=ref.CONSTRAINT_SCHEMA " +
                    " AND cols.CONSTRAINT_NAME=ref.CONSTRAINT_NAME " +
                    " AND cols.TABLE_NAME=ref.TABLE_NAME " +
                    " AND cols.COLUMN_NAME = '{3}' " +
                    " AND cols.REFERENCED_COLUMN_NAME = '{2}' ";
            }
		}
		
		internal override string DropForeignKey(string table, string externalTable,string primaryField,string relatedField)
		{
			conn.ExecuteQuery(string.Format(DropForeignKeyString,table,externalTable,primaryField,relatedField));
			string ret = "";
			if (conn.Read())
				ret=conn[0].ToString();
			conn.Close();
			return ret;
		}

        internal override List<Index> ExtractTableIndexes(string tableName, Connection conn)
        {
            List<Index> ret = new List<Index>();
            conn.ExecuteQuery("SHOW INDEX FROM " + tableName + " FROM " + ((MySqlConnectionPool)conn.Pool).DbName+" WHERE Key_Name <> 'PRIMARY' AND "+
                "Key_Name NOT IN (SELECT CONSTRAINT_NAME FROM information_schema.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA = '" + ((MySqlConnectionPool)conn.Pool).DbName+"' AND TABLE_NAME='"+tableName+"')");
            string curName = null;
            bool unique=false;
            List<string> fields = new List<string>();
            while (conn.Read())
            {
                if (curName!=null){
                    if (curName!=conn[2].ToString()){
                        ret.Add(new Index(curName,fields.ToArray(),unique,false));
                        curName = conn[2].ToString();
                        fields=new List<string>();
                    }
                }
                fields.Add(conn[4].ToString());
                unique = conn[1].ToString()=="0";
            }
            if (curName!=null){
                ret.Add(new Index(curName,fields.ToArray(),unique,false));
            }
            conn.Close();
            return ret;
        }
		
		protected override string DropPrimaryKeyString {
			get { return "ALTER TABLE {0} DROP PRIMARY KEY"; }
		}
		
		protected override string AlterFieldTypeString {
			get { return "ALTER TABLE {0} MODIFY COLUMN {1} {2} {3} NULL";}
		}

        internal override string AlterFieldType(string table, ExtractedFieldMap field, ExtractedFieldMap oldFieldInfo)
        {
            if (!field.Nullable)
                return string.Format(AlterFieldTypeString, table, field.FieldName, field.FullFieldType, "NOT");
            return string.Format(AlterFieldTypeString, table, field.FieldName, field.FullFieldType, "");
        }

        protected override string CreateTableIndexString
        {
            get
            {
                return "CREATE {3} INDEX {2} ON {0} ({1})";
            }
        }

        protected override string DropTableIndexString
        {
            get
            {
                return "DROP INDEX {1} ON {0}";
            }
        }
	}
}
