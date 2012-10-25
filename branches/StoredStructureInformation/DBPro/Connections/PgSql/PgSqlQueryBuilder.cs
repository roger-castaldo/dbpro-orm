/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 03/04/2009
 * Time: 10:35 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using System;
using System.Collections.Generic;

namespace Org.Reddragonit.Dbpro.Connections.PgSql
{
	/// <summary>
	/// Description of PgSqlQueryBuilder.
	/// </summary>
	internal class PgSqlQueryBuilder : QueryBuilder
	{
		public PgSqlQueryBuilder(ConnectionPool pool,Connection conn): base(pool,conn)
		{
		}
		
		public override string CreateParameterName(string parameter)
		{
			return ":"+parameter;
		}
		
		protected override string SelectTableNamesString {
			get {
				return "SELECT UPPER(table_name) "+
					" FROM information_schema.tables "+
					" WHERE table_type = 'BASE TABLE' "+
					" AND table_schema NOT IN ('pg_catalog', 'information_schema') "+
					" AND table_catalog = '"+((PgSqlConnectionPool)pool).DbName+"'";
			}
		}
		
		protected override string SelectTableFieldsString {
			get { return "SELECT UPPER(cols.COLUMN_NAME), " +
					" REPLACE(UPPER(DATA_TYPE),' WITHOUT TIME ZONE','') AS dtype, " +
					" (CASE  WHEN CHARACTER_MAXIMUM_LENGTH IS NOT NULL  " +
					" 	THEN CHARACTER_MAXIMUM_LENGTH   " +
					" ELSE   " +
					" (CASE UPPER(DATA_TYPE)  WHEN 'INTEGER' THEN 4   " +
					" WHEN 'BIGINT' THEN 8   " +
					" WHEN 'OID' THEN -1   " +
					" WHEN 'NUMERIC' THEN cols.NUMERIC_PRECISION " +
					" WHEN 'FLOAT' THEN 8   " +
					" WHEN 'DOUBLE' THEN 8   " +
					" WHEN 'DECIMAL' THEN 8   " +
					" WHEN 'SMALLINT' THEN 2   " +
					" WHEN 'BOOLEAN' THEN 1 " +
					" ELSE " +
					" (CASE WHEN UPPER(DATA_TYPE) LIKE '%TIMESTAMP%' THEN 8 END) " +
					" END)  " +
					" END) AS dataLength, " +
					" (CASE WHEN primaries.constraint_type IS NOT NULL THEN 'true' ELSE 'false' END) AS isPrimary, " +
					" (CASE WHEN IS_NULLABLE = 'NO' THEN 'false' ELSE 'true' END) AS isNullable, " +
					" (CASE WHEN (SELECT COUNT(*) " +
					" 	FROM pg_class " +
					" 	where relkind = 'S' " +
					" 	AND relnamespace IN (  " +
					" 		SELECT oid  " +
					" 		FROM pg_namespace  " +
					" 		WHERE nspname NOT LIKE 'pg_%'  " +
					" 		AND nspname != 'information_schema' ) " +
					" 	AND relname = 'GEN_'||cols.COLUMN_NAME) = 0 THEN 'false' ELSE 'true' END) AS AUTOGEN " +
					" from information_schema.COLUMNS cols  " +
					" LEFT JOIN (SELECT tc.table_name, column_name, constraint_type, tc.TABLE_CATALOG " +
					" 	FROM information_schema.key_column_usage kcu, " +
					" 	information_schema.table_constraints tc " +
					" 	WHERE kcu.constraint_name=tc.constraint_name " +
					" 	AND constraint_type = 'PRIMARY KEY') primaries " +
					" 	ON cols.COLUMN_NAME=primaries.COLUMN_NAME " +
					" 	AND cols.TABLE_NAME=primaries.TABLE_NAME " +
					" 	AND cols.TABLE_CATALOG=primaries.TABLE_CATALOG " +
					" WHERE cols.TABLE_CATALOG='"+((PgSqlConnectionPool)pool).DbName+"'  AND " +
					" UPPER(cols.TABLE_NAME)='{0}'"; }
		}
		
		protected override string SelectForeignKeysString {
			get {
				return "SELECT DISTINCT "+
					" UPPER(kcu.column_name) AS FOREIGN_KEY_COLUMN, "+
					" UPPER(ccu.table_name) AS PRIMARY_KEY_TABLE,  "+
					" UPPER(ccu.column_name) AS PRIMARY_KEY_COLUMN, "+
					" UPPER(rc.update_rule) AS on_update,  "+
					" UPPER(rc.delete_rule) AS on_delete "+
					" FROM information_schema.table_constraints tc  "+
					" LEFT JOIN information_schema.key_column_usage kcu  "+
					" ON tc.constraint_catalog = kcu.constraint_catalog  "+
					" AND tc.constraint_schema = kcu.constraint_schema  "+
					" AND tc.constraint_name = kcu.constraint_name  "+
					" LEFT JOIN information_schema.referential_constraints rc  "+
					" ON tc.constraint_catalog = rc.constraint_catalog  "+
					" AND tc.constraint_schema = rc.constraint_schema  "+
					" AND tc.constraint_name = rc.constraint_name  "+
					" LEFT JOIN information_schema.constraint_column_usage ccu  "+
					" ON rc.unique_constraint_catalog = ccu.constraint_catalog  "+
					" AND rc.unique_constraint_schema = ccu.constraint_schema  "+
					" AND rc.unique_constraint_name = ccu.constraint_name  "+
					" WHERE UPPER(tc.table_name) = '{0}' "+
					" AND tc.table_catalog = '"+((PgSqlConnectionPool)pool).DbName+"' "+
					" AND tc.constraint_type = 'FOREIGN KEY'";
			}
		}
		
		protected override string SelectTriggersString {
			get {
				return "SELECT UPPER(trigger_name) AS TrigName, "+
					" condition_timing||' '||event_manipulation||' ON '||UPPER(event_object_table)||' FOR EACH ROW' AS TrigCondition,  "+
					" REPLACE(REPLACE(SUBSTRING(prosrc,8),'RETURN NEW;',''),'END;','') AS TrigCode "+
					" FROM information_schema.triggers  "+
					" LEFT JOIN pg_catalog.pg_proc ON action_statement = 'EXECUTE PROCEDURE '||proname||'()' "+
					" WHERE trigger_schema NOT IN ('pg_catalog', 'information_schema') "+
					" AND trigger_catalog='"+((PgSqlConnectionPool)pool).DbName+"'";
			}
		}
		
		protected override string CreateGeneratorString {
			get { return "CREATE SEQUENCE {0} INCREMENT 1 START 1"; }
		}
		
		protected override string DropGeneratorString {
			get { return "DROP SEQUENCE {0}"; }
		}
		
		protected override string GetGeneratorValueString {
			get { return "SELECT setval('{0}',nextval('{0}')-1)";}
		}
		
		protected override string SetGeneratorValueString {
			get { return "SELECT SETVAL('{0}',{1})"; }
		}
		
		protected override string CreateTriggerString {
			get { return "CREATE FUNCTION FUNC_{0}() RETURNS trigger AS ${0}$ "+
					"BEGIN\n"+
					"{2}\n"+
					"RETURN NEW;\n"+
					"END;\n"+
					"${0}$ LANGUAGE plpgsql;\n"+
					"CREATE TRIGGER {0} {1} EXECUTE PROCEDURE FUNC_{0}();";}
		}
		
		protected override string SelectGeneratorsString {
			get { return "SELECT UPPER(relname) FROM pg_class "+
					" WHERE relkind = 'S' AND relnamespace IN "+
					" ( SELECT oid FROM pg_namespace WHERE nspname NOT LIKE 'pg_%' AND nspname != 'information_schema' )"; }
		}
		
		protected override string CreateNullConstraintString {
			get { return "ALTER TABLE {0} ALTER COLUMN {1} SET NOT NULL"; }
		}
		
		protected override string DropNotNullString {
			get { return "ALTER TABLE {0} ALTER COLUMN {1} DROP NOT NULL"; }
		}
		
		protected override string DropPrimaryKeyString {
			get { return "SELECT 'ALTER TABLE '||UPPER(tc.table_name)||' DROP CONSTRAINT \''||tc.constraint_name||'\'' AS QUERY "+
					" FROM information_schema.key_column_usage kcu,  "+
					" information_schema.table_constraints tc  "+
					" WHERE kcu.constraint_name=tc.constraint_name  "+
					" AND constraint_type = 'PRIMARY KEY' "+
					" AND UPPER(tc.table_name)='{0}' "+
					" AND UPPER(column_name)='{1}'"; }
		}
		
		internal override string DropPrimaryKey(PrimaryKey key)
		{
			string ret="";
			foreach (string str in key.Fields)
			{
				conn.ExecuteQuery(String.Format(DropPrimaryKeyString, key.Name, str));
				if (conn.Read())
					ret += conn[0].ToString()+";\n";
				conn.Close();
			}
			return ret;
		}

        internal override List<Index> ExtractTableIndexes(string tableName, Connection conn)
        {
            List<Index> ret = new List<Index>();
            //TODO: complete the extraction of indexes here
            return ret;
        }
		
		protected override string DropForeignKeyString {
			get { return "SELECT 'ALTER TABLE {0} DROP CONSTRAINT '||ccu.constraint_name AS query "+
					" FROM information_schema.table_constraints tc   "+
					" LEFT JOIN information_schema.key_column_usage kcu   "+
					" ON tc.constraint_catalog = kcu.constraint_catalog   "+
					" AND tc.constraint_schema = kcu.constraint_schema   "+
					" AND tc.constraint_name = kcu.constraint_name   "+
					" LEFT JOIN information_schema.referential_constraints rc   "+
					" ON tc.constraint_catalog = rc.constraint_catalog   "+
					" AND tc.constraint_schema = rc.constraint_schema   "+
					" AND tc.constraint_name = rc.constraint_name   "+
					" LEFT JOIN information_schema.constraint_column_usage ccu   "+
					" ON rc.unique_constraint_catalog = ccu.constraint_catalog   "+
					" AND rc.unique_constraint_schema = ccu.constraint_schema   "+
					" AND rc.unique_constraint_name = ccu.constraint_name   "+
					" WHERE UPPER(tc.table_name) = '{0}'  "+
					" AND tc.table_catalog = '"+((PgSqlConnectionPool)pool).DbName+"'  "+
					" AND tc.constraint_type = 'FOREIGN KEY' "+
					" AND UPPER(ccu.table_name) = '{1}'"; }
		}
		
		internal override string DropForeignKey(string table, string externalTable,string primaryField,string relatedField)
		{
			string ret="";
			conn.ExecuteQuery(String.Format(DropForeignKeyString, table, externalTable));
			while (conn.Read())
				ret += conn[0].ToString()+";\n";
			conn.Close();
			return ret;
		}
		
		protected override string SelectWithPagingIncludeOffset {
			get { return "{0} LIMIT {2} OFFSET {1}"; }
		}

        internal override string CreateTableIndex(string table, string[] fields, string indexName, bool unique, bool ascending)
        {
            string sfields = "";
            foreach (string str in fields)
                sfields += str + " " + (ascending ? "ASC" : "DESC") + ",";
            sfields = sfields.Substring(0, sfields.Length - 1);
            return string.Format(CreateTableIndexString, new object[]{
                table,
                sfields,
                indexName,
                (unique ? "UNIQUE" : "")
            });
        }

        protected override string DropTableIndexString
        {
            get
            {
                return "DROP INDEX {1}";
            }
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
                return "SELECT table_name,view_definition FROM information_schema.views WHERE table_schema NOT IN ('pg_catalog', 'information_schema') AND table_name !~ '^pg_'";
            }
        }

        protected override string CreateProcedureString
        {
            get
            {
                return "CREATE FUNCTION {0} ({1}) RETURNS {2} AS $$ {3} BEGIN {4} END; $$";
            }
        }

        protected override string UpdateProcedureString
        {
            get
            {
                return "REPLACE FUNCTION {0} ({1}) RETURNS {2} AS $$ {3} BEGIN {4} END; $$";
            }
        }

        protected override string DropProcedureString
        {
            get
            {
                return "DROP FUNCTION {0}";
            }
        }
	}
}
