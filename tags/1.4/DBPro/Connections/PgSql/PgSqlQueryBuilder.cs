/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 03/04/2009
 * Time: 10:35 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using System;

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
		
		protected override string SelectTableNamesString {
			get {
				return "SELECT table_name "+
					" FROM information_schema.tables "+
					" WHERE table_type = 'BASE TABLE' "+
					" AND table_schema NOT IN ('pg_catalog', 'information_schema') "+
					" AND table_catalog = '"+((PgSqlConnectionPool)pool).DbName+"'";
			}
		}
		
		protected override string SelectTableFieldsString {
			get { return "SELECT UPPER(cols.COLUMN_NAME), " +
					" UPPER(DATA_TYPE) AS dtype, " +
					" (CASE " +
					" WHEN CHARACTER_MAXIMUM_LENGTH IS NOT NULL THEN CHARACTER_MAXIMUM_LENGTH " +
					" ELSE " +
					" (CASE UPPER(DATA_TYPE) " +
					" WHEN 'INTEGER' THEN 4 " +
					" WHEN 'BIGINT' THEN 8 " +
					" WHEN 'TINYINT' THEN 1 " +
					" WHEN 'TEXT' THEN -1 " +
					" WHEN 'BLOB' THEN -1 " +
					" WHEN 'BIT' THEN 1 " +
					" WHEN 'FLOAT' THEN 8 " +
					" WHEN 'DOUBLE' THEN 8 " +
					" WHEN 'DECIMAL' THEN 8 " +
					" WHEN 'SMALLINT' THEN 2 " +
					" END) " +
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
					" cols.TABLE_NAME='{0}'"; }
		}
		
		protected override string SelectForeignKeysString {
			get {
				return "SELECT "+
					" ccu.table_name AS PRIMARY_KEY_TABLE,  "+
					" ccu.column_name AS PRIMARY_KEY_COLUMN, "+
					" kcu.column_name AS FOREIGN_KEY_COLUMN, "+
					" rc.update_rule AS on_update,  "+
					" rc.delete_rule AS on_delete "+
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
					" WHERE tc.table_name = '{0}' "+
					" AND tc.table_catalog = '"+((PgSqlConnectionPool)pool).DbName+"' "+
					" AND tc.constraint_type = 'FOREIGN KEY'";
			}
		}
		
		protected override string SelectTriggersString {
			get {
				return "SELECT UPPER(trigger_name) AS TrigName, "+
					" condition_timing||' '||event_manipulation||' ON '||event_object_table AS TrigCondition, "+
					" 'FOR EACH ROW '||action_statement AS TrigCode "+
					" FROM information_schema.triggers  "+
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
			get { return "CREATE TRIGGER {0} {1} {2}";}
		}
		
		protected override string SelectGeneratorsString {
			get { return "SELECT relname FROM pg_class "+
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
			get { return "SELECT 'ALTER TABLE '||tc.table_name||' DROP CONSTRAINT \''||tc.constraint_name||'\'' AS QUERY "+
					" FROM information_schema.key_column_usage kcu,  "+
					" information_schema.table_constraints tc  "+
					" WHERE kcu.constraint_name=tc.constraint_name  "+
					" AND constraint_type = 'PRIMARY KEY' "+
					" AND tc.table_name='{0}' "+
					" AND column_name='{1}'"; }
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
					" WHERE tc.table_name = '{0}'  "+
					" AND tc.table_catalog = '"+((PgSqlConnectionPool)pool).DbName+"'  "+
					" AND tc.constraint_type = 'FOREIGN KEY' "+
					" AND ccu.table_name = '{1}'"; }
		}
		
		internal override string DropForeignKey(string table, string externalTable)
		{
			string ret="";
			conn.ExecuteQuery(String.Format(DropForeignKeyString, table, externalTable));
			while (conn.Read())
				ret += conn[0].ToString()+";\n";
			conn.Close();
			return ret;
		}
	}
}
