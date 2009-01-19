/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 28/10/2008
 * Time: 11:18 AM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */

using System;

namespace Org.Reddragonit.Dbpro.Connections.Firebird
{
	/// <summary>
	/// Description of FBQueryBuilder.
	/// </summary>
	internal class FBQueryBuilder : QueryBuilder
	{
		public FBQueryBuilder()
		{
		}
		
		protected override string DropNotNullString {
			get { return "SELECT 'ALTER TABLE {0} DROP CONSTRAINT '||r.rdb$constraint_name "+
					"FROM rdb$relation_constraints r, "+
					"rdb$check_constraints c "+
					"WHERE r.rdb$constraint_name = c.rdb$constraint_name "+
					"AND   r.rdb$relation_name = '{0}' "+
					"AND   c.rdb$trigger_name = '{1}' "+
					"AND   r.rdb$constraint_type = 'NOT NULL'"; }
		}
		
		internal override string DropNullConstraint(string table, string field, Connection conn)
		{
			string ret = "";
			conn.ExecuteQuery(String.Format(DropNotNullString,table,field));
			conn.Read();
			ret=conn[0].ToString();
			conn.Close();
			return ret;
		}
		
		protected override string DropPrimaryKeyString {
			get { return "select 'ALTER TABLE {0} DROP CONSTRAINT '||rel.rdb$CONSTRAINT_NAME from  "+
					"rdb$relation_constraints rel, "+
					"rdb$indices idx, "+
					"rdb$index_segments seg "+
					" where  "+
					"rel.rdb$constraint_type = 'PRIMARY KEY'  "+
					"and rel.rdb$index_name = idx.rdb$index_name  "+
					"and idx.rdb$index_name = seg.rdb$index_name  "+
					"AND TRIM(rel.RDB$RELATION_NAME) = '{0}'  "+
					"AND TRIM(RDB$FIELD_NAME) = '{1}'"; }
		}
		
		protected override string DropForeignKeyString {
			get { return "select 'ALTER TABLE {0} DROP CONSTRAINT '||rel.rdb$CONSTRAINT_NAME from "+
					"rdb$relation_constraints rel, "+
					"rdb$indices idx, "+
					"rdb$index_segments seg "+
					"where "+
					"rel.rdb$constraint_type = 'FOREIGN KEY' "+
					"and rel.rdb$index_name = idx.rdb$index_name "+
					"and idx.rdb$index_name = seg.rdb$index_name "+
					"AND TRIM(rel.RDB$RELATION_NAME) = '{0}'  "+
					"AND TRIM(RDB$FIELD_NAME) = '{1}'"; }
		}
		
		internal override string DropForeignKey(string table, string field, Connection conn)
		{
			string ret = "";
			conn.ExecuteQuery(String.Format(DropForeignKeyString,table,field));
			conn.Read();
			ret=conn[0].ToString();
			conn.Close();
			return ret;
		}
		
		internal override string DropPrimaryKey(string table, string field, Connection conn)
		{
			string ret="";
			conn.ExecuteQuery(String.Format(DropPrimaryKeyString,table,field));
			conn.Read();
			ret=conn[0].ToString();
			conn.Close();
			return ret;
		}
		
		protected override string SelectTriggersString {
			get {
				return "SELECT RDB$TRIGGER_NAME, 'FOR ' ||TRIM(RDB$RELATION_NAME)||' '|| (CASE RDB$TRIGGER_INACTIVE WHEN 0 THEN 'ACTIVE' ELSE 'INACTIVE' END) || ' '|| (CASE RDB$TRIGGER_TYPE WHEN 1 THEN 'BEFORE INSERT' WHEN 2 THEN 'AFTER INSERT' WHEN 3 THEN 'BEFORE UPDATE' WHEN 4 THEN 'AFTER UPDATE' WHEN 5 THEN 'BEFORE DELETE' WHEN 6 THEN 'AFTER DELETE' END) || ' POSITION '|| CAST(RDB$TRIGGER_SEQUENCE AS VARCHAR(100)), RDB$TRIGGER_SOURCE FROM RDB$TRIGGERS where RDB$SYSTEM_FLAG<>1 and RDB$TRIGGER_SOURCE IS NOT NULL";
			}
		}
		
		protected override string SelectTableNamesString {
			get {
				return "SELECT DISTINCT(TRIM(rfr.rdb$relation_name)) from rdb$relation_fields rfr where rfr.rdb$relation_name NOT LIKE 'RDB$%' and rfr.rdb$relation_name NOT LIKE 'MON$%'";
			}
		}
		
		protected override string SelectTableFieldsString {
			get {
				return "SELECT  "+
					"	TRIM(rfr.rdb$field_name) AS ColumnName, "+
					"TRIM(CASE fld.rdb$field_type WHEN 261 THEN "+
					" (CASE WHEN fld.rdb$field_sub_type = 1 THEN  'BLOB SUB_TYPE TEXT' "+
					"  ELSE 'BLOB' END) "+
					" WHEN 14 THEN 'CHAR' "+
					" WHEN 27 THEN 'DOUBLE' "+
					" WHEN 10 THEN 'FLOAT' "+
					" WHEN 16 THEN "+
					" (CASE WHEN fld.rdb$field_sub_type = 2 THEN 'DECIMAL('||CAST(fld.rdb$field_precision as varchar(100))||', '||cast((0-fld.rdb$field_scale) as varchar(100))||')' "+
					"  ELSE 'BIGINT' END) "+
					" WHEN 8 THEN 'INTEGER' "+
					" WHEN 9 THEN 'QUAD' "+
					" WHEN 7 THEN 'SMALLINT' "+
					" WHEN 12 THEN 'DATE' "+
					" WHEN 13 THEN 'TIME' "+
					" WHEN 35 THEN 'TIMESTAMP' "+
					" WHEN 37 THEN 'VARCHAR' "+
					" ELSE 'UNKNOWN' "+
					" END) AS ColumnDataType, "+
					"(CASE fld.rdb$field_type WHEN 261 THEN -1 ELSE "+
					"fld.rdb$field_length END) AS ColumnSize, "+
					"(CASE WHEN (select count(*) from  "+
					"rdb$relation_constraints rel, "+
					"rdb$indices idx, "+
					"rdb$index_segments seg "+
					" where  "+
					"rel.rdb$constraint_type = 'PRIMARY KEY'  "+
					"and rel.rdb$index_name = idx.rdb$index_name  "+
					"and idx.rdb$index_name = seg.rdb$index_name  "+
					"and rel.rdb$relation_name = rfr.rdb$relation_name  "+
					"and seg.rdb$field_name = rfr.rdb$field_name) = 0 THEN 'false' ELSE 'true' END) AS PrimaryKey, "+
					"(CASE WHEN rfr.rdb$null_flag IS null or rfr.rdb$null_flag=0 THEN 'true' else 'false' END) AS NullFlag, "+
					"(CASE WHEN (SELECT COUNT(*)  "+
					"FROM RDB$GENERATORS gens  "+
					"where (gens.RDB$SYSTEM_FLAG is null or gens.RDB$SYSTEM_FLAG=0) AND gens.RDB$GENERATOR_NAME = 'GEN_'||rfr.rdb$field_name) = 0 THEN 'false' ELSE 'true' END) AS AUTOGEN "+
					" FROM  "+
					"rdb$relation_fields rfr  "+
					"LEFT JOIN rdb$fields fld ON rfr.rdb$field_source = fld.rdb$field_name  "+
					"LEFT JOIN rdb$relations rel ON (rfr.rdb$relation_name = rel.rdb$relation_name AND rel.rdb$system_flag IS NOT NULL)  "+
					"WHERE rfr.rdb$relation_name NOT LIKE 'RDB$%' and rfr.rdb$relation_name NOT LIKE 'MON$%' and  "+
					"rfr.rdb$relation_name = '{0}'  "+
					"ORDER BY  "+
					"rfr.rdb$field_position";
			}
		}
		
		protected override string SelectForeignKeysString {
			get {
				return "SELECT   "+
					"TRIM(pseg.rdb$field_name) AS PKColumnName,   "+
					"TRIM(pidx.rdb$relation_name) as PKTableName, "+
					"TRIM(fseg.rdb$field_name) AS FKColumnName,  "+
					"TRIM(actions.rdb$update_rule) as on_update, "+
					"TRIM(actions.rdb$delete_rule) as on_delete  "+
					"FROM  "+
					"rdb$relation_constraints rc  "+
					"inner join rdb$indices fidx ON (rc.rdb$index_name = fidx.rdb$index_name AND rc.rdb$constraint_type = 'FOREIGN KEY')  "+
					"inner join rdb$index_segments fseg ON fidx.rdb$index_name = fseg.rdb$index_name  "+
					"inner join rdb$indices pidx ON fidx.rdb$foreign_key = pidx.rdb$index_name  "+
					"inner join rdb$index_segments pseg ON (pidx.rdb$index_name = pseg.rdb$index_name AND pseg.rdb$field_position=fseg.rdb$field_position)  "+
					"inner join RDB$REF_CONSTRAINTS actions ON rc.rdb$constraint_name = actions.RDB$constraint_name  "+
					"WHERE rc.rdb$relation_name = '{0}'  "+
					"ORDER BY rc.rdb$relation_name,fseg.rdb$field_name";
			}
		}
		
		protected override string CreateGeneratorString {
			get { return "CREATE GENERATOR {0}"; }
		}
		
		protected override string DropGeneratorString {
			get { return "DROP GENERATOR {0}"; }
		}
		
		protected override string GetGeneratorValueString {
			get { return "SELECT GEN_ID({0},0) FROM RDB$DATABASE"; }
		}
		
		protected override string SetGeneratorValueString {
			get { return "SET GENERATOR {0} TO {1}"; }
		}
		
		protected override string SelectCurrentAutoGenIdValueNumberString {
			get { return "SELECT GEN_ID(GEN_{0},0) FROM RDB$DATABASE"; }
		}
		
		protected override string SetCurrentAutoGenIdValueNumberString {
			get { return "SET GENERATOR GEN_{0} TO {1}"; }
		}
		
		protected override string CreateTriggerString {
			get { return "CREATE TRIGGER {0} {1} {2}";}
		}
		
		protected override string SelectGeneratorsString {
			get { return "SELECT TRIM(RDB$GENERATOR_NAME) " +
					" FROM RDB$GENERATORS " +
					" where RDB$SYSTEM_FLAG is null or RDB$SYSTEM_FLAG=0"; }
		}
		
		protected override string CreateNullConstraintString {
			get { return "UPDATE RDB$RELATION_FIELDS SET RDB$NULL_FLAG = 1 WHERE RDB$FIELD_NAME = '{1}' AND RDB$RELATION_NAME = '{0}'"; }
		}
	}
}
