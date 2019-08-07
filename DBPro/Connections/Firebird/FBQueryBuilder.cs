/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 28/10/2008
 * Time: 11:18 AM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */

using System;
using System.Collections.Generic;
using Org.Reddragonit.Dbpro.Connections.PoolComponents;
using System.Data;

namespace Org.Reddragonit.Dbpro.Connections.Firebird
{
    /// <summary>
    /// Description of FBQueryBuilder.
    /// </summary>
    internal class FBQueryBuilder : QueryBuilder
    {
        private bool? _isAtLeastVersion3;
        protected bool isAtLeastVersion3 {
            get {
                if (!_isAtLeastVersion3.HasValue)
                {
                    Connection conn = pool.GetConnection();
                    conn.ExecuteQuery("SELECT CAST(SUBSTRING(rdb$get_context('SYSTEM', 'ENGINE_VERSION') FROM 1 FOR POSITION('.' IN rdb$get_context('SYSTEM', 'ENGINE_VERSION'))) AS INTEGER) from rdb$database");
                    conn.Read();
                    _isAtLeastVersion3 = int.Parse(conn[0].ToString()) >= 3;
                    conn.CloseConnection();
                }
                return _isAtLeastVersion3.Value;
            }
        }

		public FBQueryBuilder(ConnectionPool pool): base(pool)
		{
		}

        internal override string CreateColumn(string table, ExtractedFieldMap field)
        {
            if (field.ComputedCode != null)
                return string.Format("ALTER TABLE {0} ADD {1} {2} COMPUTED BY {3}", table, field.FieldName, field.FullFieldType, field.ComputedCode);
            return base.CreateColumn(table, field);
        }

		protected override string DropNotNullString {
			get {
                if (isAtLeastVersion3)
                    return "ALTER TABLE {0} ALTER {1} DROP NOT NULL";
                else
                    return "SELECT 'ALTER TABLE {0} DROP CONSTRAINT '||r.rdb$constraint_name " +
                        "FROM rdb$relation_constraints r, " +
                        "rdb$check_constraints c " +
                        "WHERE r.rdb$constraint_name = c.rdb$constraint_name " +
                        "AND   r.rdb$relation_name = '{0}' " +
                        "AND   c.rdb$trigger_name = '{1}' " +
                        "AND   r.rdb$constraint_type = 'NOT NULL'";
            }
		}

        private string DropNullStringV2
        {
            get
            { 
                    return "UPDATE RDB$RELATION_FIELDS SET RDB$NULL_FLAG = NULL "+
                        "WHERE RDB$FIELD_NAME = '{1}' AND RDB$RELATION_NAME = '{0}'";
            }
        }
		
		internal override string DropNullConstraint(string table, ExtractedFieldMap field)
		{
			string ret = "";
            Connection conn = pool.GetConnection();
            if (isAtLeastVersion3)
                ret = string.Format(DropNotNullString, table, field.FieldName);
            else
            {
                conn.ExecuteQuery(String.Format(DropNotNullString, table, field.FieldName));
                if (conn.Read())
                    ret = conn[0].ToString();
                conn.CloseConnection();
                if (ret == "")
                    ret = String.Format(DropNullStringV2, table, field.FieldName);
            }
			return ret;
		}
		
		protected override string DropPrimaryKeyString {
			get { return "select DISTINCT 'ALTER TABLE {0} DROP CONSTRAINT '||rel.rdb$CONSTRAINT_NAME from  "+
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
			get { return "SELECT DISTINCT 'ALTER TABLE {0} DROP CONSTRAINT '||rc.RDB$CONSTRAINT_NAME "+
					" FROM   "+
					" rdb$relation_constraints rc   "+
					" inner join rdb$indices fidx ON (rc.rdb$index_name = fidx.rdb$index_name AND rc.rdb$constraint_type = 'FOREIGN KEY')   "+
					" inner join rdb$index_segments fseg ON fidx.rdb$index_name = fseg.rdb$index_name   "+
					" inner join rdb$indices pidx ON fidx.rdb$foreign_key = pidx.rdb$index_name   "+
					" inner join rdb$index_segments pseg ON (pidx.rdb$index_name = pseg.rdb$index_name AND pseg.rdb$field_position=fseg.rdb$field_position)   "+
					" inner join RDB$REF_CONSTRAINTS actions ON rc.rdb$constraint_name = actions.RDB$constraint_name   "+
					" WHERE TRIM(rc.rdb$relation_name) = '{0}' "+
					" AND TRIM(pidx.rdb$relation_name) = '{1}'"+
                    " AND TRIM(pseg.rdb$field_name) = '{2}'"+
                    " AND TRIM(fseg.rdb$field_name) = '{3}'"; }
		}
		
		internal override string DropForeignKey(string table, string tableName,string primaryField,string relatedField)
		{
			string ret = "";
            Connection conn = pool.GetConnection();
			conn.ExecuteQuery(String.Format(DropForeignKeyString,new object[]{table.ToUpper(),tableName.ToUpper(),primaryField.ToUpper(),relatedField.ToUpper()}));
			while (conn.Read())
				ret+=conn[0].ToString().Trim()+";\n";
            conn.CloseConnection();
			return ret;
		}


		internal override string DropPrimaryKey(PrimaryKey key)
		{
			string ret="";
            Connection conn = pool.GetConnection();
			foreach (string str in key.Fields)
			{
				conn.ExecuteQuery(String.Format(DropPrimaryKeyString, key.Name.ToUpper(), str.ToUpper()));
				if (conn.Read())
					ret += conn[0].ToString().Trim()+";\n";
				conn.Close();
			}
            conn.CloseConnection();
			return ret;
		}
		
		protected override string SelectTriggersString {
			get {
				return "SELECT RDB$TRIGGER_NAME, 'FOR ' ||TRIM(RDB$RELATION_NAME)||' '|| (CASE RDB$TRIGGER_INACTIVE WHEN 0 THEN 'ACTIVE' ELSE 'INACTIVE' END) || ' '|| (CASE RDB$TRIGGER_TYPE WHEN 1 THEN 'BEFORE INSERT' WHEN 2 THEN 'AFTER INSERT' WHEN 3 THEN 'BEFORE UPDATE' WHEN 4 THEN 'AFTER UPDATE' WHEN 5 THEN 'BEFORE DELETE' WHEN 6 THEN 'AFTER DELETE' END) || ' POSITION '|| CAST(RDB$TRIGGER_SEQUENCE AS VARCHAR(100)), RDB$TRIGGER_SOURCE FROM RDB$TRIGGERS where RDB$SYSTEM_FLAG<>1 and RDB$TRIGGER_SOURCE IS NOT NULL";
			}
		}
		
		protected override string SelectTableNamesString {
			get {
                return "SELECT DISTINCT(TRIM(rfr.rdb$relation_name)) from rdb$relation_fields rfr where rfr.rdb$relation_name NOT LIKE 'RDB$%' and rfr.rdb$relation_name NOT LIKE 'MON$%' AND rfr.RDB$RELATION_NAME NOT IN (SELECT vw.RDB$RELATION_NAME FROM RDB$RELATIONS vw WHERE vw.RDB$VIEW_SOURCE IS NOT NULL)";
			}
		}
		
		protected override string SelectTableFieldsString {
			get {
				return @"SELECT  
						TRIM(rfr.rdb$field_name) AS ColumnName, 
					TRIM(CASE fld.rdb$field_type WHEN 261 THEN 
					 (CASE WHEN fld.rdb$field_sub_type = 1 THEN  'BLOB SUB_TYPE TEXT' 
					  ELSE 'BLOB' END) 
					 WHEN 14 THEN 'CHAR' 
					 WHEN 27 THEN 'DOUBLE PRECISION' 
					 WHEN 10 THEN 'FLOAT' 
					 WHEN 16 THEN 
					 (CASE WHEN fld.rdb$field_sub_type = 2 THEN 'DECIMAL('||CAST(fld.rdb$field_precision as varchar(100))||', '||cast((0-fld.rdb$field_scale) as varchar(100))||')' 
					  ELSE 'BIGINT' END) 
					 WHEN 8 THEN 'INTEGER' 
					 WHEN 9 THEN 'QUAD' 
					 WHEN 7 THEN 'SMALLINT' 
					 WHEN 12 THEN 'DATE' 
					 WHEN 13 THEN 'TIME' 
					 WHEN 35 THEN 'TIMESTAMP' 
					 WHEN 37 THEN 'VARCHAR' 
                     WHEN 23 THEN 'BOOLEAN'
					 ELSE 'UNKNOWN' 
					 END) AS ColumnDataType, 
					(CASE fld.rdb$field_type WHEN 261 THEN -1 ELSE 
					fld.rdb$field_length END) AS ColumnSize, 
					(CASE WHEN (select count(*) from  
					rdb$relation_constraints rel, 
					rdb$indices idx, 
					rdb$index_segments seg 
					 where  
					rel.rdb$constraint_type = 'PRIMARY KEY'  
					and rel.rdb$index_name = idx.rdb$index_name  
					and idx.rdb$index_name = seg.rdb$index_name  
					and rel.rdb$relation_name = rfr.rdb$relation_name  
					and seg.rdb$field_name = rfr.rdb$field_name) = 0 THEN 'false' ELSE 'true' END) AS PrimaryKey, 
					(CASE WHEN rfr.rdb$null_flag IS null or rfr.rdb$null_flag=0 THEN 'true' else 'false' END) AS NullFlag, 
					(CASE WHEN (SELECT COUNT(*)  
					FROM RDB$GENERATORS gens  
					where (gens.RDB$SYSTEM_FLAG is null or gens.RDB$SYSTEM_FLAG=0) AND gens.RDB$GENERATOR_NAME = 'GEN_'||rfr.rdb$field_name) = 0 THEN 'false' ELSE 'true' END) AS AUTOGEN,
                    fld.RDB$COMPUTED_SOURCE AS COMPUTED_CODE 
					 FROM  
					rdb$relation_fields rfr  
					LEFT JOIN rdb$fields fld ON rfr.rdb$field_source = fld.rdb$field_name  
					LEFT JOIN rdb$relations rel ON (rfr.rdb$relation_name = rel.rdb$relation_name AND rel.rdb$system_flag IS NOT NULL)  
					WHERE rfr.rdb$relation_name NOT LIKE 'RDB$%' and rfr.rdb$relation_name NOT LIKE 'MON$%' and  
					rfr.rdb$relation_name = '{0}'  
					ORDER BY  
					rfr.rdb$field_position";
			}
		}
		
		protected override string SelectForeignKeysString {
			get {
				return @"SELECT   
                    TRIM(fseg.rdb$field_name) AS FKColumnName,   
                    TRIM(pidx.rdb$relation_name) as PKTableName, 
					TRIM(pseg.rdb$field_name) AS PKColumnName,   
					TRIM(actions.rdb$update_rule) as on_update, 
					TRIM(actions.rdb$delete_rule) as on_delete,  
                    TRIM(rc.rdb$constraint_name) as conName 
					FROM  
					rdb$relation_constraints rc  
					inner join rdb$indices fidx ON (rc.rdb$index_name = fidx.rdb$index_name AND rc.rdb$constraint_type = 'FOREIGN KEY')  
					inner join rdb$index_segments fseg ON fidx.rdb$index_name = fseg.rdb$index_name  
					inner join rdb$indices pidx ON fidx.rdb$foreign_key = pidx.rdb$index_name  
					inner join rdb$index_segments pseg ON (pidx.rdb$index_name = pseg.rdb$index_name AND pseg.rdb$field_position=fseg.rdb$field_position)  
					inner join RDB$REF_CONSTRAINTS actions ON rc.rdb$constraint_name = actions.RDB$constraint_name  
					WHERE rc.rdb$relation_name = '{0}'  
					ORDER BY rc.rdb$relation_name,fseg.rdb$field_name";
			}
		}

        internal override List<Index> ExtractTableIndexes(string tableName, Connection conn)
        {
            List<Index> ret = new List<Index>();
            conn.ExecuteQuery("SELECT TRIM(ind.RDB$INDEX_NAME),ind.RDB$UNIQUE_FLAG,(CASE WHEN ind.RDB$INDEX_TYPE IS NULL THEN 0 ELSE 1 END) FROM RDB$INDICES ind "+
                "WHERE ind.RDB$RELATION_NAME = '" + tableName + "' AND ind.RDB$INDEX_NAME NOT IN (SELECT RDB$INDEX_NAME FROM RDB$RELATION_CONSTRAINTS WHERE RDB$RELATION_NAME = '" + tableName + "' AND RDB$INDEX_NAME IS NOT NULL) ORDER BY ind.RDB$INDEX_ID");
            while (conn.Read())
            {
                ret.Add(new Index(conn[0].ToString(), null, conn[1].ToString() == "1", conn[2].ToString() == "0"));
            }
            conn.Close();
            for (int x = 0; x < ret.Count; x++)
            {
                conn.ExecuteQuery("SELECT TRIM(ind.RDB$FIELD_NAME) FROM RDB$INDEX_SEGMENTS ind WHERE TRIM(ind.RDB$INDEX_NAME) = '" + ret[0].Name + "' ORDER BY ind.RDB$FIELD_POSITION");
                List<string> fields = new List<string>();
                while (conn.Read())
                {
                    fields.Add(conn[0].ToString());
                }
                conn.Close();
                Index ind = ret[x];
                ind.Fields = fields.ToArray();
                ret.RemoveAt(x);
                ret.Insert(x, ind);
            }
            return ret;
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
		
		protected override string CreateTriggerString {
			get { return "CREATE TRIGGER {0} {1} {2}";}
		}
		
		protected override string SelectGeneratorsString {
			get { return "SELECT TRIM(RDB$GENERATOR_NAME) " +
					" FROM RDB$GENERATORS " +
					" where RDB$SYSTEM_FLAG is null or RDB$SYSTEM_FLAG=0"; }
		}
		
		protected override string CreateNullConstraintString {
			get {
                if (isAtLeastVersion3)
                    return "ALTER TABLE {0} ALTER {1} SET NOT NULL";
                else
                    return "UPDATE RDB$RELATION_FIELDS SET RDB$NULL_FLAG = 1 WHERE RDB$FIELD_NAME = '{1}' AND RDB$RELATION_NAME = '{0}'";
            }
		}

		protected override string SelectWithPagingIncludeOffset
		{
			get{ return "SELECT FIRST {2} SKIP {1} * FROM ({0}) tbl"; }
		}

        internal override string AlterFieldType(string table, ExtractedFieldMap field, ExtractedFieldMap oldFieldInfo)
        {
            if ((field.FullFieldType.ToUpper().Contains("BLOB")) || (oldFieldInfo.FullFieldType.ToUpper().Contains("BLOB")))
                return DropColumn(table, field.FieldName) + ";" + CreateColumn(table, field) + ";";
            return base.AlterFieldType(table, field, oldFieldInfo);
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
                return "CREATE {3} {4} INDEX {2} ON {0} ({1})";
            }
        }

        protected override string SelectProceduresString
        {
            get
            {
                return @"SELECT proc.RDB$PROCEDURE_NAME,inps.pars_code,rets.pars_code,
SUBSTRING(proc.RDB$PROCEDURE_SOURCE FROM 1 FOR POSITION('BEGIN' IN proc.RDB$PROCEDURE_SOURCE)-1) as declares,
LEFT(TRIM(SUBSTRING(proc.RDB$PROCEDURE_SOURCE FROM POSITION('BEGIN' IN proc.RDB$PROCEDURE_SOURCE)+6)),CHAR_LENGTH(TRIM(SUBSTRING(proc.RDB$PROCEDURE_SOURCE FROM POSITION('BEGIN' IN proc.RDB$PROCEDURE_SOURCE)+6)))-4) as code
FROM RDB$PROCEDURES proc LEFT JOIN 
( SELECT LIST(TRIM(par.RDB$PARAMETER_NAME)
||' '||TRIM(CASE fld.rdb$field_type WHEN 261 THEN 
	(CASE WHEN fld.rdb$field_sub_type = 1 THEN  'BLOB SUB_TYPE TEXT' 
	ELSE 'BLOB' END) 
	WHEN 14 THEN 'CHAR' 
	WHEN 27 THEN 'DOUBLE PRECISION' 
	WHEN 10 THEN 'FLOAT' 
	WHEN 16 THEN 
	(CASE WHEN fld.rdb$field_sub_type = 2 THEN 'DECIMAL('||CAST(fld.rdb$field_precision as varchar(100))||', '||cast((0-fld.rdb$field_scale) as varchar(100))||')' 
	ELSE 'BIGINT' END) 
	WHEN 8 THEN 'INTEGER' 
	WHEN 9 THEN 'QUAD' 
	WHEN 7 THEN 'SMALLINT' 
	WHEN 12 THEN 'DATE' 
	WHEN 13 THEN 'TIME' 
	WHEN 35 THEN 'TIMESTAMP' 
	WHEN 37 THEN 'VARCHAR' 
    WHEN 23 THEN 'BOOLEAN'
	ELSE 'UNKNOWN' 
    END)||(CASE WHEN fld.rdb$field_type IN (14,37) THEN '('||fld.rdb$field_length||')' 
    ELSE '' END)
) as pars_code,par.RDB$PROCEDURE_NAME as proc_name
FROM RDB$PROCEDURE_PARAMETERS par, RDB$FIELDS fld
WHERE par.RDB$PARAMETER_TYPE = 1
AND par.RDB$FIELD_SOURCE = fld.RDB$FIELD_NAME
GROUP BY par.RDB$PROCEDURE_NAME
) rets ON proc.RDB$PROCEDURE_NAME = rets.proc_name
LEFT JOIN 
( SELECT LIST(TRIM(par.RDB$PARAMETER_NAME)
||' '||TRIM(CASE fld.rdb$field_type WHEN 261 THEN 
	(CASE WHEN fld.rdb$field_sub_type = 1 THEN  'BLOB SUB_TYPE TEXT' 
	ELSE 'BLOB' END) 
	WHEN 14 THEN 'CHAR' 
	WHEN 27 THEN 'DOUBLE PRECISION' 
	WHEN 10 THEN 'FLOAT' 
	WHEN 16 THEN 
	(CASE WHEN fld.rdb$field_sub_type = 2 THEN 'DECIMAL('||CAST(fld.rdb$field_precision as varchar(100))||', '||cast((0-fld.rdb$field_scale) as varchar(100))||')' 
	ELSE 'BIGINT' END) 
	WHEN 8 THEN 'INTEGER' 
	WHEN 9 THEN 'QUAD' 
	WHEN 7 THEN 'SMALLINT' 
	WHEN 12 THEN 'DATE' 
	WHEN 13 THEN 'TIME' 
	WHEN 35 THEN 'TIMESTAMP' 
	WHEN 37 THEN 'VARCHAR'
    WHEN 23 THEN 'BOOLEAN' 
	ELSE 'UNKNOWN' 
    END)||(CASE WHEN fld.rdb$field_type IN (14,37) THEN '('||fld.rdb$field_length||')' 
    ELSE '' END)
) as pars_code,par.RDB$PROCEDURE_NAME as proc_name
FROM RDB$PROCEDURE_PARAMETERS par, RDB$FIELDS fld
WHERE par.RDB$PARAMETER_TYPE = 0
AND par.RDB$FIELD_SOURCE = fld.RDB$FIELD_NAME
GROUP BY par.RDB$PROCEDURE_NAME
) inps ON proc.RDB$PROCEDURE_NAME = inps.proc_name";
            }
        }

        protected override string CreateProcedureString
        {
            get
            {
                return "CREATE PROCEDURE {0} {1} {2} AS {3} BEGIN {4} END";
            }
        }

        internal override string CreateProcedure(StoredProcedure procedure)
        {
            return string.Format(CreateProcedureString, new object[] { procedure.ProcedureName, (procedure.ParameterLines==null ? "" : (procedure.ParameterLines=="" ? "" : "("+procedure.ParameterLines+")")), (procedure.ReturnLine==null ? "" : "RETURNS ("+procedure.ReturnLine+")"), procedure.DeclareLines, procedure.Code });
        }

        protected override string UpdateProcedureString
        {
            get
            {
                return "ALTER PROCEDURE {0} {1} {2} AS {3} BEGIN {4} END";
            }
        }

        internal override string UpdateProcedure(StoredProcedure procedure)
        {
            return string.Format(UpdateProcedureString, new object[] { procedure.ProcedureName, (procedure.ParameterLines == null ? "" : (procedure.ParameterLines == "" ? "" : "(" + procedure.ParameterLines + ")")), (procedure.ReturnLine == null ? "" : "RETURNS (" + procedure.ReturnLine + ")"), procedure.DeclareLines, procedure.Code });
        }

        protected override string DropProcedureString
        {
            get
            {
                return "DROP PROCEDURE {0}";
            }
        }

        protected override string SelectViewsString
        {
            get
            {
                return @"SELECT TRIM(vw.RDB$RELATION_NAME),TRIM(vw.RDB$VIEW_SOURCE)
FROM RDB$RELATIONS vw
WHERE vw.RDB$VIEW_SOURCE IS NOT NULL";
            }
        }

        #region Description
        internal override string  GetAllObjectDescriptions()
        {
            return @"SELECT * FROM 
(SELECT RDB$DESCRIPTION,RDB$RELATION_NAME FROM RDB$RELATIONS
                    UNION 
                    SELECT RDB$DESCRIPTION,RDB$FIELD_NAME FROM RDB$RELATION_FIELDS 
                    UNION
                    SELECT RDB$DESCRIPTION,RDB$GENERATOR_NAME FROM RDB$GENERATORS 
                    UNION
                    SELECT RDB$DESCRIPTION,RDB$TRIGGER_NAME FROM RDB$TRIGGERS
                    UNION
                    SELECT RDB$DESCRIPTION,RDB$INDEX_NAME FROM RDB$INDICES) 
                    t WHERE t.RDB$DESCRIPTION IS NOT NULL
                    AND RDB$RELATION_NAME NOT LIKE 'RDB$%'";
        }

        internal override string SetTableDescription(string tableName, string description)
        {
            if (isAtLeastVersion3)
                return string.Format("COMMENT ON TABLE {0} IS '{1}'", tableName, description.Replace("'", "''"));
            else
                return string.Format("UPDATE RDB$RELATIONS SET RDB$DESCRIPTION='{1}' WHERE RDB$RELATION_NAME = '{0}'", tableName, description.Replace("'", "''"));
        }

        internal override string SetFieldDescription(string tableName, string fieldName, string description)
        {
            if (isAtLeastVersion3)
                return string.Format("COMMENT ON COLUMN {0}.{1} IS '{2}'", new object[] { tableName, fieldName, description.Replace("'", "''") });
            else
                return string.Format("UPDATE RDB$RELATION_FIELDS SET RDB$DESCRIPTION='{2}' WHERE RDB$RELATION_NAME = '{0}' AND RDB$FIELD_NAME = '{1}'", new object[] { tableName, fieldName, description.Replace("'", "''") });
        }

        internal override string SetGeneratorDescription(string generatorName, string description)
        {
            if (isAtLeastVersion3)
                return string.Format("COMMENT ON GENERATOR {0} IS '{1}'", generatorName, description.Replace("'", "''"));
            else
                return string.Format("UPDATE RDB$GENERATORS SET RDB$DESCRIPTION = '{1}' WHERE RDB$GENERATOR_NAME = '{0}'", generatorName,description.Replace("'","''"));
        }

        internal override string SetTriggerDescription(string triggerName, string description)
        {
            if (isAtLeastVersion3)
                return string.Format("COMMENT ON TRIGGER {0} IS '{1}'", triggerName, description.Replace("'", "''"));
            else
                return string.Format("UPDATE RDB$TRIGGERS SET RDB$DESCRIPTION = '{1}' WHERE RDB$TRIGGER_NAME = '{0}'", triggerName, description.Replace("'", "''"));
        }

        internal override string SetViewDescription(string viewName, string description)
        {
            if (isAtLeastVersion3)
                return string.Format("COMMENT ON VIEW {0} IS '{1}'", viewName, description.Replace("'", "''"));
            else
                return SetTableDescription(viewName, description);
        }

        internal override string SetIndexDescription(string indexName, string description)
        {
            if (isAtLeastVersion3)
                return string.Format("COMMENT ON INDEX {0} IS '{1}'", indexName, description.Replace("'", "''"));
            else
                return string.Format("UPDATE RDB$INDICES SET RDB$DESCRIPTION = '{1}' WHERE RDB$INDEX_NAME = '{0}'", indexName, description.Replace("'", "''"));
        }
        #endregion

        protected override string _GenerateAutogenIDQuery(sTable tbl,ref List<IDbDataParameter> parameters)
        {
            parameters[parameters.Count - 1].Direction = ParameterDirection.ReturnValue;
            return "returning " + tbl.AutoGenField;
        }
    }
}
