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
using Org.Reddragonit.Dbpro.Connections.PoolComponents;
using Org.Reddragonit.Dbpro.Structure.Attributes;
using System.Data;

namespace Org.Reddragonit.Dbpro.Connections.MySql
{
	/// <summary>
	/// Description of MySqlQueryBuilder.
	/// </summary>
	internal class MySqlQueryBuilder : QueryBuilder
	{
		public MySqlQueryBuilder(ConnectionPool pool) : base(pool)
		{
		}

        public override string CreateParameterName(string parameter)
        {
            return string.Format((MySqlConnectionPool.AssemblyVersion.Major < 6 ? "?" : "@") + "{0}", parameter);
        }

        protected override string CreateTableString
        {
            get
            {
                return base.CreateTableString +" ENGINE=InnoDB";
            }
        }

        protected override string CreateForeignKeyString
        {
            get { return "ALTER TABLE {0} ADD CONSTRAINT FOREIGN KEY ({1}) REFERENCES {2}({3}) ON UPDATE {4} ON DELETE {5}"; }
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
                return @"SELECT UPPER(COLUMN_NAME), 
                    (CASE UPPER(DATA_TYPE) WHEN 'INT' THEN 'INTEGER' ELSE UPPER(DATA_TYPE) END) dtype, 
					(CASE 
					  WHEN CHARACTER_MAXIMUM_LENGTH IS NOT NULL THEN CHARACTER_MAXIMUM_LENGTH 
					  ELSE 
					    (CASE UPPER(DATA_TYPE) 
					      WHEN 'INT' THEN 4 
					      WHEN 'BIGINT' THEN 8 
					      WHEN 'TINYINT' THEN 1 
					      WHEN 'TEXT' THEN -1 
					      WHEN 'BLOB' THEN -1 
					      WHEN 'BIT' THEN 1 
					      WHEN 'FLOAT' THEN 8 
					      WHEN 'DOUBLE' THEN 8 
					      WHEN 'DECIMAL' THEN 8 
					      WHEN 'SMALLINT' THEN 2 
                          WHEN 'DATETIME' THEN 8 
					    END) 
					END) dataLength, 
					(CASE WHEN COLUMN_KEY = 'PRI' THEN 'true' ELSE 'false' END) isPrimary, 
					(CASE WHEN IS_NULLABLE = 'NO' THEN 'false' ELSE 'true' END) isNullable, 
					(CASE WHEN EXTRA LIKE '%auto_increment%' THEN 'true' ELSE 'false' END) isIndentity,
        (SELECT SUBSTR(ACTION_STATEMENT,INSTR(ACTION_STATEMENT,CONCAT('NEW.',information_schema.COLUMNS.COLUMN_NAME)))
					from information_schema.TRIGGERS 
					WHERE TRIGGER_SCHEMA=information_schema.COLUMNS.TABLE_SCHEMA AND information_schema.TRIGGERS.TRIGGER_NAME = CONCAT('{0}_',information_schema.COLUMNS.COLUMN_NAME,'_UPDATE') ) code
					from information_schema.TRIGGERS 
					WHERE TRIGGER_SCHEMA=information_schema.TABLE_SCHEMA AND information_schema.TRIGGERS.TRIGGER_NAME = CONCAT('{0}_',information_schema.COLUMN,'_UPDATE') ) code
					 from information_schema.COLUMNS WHERE TABLE_SCHEMA='" + ((MySqlConnectionPool)pool).DbName+"' AND TABLE_NAME='{0}'";
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
					" UPPER(ref.DELETE_RULE) 'on Delete', "+
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
				return @"SELECT TRIGGER_NAME, 
					UPPER(CONCAT(ACTION_TIMING,' ',EVENT_MANIPULATION,' ON ',EVENT_OBJECT_TABLE)) comm_string, 
					CONCAT('FOR EACH ROW ',ACTION_STATEMENT) code 
					from information_schema.TRIGGERS 
					WHERE TRIGGER_SCHEMA='"+((MySqlConnectionPool)pool).DbName+"'";
			}
		}
		
		protected override string SelectCurrentIdentities {
			get {
				return "SELECT "+
					" UPPER(TABLE_NAME)," +
					" UPPER(COLUMN_NAME), "+
                    " (CASE UPPER(DATA_TYPE) WHEN 'INT' THEN 'INTEGER' ELSE UPPER(DATA_TYPE) END) " +
					" from information_schema.COLUMNS "+
					" WHERE TABLE_SCHEMA='"+((MySqlConnectionPool)pool).DbName+"' "+
					" AND EXTRA LIKE '%auto_increment%'";
			}
		}
		
		internal override string SelectIdentities()
		{
			string ret="";
            Connection conn = pool.GetConnection();
			conn.ExecuteQuery(SelectCurrentIdentities);
			while(conn.Read())
			{
				ret+=String.Format("SELECT '{0}','{1}','{2}', "+
				                   " (CASE ISNULL(MAX({1})) "+
				                   " WHEN 1 THEN '0' "+
				                   " ELSE CONVERT(MAX({1}),CHAR(250)) END) VAL FROM {0} \nUNION\n",
				                   conn[0].ToString(),
				                   conn[1].ToString(),
				                   conn[2].ToString());
			}
            conn.CloseConnection();
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
            Connection conn = pool.GetConnection();
			conn.ExecuteQuery(string.Format(DropForeignKeyString,table,externalTable,primaryField,relatedField));
			string ret = "";
			if (conn.Read())
				ret=conn[0].ToString();
            conn.CloseConnection();
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

        private const string _CREATE_COMPUTED_TRIGGERS = @"DELIMITER |

CREATE TRIGGER {0}_{1}_insert BEFORE INSERT ON {0}
  FOR EACH ROW BEGIN
    SET NEW.{1} = {2};
  END;
|

CREATE TRIGGER {0}_{1}_update BEFORE UPDATE ON {0}
  FOR EACH ROW BEGIN
    SET NEW.{1} = {2};
  END;
|

DELIMITER ;";

        internal override string AlterFieldType(string table, ExtractedFieldMap field, ExtractedFieldMap oldFieldInfo)
        {
            string ret = "";
            if (!field.Nullable)
                ret = string.Format(AlterFieldTypeString, table, field.FieldName, field.FullFieldType, "NOT");
            else
                ret = string.Format(AlterFieldTypeString, table, field.FieldName, field.FullFieldType, "");
            if (field.ComputedCode!=null)
            {
                if (oldFieldInfo.ComputedCode != null)
                {
                    if (oldFieldInfo.ComputedCode != field.ComputedCode)
                    {
                        ret += "\n" + this.DropTrigger(table + "_" + field.FieldName + "_UPDATE") + "\n" + this.DropTrigger(table + "_" + field.FieldName + "_DELETE");
                        ret += "\n" + string.Format(_CREATE_COMPUTED_TRIGGERS, table, field.FieldName, field.ComputedCode);
                    }
                }
                else
                    ret += "\n" + string.Format(_CREATE_COMPUTED_TRIGGERS, table, field.FieldName, field.ComputedCode);

            }else if (oldFieldInfo.ComputedCode!=null)
                ret += "\n" + this.DropTrigger(table + "_" + field.FieldName + "_UPDATE") + "\n" + this.DropTrigger(table + "_" + field.FieldName + "_DELETE");
            return ret;
        }

        internal override string CreateColumn(string table, ExtractedFieldMap field)
        {
            if (field.ComputedCode!=null)
                return base.CreateColumn(table, field) + "\n" + string.Format(_CREATE_COMPUTED_TRIGGERS, table, field.FieldName, field.ComputedCode);
            return base.CreateColumn(table, field);
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

        protected override string SelectViewsString
        {
            get
            {
                return "SELECT TABLE_NAME,VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS";
            }
        }

        protected override string SelectProceduresString
        {
            get
            {
                return @"SELECT prc.param_list,
CONCAT(prc.returns,' ', 
(CASE WHEN prc.is_deterministic = 'YES' THEN 'DETERMINISTIC' ELSE 'NOT DETERMINISTIC' END)
,' ',prc.sql_data_access), 
SUBSTRING(rtns.ROUTINE_DEFINITION,6,LENGTH(rtns.ROUTINE_DEFINITION)-10) as `code`
FROM INFORMATION_SCHEMA.ROUTINES rtns, mysql.proc prc
WHERE rtns.ROUTINE_TYPE = 'FUNCTION'
AND rtns.ROUTINE_SCHEMA = prc.db
AND rtns.ROUTINE_NAME = prc.`NAME`";
            }
        }

        protected override string CreateProcedureStringWithReturn
        {
            get
            {
                return "CREATE FUNCTION {0} ({1}) RETURNS {2} BEGIN {3} {4} END";
            }
        }

        protected override string UpdateProcedureStringWithReturn
        {
            get
            {
                return "ALTER FUNCTION {0} ({1}) RETURNS {2} BEGIN {3} {4} END";
            }
        }

        protected override string CreateProcedureString
        {
            get
            {
                return "CREATE FUNCTION {0} ({1}) BEGIN {2} {3} END";
            }
        }

        protected override string UpdateProcedureString
        {
            get
            {
                return "ALTER FUNCTION {0} ({1}) BEGIN {2} {3} END";
            }
        }

        protected override string CreateProcedureStringNoParametersWithReturn
        {
            get
            {
                return "CREATE FUNCTION {0} RETURNS {1} BEGIN {2} {3} END";
            }
        }

        protected override string UpdateProcedureStringNoParametersWithReturn
        {
            get
            {
                return "ALTER FUNCTION {0} RETURNS {1} BEGIN {2} {3} END";
            }
        }

        protected override string CreateProcedureStringNoParameters
        {
            get
            {
                return "CREATE FUNCTION {0} BEGIN {1} {2} END";
            }
        }

        protected override string UpdateProcedureStringNoParameters
        {
            get
            {
                return "ALTER FUNCTION {0} BEGIN {1} {2} END";
            }
        }

        protected override string DropProcedureString
        {
            get
            {
                return "DROP FUNCTION {0}";
            }
        }

        #region Description
        internal override string GetAllObjectDescriptions()
        {
            return string.Format(@"SELECT * FROM ( SELECT TABLE_COMMENT,TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA='{0} '
                                    UNION
                                    SELECT COLUMN_COMMENT,COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='{0}'
                                    UNION 
                                    SELECT SUBSTRING(b.body, b.start, (b.eind - b.start)),b.TRIGGER_NAME FROM (SELECT a.body,locate('/**@DESCRIPTION:',a.body) as start,locate('**/',a.body,locate('/**@DESCRIPTION:',a.body)) as eind,a.TRIGGER_NAME FROM (SELECT t.ACTION_STATEMENT as body,t.TRIGGER_NAME FROM information_schema.triggers t WHERE t.TRIGGER_SCHEMA='{0}' AND t.ACTION_STATEMENT LIKE '%/**@DESCRIPTION:%') a ) b 
                                    UNION
                                    SELECT COMMENT,INDEX_NAME FROM information_schema.statistics WHERE INDEX_SCHEMA='{0}') t WHERE t.TABLE_COMMENT IS NOT NULL AND t.TABLE_COMMENT<>''", ((MySqlConnectionPool)pool).DbName);
        }

        internal override string SetTableDescription(string tableName, string description)
        {
            return string.Format("ALTER TABLE {0} COMMENT '{1}'", tableName, description.Replace("'", "''"));
        }

        internal override string SetFieldDescription(string tableName, string fieldName, string description)
        {
            sTable tbl = pool.Mapping[pool.Mapping[tableName]];
            List<string> pkeys = new List<string>(tbl.PrimaryKeyFields);
            foreach (sTableField fld in tbl.Fields){
                if (fld.Name == fieldName){
                    ExtractedFieldMap efm = new ExtractedFieldMap(fieldName, MySqlConnection._TranslateFieldType(fld.Type, fld.Length), fld.Length, pkeys.Contains(fieldName), fld.Nullable,fld.ComputedCode);
                    return string.Format(
                        AlterFieldType(tableName,efm,efm)+(tbl.AutoGenField==fld.Name && pkeys.Count==1 ? " AUTO_INCREMENT ":"")+" COMMENT '{0}'",
                            description.Replace("'", "''")
                    );
                }
            }
            return null;
        }

        internal override string SetTriggerDescription(string triggerName, string description)
        {
            return string.Format(@"SELECT 
					CONCAT('ALTER TRIGGER ',TRIGGER_NAME,' ',CONCAT(ACTION_TIMING,' ',EVENT_MANIPULATION,' ON ',EVENT_OBJECT_TABLE),
                    ' FOR EACH ROW /**@DESCRIPTION:{1}**/ ',(CASE WHEN ACTION_STATEMENT LIKE '%/**@DESCRIPTION:%' 
                    THEN SUBSTRING(ACTION_STATEMENT,locate('**/',ACTION_STATEMENT,locate('/**@DESCRIPTION:',ACTION_STATEMENT))+3)
                    ELSE ACTION_STATEMENT END))
					from information_schema.TRIGGERS 
					WHERE TRIGGER_SCHEMA='{2}' 
                    AND TRIGGER_NAME = '{0}'"
                , new object[]{triggerName, description.Replace("'", "''"),((MySqlConnectionPool)pool).DbName});
        }

        internal override string SetViewDescription(string viewName, string description)
        {
            throw new Exception("Method Not Implemented");
        }

        internal override string SetIndexDescription(string indexName, string description)
        {
            string origName;
            sTable tbl = pool.Translator.GetTableForIndex(indexName,out origName).Value;
            string ret = DropTableIndex(tbl.Name, indexName);
            Type type = pool.Mapping[tbl.Name];
            foreach (TableIndex ti in type.GetCustomAttributes(typeof(TableIndex), false))
            {
                if (ti.Name == origName)
                {
                    List<string> tfields = new List<string>();
                    foreach (string str in ti.Fields)
                    {
                        sTableField[] flds = tbl[str];
                        if (flds.Length == 0)
                            tfields.Add(str);
                        else
                        {
                            foreach (sTableField f in flds)
                                tfields.Add(f.Name);
                        }
                    }
                    ret += CreateTableIndex(tbl.Name, tfields.ToArray(), indexName, ti.Unique, ti.Ascending) + " COMMENT '" + description.Replace("'", "''");
                    break;
                }
            }
            return ret;
        }
        #endregion

        protected override string _GenerateAutogenIDQuery(sTable tbl, ref List<IDbDataParameter> parameters)
        {
            parameters[parameters.Count - 1].Direction = ParameterDirection.Output;
            string ret = "; SET "+CreateParameterName(tbl.AutoGenField)+" = (SELECT MAX(" + tbl.AutoGenField + ") FROM " + tbl.Name;
            if (tbl.PrimaryKeyFields.Length > 1)
            {
                ret += " WHERE ";
                foreach (string prop in tbl.PrimaryKeyProperties)
                {
                    foreach (sTableField fld in tbl[prop])
                    {
                        if (!Utility.StringsEqual(fld.Name,tbl.AutoGenField))
                            ret += fld.Name + " = " + CreateParameterName(fld.Name) + " AND ";
                    }
                }
                if (ret.EndsWith("WHERE "))
                    ret = ret.Substring(0, ret.Length - 6);
                return (ret.EndsWith("AND ") ? ret.Substring(0, ret.Length - 4) : ret)+")";
            }
            return ret+")";
        }
    }
}
