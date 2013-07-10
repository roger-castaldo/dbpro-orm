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
using System.Data;
using Org.Reddragonit.Dbpro.Structure.Attributes;
using System.Reflection;
using Org.Reddragonit.Dbpro.Connections.PoolComponents;

namespace Org.Reddragonit.Dbpro.Connections.PgSql
{
	/// <summary>
	/// Description of PgSqlConnection.
	/// </summary>
	public class PgSqlConnection : Connection
	{
        internal const string _ASSEMBLY_NAME = "Npgsql";
        private const string _CONNECTION_TYPE_NAME = "Npgsql.NpgsqlConnection";
        private const string _COMMAND_TYPE_NAME = "Npgsql.NpgsqlCommand";

		public PgSqlConnection(ConnectionPool pool,string connectionString,bool Readonly,bool exclusiveLock) : base(pool,connectionString,Readonly,exclusiveLock)
		{
            if (Utility.LocateType(_CONNECTION_TYPE_NAME) == null)
                Assembly.Load(_ASSEMBLY_NAME);
		}
		
		internal override string DefaultTableString {
			get {
				return "information_schema.tables";
			}
		}

        internal override IDbTransaction EstablishExclusiveTransaction()
        {
            return conn.BeginTransaction(IsolationLevel.Serializable);
        }
		
		protected override IDbCommand EstablishCommand()
		{
            return (IDbCommand)Utility.LocateType(_COMMAND_TYPE_NAME).GetConstructor(new Type[] { typeof(string), Utility.LocateType(_CONNECTION_TYPE_NAME) }).Invoke(new object[] { "", conn });
		}
		
		protected override IDbConnection EstablishConnection()
		{
            return (IDbConnection)Utility.LocateType(_CONNECTION_TYPE_NAME).GetConstructor(new Type[] { typeof(String) }).Invoke(new object[] { connectionString });
		}

        internal override void GetAddAutogen(ExtractedTableMap map, out List<IdentityField> identities, out List<Generator> generators, out List<Trigger> triggers, out List<StoredProcedure> procedures)
		{
            Type t = Pool.Mapping[map.TableName];
            PropertyInfo pi = Pool.Mapping[map.TableName, map.PrimaryKeys[0].FieldName];
            bool imediate = t == null;
            if (imediate)
                t = Pool.Mapping.GetTypeForIntermediateTable(map.TableName, out pi);
			identities=null;
			generators = new List<Generator>();
			triggers=new List<Trigger>();
            procedures = new List<StoredProcedure>();
			ExtractedFieldMap field = map.PrimaryKeys[0];
			if ((map.PrimaryKeys.Count>1)&&(!field.AutoGen))
			{
				foreach (ExtractedFieldMap efm in map.PrimaryKeys)
				{
					if (efm.AutoGen)
					{
						field=efm;
						break;
					}
				}
			}
			if (field.Type.ToUpper().Contains("DATE")||field.Type.ToUpper().Contains("TIME"))
			{
                triggers.Add(new Trigger((imediate ? Pool.Translator.GetInsertIntermediateTriggerName(t, pi, this) : Pool.Translator.GetInsertTriggerName(t, this)), "BEFORE INSERT ON " + map.TableName + " FOR EACH ROW",
				                         "    NEW." + field.FieldName + " := CURRENT_TIMESTAMP;\n"));
			}else if (field.Type.ToUpper().Contains("INT"))
			{
				if (map.PrimaryKeys.Count==1)
				{
                    Generator gen = new Generator((imediate ? Pool.Translator.GetIntermediateGeneratorName(t, pi, this) : Pool.Translator.GetGeneratorName(t, pi, this)));
					gen.Value=1;
					generators.Add(gen);
                    triggers.Add(new Trigger((imediate ? Pool.Translator.GetInsertIntermediateTriggerName(t, pi, this) : Pool.Translator.GetInsertTriggerName(t, this)), "BEFORE INSERT ON " + map.TableName + " FOR EACH ROW",
					                         "    NEW." + field.FieldName + " := nextval('"+gen.Name + "');\n"));
				}else{
					string code="";
					string queryFields="";
					foreach (ExtractedFieldMap efm in map.PrimaryKeys)
					{
						if (!efm.AutoGen)
						{
							queryFields+=" AND "+efm.FieldName+" = NEW."+efm.FieldName;
						}
					}
					code+="NEW."+field.FieldName+" := (SELECT COALESCE(MAX("+field.FieldName+"),-1)+1 FROM "+map.TableName+" WHERE ";
					code+=queryFields.Substring(4)+");";
                    triggers.Add(new Trigger((imediate ? Pool.Translator.GetInsertIntermediateTriggerName(t, pi, this) : Pool.Translator.GetInsertTriggerName(t, this)), "BEFORE INSERT ON " + map.TableName + " FOR EACH ROW", code));
				}
			}else
				throw new Exception("Unable to create autogenerator for non date or digit type.");
		}
		
		internal override void GetDropAutogenStrings(ExtractedTableMap map, out List<IdentityField> identities, out List<Generator> generators, out List<Trigger> triggers)
		{
            Type t = Pool.Mapping[map.TableName];
            PropertyInfo pi = Pool.Mapping[map.TableName, map.PrimaryKeys[0].FieldName];
            bool imediate = t == null;
            if (imediate)
                t = Pool.Mapping.GetTypeForIntermediateTable(map.TableName, out pi);
			identities=null;
			triggers=new List<Trigger>();
			generators=new List<Generator>();
			ExtractedFieldMap field = map.PrimaryKeys[0];
			if ((map.PrimaryKeys.Count>1)&&(!field.AutoGen))
			{
				foreach (ExtractedFieldMap efm in map.PrimaryKeys)
				{
					if (efm.AutoGen)
					{
						field=efm;
						break;
					}
				}
			}
			if (field.Type.ToUpper().Contains("DATE") || field.Type.ToUpper().Contains("TIME"))
			{
                triggers.Add(new Trigger((imediate ? Pool.Translator.GetInsertIntermediateTriggerName(t, pi, this) : Pool.Translator.GetInsertTriggerName(t, this)), "", ""));
			}
			else if (field.Type.ToUpper().Contains("INT"))
			{
                triggers.Add(new Trigger((imediate ? Pool.Translator.GetInsertIntermediateTriggerName(t, pi, this) : Pool.Translator.GetInsertTriggerName(t, this)), "", ""));
                generators.Add(new Generator((imediate ? Pool.Translator.GetIntermediateGeneratorName(t, pi, this) : Pool.Translator.GetGeneratorName(t, pi, this))));
			}
			else
			{
				throw new Exception("Unable to create autogenerator for non date or digit type.");
			}
		}
		
		internal override List<Trigger> GetVersionTableTriggers(ExtractedTableMap table, VersionField.VersionTypes versionType)
		{
            Type t = Pool.Mapping.GetTypeForVersionTable(table.TableName);
			List<Trigger> ret = new List<Trigger>();
			string tmp = "";
			tmp += "\tINSERT INTO " + table.TableName + "(" + table.Fields[0].FieldName;
			for (int x = 1; x < table.Fields.Count; x++)
			{
				ExtractedFieldMap efm = table.Fields[x];
				tmp += "," + efm.FieldName;
			}
			tmp += ") VALUES(";
			if (table.Fields[0].Type=="DATETIME")
				tmp+="CURRENT_DATE()";
			else
				tmp+="0";
			for (int x = 1; x < table.Fields.Count; x++)
			{
				ExtractedFieldMap efm = table.Fields[x];
				tmp += ",NEW." + efm.FieldName;
			}
			tmp += ");";
			ret.Add(new Trigger(Pool.Translator.GetVersionInsertTriggerName(t,this),
			                    "AFTER INSERT ON " + Pool.Mapping[t].Name+ " FOR EACH ROW",
			                    tmp));
			ret.Add(new Trigger(Pool.Translator.GetVersionUpdateTriggerName(t,this),
			                    "AFTER UPDATE ON " + Pool.Mapping[t].Name+" FOR EACH ROW",
			                    tmp));
			return ret;
		}

        internal override List<Trigger> GetDeleteParentTrigger(ExtractedTableMap table, ExtractedTableMap parent)
        {
            List<Trigger> ret = new List<Trigger>();
            string tmp = "\tDELETE FROM " + parent.TableName + " WHERE ";
            foreach (ExtractedFieldMap efm in parent.PrimaryKeys)
                tmp += efm.FieldName + " =OLD." + efm.FieldName + " AND ";
            ret.Add(new Trigger(Pool.Translator.GetDeleteParentTriggerName(Pool.Mapping[table.TableName],this),
                "AFTER DELETE ON " + parent.TableName + " FOR EACH ROW",
                tmp.Substring(0, tmp.Length - 4) + ";"));
            return ret;
        }
		
		internal override string TranslateFieldType(Org.Reddragonit.Dbpro.Structure.Attributes.FieldType type, int fieldLength)
		{
			string ret=null;
			switch(type)
			{
				case FieldType.BOOLEAN:
					ret="BOOLEAN";
					break;
				case FieldType.BYTE:
					if (fieldLength==1)
						ret="NUMERIC(3,0)";
					else
						ret="OID";
					break;
				case FieldType.CHAR:
					ret="CHARACTER("+fieldLength.ToString()+")";
					break;
				case FieldType.DATE:
					ret="DATE";
					break;
				case FieldType.DATETIME:
					ret="DATETIME";
					break;
				case FieldType.TIME:
					ret="TIME";
					break;
				case FieldType.DECIMAL:
					ret="DECIMAL(18,9)";
					break;
				case FieldType.DOUBLE:
					ret="DOUBLE PRECISION";
					break;
				case FieldType.FLOAT:
					ret="FLOAT";
					break;
				case FieldType.IMAGE:
					ret="OID";
					break;
				case FieldType.INTEGER:
				case FieldType.ENUM:
					ret="INTEGER";
					break;
                case FieldType.UNSIGNED_INTEGER:
                    ret = "CHAR(4)";
                    break;
				case FieldType.LONG:
					ret="BIGINT";
					break;
                case FieldType.UNSIGNED_LONG:
                    ret = "CHAR(8)";
                    break;
				case FieldType.MONEY:
					ret="DECIMAL(18,4)";
					break;
				case FieldType.SHORT:
					ret = "SMALLINT";
					break;
                case FieldType.UNSIGNED_SHORT:
                    ret = "CHAR(2)";
                    break;
				case FieldType.STRING:
					ret="CHARACTER VARYING("+fieldLength.ToString()+")";
					break;
			}
			return ret;
		}
		
		internal override bool UsesGenerators {
			get { return true; }
		}
		
		internal override bool UsesIdentities {
			get { return false; }
		}

        internal override void DisableAutogens()
        {
            string query = "";
            this.ExecuteQuery(queryBuilder.SelectTriggers());
            while (this.Read())
            {
                string tbl = this[1].ToString().Substring(this[1].ToString().IndexOf(" ON ") + 4);
                tbl = tbl.Substring(0,tbl.IndexOf(" FOR EACH ROW"));
                query += "ALTER TABLE " + tbl + " DISABLE TRIGGER " + this[0].ToString() + ";\n";
            }
            this.Close();
            Logger.LogLine("Disabling all autogens in postgresql database with query: " + query);
            foreach (string str in query.Split('\n'))
            {
                if (str.Trim().Length > 0)
                    this.ExecuteNonQuery(str);
            }
        }

        internal override void EnableAndResetAutogens()
        {
            string query = "";
            this.ExecuteQuery(queryBuilder.SelectTriggers());
            while (this.Read())
            {
                string tbl = this[1].ToString().Substring(this[1].ToString().IndexOf(" ON ") + 4);
                tbl = tbl.Substring(0, tbl.IndexOf(" FOR EACH ROW"));
                query += "ALTER TABLE " + tbl + " ENABLE TRIGGER " + this[0].ToString() + ";\n";
            }
            this.Close();
            foreach (Type t in Pool.Mapping.Types)
            {
                sTable tm = Pool.Mapping[t];
                if (tm.AutoGenField!=null)
                {
                    if ((tm.PrimaryKeyFields.Length == 1) && (tm[tm.AutoGenProperty][0].Type == FieldType.INTEGER || tm[tm.AutoGenProperty][0].Type == FieldType.LONG || tm[tm.AutoGenProperty][0].Type == FieldType.SHORT))
                    {
                        this.ExecuteQuery("SELECT (CASE WHEN MAX(" + tm.AutoGenField + ") IS NULL THEN 0 ELSE MAX(" + tm.AutoGenField + ") END)+1 FROM " + tm.Name);
                        this.Read();
                        query += queryBuilder.SetGeneratorValue(Pool.Translator.GetGeneratorName(t,Pool.Mapping[tm.Name,tm.AutoGenProperty],this), long.Parse(this[0].ToString())) + "\n";
                        this.Close();
                    }
                }
                foreach(string prop in tm.Properties)
                {
                    PropertyInfo pi = t.GetProperty(prop, Utility._BINDING_FLAGS);
                    if (Utility.IsEnum(pi.PropertyType))
                    {
                        this.ExecuteQuery("SELECT (CASE WHEN MAX(ID) IS NULL THEN 0 ELSE MAX(ID) END)+1 FROM " + Pool.Enums[(pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType)]);
                        this.Read();
                        query += queryBuilder.SetGeneratorValue(Pool.Translator.GetEnumGeneratorName((pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType),this), long.Parse(this[0].ToString())) + "\n";
                        this.Close();
                    }
                }
            }
            Logger.LogLine("Resetting and enabling all autogens in postgresql database with query: " + query);
            foreach (string str in query.Split('\n'))
            {
                if (str.Trim().Length > 0)
                    this.ExecuteNonQuery(str);
            }
        }
	}
}
