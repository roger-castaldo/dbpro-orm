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
using Org.Reddragonit.Dbpro.Structure.Mapping;
using System.Reflection;

namespace Org.Reddragonit.Dbpro.Connections.PgSql
{
	/// <summary>
	/// Description of PgSqlConnection.
	/// </summary>
	public class PgSqlConnection : Connection
	{
        internal const string _ASSEMBLY_NAME = "Npgsql";
        internal const string _PARAMETER_TYPE_NAME = "Npgsql.NpgsqlParameter";
        private const string _CONNECTION_TYPE_NAME = "Npgsql.NpgsqlConnection";
        private const string _COMMAND_TYPE_NAME = "Npgsql.NpgsqlCommand";

		public PgSqlConnection(ConnectionPool pool,string connectionString,bool Readonly,bool exclusiveLock) : base(pool,connectionString,Readonly,exclusiveLock)
		{
            if (Utility.LocateType(_PARAMETER_TYPE_NAME) == null)
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
		
		public override IDbDataParameter CreateParameter(string parameterName, object parameterValue)
		{
            if (parameterValue != null)
            {
                if (parameterValue.GetType().IsEnum)
                {
                    if (parameterValue != null)
                        parameterValue = Pool.GetEnumID(parameterValue.GetType(), parameterValue.ToString());
                    else
                        parameterValue = (int?)null;
                }
            }
            if ((parameterValue is uint) || (parameterValue is UInt32))
            {
                parameterValue = System.Text.ASCIIEncoding.ASCII.GetString(System.BitConverter.GetBytes(uint.Parse(parameterValue.ToString()))).ToCharArray();
            }
            else if ((parameterValue is UInt16) || (parameterValue is ushort))
            {
                parameterValue = System.Text.ASCIIEncoding.ASCII.GetString(System.BitConverter.GetBytes(ushort.Parse(parameterValue.ToString()))).ToCharArray();
            }
            else if ((parameterValue is ulong) || (parameterValue is Int64))
            {
                parameterValue = System.Text.ASCIIEncoding.ASCII.GetString(System.BitConverter.GetBytes(ulong.Parse(parameterValue.ToString()))).ToCharArray();
            }
            return (IDbDataParameter)Utility.LocateType(_PARAMETER_TYPE_NAME).GetConstructor(new Type[] { typeof(string), typeof(object) }).Invoke(new object[] { parameterName, parameterValue });
		}
		
		internal override IDbDataParameter CreateParameter(string parameterName, object parameterValue, Org.Reddragonit.Dbpro.Structure.Attributes.FieldType type, int fieldLength)
		{
			return CreateParameter(parameterName,parameterValue);
		}
		
		protected override IDbCommand EstablishCommand()
		{
            return (IDbCommand)Utility.LocateType(_COMMAND_TYPE_NAME).GetConstructor(new Type[] { typeof(string), Utility.LocateType(_CONNECTION_TYPE_NAME) }).Invoke(new object[] { "", conn });
		}
		
		protected override IDbConnection EstablishConnection()
		{
            return (IDbConnection)Utility.LocateType(_CONNECTION_TYPE_NAME).GetConstructor(new Type[] { typeof(String) }).Invoke(new object[] { connectionString });
		}
		
		internal override void GetAddAutogen(ExtractedTableMap map, ConnectionPool pool, out List<IdentityField> identities, out List<Generator> generators, out List<Trigger> triggers)
		{
			identities=null;
			generators = new List<Generator>();
			triggers=new List<Trigger>();
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
				triggers.Add(new Trigger(pool.CorrectName(map.TableName+"_"+field.FieldName+"_GEN"),"BEFORE INSERT ON "+map.TableName+" FOR EACH ROW",
				                         "    NEW." + field.FieldName + " := CURRENT_TIMESTAMP;\n"));
			}else if (field.Type.ToUpper().Contains("INT"))
			{
				if (map.PrimaryKeys.Count==1)
				{
					Generator gen = new Generator(pool.CorrectName("GEN_"+map.TableName+"_"+field.FieldName));
					gen.Value=1;
					generators.Add(gen);
					triggers.Add(new Trigger(pool.CorrectName(map.TableName+"_"+field.FieldName+"_GEN"),"BEFORE INSERT ON "+map.TableName+" FOR EACH ROW",
					                         "    NEW." + field.FieldName + " := nextval('"+pool.CorrectName("GEN_" + map.TableName + "_" + field.FieldName) + "');\n"));
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
					triggers.Add(new Trigger(pool.CorrectName(map.TableName+"_"+field.FieldName+"_GEN"),"BEFORE INSERT ON "+map.TableName+" FOR EACH ROW",code));
				}
			}else
				throw new Exception("Unable to create autogenerator for non date or digit type.");
		}
		
		internal override void GetDropAutogenStrings(ExtractedTableMap map, ConnectionPool pool, out List<IdentityField> identities, out List<Generator> generators, out List<Trigger> triggers)
		{
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
				triggers.Add(new Trigger(pool.CorrectName(map.TableName+"_"+field.FieldName+"_GEN"),"",""));
			}
			else if (field.Type.ToUpper().Contains("INT"))
			{
				triggers.Add(new Trigger(pool.CorrectName(map.TableName+"_"+field.FieldName+"_GEN"),"",""));
				generators.Add(new Generator(pool.CorrectName("GEN_"+map.TableName+"_"+field.FieldName)));
			}
			else
			{
				throw new Exception("Unable to create autogenerator for non date or digit type.");
			}
		}
		
		internal override List<Trigger> GetVersionTableTriggers(ExtractedTableMap table, VersionField.VersionTypes versionType, ConnectionPool pool)
		{
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
			ret.Add(new Trigger(pool.CorrectName(queryBuilder.VersionTableInsertTriggerName(queryBuilder.RemoveVersionName(table.TableName))),
			                    "AFTER INSERT ON " + queryBuilder.RemoveVersionName(table.TableName)+ " FOR EACH ROW",
			                    tmp));
			ret.Add(new Trigger(pool.CorrectName(queryBuilder.VersionTableUpdateTriggerName(table.TableName)),
			                    "AFTER UPDATE ON " + table.TableName+" FOR EACH ROW",
			                    tmp));
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
		
		private PgSqlQueryBuilder _builder=null;
		internal override QueryBuilder queryBuilder {
			get {
				if (_builder==null)
					_builder=new PgSqlQueryBuilder(Pool,this);
				return _builder;
			}
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
            foreach (Type t in ClassMapper.TableTypesForConnection(Pool.ConnectionName))
            {
                TableMap tm = ClassMapper.GetTableMap(t);
                if (tm.ContainsAutogenField)
                {
                    if ((tm.PrimaryKeys.Count == 1) && (tm.PrimaryKeys[0].FieldType == FieldType.INTEGER || tm.PrimaryKeys[0].FieldType == FieldType.LONG || tm.PrimaryKeys[0].FieldType == FieldType.SHORT))
                    {
                        this.ExecuteQuery("SELECT (CASE WHEN MAX(" + Pool.CorrectName(tm.PrimaryKeys[0].FieldName) + ") IS NULL THEN 0 ELSE MAX(" + Pool.CorrectName(tm.PrimaryKeys[0].FieldName) + ") END)+1 FROM " + Pool.CorrectName(tm.Name));
                        this.Read();
                        query += queryBuilder.SetGeneratorValue(Pool.CorrectName("GEN_" + tm.Name + "_" + tm.PrimaryKeys[0].FieldName), long.Parse(this[0].ToString())) + "\n";
                        this.Close();
                    }
                }
                foreach (InternalFieldMap ifm in tm.Fields)
                {
                    if (ifm.FieldType == FieldType.ENUM)
                    {
                        this.ExecuteQuery("SELECT (CASE WHEN MAX(ID) IS NULL THEN 0 ELSE MAX(ID) END)+1 FROM " + Pool._enumTableMaps[ifm.ObjectType]);
                        this.Read();
                        query += queryBuilder.SetGeneratorValue(Pool.CorrectName("GEN_" + Pool._enumTableMaps[ifm.ObjectType] + "_ID"), long.Parse(this[0].ToString())) + "\n";
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
