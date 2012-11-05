/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 16/03/2009
 * Time: 8:46 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using System;
using System.Data;
using System.Collections.Generic;
using Org.Reddragonit.Dbpro.Structure.Attributes;
using System.Reflection;

namespace Org.Reddragonit.Dbpro.Connections.MySql
{
	/// <summary>
	/// Description of MySqlConnection.
	/// </summary>
	public class MySqlConnection : Connection
	{
        internal const string _ASSEMBLY_NAME = "MySql.Data";
        internal const string _PARAMETER_NAME = "MySql.Data.MySqlClient.MySqlParameter";
        private const string _SQL_DB_TYPE_ENUM = "MySql.Data.MySqlClient.MySqlDbType";
        private const string _CONNECTION_TYPE_NAME = "MySql.Data.MySqlClient.MySqlConnection";
        private const string _COMMAND_TYPE_NAME = "MySql.Data.MySqlClient.MySqlCommand";

		
		private QueryBuilder _queryBuilder = null;
		internal override QueryBuilder queryBuilder {
			get {
				if (_queryBuilder==null)
					_queryBuilder=new MySqlQueryBuilder(Pool,this);
				return _queryBuilder;
			}
		}
		
		internal override string DefaultTableString {
			get {
				return "information_schema.COLUMNS";
			}
		}
		
		internal override bool UsesIdentities {
			get { return true; }
		}
		
		public MySqlConnection(ConnectionPool pool,string connectionString,bool Readonly,bool exclusiveLock) :base(pool,connectionString,Readonly,exclusiveLock)
		{
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
            return (IDbDataParameter)Utility.LocateType(_PARAMETER_NAME).GetConstructor(new Type[] { typeof(string), typeof(object) }).Invoke(new object[] { parameterName, parameterValue });
		}
		
		internal override IDbDataParameter CreateParameter(string parameterName, object parameterValue, FieldType type, int fieldLength)
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
			IDbDataParameter ret= CreateParameter(parameterName,parameterValue);
			if (((type==FieldType.CHAR)||(type==FieldType.STRING))
			    &&((fieldLength == -1)||(fieldLength>65350)))
			{
                Type t = Utility.LocateType(_PARAMETER_NAME);
                PropertyInfo pi = t.GetProperty("MySqlDbType", Utility._BINDING_FLAGS);
                pi.SetValue(ret, Enum.Parse(Utility.LocateType(_SQL_DB_TYPE_ENUM), "Text"), new object[] { });
			}
			return ret;
		}
		
		protected override IDbCommand EstablishCommand()
		{
            return (IDbCommand)Utility.LocateType(_COMMAND_TYPE_NAME).GetConstructor(new Type[]{typeof(string),Utility.LocateType(_CONNECTION_TYPE_NAME)}).Invoke(new object[]{"",conn});
		}
		
		protected override IDbConnection EstablishConnection()
		{
			return (IDbConnection)Utility.LocateType(_CONNECTION_TYPE_NAME).GetConstructor(new Type[]{typeof(string)}).Invoke(new object[]{connectionString});
		}

        internal override void GetAddAutogen(ExtractedTableMap map, out System.Collections.Generic.List<IdentityField> identities, out System.Collections.Generic.List<Generator> generators, out System.Collections.Generic.List<Trigger> triggers, out List<StoredProcedure> procedures)
		{
            Type t = Pool.Mapping[map.TableName];
			identities=new List<IdentityField>();
			generators=null;
			triggers=new List<Trigger>();
            procedures = null;
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
				triggers.Add(new Trigger(Pool.Translator.GetInsertTriggerName(t,this),"BEFORE INSERT ON "+map.TableName,
				                         "FOR EACH ROW BEGIN\n SET NEW."+field.FieldName+" = CURRENT_DATE();\n END"));
			}else if (field.Type.ToUpper().Contains("INT"))
			{
				if (map.PrimaryKeys.Count==1)
				{
					identities.Add(new IdentityField(map.TableName,field.FieldName,field.FullFieldType,"0"));
				}else{
					string code = "FOR EACH ROW\nBEGIN\n";
					string queryFields="";
					foreach (ExtractedFieldMap efm in map.PrimaryKeys)
					{
						if (!efm.AutoGen)
							queryFields+=" AND "+efm.FieldName+" = NEW."+efm.FieldName;
					}
					code+="SET NEW."+field.FieldName+" = (SELECT (CASE WHEN MAX("+field.FieldName+") IS NULL THEN 0 ELSE MAX("+field.FieldName+")+1 END) FROM "+map.TableName+" WHERE ";
					code+=queryFields.Substring(4)+");\n";
					code+="END";
					triggers.Add(new Trigger(Pool.Translator.GetInsertTriggerName(t,this), "BEFORE INSERT ON " + map.TableName, code));
				}
			}else
				throw new Exception("Unable to create autogenerator for non date or digit type.");
		}
		
		internal override void GetDropAutogenStrings(ExtractedTableMap map, out System.Collections.Generic.List<IdentityField> identities, out System.Collections.Generic.List<Generator> generators, out System.Collections.Generic.List<Trigger> triggers)
		{
            Type t = Pool.Mapping[map.TableName];
			identities=new List<IdentityField>();
			triggers = new List<Trigger>();
			generators = null;
			ExtractedFieldMap field = map.PrimaryKeys[0];
			if ((map.PrimaryKeys.Count > 1) && (!field.AutoGen))
			{
				foreach (ExtractedFieldMap efm in map.PrimaryKeys)
				{
					if (efm.AutoGen)
					{
						field = efm;
						break;
					}
				}
			}
			if (field.Type.ToUpper().Contains("DATE") || field.Type.ToUpper().Contains("TIME"))
			{
				triggers.Add(new Trigger(Pool.Translator.GetInsertTriggerName(t,this), "", ""));
			}
			else if (field.Type.ToUpper().Contains("INT"))
			{
				if (map.PrimaryKeys.Count>1)
					triggers.Add(new Trigger(Pool.Translator.GetInsertTriggerName(t,this), "", ""));
				else
					identities.Add(new IdentityField(map.TableName, field.FieldName, field.FullFieldType,""));
			}
			else
			{
				throw new Exception("Unable to create autogenerator for non date or digit type.");
			}
		}
		
		internal override List<Trigger> GetVersionTableTriggers(ExtractedTableMap table, VersionField.VersionTypes versionType)
		{
			List<Trigger> ret = new List<Trigger>();
			string tmp = "FOR EACH ROW\nBEGIN\n";
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
			tmp += ");\nEND\n\n";
            Type t = Pool.Mapping.GetTypeForVersionTable(table.TableName);
			ret.Add(new Trigger(Pool.Translator.GetVersionInsertTriggerName(t,this),
			                    "AFTER INSERT ON " + Pool.Mapping[t].Name,
			                    tmp));
			ret.Add(new Trigger(Pool.Translator.GetVersionUpdateTriggerName(t,this),
			                    "AFTER UPDATE ON " + Pool.Mapping[t].Name,
			                    tmp));
			return ret;
		}

        internal override List<Trigger> GetDeleteParentTrigger(ExtractedTableMap table, ExtractedTableMap parent)
        {
            List<Trigger> ret = new List<Trigger>();
            string tmp = "FOR EACH ROW\nBEGIN\n";
            tmp += "\tDELETE FROM " + parent.TableName + " WHERE ";
            foreach (ExtractedFieldMap efm in parent.PrimaryKeys)
                tmp += efm.FieldName + " = old." + efm.FieldName + " AND ";
            ret.Add(new Trigger(Pool.Translator.GetDeleteParentTriggerName(Pool.Mapping[table.TableName],this),
                "AFTER DELETE ON " + parent.TableName,
                tmp.Substring(0, tmp.Length - 4) + ";"));
            return ret;
        }

        internal static string _TranslateFieldType(Org.Reddragonit.Dbpro.Structure.Attributes.FieldType type, int fieldLength) {
            string ret = null;
            switch (type)
            {
                case FieldType.BOOLEAN:
                    ret = "BIT";
                    break;
                case FieldType.BYTE:
                    if (fieldLength == 1)
                        ret = "TINYINT";
                    else if ((fieldLength == -1) || (fieldLength > 6535))
                        ret = "BLOB";
                    else
                        ret = "VARCHAR(" + fieldLength.ToString() + ") CHARACTER SET BINARY";
                    break;
                case FieldType.CHAR:
                    if ((fieldLength == -1) || (fieldLength > 6535))
                        ret = "TEXT";
                    else
                        ret = "CHAR(" + fieldLength.ToString() + ")";
                    break;
                case FieldType.DATE:
                    ret = "DATE";
                    break;
                case FieldType.DATETIME:
                    ret = "DATETIME";
                    break;
                case FieldType.TIME:
                    ret = "TIME";
                    break;
                case FieldType.DECIMAL:
                    ret = "DECIMAL(18,9)";
                    break;
                case FieldType.DOUBLE:
                    ret = "DOUBLE";
                    break;
                case FieldType.FLOAT:
                    ret = "FLOAT";
                    break;
                case FieldType.IMAGE:
                    ret = "BLOB";
                    break;
                case FieldType.INTEGER:
                case FieldType.ENUM:
                    ret = "INTEGER";
                    break;
                case FieldType.UNSIGNED_INTEGER:
                    ret = "CHAR(4)";
                    break;
                case FieldType.LONG:
                    ret = "BIGINT";
                    break;
                case FieldType.UNSIGNED_LONG:
                    ret = "CHAR(8)";
                    break;
                case FieldType.MONEY:
                    ret = "DECIMAL(18,4)";
                    break;
                case FieldType.SHORT:
                    ret = "SMALLINT";
                    break;
                case FieldType.UNSIGNED_SHORT:
                    ret = "CHAR(2)";
                    break;
                case FieldType.STRING:
                    if ((fieldLength == -1) || (fieldLength > 65535))
                        ret = "TEXT";
                    else
                        ret = "VARCHAR(" + fieldLength.ToString() + ")";
                    break;
            }
            return ret;
        }
		
		internal override string TranslateFieldType(Org.Reddragonit.Dbpro.Structure.Attributes.FieldType type, int fieldLength)
		{
            return _TranslateFieldType(type, fieldLength);
		}

        internal override void DisableAutogens()
        {
            string query = "";
            this.ExecuteQuery(queryBuilder.SelectTriggers());
            while (this.Read())
            {
                query += "DISABLE TRIGGER "+this[0].ToString()+this[1].ToString().Substring(this[1].ToString().IndexOf(" ON "))+";\n";
            }
            this.Close();
            Logger.LogLine("Disabling all autogens in mysql database with query: " + query);
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
                query += "ENABLE TRIGGER " + this[0].ToString() + this[1].ToString().Substring(this[1].ToString().IndexOf(" ON ")) + ";\n";
            }
            this.Close();
            List<IdentityField> identities = new List<IdentityField>();
            this.ExecuteQuery(this.queryBuilder.SelectIdentities());
            while (this.Read())
            {
                identities.Add(new IdentityField((string)this[0], (string)this[1], (string)this[2], (string)this[3]));
            }
            this.Close();
            foreach (IdentityField id in identities)
            {
                this.ExecuteQuery("SELECT ISNULL(MAX(" + id.FieldName + "),0)+1 FROM " + id.TableName);
                this.Read();
                query += queryBuilder.SetIdentityFieldValue(new IdentityField(id.TableName, id.FieldName, id.FieldType, this[0].ToString())) + "\n";
                this.Close();
            }
            Logger.LogLine("Resetting and enabling all autogens in mysql database with query: " + query);
            foreach (string str in query.Split('\n'))
            {
                if (str.Trim().Length > 0)
                    this.ExecuteNonQuery(str);
            }
        }
	}
}
