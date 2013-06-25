/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 23/03/2008
 * Time: 9:30 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */

using System;
using System.Collections.Generic;
using Org.Reddragonit.Dbpro.Connections;
using FieldType = Org.Reddragonit.Dbpro.Structure.Attributes.FieldType;
using VersionTypes = Org.Reddragonit.Dbpro.Structure.Attributes.VersionField.VersionTypes;
using System.Text;
using System.Data;
using System.Reflection;
using System.Xml;
using System.IO;
using Org.Reddragonit.Dbpro.Connections.PoolComponents;

namespace Org.Reddragonit.Dbpro.Connections.Firebird
{
	/// <summary>
	/// Description of FBConnection.
	/// </summary>
	public class FBConnection : Connection
	{
        internal const string _ASSEMBLY_NAME = "FirebirdSql.Data.FirebirdClient";
        internal const string _PARAMETER_CLASS_NAME = "FirebirdSql.Data.FirebirdClient.FbParameter";
        private const string _COMMAND_CLASS_NAME = "FirebirdSql.Data.FirebirdClient.FbCommand";
        private const string _CONNECTION_CLASS_NAME = "FirebirdSql.Data.FirebirdClient.FbConnection";

		private QueryBuilder _builder;
		internal override QueryBuilder queryBuilder {
			get {
				if (_builder==null)
					_builder=new FBQueryBuilder(Pool,this);
				return _builder;
			}
		}
		
		internal override string DefaultTableString {
			get {
				return "RDB$DATABASE";
			}
		}
		
		internal override bool UsesGenerators {
			get { return true; }
		}

        internal override System.Data.IDbTransaction EstablishExclusiveTransaction()
        {
            return conn.BeginTransaction(IsolationLevel.Serializable);
        }
		
		public FBConnection(ConnectionPool pool,string connectionString,bool Readonly,bool exclusiveLock) : base(pool,connectionString,Readonly,exclusiveLock)
		{
		}
		
		internal override void GetAddAutogen(ExtractedTableMap map, out List<IdentityField> identities, out List<Generator> generators, out List<Trigger> triggers,out List<StoredProcedure> procedures)
		{
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
            Type t = Pool.Mapping[map.TableName];
            PropertyInfo pi = Pool.Mapping[map.TableName, map.PrimaryKeys[0].FieldName];
            bool imediate = t == null;
            if (imediate)
                t = Pool.Mapping.GetTypeForIntermediateTable(map.TableName, out pi);
			if (field.Type.ToUpper().Contains("DATE")||field.Type.ToUpper().Contains("TIME"))
			{
				triggers.Add(new Trigger((imediate ? Pool.Translator.GetInsertIntermediateTriggerName(t,pi,this) : Pool.Translator.GetInsertTriggerName(t,this)),"FOR "+map.TableName+" ACTIVE BEFORE INSERT POSITION 0",
				                         "AS \n" +
				                         "BEGIN \n" +
				                         "    NEW." + field.FieldName + " = CURRENT_TIMESTAMP;\n" +
				                         "END"));
			}else if (field.Type.ToUpper().Contains("INT"))
			{
				if (map.PrimaryKeys.Count==1)
				{
					generators.Add(new Generator((imediate ? Pool.Translator.GetIntermediateGeneratorName(t,pi,this) : Pool.Translator.GetGeneratorName(t,pi,this))));
                    triggers.Add(new Trigger((imediate ? Pool.Translator.GetInsertIntermediateTriggerName(t, pi, this) : Pool.Translator.GetInsertTriggerName(t, this)), "FOR " + map.TableName + " ACTIVE BEFORE INSERT POSITION 0",
					                         "AS \n" +
					                         "BEGIN \n" +
					                         "    NEW." + field.FieldName + " = GEN_ID("+generators[generators.Count-1].Name + ",1);\n" +
					                         "END"));
				}else{
					string code = "AS \n";
					string declares="";
					string sets="";
					string queryFields="";
					foreach (ExtractedFieldMap efm in map.PrimaryKeys)
					{
						declares+="DECLARE VARIABLE "+efm.FieldName+" "+efm.FullFieldType+";\n";
						if (!efm.AutoGen)
						{
							sets+=efm.FieldName+" = new."+efm.FieldName+";\n";
							queryFields+=" AND "+efm.FieldName+" = :"+efm.FieldName;
						}
					}
					code+=declares;
					code+="BEGIN \n";
					code+=sets;
					code+="SELECT MAX("+field.FieldName+") FROM "+map.TableName+" WHERE ";
					code+=queryFields.Substring(4)+" INTO :"+field.FieldName+";\n";
					code+="IF ("+field.FieldName+" is NULL)\n";
					code+="\tTHEN "+field.FieldName+" = 0;\n";
					code+="NEW."+field.FieldName+" = "+field.FieldName+"+1;\n";
					code+="END";
                    triggers.Add(new Trigger((imediate ? Pool.Translator.GetInsertIntermediateTriggerName(t, pi, this) : Pool.Translator.GetInsertTriggerName(t, this)), "FOR " + map.TableName + " ACTIVE BEFORE INSERT POSITION 0", code));
				}
            }else if (field.Type.ToUpper().Contains("VARCHAR"))
            {
                XmlDocument doc = new XmlDocument();
                doc.LoadXml(new StreamReader(Assembly.GetAssembly(typeof(FBConnection)).GetManifestResourceStream("Org.Reddragonit.Dbpro.Connections.Firebird.StringIDProcedures.xml")).ReadToEnd());
                foreach (XmlElement proc in doc.GetElementsByTagName("Procedure"))
                    procedures.Add(new StoredProcedure(proc.ChildNodes[0].InnerText,
                        proc.ChildNodes[1].InnerText,
                        proc.ChildNodes[2].InnerText,
                        proc.ChildNodes[3].InnerText,
                        proc.ChildNodes[4].InnerText));
                string code = "AS \n";
                code += "DECLARE VARIABLE IDVAL VARCHAR(38);\n"+
                    "DECLARE VARIABLE CNT BIGINT;\n";
                code += "BEGIN \n";
                code += "CNT=1;\n";
                code += "WHILE (CNT>0) DO\n";
                code += "BEGIN\n";
                code += "EXECUTE PROCEDURE GENERATE_UNIQUE_ID returning_values IDVAL;\n";
                code += "SELECT COUNT(*) FROM " + map.TableName + " WHERE ";
                code += field.FieldName+" = :IDVAL INTO :CNT;\n";
                code += "END\n";
                code += "NEW." + field.FieldName + " = IDVAL;\n";
                code += "END";
                triggers.Add(new Trigger((imediate ? Pool.Translator.GetInsertIntermediateTriggerName(t, pi, this) : Pool.Translator.GetInsertTriggerName(t, this)), "FOR " + map.TableName + " ACTIVE BEFORE INSERT POSITION 0", code));
            }
            else
                throw new Exception("Unable to create autogenerator for non date or digit type.");
		}

		internal override void GetDropAutogenStrings(ExtractedTableMap map, out List<IdentityField> identities, out List<Generator> generators, out List<Trigger> triggers)
		{
            Type t = Pool.Mapping[map.TableName];
            PropertyInfo pi = Pool.Mapping[map.TableName,map.PrimaryKeys[0].FieldName];
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
            if (field.Type.ToUpper().Contains("DATE") || field.Type.ToUpper().Contains("TIME") || field.Type.ToUpper().Contains("VARCHAR"))
			{
				triggers.Add(new Trigger(Pool.Translator.GetInsertTriggerName(t,this),"",""));
			}
			else if (field.Type.ToUpper().Contains("INT"))
			{
				triggers.Add(new Trigger(Pool.Translator.GetInsertTriggerName(t,this),"",""));
				generators.Add(new Generator((imediate ? Pool.Translator.GetIntermediateGeneratorName(t,pi,this) : Pool.Translator.GetGeneratorName(t,pi,this))));
			}
			else
			{
				throw new Exception("Unable to create autogenerator for non date or digit type.");
			}
		}
		
		internal override List<Trigger> GetVersionTableTriggers(ExtractedTableMap table,VersionTypes versionType)
		{
            Type t = Pool.Mapping.GetTypeForVersionTable(table.TableName);
			List<Trigger> ret = new List<Trigger>();
			string tmp = "AS \n";
			for (int x=1;x<table.Fields.Count;x++)
			{
				ExtractedFieldMap efm = table.Fields[x];
				tmp+="DECLARE VARIABLE "+efm.FieldName+" "+efm.FullFieldType+";\n";
			}
			tmp+="BEGIN\n";
			for (int x=1;x<table.Fields.Count;x++)
			{
				ExtractedFieldMap efm = table.Fields[x];
				tmp+="\t"+efm.FieldName+" = new."+efm.FieldName+";\n";
			}
			tmp+="\tINSERT INTO "+table.TableName+"("+table.Fields[0].FieldName;
			for (int x=1;x<table.Fields.Count;x++)
			{
				ExtractedFieldMap efm = table.Fields[x];
				tmp+=","+efm.FieldName;
			}
			tmp+=") VALUES(null";
			for (int x=1;x<table.Fields.Count;x++)
			{
				ExtractedFieldMap efm = table.Fields[x];
				tmp+=",:"+efm.FieldName;
			}
			tmp+=");\nEND\n\n";
			ret.Add(new Trigger(Pool.Translator.GetVersionInsertTriggerName(t,this),
			                    "FOR "+Pool.Mapping[t].Name+" ACTIVE AFTER INSERT POSITION 0",
			                    tmp));
			ret.Add(new Trigger(Pool.Translator.GetVersionUpdateTriggerName(t,this),
			                    "FOR "+Pool.Mapping[t].Name+" ACTIVE AFTER UPDATE POSITION 0",
			                    tmp));
			return ret;
		}

        internal override List<Trigger> GetDeleteParentTrigger(ExtractedTableMap table, ExtractedTableMap parent)
        {
            List<Trigger> ret = new List<Trigger>();
            string tmp = "AS \nBEGIN\n";
            tmp += "DELETE FROM " + parent.TableName + " WHERE ";
            foreach (ExtractedFieldMap efm in parent.PrimaryKeys)
                tmp += efm.FieldName + " = old." + efm.FieldName+" AND ";
            ret.Add(new Trigger(Pool.Translator.GetDeleteParentTriggerName(Pool.Mapping[table.TableName],this),
                "FOR " + table.TableName + " ACTIVE AFTER DELETE POSITION 0",
                tmp.Substring(0,tmp.Length-4)+";\nEND\n\n"));
            return ret;
        }
		
		internal override System.Data.IDbDataParameter CreateParameter(string parameterName, object parameterValue, FieldType type, int fieldLength)
		{
			return CreateParameter(parameterName,parameterValue);
		}
		
		public override System.Data.IDbDataParameter CreateParameter(string parameterName, object parameterValue)
		{
            if (parameterValue != null)
            {
                if (Utility.IsEnum(parameterValue.GetType()))
                {
                    if (parameterValue != null)
                        parameterValue = Pool.GetEnumID(parameterValue.GetType(), parameterValue.ToString());
                    else
                        parameterValue = (int?)null;
                }
            }
            if (parameterValue is bool)
			{
				if ((bool)parameterValue)
					parameterValue='T';
				else
					parameterValue='F';
            }
            else if ((parameterValue is uint) || (parameterValue is UInt32)){
				parameterValue = BitConverter.ToInt32(BitConverter.GetBytes(uint.Parse(parameterValue.ToString())),0);
            }else if ((parameterValue is UInt16)||(parameterValue is ushort)){
				parameterValue = BitConverter.ToInt16(BitConverter.GetBytes(ushort.Parse(parameterValue.ToString())),0);
            }
            else if ((parameterValue is ulong) || (parameterValue is UInt64)){
				parameterValue = BitConverter.ToInt64(BitConverter.GetBytes(ulong.Parse(parameterValue.ToString())),0);
            }
            return (System.Data.IDbDataParameter)Utility.LocateType(_PARAMETER_CLASS_NAME).GetConstructor(new Type[] { typeof(string), typeof(object) }).Invoke(new object[] { parameterName, parameterValue });
		}

        public override object GetValue(int i)
        {
            if ((reader.GetDataTypeName(i)=="CHAR")&&(reader[i].ToString().Length == 1) && ((reader[i].ToString() == "T") || (reader[i].ToString() == "F")))
                return this.GetBoolean(i);
            return base.GetValue(i);
        }

        public override Type GetFieldType(int i)
        {
            if ((reader.GetDataTypeName(i) == "CHAR") && (reader[i].ToString().Length == 1) && ((reader[i].ToString() == "T") || (reader[i].ToString() == "F")))
                return typeof(bool);
            return base.GetFieldType(i);
        }

        public override bool GetBoolean(int i)
        {
            if ((reader.GetDataTypeName(i) == "CHAR") && (reader[i].ToString().Length == 1) && ((reader[i].ToString() == "T") || (reader[i].ToString() == "F")))
                return reader[i].ToString() == "T";
            return base.GetBoolean(i);
        }
		
		protected override System.Data.IDbCommand EstablishCommand()
		{
            return (System.Data.IDbCommand)Utility.LocateType(_COMMAND_CLASS_NAME).GetConstructor(new Type[] { typeof(string), Utility.LocateType(_CONNECTION_CLASS_NAME) }).Invoke(new object[] { "", conn });
		}
		
		protected override System.Data.IDbConnection EstablishConnection()
		{
            return (IDbConnection)Utility.LocateType(_CONNECTION_CLASS_NAME).GetConstructor(new Type[] { typeof(string) }).Invoke(new object[] { connectionString });
		}
		
		internal override string TranslateFieldType(FieldType type, int fieldLength)
		{
			string ret=null;
			switch(type)
			{
				case FieldType.BOOLEAN:
					ret="CHAR(1)";
					break;
				case FieldType.BYTE:
					if ((fieldLength==-1)||(fieldLength>32767))
						ret="BLOB";
					else
						ret="CHAR("+fieldLength.ToString()+")";
					break;
				case FieldType.CHAR:
					if ((fieldLength==-1)||(fieldLength>32767))
						ret="BLOB SUB_TYPE TEXT";
					else
						ret="CHAR("+fieldLength.ToString()+")";
					break;
				case FieldType.DATE:
				case FieldType.DATETIME:
				case FieldType.TIME:
					ret="TIMESTAMP";
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
					ret="BLOB";
					break;
				case FieldType.INTEGER:
				case FieldType.ENUM:
				case FieldType.UNSIGNED_INTEGER:
					ret="INTEGER";
					break;
				case FieldType.LONG:
				case FieldType.UNSIGNED_LONG:
					ret="BIGINT";
					break;
				case FieldType.MONEY:
					ret="DECIMAL(18,4)";
					break;
				case FieldType.SHORT:
				case FieldType.UNSIGNED_SHORT:
					ret = "SMALLINT";
					break;
				case FieldType.STRING:
					if ((fieldLength==-1)||(fieldLength>32767))
						ret="BLOB SUB_TYPE TEXT";
					else
						ret="VARCHAR("+fieldLength.ToString()+")";
					break;
			}
			return ret;
		}

        internal override void DisableAutogens()
        {
            string query="";
            this.ExecuteQuery(queryBuilder.SelectTriggers());
            while (this.Read())
            {
                query += "ALTER TRIGGER " + this[0].ToString() + " INACTIVE;\n";
            }
            this.Close();
            Logger.LogLine("Disabling all autogens in firebird database with query: " + query);
            foreach (string str in query.Split('\n'))
            {
                if (str.Trim().Length>0)
                    this.ExecuteNonQuery(str);
            }
        }

        internal override void EnableAndResetAutogens()
        {
            string query = "";
            this.ExecuteQuery(queryBuilder.SelectTriggers());
            while (this.Read())
            {
                query += "ALTER TRIGGER " + this[0].ToString() + " ACTIVE;\n";
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
                foreach (string prop in tm.Properties)
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
            Logger.LogLine("Resetting and enabling all autogens in firebird database with query: " + query);
            foreach (string str in query.Split('\n'))
            {
                if (str.Trim().Length > 0)
                    this.ExecuteNonQuery(str);
            }
        }
	}
}
