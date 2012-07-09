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
using Org.Reddragonit.Dbpro.Structure.Mapping;
using FieldType = Org.Reddragonit.Dbpro.Structure.Attributes.FieldType;
using VersionTypes = Org.Reddragonit.Dbpro.Structure.Attributes.VersionField.VersionTypes;
using System.Text;
using System.Data;
using System.Reflection;

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
		
		internal override void GetAddAutogen(ExtractedTableMap map,ConnectionPool pool, out List<IdentityField> identities, out List<Generator> generators, out List<Trigger> triggers)
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
				triggers.Add(new Trigger(pool.CorrectName(map.TableName+"_"+field.FieldName+"_GEN"),"FOR "+map.TableName+" ACTIVE BEFORE INSERT POSITION 0",
				                         "AS \n" +
				                         "BEGIN \n" +
				                         "    NEW." + field.FieldName + " = CURRENT_TIMESTAMP;\n" +
				                         "END"));
			}else if (field.Type.ToUpper().Contains("INT"))
			{
				if (map.PrimaryKeys.Count==1)
				{
					generators.Add(new Generator(pool.CorrectName("GEN_"+map.TableName+"_"+field.FieldName)));
					triggers.Add(new Trigger(pool.CorrectName(map.TableName+"_"+field.FieldName+"_GEN"),"FOR "+map.TableName+" ACTIVE BEFORE INSERT POSITION 0",
					                         "AS \n" +
					                         "BEGIN \n" +
					                         "    NEW." + field.FieldName + " = GEN_ID("+pool.CorrectName("GEN_" + map.TableName + "_" + field.FieldName) + ",1);\n" +
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
					code+="\tTHEN "+field.FieldName+" = -1;\n";
					code+="NEW."+field.FieldName+" = "+field.FieldName+"+1;\n";
					code+="END";
					triggers.Add(new Trigger(pool.CorrectName(map.TableName+"_"+field.FieldName+"_GEN"),"FOR "+map.TableName+" ACTIVE BEFORE INSERT POSITION 0",code));
				}
            }else if (field.Type.ToUpper().Contains("VARCHAR"))
            {
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
                triggers.Add(new Trigger(pool.CorrectName(map.TableName + "_" + field.FieldName + "_GEN"), "FOR " + map.TableName + " ACTIVE BEFORE INSERT POSITION 0", code));
            }
            else
                throw new Exception("Unable to create autogenerator for non date or digit type.");
		}

		internal override void GetDropAutogenStrings(ExtractedTableMap map,ConnectionPool pool, out List<IdentityField> identities, out List<Generator> generators, out List<Trigger> triggers)
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
            if (field.Type.ToUpper().Contains("DATE") || field.Type.ToUpper().Contains("TIME") || field.Type.ToUpper().Contains("VARCHAR"))
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
		
		internal override List<Trigger> GetVersionTableTriggers(ExtractedTableMap table,VersionTypes versionType,ConnectionPool pool)
		{
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
			ret.Add(new Trigger(pool.CorrectName(queryBuilder.VersionTableInsertTriggerName(queryBuilder.RemoveVersionName(table.TableName))),
			                    "FOR "+queryBuilder.RemoveVersionName(table.TableName)+" ACTIVE AFTER INSERT POSITION 0",
			                    tmp));
			ret.Add(new Trigger(pool.CorrectName(queryBuilder.VersionTableUpdateTriggerName(table.TableName)),
			                    "FOR "+table.TableName+" ACTIVE AFTER UPDATE POSITION 0",
			                    tmp));
			return ret;
		}

        internal override List<Trigger> GetDeleteParentTrigger(ExtractedTableMap table, ExtractedTableMap parent, ConnectionPool pool)
        {
            List<Trigger> ret = new List<Trigger>();
            string tmp = "AS \nBEGIN\n";
            tmp += "DELETE FROM " + parent.TableName + " WHERE ";
            foreach (ExtractedFieldMap efm in parent.PrimaryKeys)
                tmp += pool.CorrectName(efm.FieldName) + " = old." + pool.CorrectName(efm.FieldName)+" AND ";
            ret.Add(new Trigger(pool.CorrectName(table.TableName + "_" + parent.TableName + "_AUTO_DELETE"),
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
                if (parameterValue.GetType().IsEnum)
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
                        query += queryBuilder.SetGeneratorValue(Pool.CorrectName("GEN_" + Pool._enumTableMaps[ifm.ObjectType]+"_ID"), long.Parse(this[0].ToString())) + "\n";
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
