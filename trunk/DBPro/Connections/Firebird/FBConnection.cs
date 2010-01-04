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
using FirebirdSql.Data.FirebirdClient;
using Org.Reddragonit.Dbpro.Connections;
using Org.Reddragonit.Dbpro.Structure.Mapping;
using FieldType = Org.Reddragonit.Dbpro.Structure.Attributes.FieldType;
using VersionTypes = Org.Reddragonit.Dbpro.Structure.Attributes.VersionField.VersionTypes;
using System.Text;

namespace Org.Reddragonit.Dbpro.Connections.Firebird
{
	/// <summary>
	/// Description of FBConnection.
	/// </summary>
	public class FBConnection : Connection
	{

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
		
		public FBConnection(ConnectionPool pool,string connectionString) : base(pool,connectionString)
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
		
		internal override System.Data.IDbDataParameter CreateParameter(string parameterName, object parameterValue, FieldType type, int fieldLength)
		{
			return CreateParameter(parameterName,parameterValue);
		}
		
		public override System.Data.IDbDataParameter CreateParameter(string parameterName, object parameterValue)
		{
			if (parameterValue is bool)
			{
				if ((bool)parameterValue)
					parameterValue='T';
				else
					parameterValue='F';
            }
            else if ((parameterValue is uint) || (parameterValue is UInt32))
            {
                parameterValue = System.Text.ASCIIEncoding.ASCII.GetString(System.BitConverter.GetBytes((uint)parameterValue)).ToCharArray();
            }else if ((parameterValue is UInt16)||(parameterValue is ushort)){
                parameterValue = System.Text.ASCIIEncoding.ASCII.GetString(System.BitConverter.GetBytes((ushort)parameterValue)).ToCharArray();
            }
            else if ((parameterValue is ulong) || (parameterValue is Int64)){
                parameterValue = System.Text.ASCIIEncoding.ASCII.GetString(System.BitConverter.GetBytes((ulong)parameterValue)).ToCharArray();
            }
			return new FbParameter(parameterName,parameterValue);
		}
		
		protected override System.Data.IDbCommand EstablishCommand()
		{
			return new FbCommand("",(FbConnection)conn);
		}
		
		protected override System.Data.IDbConnection EstablishConnection()
		{
			return new FbConnection(connectionString);
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
					if ((fieldLength==-1)||(fieldLength>32767))
						ret="BLOB SUB_TYPE TEXT";
					else
						ret="VARCHAR("+fieldLength.ToString()+")";
					break;
			}
			return ret;
		}
	}
}
