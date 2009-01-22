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
					_builder=new FBQueryBuilder(Pool);
				return _builder;
			}
		}
		
		internal override bool UsesGenerators {
			get { return true; }
		}
		
		public FBConnection(ConnectionPool pool,string connectionString) : base(pool,connectionString)
		{

		}
		
		internal override void GetAddAutogen(string tableName,ExtractedFieldMap field,ConnectionPool pool, out List<string> queryStrings, out List<Generator> generators, out List<Trigger> triggers)
		{
			queryStrings=null;
			generators = new List<Generator>();
			triggers=new List<Trigger>();
			if (field.Type.ToUpper().Contains("DATE")||field.Type.ToUpper().Contains("TIME"))
			{
				triggers.Add(new Trigger(pool.CorrectName(tableName+"_"+field.FieldName+"_GEN"),"FOR "+tableName+" ACTIVE BEFORE INSERT POSITION 0",
				                         "AS \n" +
				                         "BEGIN \n" +
				                         "    NEW." + field.FieldName + " = CURRENT_TIMESTAMP;\n" +
				                         "END"));
			}else if (field.Type.ToUpper().Contains("INT"))
			{
				generators.Add(new Generator(pool.CorrectName("GEN_"+tableName+"_"+field.FieldName)));
				triggers.Add(new Trigger(pool.CorrectName(tableName+"_"+field.FieldName+"_GEN"),"FOR "+tableName+" ACTIVE BEFORE INSERT POSITION 0",
				                         "AS \n" +
				                         "BEGIN \n" +
				                         "    NEW." + field.FieldName + " = GEN_ID("+pool.CorrectName("GEN_" + tableName + "_" + field.FieldName) + ",1);\n" +
				                         "END"));
			}else
				throw new Exception("Unable to create autogenerator for non date or digit type.");
		}

		internal override void GetDropAutogenStrings(string tableName, ExtractedFieldMap field,ConnectionPool pool, out List<string> queryStrings, out List<Generator> generators, out List<Trigger> triggers)
		{
			queryStrings=null;
			triggers=new List<Trigger>();
			generators=new List<Generator>();
			if (field.Type.ToUpper().Contains("DATE") || field.Type.ToUpper().Contains("TIME"))
			{
				triggers.Add(new Trigger(pool.CorrectName(tableName+"_"+field.FieldName+"_GEN"),"",""));
			}
			else if (field.Type.ToUpper().Contains("INT"))
			{
				triggers.Add(new Trigger(pool.CorrectName(tableName+"_"+field.FieldName+"_GEN"),"",""));
				generators.Add(new Generator(pool.CorrectName("GEN_"+tableName+"_"+field.FieldName)));
			}
			else
			{
				throw new Exception("Unable to create autogenerator for non date or digit type.");
			}
		}
		
		internal override List<Trigger> GetVersionTableTriggers(ExtractedTableMap table,VersionTypes versionType,ConnectionPool pool)
		{
			List<Trigger> ret = new List<Trigger>();
			string tmp = "AS \n"+
				"DECLARE VARIABLE "+table.Fields[0].FieldName+" ";
			switch(versionType)
			{
				case VersionTypes.NUMBER:
					tmp+="BIGINT;\n";
					break;
				case VersionTypes.DATESTAMP:
					tmp+="TIMESTAMP;\n";
					break;
			}
			for (int x=1;x<table.Fields.Count;x++)
			{
				ExtractedFieldMap efm = table.Fields[x];
				if (efm.Type.ToUpper().Contains("CHAR"))
				{
					tmp+= "DECLARE VARIABLE "+efm.FieldName+" "+efm.Type+"("+efm.Size.ToString()+");\n";
				}else{
					tmp+="DECLARE VARIABLE "+efm.FieldName+" "+efm.Type+";\n";
				}
			}
			tmp+="BEGIN\n";
			for (int x=1;x<table.Fields.Count;x++)
			{
				ExtractedFieldMap efm = table.Fields[x];
				tmp+="\t"+efm.FieldName+" = new."+efm.FieldName+";\n";
			}
			switch(versionType)
			{
				case VersionTypes.NUMBER:
					tmp+="\t"+table.Fields[0].FieldName+" = 0;\n";
					break;
				case VersionTypes.DATESTAMP:
					tmp+="\t"+table.Fields[0].FieldName+" = CURRENT_TIMESTAMP;\n";
					break;
			}
			tmp+="\tINSERT INTO "+table.TableName+"("+table.Fields[0].FieldName;
			for (int x=1;x<table.Fields.Count;x++)
			{
				ExtractedFieldMap efm = table.Fields[x];
				tmp+=","+efm.FieldName;
			}
			tmp+=") VALUES(:"+table.Fields[0].FieldName ;
			for (int x=1;x<table.Fields.Count;x++)
			{
				ExtractedFieldMap efm = table.Fields[x];
				tmp+=",:"+efm.FieldName;
			}
			tmp+=");\nEND\n\n";
			ret.Add(new Trigger(pool.CorrectName(queryBuilder.VersionTableInsertTriggerName(table.TableName)),
			                    "FOR "+table.TableName+" ACTIVE AFTER INSERT POSITION 0",
			                    tmp));
			tmp = tmp.Replace("AFTER INSERT","BEFORE UPDATE").Replace("_INSERT","_UPDATE");
			switch(versionType)
			{
				case VersionTypes.NUMBER:
					string maxQuery = "SELECT (MAX("+table.Fields[0].FieldName+")+1) as mid FROM "+table.TableName+"WHERE ";
					for (int x=1;x<table.Fields.Count;x++)
					{
						ExtractedFieldMap efm = table.Fields[x];
						if (efm.PrimaryKey)
						{
							maxQuery+=efm.FieldName+" = :"+efm.FieldName+" AND ";
						}
					}
					if (maxQuery.EndsWith(" WHERE "))
					{
						maxQuery=maxQuery.Substring(0,maxQuery.Length-7);
					}else
					{
						maxQuery=maxQuery.Substring(0,maxQuery.Length-4);
					}
					tmp.Replace("\t"+table.Fields[0].FieldName+" = 0;\n","\t"+table.Fields[0].FieldName +" = ("+maxQuery+");\n");
					break;
			}
			ret.Add(new Trigger(pool.CorrectName(queryBuilder.VersionTableUpdateTriggerName(table.TableName)),
			                    "FOR "+table.TableName+" ACTIVE AFTER UPDATE POSITION 0",
			                    tmp));
			return ret;
		}
		
		internal override System.Data.IDbDataParameter CreateParameter(string parameterName, object parameterValue)
		{
			if (parameterValue is bool)
			{
				if ((bool)parameterValue)
					parameterValue=1;
				else
					parameterValue=0;
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
					ret="DOUBLE";
					break;
				case FieldType.FLOAT:
					ret="FLOAT";
					break;
				case FieldType.IMAGE:
					ret="BLOB";
					break;
				case FieldType.INTEGER:
					ret="INTEGER";
					break;
				case FieldType.LONG:
					ret="BIGINT";
					break;
				case FieldType.MONEY:
					ret="DECIMAL(18,4)";
					break;
				case FieldType.SHORT:
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
	}
}
