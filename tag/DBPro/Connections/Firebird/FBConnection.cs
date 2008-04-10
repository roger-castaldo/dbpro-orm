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
using FieldType = Org.Reddragonit.Dbpro.Structure.Attributes.Field.FieldType;

namespace Org.Reddragonit.Dbpro.Connections.Firebird
{
	/// <summary>
	/// Description of FBConnection.
	/// </summary>
	public class FBConnection : Connection
	{
		
		public FBConnection(ConnectionPool pool,string connectionString) : base(pool,connectionString)
		{
			
		}
		
		protected override System.Data.IDbDataParameter CreateParameter(string parameterName, object parameterValue)
		{
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
		
		protected override string TranslateFieldType(FieldType type, int fieldLength)
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
					ret="DECIMAL";
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
		
		protected override List<string> ConstructCreateStrings(Org.Reddragonit.Dbpro.Structure.Table table)
		{
			List<string> ret = new List<string>();
			TableMap t = ClassMapper.GetTableMap(table.GetType());
			string tmp="CREATE TABLE "+t.Name+"(\n";
			foreach (FieldMap f in t.Fields)
			{
				InternalFieldMap ifm = (InternalFieldMap)f;
				tmp+="\t"+ifm.FieldName+" "+TranslateFieldType(ifm.FieldType,ifm.FieldLength);
				if (!ifm.Nullable)
				{
					tmp+=" NOT NULL";
				}
				tmp+=",\n";
			}
			if (t.PrimaryKeys.Count>0)
			{
				tmp+="\tPRIMARY KEY(";
				foreach (FieldMap f in t.PrimaryKeys)
				{
					if (f is InternalFieldMap)
					{
						InternalFieldMap ifm = (InternalFieldMap)f;
						tmp+=ifm.FieldName+",";
					}
				}
				tmp=tmp.Substring(0,tmp.Length-1);
				tmp+="),\n";
			}
			if (t.ForiegnTables.Count>0)
			{
				foreach (Type type in t.ForiegnTables)
				{
                    if (t.GetFieldInfoForForiegnTable(type).IsArray)
                    {
                        TableMap ext = ClassMapper.GetTableMap(type);
                        string externalTable = "CREATE TABLE "+t.Name+"_"+ext.Name+"(";
                        string pkeys = "\nPRIMARY KEYS(";
                        string fkeys = "\nFORIEGN KEYS(";
                        string fields = "";
                        foreach (InternalFieldMap f in t.PrimaryKeys)
                        {
                            externalTable += "\n\t" + f.FieldName + " " + TranslateFieldType(f.FieldType, f.FieldLength)+",";
                            pkeys += f.FieldName + ",";
                            fkeys += f.FieldName + ",";
                        }
                        fkeys = fkeys.Substring(0, fkeys.Length - 1);
                        fkeys += ")\nREFERENCES " + t.Name + "(" + fkeys.Replace("\nFORIEGN KEYS(", "").Replace(")","") + ")\n\t\tON UPDATE CASCADE ON DELETE CASCADE,";
                        fkeys += "\nFORIEGN KEYS(";
                        foreach (InternalFieldMap f in ext.PrimaryKeys)
                        {
                            externalTable += "\n\t" + f.FieldName + " " + TranslateFieldType(f.FieldType, f.FieldLength) + ",";
                            pkeys += f.FieldName + ",";
                            fkeys += f.FieldName + ",";
                            fields += f.FieldName + ",";
                        }
                        fkeys = fkeys.Substring(0, fkeys.Length - 1);
                        fkeys += ")\nREFERENCES " + ext.Name + "(" + fields.Substring(0, fields.Length - 1) + ")\nON UPDATE CASCADE ON DELETE CASCADE\n";
                        pkeys = pkeys.Substring(0, pkeys.Length - 1) + "),";
                        externalTable =externalTable+pkeys+fkeys +");";
                        ret.Add(externalTable);
                    }
                    else
                    {
                        tmp += "\tFOREIGN KEY(";
                        foreach (InternalFieldMap ifm in ClassMapper.GetTableMap(type).PrimaryKeys)
                        {
                            tmp += ifm.FieldName + ",";
                        }
                        tmp = tmp.Substring(0, tmp.Length - 1);
                        tmp += ")\n\t\tREFERENCES " + ClassMapper.GetTableMap(type).Name + "(";
                        foreach (InternalFieldMap ifm in ClassMapper.GetTableMap(type).PrimaryKeys)
                        {
                            tmp += ifm.FieldName + ",";
                        }
                        tmp = tmp.Substring(0, tmp.Length - 1);
                        tmp += ")\n\t\tON UPDATE " + t.GetFieldInfoForForiegnTable(type).OnUpdate.ToString() + "\n";
                        tmp += "\t\tON DELETE " + t.GetFieldInfoForForiegnTable(type).OnDelete.ToString() + ",\n";
                    }
				}
			}
			tmp=tmp.Substring(0,tmp.Length-2)+"\n";
			tmp+=");\n\n";
			ret.Insert(0,tmp);
			foreach (InternalFieldMap f in t.PrimaryKeys)
			{
				if (f.AutoGen)
				{
					switch(f.FieldType)
					{
						case FieldType.DATE:
						case FieldType.DATETIME:
						case FieldType.TIME:
							tmp="CREATE TRIGGER "+t.Name+"_"+f.FieldName+"_GEN FOR "+t.Name+"\n"+
								"ACTIVE \n"+
								"BEFORE INSERT\n"+
								"POSITION 0 \n"+
								"AS \n"+
								"BEGIN \n"+
								"    NEW."+f.FieldName+" = CURRENT_TIMESTAMP;\n"+
								"END\n\n";
							break;
						case FieldType.INTEGER:
						case FieldType.LONG:
						case FieldType.SHORT:
							ret.Add("CREATE GENERATOR GEN_"+t.Name+"_"+f.FieldName+";\n");
							tmp="CREATE TRIGGER "+t.Name+"_"+f.FieldName+"_GEN FOR "+t.Name+"\n"+
								"ACTIVE \n"+
								"BEFORE INSERT\n"+
								"POSITION 0 \n"+
								"AS \n"+
								"BEGIN \n"+
								"    NEW."+f.FieldName+" = GEN_ID(GEN_"+t.Name+"_"+f.FieldName+",1);\n"+
								"END\n\n";
							break;
						default:
							throw new Exception("Unable to create autogenerator for non date or digit type.");
							break;
					}
					ret.Insert(1,tmp);
				}
			}
			return ret;
		}
		
	}
}