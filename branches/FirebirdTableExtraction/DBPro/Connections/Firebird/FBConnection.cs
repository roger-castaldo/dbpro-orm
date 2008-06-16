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

		private const string SelectTableListQuery = "SELECT " +
			"rfr.rdb$relation_name AS TableName, " +
			"rfr.rdb$field_name AS ColumnName, " +
			"rfr.rdb$field_position AS ColumnPosition, " +
			" (CASE fld.rdb$field_type WHEN 261 THEN " +
			" (CASE WHEN fld.rdb$field_sub_type = 1 THEN  'BLOB SUB_TYPE TEXT' " +
			" ELSE 'BLOB' END) " +
			" WHEN 14 THEN 'CHAR' " +
			" WHEN 27 THEN 'DOUBLE' " +
			" WHEN 10 THEN 'FLOAT' " +
			" WHEN 16 THEN  " +
			" (CASE WHEN fld.rdb$field_sub_type = 2 THEN 'DECIMAL('||CAST(fld.rdb$field_precision as varchar(100))||', '||cast((0-fld.rdb$field_scale) as varchar(100))||')'  " +
			" ELSE 'BIGINT' END) " +
			" WHEN 8 THEN 'INTEGER' " +
			" WHEN 9 THEN 'QUAD' " +
			" WHEN 7 THEN 'SMALLINT' " +
			" WHEN 12 THEN 'DATE' " +
			" WHEN 13 THEN 'TIME' " +
			" WHEN 35 THEN 'TIMESTAMP' " +
			" WHEN 37 THEN 'VARCHAR' " +
			" ELSE 'UNKNOWN' " +
			" END) AS ColumnDataType, " +
			"fld.rdb$field_sub_type AS ColumnSubType, " +
			"fld.rdb$field_length AS ColumnSize, " +
			"fld.rdb$field_precision AS ColumnPrecision, " +
			"fld.rdb$field_scale AS ColumnScale, " +
			"(CASE WHEN rfr.rdb$null_flag IS null or rfr.rdb$null_flag=0 THEN 'true' else 'false' END) AS NullFlag,  " +
			"fld.rdb$default_source AS DefaultValue, " +
			"(select count(*) from " +
			"rdb$relation_constraints rel, " +
			"rdb$indices idx, " +
			"rdb$index_segments seg " +
			"where " +
			"rel.rdb$constraint_type = 'PRIMARY KEY' " +
			"and rel.rdb$index_name = idx.rdb$index_name " +
			"and idx.rdb$index_name = seg.rdb$index_name " +
			"and rel.rdb$relation_name = rfr.rdb$relation_name " +
			"and seg.rdb$field_name = rfr.rdb$field_name) AS PrimaryKey, " +
			"(select count(*) from " +
			"rdb$relation_constraints rel, " +
			"rdb$indices idx, " +
			"rdb$index_segments seg " +
			"where " +
			"rel.rdb$constraint_type = 'FOREIGN KEY' " +
			"and rel.rdb$index_name = idx.rdb$index_name " +
			"and idx.rdb$index_name = seg.rdb$index_name " +
			"and rel.rdb$relation_name = rfr.rdb$relation_name " +
			"and seg.rdb$field_name = rfr.rdb$field_name) AS ForeignKey " +
			"FROM " +
			"rdb$relation_fields rfr " +
			"LEFT JOIN rdb$fields fld ON rfr.rdb$field_source = fld.rdb$field_name " +
			"LEFT JOIN rdb$relations rel ON (rfr.rdb$relation_name = rel.rdb$relation_name AND rel.rdb$system_flag IS NOT NULL) " +
			"WHERE rfr.rdb$relation_name NOT LIKE 'RDB$%' and rfr.rdb$relation_name NOT LIKE 'MON$%'" +
			"ORDER BY " +
			"rfr.rdb$relation_name, rfr.rdb$field_position";
		
		private const string AutoGenQuery="SELECT RDB$GENERATOR_NAME " +
			" FROM RDB$GENERATORS " +
			" where RDB$SYSTEM_FLAG is null or RDB$SYSTEM_FLAG=0";
		
		private const string SelectReferences =
			"SELECT " +
			"pidx.rdb$relation_name AS FKTableName, " +
			"pseg.rdb$field_name AS FKColumnName, " +
			"rc.rdb$constraint_name AS FKName, " +
			"rc.rdb$relation_name AS PKTableName, " +
			"fseg.rdb$field_name AS PKColumnName, " +
			"fidx.rdb$foreign_key AS PKName," +
			"actions.rdb$update_rule as on_update, "+
			"actions.rdb$delete_rule as on_delete " +
			"FROM " +
			"rdb$relation_constraints rc " +
			"inner join rdb$indices fidx ON (rc.rdb$index_name = fidx.rdb$index_name AND rc.rdb$constraint_type = 'FOREIGN KEY') " +
			"inner join rdb$index_segments fseg ON fidx.rdb$index_name = fseg.rdb$index_name " +
			"inner join rdb$indices pidx ON fidx.rdb$foreign_key = pidx.rdb$index_name " +
			"inner join rdb$index_segments pseg ON (pidx.rdb$index_name = pseg.rdb$index_name AND pseg.rdb$field_position=fseg.rdb$field_position) " +
			"inner join RDB$REF_CONSTRAINTS actions ON rc.rdb$constraint_name = actions.RDB$constraint_name " +
			"ORDER BY rc.rdb$relation_name,fseg.rdb$field_name";
		
		private const string SelectContraints =
			"SELECT command FROM " +
			" (select 'ALTER TABLE '||rdb$relation_name||' DROP CONSTRAINT '||rdb$constraint_name||';' as command,2 as order_mode " +
			" from rdb$relation_constraints " +
			" where rdb$constraint_type = 'FOREIGN KEY' " +
			" UNION " +
			" select 'ALTER TABLE '||rdb$relation_name||' DROP CONSTRAINT '||rdb$constraint_name||';' as command,1 as order_mode " +
			" from rdb$relation_constraints " +
			" where rdb$constraint_type = 'PRIMARY KEY' " +
			" UNION " +
			" select 'ALTER TABLE '||rdb$relation_name||' DROP CONSTRAINT '||rdb$constraint_name||';' as command,0 as order_mode " +
			" from rdb$relation_constraints " +
			" where rdb$constraint_type = 'NOT NULL') ORDER BY order_mode DESC";
		
		public FBConnection(ConnectionPool pool,string connectionString) : base(pool,connectionString)
		{

		}
		
		internal override string GetAlterFieldTypeString(string table, string field, string type,long size)
		{
			if (type.ToUpper().Contains("CHAR"))
			{
				return "ALTER TABLE "+table+" ALTER COLUMN "+field+" "+type+"("+size.ToString()+")";
			}else{
				return "ALTER TABLE "+table+" ALTER COLUMN "+field+" "+type;
			}
		}
		
		internal override string GetForiegnKeyCreateString(string table, List<string> fields, string foriegnTable, List<string> foriegnFields)
		{
			string ret="ALTER TABLE "+table+" ADD FOREIGN KEY (";
			foreach (string str in fields)
			{
				ret+=str+",";
			}
			ret=ret.Substring(0,ret.Length-1);
			ret+=") REFERENCES "+foriegnTable+"(";
			foreach(string str in foriegnFields)
			{
				ret+=str+",";
			}
			return ret.Substring(0,ret.Length-1)+")";
		}
		
		internal override string GetNullConstraintCreateString(string table, string field)
		{
			return "ALTER TABLE "+table+" ADD CHECK ("+field+" IS NOT NULL);";
		}
		
		internal override string GetPrimaryKeyCreateString(string table, List<string> fields)
		{
			string ret = "ALTER TABLE "+table+" ADD PRIMARY KEY (";
			foreach(string str in fields)
			{
				ret+=str+",";
			}
			return ret.Substring(0,ret.Length-1)+")";
		}
		
		internal override string GetCreateColumnString(string table, string field, string type,long size)
		{
			if (type.ToUpper().Contains("CHAR"))
			{
				return "ALTER TABLE "+table+" ADD COLUMN "+field+" "+type+"("+size.ToString()+")";
			}else{
				return "ALTER TABLE "+table+" ADD COLUMN "+field+" "+type;
			}
		}
		
		internal override List<string> GetCreateTableStringsForAlterations(Connection.ExtractedTableMap table)
		{
			List<string> ret = new List<string>();
			string tmp="CREATE TABLE "+table.TableName+"(\n";
			foreach (ExtractedFieldMap f in table.Fields)
			{
				tmp+="\t"+f.FieldName+" "+f.Type;
				if (f.Type.ToUpper().Contains("CHAR"))
				{
					tmp+="("+f.Size.ToString()+")";
				}
				tmp+=",\n";
			}
			ret.Add(tmp.Substring(0,tmp.Length-2)+")");
			foreach (ExtractedFieldMap f in table.Fields)
			{
				if ((f.PrimaryKey)&&(f.AutoGen))
				{
					if (f.Type.ToUpper().Contains("DATE")||f.Type.ToUpper().Contains("TIME"))
					{
						tmp="CREATE TRIGGER "+table.TableName+"_"+f.FieldName+"_GEN FOR "+table.TableName+"\n"+
							"ACTIVE \n"+
							"BEFORE INSERT\n"+
							"POSITION 0 \n"+
							"AS \n"+
							"BEGIN \n"+
							"    NEW."+f.FieldName+" = CURRENT_TIMESTAMP;\n"+
							"END\n\n";
					}else if (f.Type.ToUpper().Contains("INT"))
					{
						ret.Add("CREATE GENERATOR GEN_"+table.TableName+"_"+f.FieldName+";\n");
						tmp="CREATE TRIGGER "+table.TableName+"_"+f.FieldName+"_GEN FOR "+table.TableName+"\n"+
							"ACTIVE \n"+
							"BEFORE INSERT\n"+
							"POSITION 0 \n"+
							"AS \n"+
							"BEGIN \n"+
							"    NEW."+f.FieldName+" = GEN_ID(GEN_"+table.TableName+"_"+f.FieldName+",1);\n"+
							"END\n\n";
					}else{
						throw new Exception("Unable to create autogenerator for non date or digit type.");
					}
					ret.Insert(1,tmp);
				}
			}
			return ret;
		}
		
		internal override string GetDropTableString(string table)
		{
			return "DROP TABLE "+table;
		}
		
		internal override string GetDropColumnString(string table, string field)
		{
			return "ALTER TABLE "+table+" DROP COLUMN "+field;
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

		internal override List<Connection.ExtractedTableMap> GetTableList()
		{
			List<ExtractedTableMap> ret = new List<ExtractedTableMap>();
			this.ExecuteQuery(SelectTableListQuery);
			while (this.Read())
			{
				ExtractedTableMap map = new ExtractedTableMap(null);
				foreach (ExtractedTableMap m in ret)
				{
					if (m.TableName == this["TABLENAME"].ToString().Trim(" ".ToCharArray()))
					{
						map = m;
						break;
					}
				}
				if (map.TableName==null )
				{
					map = new ExtractedTableMap(this["TABLENAME"].ToString().Trim(" ".ToCharArray()));
					ret.Add(map);
				}
				map.Fields.Add(new ExtractedFieldMap(this["COLUMNNAME"].ToString().Trim(" ".ToCharArray()), this["COLUMNDATATYPE"].ToString().Trim(" ".ToCharArray()),
				                                     long.Parse(this["COLUMNSIZE"].ToString()), int.Parse(this["PRIMARYKEY"].ToString())>0, bool.Parse(this["NULLFLAG"].ToString())));
			}
			this.Close();
			this.ExecuteQuery(SelectReferences);
			int x=0;
			while (this.Read())
			{
				while ((x<ret.Count) && (!ret[x].TableName.Equals(this["PKTableName"].ToString().Trim(" ".ToCharArray()))))
				{
					x+=1;
				}
				if (x!=ret.Count)
				{
					ExtractedTableMap etm = ret[x];
					for(int y=0;y<etm.Fields.Count;y++)
					{
						ExtractedFieldMap efm=etm.Fields[y];
						if (efm.FieldName==this["PKColumnName"].ToString().Trim(" ".ToCharArray()))
						{
							efm.ExternalTable=this["FKTableName"].ToString().Trim(" ".ToCharArray());
							efm.ExternalField=this["FKColumnName"].ToString().Trim(" ".ToCharArray());
							efm.UpdateAction=this["on_update"].ToString();
							efm.DeleteAction=this["on_delete"].ToString();
							etm.Fields.RemoveAt(y);
							etm.Fields.Insert(y,efm);
							break;
						}
					}
					ret.RemoveAt(x);
					ret.Insert(x,etm);
				}
			}
			this.Close();
			this.ExecuteQuery(AutoGenQuery);
			while (this.Read())
			{
				bool foundMatch=false;
				x=0;
				while (x<ret.Count)
				{
					if (((string)this[0]).StartsWith("GEN_"+ret[x].TableName))
					{
						ExtractedTableMap etm = ret[x];
						for (int y=0;y<etm.Fields.Count;y++)
						{
							ExtractedFieldMap efm = etm.Fields[y];
							if (this.GetString(0)=="GEN_"+etm.TableName+"_"+efm.FieldName)
							{
								foundMatch=true;
								efm.AutoGen=true;
								etm.Fields.RemoveAt(y);
								etm.Fields.Insert(y,efm);
								break;
							}
						}
						if (foundMatch)
						{
							ret.RemoveAt(x);
							ret.Insert(x,etm);
							break;
						}
					}
					x+=1;
				}
			}
			return ret;
		}
		
		internal override List<string> GetDropConstraintsScript()
		{
			List<string> ret = new List<string>();
			this.ExecuteQuery(SelectContraints);
			while (this.Read())
			{
				ret.Add((string)this[0]);
			}
			this.Close();
			return ret;
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
		
		protected override List<string> ConstructCreateStrings(Org.Reddragonit.Dbpro.Structure.Table table)
		{
			List<string> ret = new List<string>();
			TableMap t = ClassMapper.GetTableMap(table.GetType());
			string tmp="CREATE TABLE "+t.Name+"(\n";
			foreach (FieldMap f in t.Fields)
			{
				InternalFieldMap ifm = (InternalFieldMap)f;
				tmp+="\t"+ifm.FieldName+" ".ToCharArray()+TranslateFieldType(ifm.FieldType,ifm.FieldLength);
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
			if (t.ForiegnTablesCreate.Count>0)
			{
				foreach (Type type in t.ForiegnTablesCreate)
				{
					if (t.GetFieldInfoForForiegnTable(type).IsArray)
					{
						TableMap ext = ClassMapper.GetTableMap(type);
						string externalTable = "CREATE TABLE "+t.Name+"_"+ext.Name+"(";
						string pkeys = "\nPRIMARY KEY(";
						string fkeys = "\nFOREIGN KEY(";
						string fields = "";
						foreach (InternalFieldMap f in t.PrimaryKeys)
						{
							externalTable += "\n\t" + f.FieldName + " ".ToCharArray() + TranslateFieldType(f.FieldType, f.FieldLength)+",";
							pkeys += f.FieldName + ",";
							fkeys += f.FieldName + ",";
						}
						fkeys = fkeys.Substring(0, fkeys.Length - 1);
						fkeys += ")\nREFERENCES " + t.Name + "(" + fkeys.Replace("\nFOREIGN KEY(", "").Replace(")","") + ")\n\t\tON UPDATE CASCADE ON DELETE CASCADE,";
						fkeys += "\nFOREIGN KEY(";
						foreach (InternalFieldMap f in ext.PrimaryKeys)
						{
							externalTable += "\n\t" + f.FieldName + " ".ToCharArray() + TranslateFieldType(f.FieldType, f.FieldLength) + ",";
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
