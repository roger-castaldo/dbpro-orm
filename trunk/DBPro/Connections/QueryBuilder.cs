/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 23/10/2008
 * Time: 11:45 AM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */

using System;
using System.Collections.Generic;
using System.Data;
using Org.Reddragonit.Dbpro.Structure.Mapping;
using Org.Reddragonit.Dbpro.Structure;
using FieldNamePair = Org.Reddragonit.Dbpro.Structure.Mapping.TableMap.FieldNamePair;
using ExtractedTableMap = Org.Reddragonit.Dbpro.Connections.ExtractedTableMap;
using ExtractedFieldMap = Org.Reddragonit.Dbpro.Connections.ExtractedFieldMap;
using VersionTypes = Org.Reddragonit.Dbpro.Structure.Attributes.VersionField.VersionTypes;
using FieldType = Org.Reddragonit.Dbpro.Structure.Attributes.FieldType;

namespace Org.Reddragonit.Dbpro.Connections
{
	/// <summary>
	/// Description of QueryBuilder.
	/// </summary>
	internal class QueryBuilder
	{
		
		public QueryBuilder()
		{
		}
		
		#region VersionTableNaming
		internal string VersionTableInsertTriggerName(string table)
		{
			return table+"_VERSION_INSERT";
		}
		
		internal string VersionTableUpdateTriggerName(string table)
		{
			return table+"_VERSION_UPDATE";
		}
		
		internal string VersionTableName(string table)
		{
			return table+"_VERSION";
		}
		
		internal string VersionFieldName(string table)
		{
			return table+"_VERSION_ID";
		}
		#endregion
		
		#region abstracts
		#region Triggers
		protected virtual string SelectTriggersString{
			get{
				throw new Exception("Method Not Implemented.");
			}
		}
		
		protected virtual string SelectGeneratorsString{
			get{
				throw new Exception("Method Not Implemented.");
			}
		}
			
		
		protected virtual string CreateGeneratorString{
			get{
				throw new Exception("Method Not Implemented.");
			}
		}
		
		protected virtual string DropGeneratorString{
			get{
				throw new Exception("Method Not Implemented.");
			}
		}
		
		protected virtual string GetGeneratorValueString{
			get{
				throw new Exception("Method Not Implemented.");
			}
		}
		
		protected virtual string SetGeneratorValueString{
			get{
				throw new Exception("Method Not Implemented.");
			}
		}
		#endregion
		
		#region TableStructure
		protected virtual string SelectTableNamesString{
			get{
				throw new Exception("Method Not Implemented.");
			}
		}
		
		protected virtual string SelectTableFieldsString{
			get{
				throw new Exception("Method Not Implemented.");
			}
		}
		
		protected virtual string SelectForeignKeysString{
			get{
				throw new Exception("Method Not Implemented.");
			}
		}
		
		protected virtual string SelectCurrentAutoGenIdValueNumberString{
			get{
				throw new Exception("Method Not Implemented.");
			}
		}
		
		protected virtual string SetCurrentAutoGenIdValueNumberString{
			get{
				throw new Exception("Method Not Implemented.");
			}
		}
		
		protected virtual string DropNotNullString{
			get{
				throw new Exception("Method Not Implemented.");
			}
		}
		
		protected virtual string DropPrimaryKeyString{
			get{
				throw new Exception("Method Not Implemented.");
			}
		}
		
		protected virtual string DropForeignKeyString{
			get{
				throw new Exception("Method Not Implemented.");
			}
		}
		#endregion
		#endregion
		
		#region virtual
		
		#region Selecting
		protected virtual string SelectWithConditions
		{
			get{return "SELECT {0} FROM {1} WHERE {2}";}
		}
		
		protected virtual string SelectWithoutConditions
		{
			get{return "SELECT {0} FROM {1}";}
		}
		
		protected virtual string SelectMaxWithConditions
		{
			get{return "SELECT MAX({0}) FROM {1} WHERE {2}";}
		}
		
		protected virtual string SelectMaxWithoutConditions
		{
			get{return "SELECT MAX({0}) FROM {1}";}
		}
		
		protected virtual string OrderBy
		{
			get{return " ORDER BY {0}";}
		}
		#endregion
		
		#region Insert
		protected virtual string InsertString
		{
			get{return "INSERT INTO {0}({1}) VALUES ({2})";}
		}
		#endregion
		
		#region Delete 
		protected virtual string DeleteWithConditions
		{
			get{return "DELETE FROM {0} WHERE {1}";}
		}
		
		protected virtual string DeleteWithoutConditions
		{
			get{return "DELETE FROM {0}";}
		}
		#endregion
		
		#region Update
		protected virtual string UpdateWithConditions
		{
			get{return "UPDATE {0} SET {1} WHERE {2}";}
		}
		
		protected virtual string UpdateWithoutConditions
		{
			get{return "UPDATE {0} SET {1}";}
		}
		#endregion
		
		#region AlterFields
		protected virtual string AlterFieldTypeString
		{
			get{return "ALTER TABLE {0} ALTER COLUMN {1} TYPE {2}";}
		}
		
		protected virtual string CreatePrimaryKeyString
		{
			get{return "ALTER TABLE {0} ADD PRIMARY KEY({1})";}
		}
		
		protected virtual string CreateNullConstraintString
		{
			get{return "ALTER TABLE {0} ADD CONSTRAINT nn_{1} {1} NOT NULL";}
		}
		
		protected virtual string CreateForeignKeyString
		{
			get{return "ALTER TABLE {0} ADD Foreign KEY ({1}) REFERENCES {2}({3}) ON UPDATE {4} ON DELETE {5}";}
		}
		
		protected virtual string CreateColumnString
		{
			get{return "ALTER TABLE {0} ADD {1} {2}";}
		}
		
		protected virtual string DropColumnString
		{
			get{return "ALTER TABLE {0} DROP {1}";}
		}
		#endregion
		
		#region Tables
		protected virtual string DropTableString
		{
			get{return "DROP TABLE {0}";}
		}
		
		protected virtual string DropTriggerString
		{
			get{return "DROP TRIGGER {0}";}
		}
		
		protected virtual string CreateTableString{
			get{return "CREATE TABLE {0} ( {1} )";}
		}
		
		protected virtual string CreateTriggerString{
			get{return "CREATE TRIGGER {0} {1} {2}";}
		}
		#endregion
		
		#endregion
		
		#region Metadata
		internal string SelectTableNames()
		{
			return SelectTableNamesString;
		}
		
		internal string SelectTableFields(string tableName)
		{
			return String.Format(SelectTableFieldsString,tableName);
		}
		
		internal string SelectForeignKeys(string tableName)
		{
			return String.Format(SelectForeignKeysString,tableName);
		}
		
		internal string SelectCurrentAutoGenIdNumberValue(string fieldName)
		{
			return String.Format(SelectCurrentAutoGenIdValueNumberString,fieldName);
		}
		
		internal string SetCurrentAutoGenIdNumberValue(string fieldName,long IdValue)
		{
			return String.Format(SetCurrentAutoGenIdValueNumberString,fieldName,IdValue.ToString());
		}
		#endregion
		
		#region TableAlterations
		internal string DropColumn(string table, string field)
		{
			return string.Format(DropColumnString,table,field);
		}
		
		internal string DropTable(string table)
		{
			return string.Format(DropTableString,table);
		}
		
		internal string DropTrigger(string trigger)
		{
			return string.Format(DropTriggerString,trigger);
		}
		
		internal string CreateTrigger(string name,string conditions,string code)
		{
			return string.Format(CreateTriggerString,name,conditions,code);
		}
		
		internal string SelectTriggers(){
			return SelectTriggersString;
		}
		
		internal string SelectGenerators(){
			return SelectGeneratorsString;
		}
		
		internal string CreateGenerator(string name){
			return string.Format(CreateGeneratorString,name);
		}
		
		internal string DropGenerator(string name){
			return string.Format(DropGeneratorString,name);
		}
		
		internal string GetGeneratorValue(string name){
			return string.Format(GetGeneratorValueString,name);
		}
		
		internal string SetGeneratorValue(string name,long value){
			return string.Format(SetGeneratorValueString,name,value.ToString());
		}
		#endregion
		
		#region TableAltering
		internal string CreateColumn(string table, string field, string type,long size)
		{
			if (type.ToUpper().Contains("CHAR"))
			{
				return string.Format(CreateColumnString,table,field,type+"("+size.ToString()+")");
			}else{
				return string.Format(CreateColumnString,table,field,type);
			}
		}
		
		internal string AlterFieldType(string table, string field, string type,long size)
		{
			if (type.Contains("CHAR"))
				return string.Format(AlterFieldTypeString,table,field,type+"("+size.ToString()+")");
			else
				return string.Format(AlterFieldTypeString,table,field,type);
		}
		
		internal virtual string DropPrimaryKey(string table,string field,Connection conn)
		{
			return String.Format(DropPrimaryKeyString,table,field);
		}
		
		internal string CreatePrimaryKey(string table, List<string> fields)
		{
			string ret="";
			foreach(string str in fields)
			{
				ret+=str+",";
			}
			return string.Format(CreatePrimaryKeyString,table,ret.Substring(0,ret.Length-1));
		}
		
		internal string CreateNullConstraint(string table,string field)
		{
			return string.Format(CreateNullConstraintString,table,field);
		}
		
		internal virtual string DropNullConstraint(string table,string field, Connection conn)
		{
			return String.Format(DropNotNullString,table,field);
		}
		
		internal virtual string DropForeignKey(string table,string field,Connection conn)
		{
			return string.Format(DropForeignKeyString,table,field);
		}
		
		internal string CreateForeignKey(string table, List<string> fields, string ForeignTable, List<string> ForeignFields,string UpdateAction,string DeleteAction)
		{
			string field="";
			foreach (string str in fields)
			{
				field+=str+",";
			}
			field=field.Substring(0,field.Length-1);
			string Foreigns = "";
			foreach (string str in ForeignFields)
			{
				Foreigns+=str+",";
			}
			Foreigns=Foreigns.Substring(0,Foreigns.Length-1);
			return string.Format(CreateForeignKeyString,table,field,ForeignTable,Foreigns,UpdateAction,DeleteAction);
		}
		
		internal string CreateTable(ExtractedTableMap table)
		{
			string fields = "";
			foreach (ExtractedFieldMap efm in table.Fields)
			{
				if (efm.Type.ToUpper().Contains("CHAR"))
					fields+="\t"+efm.FieldName+" "+efm.Type+"("+efm.Size.ToString()+"),\n";
				else
					fields+="\t"+efm.FieldName+" "+efm.Type+",\n";
			}
			fields = fields.Substring(0,fields.Length-2);
			return String.Format(CreateTableString,table.TableName,fields);
		}
		#endregion
		
		#region Inserts
		internal string Insert(Table table,out List<IDbDataParameter> insertParameters,out string select,out List<IDbDataParameter> selectParameters,Connection conn)
		{
			TableMap map = ClassMapper.GetTableMap(table.GetType());
			insertParameters=new List<IDbDataParameter>();
			selectParameters=new List<IDbDataParameter>();
			select=string.Format(SelectWithoutConditions,"*",map.Name);
			try{
				string values="";
				string fields = "";
				List<SelectParameter> pars = new List<SelectParameter>();
				foreach (FieldNamePair fnp in ClassMapper.GetTableMap(table.GetType()).FieldNamePairs)
				{
					if (map[fnp] is ExternalFieldMap)
					{
						if (!((ExternalFieldMap)map[fnp]).IsArray)
						{
							TableMap relatedTableMap = ClassMapper.GetTableMap(table.GetType().GetProperty(fnp.ClassFieldName).PropertyType);
							if (table.GetType().GetProperty(fnp.ClassFieldName).GetValue(table, new object[0]) == null)
							{
								foreach (FieldMap fm in relatedTableMap.PrimaryKeys)
								{
									values += relatedTableMap.GetTableFieldName(fm) + ",";
									insertParameters.Add(conn.CreateParameter("@" + relatedTableMap.GetTableFieldName(fm), null));
									pars.Add(new SelectParameter(relatedTableMap.GetClassFieldName(fm),null));
								}
							}
							else
							{
								Table relatedTable = (Table)table.GetType().GetProperty(fnp.ClassFieldName).GetValue(table, new object[0]);
								foreach (FieldMap fm in relatedTableMap.PrimaryKeys)
								{
									pars.Add(new SelectParameter(relatedTableMap.GetClassFieldName(fm),relatedTable.GetType().GetProperty(relatedTableMap.GetClassFieldName(fm)).GetValue(relatedTable, new Object[0])));
								}
							}
						}
					}
					else if (!map[fnp].AutoGen)
					{
						fields += "," + map.Name + "." + fnp.TableFieldName;
						values += fnp.TableFieldName + ",";
						if (table.IsFieldNull(fnp.ClassFieldName))
						{
							insertParameters.Add(conn.CreateParameter("@" + fnp.TableFieldName,null));
							pars.Add(new SelectParameter(fnp.ClassFieldName,null));
						}
						else
						{
							insertParameters.Add(conn.CreateParameter("@" + fnp.TableFieldName, table.GetType().GetProperty(fnp.ClassFieldName).GetValue(table, new object[0])));
							pars.Add(new SelectParameter(fnp.ClassFieldName,table.GetType().GetProperty(fnp.ClassFieldName).GetValue(table, new object[0])));
						}
					}
				}
				values=values.Substring(0,values.Length-1);
				foreach (InternalFieldMap ifm in map.Fields)
				{
					if (!ifm.AutoGen)
					{
						if (table.IsFieldNull(map.GetClassFieldName(ifm)))
							pars.Add(new SelectParameter(map.GetClassFieldName(ifm),null));
						else
							pars.Add(new SelectParameter(map.GetClassFieldName(ifm),table.GetType().GetProperty(map.GetClassFieldName(ifm)).GetValue(table, new object[0])));
					}
				}
				select = Select(table.GetType(),pars,out selectParameters,conn);
				string conditions = "";
				foreach (InternalFieldMap f in map.InternalPrimaryKeys)
				{
					if (f.AutoGen)
					{
						conditions+=" AND "+map.GetTableFieldName(f)+" IN ("+SelectMax(table.GetType(),map.GetTableFieldName(f),pars,out selectParameters,conn)+")";
					}
				}
				if (!select.Contains(" WHERE ")&&(conditions.Length>0))
					select+=" WHERE "+conditions.Substring(4);
				else
					select+=conditions;
				return string.Format(InsertString,map.Name,values,"@"+values.Replace(",",",@"));
			}catch (Exception e)
			{
				System.Diagnostics.Debug.WriteLine(e.Message);
				return null;
			}
		}
		#endregion
		
		#region Updates
		internal Dictionary<string,List<List<IDbDataParameter>>> UpdateMapArray(Table table,ExternalFieldMap efm,Connection conn)
		{
			Dictionary<string, List<List<IDbDataParameter>>> ret = new Dictionary<string, List<List<IDbDataParameter>>>();
			try{
				TableMap map = ClassMapper.GetTableMap(table.GetType());
				TableMap relatedMap = ClassMapper.GetTableMap(efm.Type);
				string delString = "DELETE FROM " + map.Name + "_" + relatedMap.Name + " WHERE ";
				List<IDbDataParameter> pars = new List<IDbDataParameter>();
				foreach (InternalFieldMap ifm in map.PrimaryKeys)
				{
					delString += ifm.FieldName + " = @" + ifm.FieldName + " AND ";
					pars.Add(conn.CreateParameter("@" + ifm.FieldName, table.GetType().GetProperty(map.GetClassFieldName(ifm)).GetValue(table, new object[0])));
				}
				ret.Add(delString.Substring(0, delString.Length - 4),new List<List<IDbDataParameter>>());
				ret[delString.Substring(0, delString.Length - 4)].Add(pars);
				Table[] values = (Table[])table.GetType().GetProperty(map.GetClassFieldName(efm)).GetValue(table, new object[0]);
				delString = "INSERT INTO " + map.Name + "_" + relatedMap.Name + "(";
				string valueString = "VALUES(";
				foreach (InternalFieldMap ifm in map.PrimaryKeys)
				{
					delString += ifm.FieldName + ",";
					valueString += "@" + ifm.FieldName + ",";
				}
				foreach (InternalFieldMap ifm in relatedMap.PrimaryKeys)
				{
					delString += ifm.FieldName + ",";
					valueString += "@" + ifm.FieldName + ",";
				}
				delString = delString.Substring(0, delString.Length - 1) + ") " + valueString.Substring(0, valueString.Length - 1) + ")";
				ret.Add(delString,new List<List<IDbDataParameter>>());
				foreach (Table t in values)
				{
					foreach (InternalFieldMap ifm in relatedMap.PrimaryKeys)
					{
						for (int x = 0; x < pars.Count; x++)
						{
							if (pars[x].ParameterName == "@" + ifm.FieldName)
							{
								pars.RemoveAt(x);
								break;
							}
						}
						pars.Add(conn.CreateParameter("@" + ifm.FieldName, t.GetType().GetProperty(relatedMap.GetClassFieldName(ifm)).GetValue(t, new object[0])));
					}
					ret[delString].Add(pars);
				}
			}catch (Exception e)
			{
				System.Diagnostics.Debug.WriteLine(e.Message);
				return null;
			}
			return ret;
		}
		
		internal string Update(Table table,out List<IDbDataParameter> queryParameters,Connection conn)
		{
			queryParameters = new List<IDbDataParameter>();
			try{
				TableMap map = ClassMapper.GetTableMap(table.GetType());
				string fields = "";
				string conditions = "";
				foreach (FieldNamePair fnp in map.FieldNamePairs)
				{
					if (map[fnp] is ExternalFieldMap)
					{
						if (!((ExternalFieldMap)map[fnp]).IsArray)
						{
							TableMap relatedTableMap = ClassMapper.GetTableMap(table.GetType().GetProperty(fnp.ClassFieldName).PropertyType);
							if (table.GetType().GetProperty(fnp.ClassFieldName).GetValue(table, new object[0]) == null)
							{
								foreach (FieldMap fm in relatedTableMap.PrimaryKeys)
								{
									fields += relatedTableMap.GetTableFieldName(fm) + " = @" + relatedTableMap.GetTableFieldName(fm) + ", ";
									queryParameters.Add(conn.CreateParameter("@" + relatedTableMap.GetTableFieldName(fm), null));
								}
							}
							else
							{
								Table relatedTable = (Table)table.GetType().GetProperty(fnp.ClassFieldName).GetValue(table, new object[0]);
								foreach (FieldMap fm in relatedTableMap.PrimaryKeys)
								{
									fields += relatedTableMap.GetTableFieldName(fm) + " = @" + relatedTableMap.GetTableFieldName(fm) + ", ";
									queryParameters.Add(conn.CreateParameter("@" + relatedTableMap.GetTableFieldName(fm), relatedTable.GetType().GetProperty(relatedTableMap.GetClassFieldName(fm)).GetValue(relatedTable, new Object[0])));
								}
							}
							if (map[fnp].PrimaryKey || !map.HasPrimaryKeys)
							{
								Table relatedTable = (Table)table.GetInitialPrimaryValue(fnp);
								if (relatedTable == null)
								{
									foreach (FieldMap fm in relatedTableMap.PrimaryKeys)
									{
										conditions += relatedTableMap.GetTableFieldName(fm) + " is null AND ";
									}
								}
								else
								{
									foreach (FieldMap fm in relatedTableMap.PrimaryKeys)
									{
										conditions += relatedTableMap.GetTableFieldName(fm) + " = @init_" + relatedTableMap.GetTableFieldName(fm) + " AND ";
										queryParameters.Add(conn.CreateParameter("@init_" + relatedTableMap.GetTableFieldName(fm), relatedTable.GetType().GetProperty(relatedTableMap.GetClassFieldName(fm)).GetValue(relatedTable, new Object[0])));
									}
								}
							}
						}
					}
					else
					{
						fields += fnp.TableFieldName+" = @"+fnp.TableFieldName +", ";
						if (table.GetType().GetProperty(fnp.ClassFieldName).GetValue(table, new object[0]) == null)
						{
							queryParameters.Add(conn.CreateParameter("@" + fnp.TableFieldName, null));
						}
						else
						{
							queryParameters.Add(conn.CreateParameter("@" + fnp.TableFieldName, table.GetType().GetProperty(fnp.ClassFieldName).GetValue(table, new object[0])));
						}
						if (map[fnp].PrimaryKey || !map.HasPrimaryKeys)
						{
							if (table.GetInitialPrimaryValue(fnp) == null)
							{
								conditions += fnp.TableFieldName + " IS NULL AND ";
							}
							else
							{
								conditions += fnp.TableFieldName + " = @init_"+fnp.TableFieldName +" AND ";
								queryParameters.Add(conn.CreateParameter("@init_" + fnp.TableFieldName, table.GetInitialPrimaryValue(fnp)));
							}
						}
					}
				}
				if (conditions.Length>0)
				{
					return String.Format(UpdateWithConditions,map.Name,fields,conditions.Substring(0,conditions.Length-4));
				}else
					return String.Format(UpdateWithoutConditions,map.Name,fields);
			}catch (Exception e)
			{
				System.Diagnostics.Debug.WriteLine(e.Message);
			}
			return null;
		}
		#endregion
		
		#region Selects
		private string GetSubqueryTable(TableMap map,Type type,ref int count)
		{
			if (map.ParentType==null)
				return map.Name;
			TableMap parentMap = ClassMapper.GetTableMap(map.ParentType);
			string fields = "";
			string tables = map.Name+" table_"+count.ToString()+", "+GetSubqueryTable(parentMap,map.ParentType,ref count)+" table_"+((int)(count+1)).ToString();
			string where ="";
			foreach (FieldNamePair fnp in map.FieldNamePairs)
			{
				if (map[fnp] is ExternalFieldMap)
				{
					if (!((ExternalFieldMap)map[fnp]).IsArray)
					{
						TableMap relatedMap = ClassMapper.GetTableMap(type.GetProperty(fnp.ClassFieldName).PropertyType);
						foreach (InternalFieldMap ifm in relatedMap.PrimaryKeys)
						{
							fields+="table_"+count.ToString()+"."+ifm.FieldName+",";
						}
					}
				}else{
					fields+="table_"+count.ToString()+"."+fnp.TableFieldName+",";
				}
			}
			foreach (InternalFieldMap ifm in parentMap.PrimaryKeys)
			{
				where+="table_"+count.ToString()+"."+ifm.FieldName+" = table_"+(count+1).ToString()+"."+ifm.FieldName+" AND";
			}
			count++;
			foreach (string str in map.ParentDatabaseFieldNames)
			{
				fields+="table_"+count.ToString()+"."+str+",";
			}
			fields=fields.Substring(0,fields.Length-1);
			count++;
			if (where.EndsWith(" AND"))
				where = where.Substring(0,where.Length-4);
			return String.Format("("+SelectWithConditions+")",fields,tables,where);
		}
		
		private bool ObtainFieldTableWhereList(out string fields,out string tables,out string joins,out string where,System.Type type)
		{
			fields="";
			tables="";
			joins="";
			where="";
			int count=0;
			try{
				TableMap map = ClassMapper.GetTableMap(type);
				joins=GetSubqueryTable(map,type,ref count)+" main_table";
				foreach (FieldNamePair fnp in map.FieldNamePairs)
				{
					if (map[fnp] is ExternalFieldMap)
					{
						if (!((ExternalFieldMap)map[fnp]).IsArray)
						{
							TableMap relatedTableMap = ClassMapper.GetTableMap(type.GetProperty(fnp.ClassFieldName).PropertyType);
							if (map.GetFieldInfoForForeignTable(type.GetProperty(fnp.ClassFieldName).PropertyType).Nullable)
							{
								joins += " LEFT JOIN " + relatedTableMap.Name + " ON ";
								foreach (FieldMap fm in relatedTableMap.PrimaryKeys)
								{
									joins += "main_table." + relatedTableMap.GetTableFieldName(fm) + " = " + relatedTableMap.Name + "." + relatedTableMap.GetTableFieldName(fm) + " AND ";
								}
								joins = joins.Substring(0, joins.Length - 4);
							}
							else
							{
								tables += "," + relatedTableMap.Name;
								foreach (FieldMap fm in relatedTableMap.PrimaryKeys)
								{
									where += "main_table." + relatedTableMap.GetTableFieldName(fm) + " = " + relatedTableMap.Name + "." + relatedTableMap.GetTableFieldName(fm) + " AND ";
								}
							}
							foreach (FieldMap fm in relatedTableMap.Fields)
							{
								fields += "," + relatedTableMap.Name + "." + relatedTableMap.GetTableFieldName(fm);
							}
						}
					}
					else
					{
						fields += ",main_table." + fnp.TableFieldName;
					}
				}
				foreach (string str in map.ParentDatabaseFieldNames)
				{
					fields+=",main_table."+str;
				}
				fields=fields.Substring(1);
				if (where.Length>0)
				{
					where.Substring(0,where.Length-4);
				}
			}catch (Exception e)
			{
				return false;
			}
			return true;
		}
		
		internal string SelectAll(System.Type type)
		{
			List<IDbDataParameter> pars = new List<IDbDataParameter>();
			return Select(type,null,out pars,null);
		}
		
		internal string SelectMax(System.Type type,string maxField,List<SelectParameter> parameters,out List<IDbDataParameter> queryParameters,Connection conn)
		{
			TableMap map = ClassMapper.GetTableMap(type);
			string fields="";
			string tables="";
			string joins="";
			string where="";
			bool startAnd=false;
			queryParameters = new List<IDbDataParameter>();
			if (ObtainFieldTableWhereList(out fields,out tables, out joins,out where, type))
			{
				fields = maxField;
				if ((parameters!=null)&&(parameters.Count>0))
				{
					startAnd=(where.Length>0);
					int parCount=0;
					foreach (SelectParameter par in parameters)
					{
						foreach (FieldNamePair f in map.FieldNamePairs)
						{
							if (par.FieldName==f.ClassFieldName)
							{
								where+=" AND "+f.TableFieldName+" = @parameter_"+parCount.ToString();
								queryParameters.Add(conn.CreateParameter("@parameter_"+parCount.ToString(),par.FieldValue));
								parCount++;
							}
						}
					}
					if (!startAnd)
						where = where.Substring(4);
				}
				if (where.Length>0)
					return String.Format(SelectMaxWithConditions ,fields,joins+tables,where);
				else
					return String.Format(SelectMaxWithoutConditions,fields,joins+tables);
			}else
				return null;
		}
		
		internal string Select(System.Type type,List<SelectParameter> parameters,out List<IDbDataParameter> queryParameters,Connection conn)
		{
			TableMap map = ClassMapper.GetTableMap(type);
			string fields="";
			string tables="";
			string joins="";
			string where="";
			bool startAnd=false;
			queryParameters=new List<IDbDataParameter>();
			if (ObtainFieldTableWhereList(out fields,out tables, out joins,out where,type))
			{
				if ((parameters!=null)&&(parameters.Count>0))
				{
					startAnd=(where.Length>0);
					int parCount=0;
					foreach (SelectParameter par in parameters)
					{
						foreach (FieldNamePair f in map.FieldNamePairs)
						{
							if (par.FieldName==f.ClassFieldName)
							{
								where+=" AND "+f.TableFieldName+" = @parameter_"+parCount.ToString();
								queryParameters.Add(conn.CreateParameter("@parameter_"+parCount.ToString(),par.FieldValue));
								parCount++;
							}
						}
					}
					if (!startAnd)
						where = where.Substring(4);
				}
				if (where.Length>0)
					return String.Format(SelectWithConditions,fields,joins+tables,where);
				else
					return String.Format(SelectWithoutConditions,fields,joins+tables);
			}else
				return null;
		}
		#endregion
	}
}
