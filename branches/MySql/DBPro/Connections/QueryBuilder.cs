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
		
		private ConnectionPool _pool;
		protected ConnectionPool pool{
			get{return _pool;}
		}
		
		private Connection _conn;
		protected Connection conn{
			get{return _conn;}
		}
		
		public QueryBuilder(ConnectionPool pool,Connection conn)
		{
			_pool=pool;
			_conn=conn;
		}
		
		public virtual string CreateParameterName(string parameter)
		{
			return "@"+parameter;
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
		
		internal string RemoveVersionName(string table)
		{
			return table.Substring(0,table.Length-8);
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
		
		protected virtual string SelectCurrentIdentities{
			get{
				throw new Exception("Method Not Implemented.");
			}
		}
		
		protected virtual string CreateIdentityString{
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
				return "ALTER TABLE {0} DROP PRIMARY KEY({1})";
			}
		}
		
		protected virtual string DropForeignKeyString{
			get{
				throw new Exception("Method Not Implemented.");
			}
		}
		
		protected virtual string SetIdentityFieldValueString{
			get{
				throw new Exception("Method Not Implemented.");
			}
		}
		
		protected virtual string DropIdentityFieldString{
			get{
				throw new Exception("Method Not Implemented.");
			}
		}
		#endregion
		#endregion
		
		#region virtual
		
		#region Selecting
		internal virtual string SelectWithConditions
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
		
		internal virtual string SelectIdentities()
		{
			return SelectCurrentIdentities;
		}
		
		internal string SelectTableFields(string tableName)
		{
			return String.Format(SelectTableFieldsString,tableName);
		}
		
		internal string SelectForeignKeys(string tableName)
		{
			return String.Format(SelectForeignKeysString,tableName);
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
		
		internal string DropIdentityField(IdentityField field)
		{
			return string.Format(DropIdentityFieldString,field.TableName,field.FieldName,field.FieldType,field.CurValue);
		}
		
		internal string CreateIdentityField(IdentityField field)
		{
			return string.Format(CreateIdentityString,field.TableName,field.FieldName,field.FieldType,field.CurValue);
		}
		
		internal string CreateTrigger(Trigger trigger)
		{
			return string.Format(CreateTriggerString,trigger.Name,trigger.Conditions,trigger.Code);
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
		
		internal string SetIdentityFieldValue(IdentityField field)
		{
			return string.Format(SetIdentityFieldValueString,field.TableName,field.FieldName,field.FieldType,field.CurValue);
		}
		#endregion
		
		#region TableAltering
		internal string CreateColumn(string table,ExtractedFieldMap field)
		{
			return string.Format(CreateColumnString,table,field.FieldName,field.FullFieldType);
		}
		
		internal virtual string AlterFieldType(string table, ExtractedFieldMap field)
		{
			return string.Format(AlterFieldTypeString,table,field.FieldName,field.FullFieldType);
		}
		
		internal virtual string DropPrimaryKey(PrimaryKey key)
		{
			string fields="";
			foreach (string str in key.Fields)
				fields+=str+",";
			return String.Format(DropPrimaryKeyString,key.Name,fields.Substring(0,fields.Length-1));
		}
		
		internal string CreatePrimaryKey(PrimaryKey key)
		{
			string ret="";
			foreach(string str in key.Fields)
			{
				ret+=str+",";
			}
			return string.Format(CreatePrimaryKeyString,key.Name,ret.Substring(0,ret.Length-1));
		}
		
		internal string CreateNullConstraint(string table,ExtractedFieldMap field)
		{
			return string.Format(CreateNullConstraintString,table,field.FieldName,field.FullFieldType);
		}
		
		internal virtual string DropNullConstraint(string table,ExtractedFieldMap field)
		{
			return String.Format(DropNotNullString,table,field.FieldName,field.FullFieldType);
		}
		
		internal virtual string DropForeignKey(string table,string externalTable)
		{
			return string.Format(DropForeignKeyString,table,externalTable);
		}
		
		internal string CreateForeignKey(ForeignKey key)
		{
			string field="";
			foreach (string str in key.InternalFields)
			{
				field+=str+",";
			}
			field=field.Substring(0,field.Length-1);
			string Foreigns = "";
			foreach (string str in key.ExternalFields)
			{
				Foreigns+=str+",";
			}
			Foreigns=Foreigns.Substring(0,Foreigns.Length-1);
			return string.Format(CreateForeignKeyString,key.InternalTable,field,key.ExternalTable,Foreigns,key.OnUpdate,key.OnDelete);
		}
		
		internal string CreateTable(ExtractedTableMap table)
		{
			string fields = "";
			foreach (ExtractedFieldMap efm in table.Fields)
			{
				fields+="\t"+efm.FieldName+" "+efm.FullFieldType+",\n";
			}
			fields = fields.Substring(0,fields.Length-2);
			return String.Format(CreateTableString,table.TableName,fields);
		}
		#endregion
		
		#region Inserts
		internal string Insert(Table table,out List<IDbDataParameter> insertParameters,out string select,out List<IDbDataParameter> selectParameters)
		{
			TableMap map = ClassMapper.GetTableMap(table.GetType());
			insertParameters=new List<IDbDataParameter>();
			selectParameters=new List<IDbDataParameter>();
			string whereConditions = "";
			select=null;
			try{
				string values="";
				string parameters="";
				foreach (FieldNamePair fnp in ClassMapper.GetTableMap(table.GetType()).FieldNamePairs)
				{
					if (map[fnp] is ExternalFieldMap)
					{
						if (!((ExternalFieldMap)map[fnp]).IsArray)
						{
							ExternalFieldMap efm = (ExternalFieldMap)map[fnp];
							TableMap relatedTableMap = ClassMapper.GetTableMap(efm.Type);
							if (table.GetField(fnp.ClassFieldName) == null)
							{
								foreach (FieldMap fm in relatedTableMap.PrimaryKeys)
								{
									values += conn.Pool.CorrectName(efm.AddOnName+"_"+relatedTableMap.GetTableFieldName(fm)) + ",";
									insertParameters.Add(conn.CreateParameter(conn.Pool.CorrectName(CreateParameterName(efm.AddOnName+"_"+relatedTableMap.GetTableFieldName(fm))), null));
									parameters+=","+conn.Pool.CorrectName(CreateParameterName(efm.AddOnName+"_"+relatedTableMap.GetTableFieldName(fm)));
									whereConditions+=" AND "+conn.Pool.CorrectName(efm.AddOnName+"_"+relatedTableMap.GetTableFieldName(fm))+" IS NULL ";
								}
							}
							else
							{
								Table relatedTable = (Table)table.GetField(fnp.ClassFieldName);
								foreach (FieldMap fm in relatedTableMap.PrimaryKeys)
								{
									values += conn.Pool.CorrectName(efm.AddOnName+"_"+relatedTableMap.GetTableFieldName(fm)) + ",";
									insertParameters.Add(conn.CreateParameter(conn.Pool.CorrectName(CreateParameterName(efm.AddOnName+"_"+relatedTableMap.GetTableFieldName(fm))), relatedTable.GetField(relatedTableMap.GetClassFieldName(fm)),((InternalFieldMap)fm).FieldType,((InternalFieldMap)fm).FieldLength));
									parameters+=","+conn.Pool.CorrectName(CreateParameterName(efm.AddOnName+"_"+relatedTableMap.GetTableFieldName(fm)));
									whereConditions+=" AND "+conn.Pool.CorrectName(efm.AddOnName+"_"+relatedTableMap.GetTableFieldName(fm))+" =  "+conn.Pool.CorrectName(CreateParameterName(efm.AddOnName+"_"+relatedTableMap.GetTableFieldName(fm)));
								}
							}
						}
					}
					else if (!map[fnp].AutoGen&&!map[fnp].IsArray)
					{
						values += fnp.TableFieldName + ",";
						parameters+=","+CreateParameterName(fnp.TableFieldName);
						if (table.IsFieldNull(fnp.ClassFieldName))
						{
							insertParameters.Add(conn.CreateParameter(CreateParameterName(fnp.TableFieldName),null));
							whereConditions+=" AND "+fnp.TableFieldName+" IS NULL ";
						}
						else
						{
							insertParameters.Add(conn.CreateParameter(CreateParameterName(fnp.TableFieldName), table.GetField(fnp.ClassFieldName),((InternalFieldMap)map[fnp]).FieldType,((InternalFieldMap)map[fnp]).FieldLength));
							whereConditions+=" AND "+fnp.TableFieldName+" = "+CreateParameterName(fnp.TableFieldName);
						}
					}
				}
				values=values.Substring(0,values.Length-1);
				parameters=parameters.Substring(1);
				whereConditions=whereConditions.Substring(4);
				if (map.ContainsAutogenField)
				{
					foreach (InternalFieldMap f in map.InternalPrimaryKeys)
					{
						if (f.AutoGen)
						{
							select=string.Format(SelectMaxWithConditions,f.FieldName,map.Name,whereConditions);
							selectParameters.AddRange(insertParameters);
							break;
						}
					}
				}
				return string.Format(InsertString,map.Name,values,parameters);
			}catch (Exception e)
			{
				System.Diagnostics.Debug.WriteLine(e.Message);
				return null;
			}
		}
		
		internal string Insert(string table,string fields,string parameters)
		{
			return string.Format(InsertString,table,fields,parameters);
		}
		#endregion
		
		#region Deletes
		internal string Delete(Table table,out List<IDbDataParameter> parameters)
		{
			parameters = new List<IDbDataParameter>();
			try{
				string conditions="";
				TableMap map = ClassMapper.GetTableMap(table.GetType());
				foreach(InternalFieldMap ifm in map.PrimaryKeys)
				{
					conditions+=ifm.FieldName+" = "+pool.CorrectName(CreateParameterName(ifm.FieldName))+" AND ";
					parameters.Add(conn.CreateParameter(pool.CorrectName(CreateParameterName(ifm.FieldName)),table.GetField(map.GetClassFieldName(ifm.FieldName)),ifm.FieldType,ifm.FieldLength));
				}
				return string.Format(DeleteWithConditions,map.Name,conditions);
			}catch(Exception e)
			{
				System.Diagnostics.Debug.WriteLine(e.Message);
				return null;
			}
		}
		
		internal string Delete(string tableName,string conditions)
		{
			return string.Format(DeleteWithConditions,tableName,conditions);
		}
		#endregion
		
		#region Updates
		internal Dictionary<string,List<List<IDbDataParameter>>> UpdateMapArray(Table table,ExternalFieldMap efm)
		{
			Dictionary<string, List<List<IDbDataParameter>>> ret = new Dictionary<string, List<List<IDbDataParameter>>>();
			try{
				TableMap map = ClassMapper.GetTableMap(table.GetType());
				Table[] values = (Table[])table.GetField(map.GetClassFieldName(efm));
				if (values!=null)
				{
					TableMap relatedMap = ClassMapper.GetTableMap(efm.Type);
					string delString = "DELETE FROM " + map.Name + "_" + relatedMap.Name + " WHERE ";
					List<IDbDataParameter> pars = new List<IDbDataParameter>();
					foreach (InternalFieldMap ifm in map.PrimaryKeys)
					{
						delString += map.Name+"_"+ifm.FieldName + " = " + CreateParameterName(map.Name+"_"+ifm.FieldName) + " AND ";
						pars.Add(conn.CreateParameter(CreateParameterName(map.Name+"_" + ifm.FieldName), table.GetField(map.GetClassFieldName(ifm)),ifm.FieldType,ifm.FieldLength));
					}
					ret.Add(delString.Substring(0, delString.Length - 4),new List<List<IDbDataParameter>>());
					ret[delString.Substring(0, delString.Length - 4)].Add(pars);
					delString = "INSERT INTO " + map.Name + "_" + relatedMap.Name + "(";
					string valueString = "VALUES(";
					foreach (InternalFieldMap ifm in map.PrimaryKeys)
					{
						delString += map.Name+"_"+ifm.FieldName + ",";
						valueString += CreateParameterName(map.Name +"_"+ ifm.FieldName) + ",";
					}
					foreach (InternalFieldMap ifm in relatedMap.PrimaryKeys)
					{
						delString += relatedMap.Name+"_"+ifm.FieldName + ",";
						valueString += CreateParameterName(relatedMap.Name+"_" + ifm.FieldName) + ",";
					}
					delString = delString.Substring(0, delString.Length - 1) + ") " + valueString.Substring(0, valueString.Length - 1) + ")";
					ret.Add(delString,new List<List<IDbDataParameter>>());
					foreach (Table t in values)
					{
						foreach (InternalFieldMap ifm in relatedMap.PrimaryKeys)
						{
							for (int x = 0; x < pars.Count; x++)
							{
								if (pars[x].ParameterName == CreateParameterName(relatedMap.Name+"_" + ifm.FieldName))
								{
									pars.RemoveAt(x);
									break;
								}
							}
							pars.Add(conn.CreateParameter(CreateParameterName(relatedMap.Name+"_"+ifm.FieldName), t.GetField(relatedMap.GetClassFieldName(ifm)),ifm.FieldType,ifm.FieldLength));
						}
						ret[delString].Add(pars);
					}
				}
			}catch (Exception e)
			{
				System.Diagnostics.Debug.WriteLine(e.Message);
				return null;
			}
			return ret;
		}
		
		internal string Update(Table table,out List<IDbDataParameter> queryParameters)
		{
			queryParameters = new List<IDbDataParameter>();
			if ((table.ChangedFields==null)||(table.ChangedFields.Count==0))
				return "";
			try{
				TableMap map = ClassMapper.GetTableMap(table.GetType());
				string fields = "";
				string conditions = "";
				List<string> changedFields=table.ChangedFields;
				foreach (FieldNamePair fnp in map.FieldNamePairs)
				{
					if (changedFields.Contains(fnp.ClassFieldName))
					{
						if (map[fnp] is ExternalFieldMap)
						{
							if (!((ExternalFieldMap)map[fnp]).IsArray)
							{
								ExternalFieldMap efm = (ExternalFieldMap)map[fnp];
								TableMap relatedTableMap = ClassMapper.GetTableMap(efm.Type);
								if (table.GetField(fnp.ClassFieldName) == null)
								{
									foreach (FieldMap fm in relatedTableMap.PrimaryKeys)
									{
										fields += conn.Pool.CorrectName(efm.AddOnName+"_"+relatedTableMap.GetTableFieldName(fm)) + " = " + CreateParameterName(conn.Pool.CorrectName(efm.AddOnName+"_"+relatedTableMap.GetTableFieldName(fm))) + ", ";
										queryParameters.Add(conn.CreateParameter(CreateParameterName(conn.Pool.CorrectName(efm.AddOnName+"_"+relatedTableMap.GetTableFieldName(fm))), null));
									}
								}
								else
								{
									Table relatedTable = (Table)table.GetField(fnp.ClassFieldName);
									foreach (FieldMap fm in relatedTableMap.PrimaryKeys)
									{
										fields += conn.Pool.CorrectName(efm.AddOnName+"_"+relatedTableMap.GetTableFieldName(fm)) + " = " + CreateParameterName(conn.Pool.CorrectName(efm.AddOnName+"_"+relatedTableMap.GetTableFieldName(fm))) + ", ";
										queryParameters.Add(conn.CreateParameter(CreateParameterName(conn.Pool.CorrectName(efm.AddOnName+"_"+relatedTableMap.GetTableFieldName(fm))), relatedTable.GetField(relatedTableMap.GetClassFieldName(fm)),((InternalFieldMap)fm).FieldType,((InternalFieldMap)fm).FieldLength));
									}
								}
								if (map[fnp].PrimaryKey || !map.HasPrimaryKeys)
								{
									Table relatedTable = (Table)table.GetInitialPrimaryValue(fnp.ClassFieldName);
									if (relatedTable == null)
									{
										foreach (FieldMap fm in relatedTableMap.PrimaryKeys)
										{
											conditions += conn.Pool.CorrectName(efm.AddOnName+"_"+relatedTableMap.GetTableFieldName(fm)) + " is null AND ";
										}
									}
									else
									{
										foreach (FieldMap fm in relatedTableMap.PrimaryKeys)
										{
											conditions += conn.Pool.CorrectName(efm.AddOnName+"_"+relatedTableMap.GetTableFieldName(fm)) + " = "+CreateParameterName("init_" + conn.Pool.CorrectName(efm.AddOnName+"_"+relatedTableMap.GetTableFieldName(fm))) + " AND ";
											queryParameters.Add(conn.CreateParameter(CreateParameterName("init_" + conn.Pool.CorrectName(efm.AddOnName+"_"+relatedTableMap.GetTableFieldName(fm))), relatedTable.GetField(relatedTableMap.GetClassFieldName(fm)),((InternalFieldMap)fm).FieldType,((InternalFieldMap)fm).FieldLength));
										}
									}
								}
							}
						}
						else if (!map[fnp].IsArray)
						{
							fields += fnp.TableFieldName+" = "+CreateParameterName(fnp.TableFieldName) +", ";
							if (table.GetField(fnp.ClassFieldName) == null)
							{
								queryParameters.Add(conn.CreateParameter(CreateParameterName(fnp.TableFieldName), null));
							}
							else
							{
								queryParameters.Add(conn.CreateParameter(CreateParameterName(fnp.TableFieldName), table.GetField(fnp.ClassFieldName),((InternalFieldMap)map[fnp]).FieldType,((InternalFieldMap)map[fnp]).FieldLength));
							}
							if (map[fnp].PrimaryKey || !map.HasPrimaryKeys)
							{
								if (table.GetInitialPrimaryValue(fnp.ClassFieldName) == null)
								{
									conditions += fnp.TableFieldName + " IS NULL AND ";
								}
								else
								{
									conditions += fnp.TableFieldName + " = "+CreateParameterName("init_"+fnp.TableFieldName) +" AND ";
									queryParameters.Add(conn.CreateParameter(CreateParameterName("init_" + fnp.TableFieldName), table.GetInitialPrimaryValue(fnp.ClassFieldName),((InternalFieldMap)map[fnp]).FieldType,((InternalFieldMap)map[fnp]).FieldLength));
								}
							}
						}
					}
				}
				fields = fields.Substring(0,fields.Length-2);
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
						ExternalFieldMap efm = (ExternalFieldMap)map[fnp];
						TableMap relatedMap = ClassMapper.GetTableMap(efm.Type);
						foreach (InternalFieldMap ifm in relatedMap.PrimaryKeys)
						{
							fields+="table_"+count.ToString()+"."+pool.CorrectName(efm.AddOnName+"_"+ifm.FieldName)+",";
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
						if (!map[fnp].IsArray)
						{
							ExternalFieldMap efm = (ExternalFieldMap)map[fnp];
							foreach (InternalFieldMap ifm in ClassMapper.GetTableMap(efm.Type).PrimaryKeys)
							{
								fields+=",main_table."+pool.CorrectName(efm.AddOnName+"_"+ifm.FieldName);
							}
						}
					}
					else if (!map[fnp].IsArray)
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
			return Select(type,null,out pars);
		}
		
		internal string SelectMax(System.Type type,string maxField,List<SelectParameter> parameters,out List<IDbDataParameter> queryParameters)
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
						bool found=false;
						foreach (FieldNamePair f in map.FieldNamePairs)
						{
							if (par.FieldName==f.ClassFieldName)
							{
								where+=" AND "+f.TableFieldName+" = "+CreateParameterName("parameter_"+parCount.ToString());
								queryParameters.Add(conn.CreateParameter(CreateParameterName("parameter_"+parCount.ToString()),par.FieldValue,((InternalFieldMap)map[f]).FieldType,((InternalFieldMap)map[f]).FieldLength));
								parCount++;
								found=true;
								break;
							}else if (par.FieldName==f.TableFieldName)
							{
								where+=" AND "+f.TableFieldName+" = "+CreateParameterName("parameter_"+parCount.ToString());
								queryParameters.Add(conn.CreateParameter(CreateParameterName("parameter_"+parCount.ToString()),par.FieldValue,((InternalFieldMap)map[f]).FieldType,((InternalFieldMap)map[f]).FieldLength));
								parCount++;
								found=true;
								break;
							}
						}
						if (!found)
						{
							where+=" AND "+par.FieldName+" = "+CreateParameterName("parameter_"+parCount.ToString());
							queryParameters.Add(conn.CreateParameter(CreateParameterName("parameter_"+parCount.ToString()),par.FieldValue));
							parCount++;
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
		
		internal string Select(System.Type type,List<SelectParameter> parameters,out List<IDbDataParameter> queryParameters)
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
						bool found=false;
						foreach (FieldNamePair f in map.FieldNamePairs)
						{
							if (par.FieldName==f.ClassFieldName)
							{
								where+=" AND "+f.TableFieldName+" = "+CreateParameterName("parameter_"+parCount.ToString());
								queryParameters.Add(conn.CreateParameter(CreateParameterName("parameter_"+parCount.ToString()),par.FieldValue,((InternalFieldMap)map[f]).FieldType,((InternalFieldMap)map[f]).FieldLength));
								parCount++;
								found=true;
								break;
							}else if (par.FieldName==f.TableFieldName)
							{
								where+=" AND "+f.TableFieldName+" = "+CreateParameterName("parameter_"+parCount.ToString());
								queryParameters.Add(conn.CreateParameter(CreateParameterName("parameter_"+parCount.ToString()),par.FieldValue,((InternalFieldMap)map[f]).FieldType,((InternalFieldMap)map[f]).FieldLength));
								parCount++;
								found=true;
								break;
							}
						}
						if (!found)
						{
							where+=" AND "+par.FieldName+" = "+CreateParameterName("parameter_"+parCount.ToString());
							queryParameters.Add(conn.CreateParameter(CreateParameterName("parameter_"+parCount.ToString()),par.FieldValue));
							parCount++;
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
