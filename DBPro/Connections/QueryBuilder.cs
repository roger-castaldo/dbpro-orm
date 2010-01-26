/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 23/10/2008
 * Time: 11:45 AM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */

using Org.Reddragonit.Dbpro.Connections.Parameters;
using System;
using System.Collections.Generic;
using System.Data;
using Org.Reddragonit.Dbpro.Structure;
using Org.Reddragonit.Dbpro.Structure.Mapping;
using ExtractedFieldMap = Org.Reddragonit.Dbpro.Connections.ExtractedFieldMap;
using ExtractedTableMap = Org.Reddragonit.Dbpro.Connections.ExtractedTableMap;
using FieldNamePair = Org.Reddragonit.Dbpro.Structure.Mapping.TableMap.FieldNamePair;
using FieldType = Org.Reddragonit.Dbpro.Structure.Attributes.FieldType;
using VersionTypes = Org.Reddragonit.Dbpro.Structure.Attributes.VersionField.VersionTypes;

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

        protected virtual string SelectMinWithConditions
        {
            get { return "SELECT MIN({0}) FROM {1} WHERE {2}"; }
        }

        protected virtual string SelectMinWithoutConditions
        {
            get { return "SELECT Min({0}) FROM {1}"; }
        }
		
		protected virtual string OrderBy
		{
			get{return "{0} ORDER BY {1}";}
		}

		protected virtual string SelectWithPagingIncludeOffset
		{
			get { return "{0} LIMIT {1},{2}"; }
		}
		
		protected virtual string SelectCountString{
			get{return "SELECT COUNT(*) FROM({0}) tbl";}
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
		
		internal virtual string AlterFieldType(string table, ExtractedFieldMap field,ExtractedFieldMap oldFieldInfo)
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
				foreach (FieldNamePair fnp in map.FieldNamePairs)
				{
					if (map[fnp] is ExternalFieldMap)
					{
						if (!((ExternalFieldMap)map[fnp]).IsArray)
						{
							ExternalFieldMap efm = (ExternalFieldMap)map[fnp];
							TableMap relatedTableMap = ClassMapper.GetTableMap(efm.Type);
							if (table.GetField(fnp.ClassFieldName) == null)
							{
								foreach (InternalFieldMap fm in relatedTableMap.PrimaryKeys)
								{
									values += conn.Pool.CorrectName(efm.AddOnName+"_"+fm.FieldName) + ",";
                                    insertParameters.Add(conn.CreateParameter(conn.Pool.CorrectName(CreateParameterName(efm.AddOnName + "_" + fm.FieldName)), null, fm.FieldType, fm.FieldLength));
									parameters+=","+conn.Pool.CorrectName(CreateParameterName(efm.AddOnName+"_"+fm.FieldName));
                                    whereConditions += " AND " + conn.Pool.CorrectName(efm.AddOnName + "_" + fm.FieldName) + " IS NULL ";
								}
							}
							else
							{
								Table relatedTable = (Table)table.GetField(fnp.ClassFieldName);
								foreach (InternalFieldMap ifm in relatedTableMap.PrimaryKeys)
								{
                                    object val = null;
                                    if (relatedTableMap.GetClassFieldName(ifm) == null)
                                        val = LocateFieldValue(relatedTable, relatedTableMap, ifm.FieldName,_pool);
                                    else
                                        val = relatedTable.GetField(relatedTableMap.GetClassFieldName(ifm));
                                    string fieldName = relatedTableMap.GetTableFieldName(ifm);
                                    if (fieldName == null)
                                        fieldName = ifm.FieldName;
									values += conn.Pool.CorrectName(efm.AddOnName+"_"+fieldName) + ",";
									insertParameters.Add(conn.CreateParameter(conn.Pool.CorrectName(CreateParameterName(efm.AddOnName+"_"+fieldName)), val,ifm.FieldType,ifm.FieldLength));
									parameters+=","+conn.Pool.CorrectName(CreateParameterName(efm.AddOnName+"_"+fieldName));
									whereConditions+=" AND "+conn.Pool.CorrectName(efm.AddOnName+"_"+fieldName)+" =  "+conn.Pool.CorrectName(CreateParameterName(efm.AddOnName+"_"+fieldName));
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
							insertParameters.Add(conn.CreateParameter(CreateParameterName(fnp.TableFieldName),null,((InternalFieldMap)map[fnp]).FieldType,((InternalFieldMap)map[fnp]).FieldLength));
							whereConditions+=" AND "+fnp.TableFieldName+" IS NULL ";
						}
						else
						{
							if (((InternalFieldMap)map[fnp]).FieldType==FieldType.ENUM)
							{
								insertParameters.Add(conn.CreateParameter(CreateParameterName(fnp.TableFieldName), conn.Pool.GetEnumID(map[fnp].ObjectType,table.GetField(fnp.ClassFieldName).ToString())));
								whereConditions+=" AND "+fnp.TableFieldName+" = "+CreateParameterName(fnp.TableFieldName);
							}else{
								insertParameters.Add(conn.CreateParameter(CreateParameterName(fnp.TableFieldName), table.GetField(fnp.ClassFieldName),((InternalFieldMap)map[fnp]).FieldType,((InternalFieldMap)map[fnp]).FieldLength));
								whereConditions+=" AND "+fnp.TableFieldName+" = "+CreateParameterName(fnp.TableFieldName);
							}
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
				Logger.LogLine(e.Message);
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
                    object val = null;
                    if (map.GetClassFieldName(ifm) == null)
                        val = LocateFieldValue(table,map , ifm.FieldName,_pool);
                    else
                        val = table.GetField(map.GetClassFieldName(ifm));
					parameters.Add(conn.CreateParameter(pool.CorrectName(CreateParameterName(ifm.FieldName)),val,ifm.FieldType,ifm.FieldLength));
				}
                if (conditions.Length > 0)
                    conditions = conditions.Substring(0, conditions.Length - 4);
				return string.Format(DeleteWithConditions,map.Name,conditions);
			}catch(Exception e)
			{
				Logger.LogLine(e.Message);
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
                        delString += "parent_" + ifm.FieldName + " = " + CreateParameterName("parent_" + ifm.FieldName) + " AND ";
                        if (map.GetClassFieldName(ifm) == null)
                            pars.Add(conn.CreateParameter(CreateParameterName("parent_" + ifm.FieldName), LocateFieldValue(table,map,ifm.FieldName,pool), ifm.FieldType, ifm.FieldLength));
                        else
                            pars.Add(conn.CreateParameter(CreateParameterName("parent_" + ifm.FieldName), table.GetField(map.GetClassFieldName(ifm)), ifm.FieldType, ifm.FieldLength));
					}
					ret.Add(delString.Substring(0, delString.Length - 4),new List<List<IDbDataParameter>>());
					ret[delString.Substring(0, delString.Length - 4)].Add(pars);
					delString = "INSERT INTO " + map.Name + "_" + relatedMap.Name + "(";
					string valueString = "VALUES(";
					foreach (InternalFieldMap ifm in map.PrimaryKeys)
					{
						delString += "parent_"+ifm.FieldName + ",";
                        valueString += CreateParameterName("parent_" + ifm.FieldName) + ",";
					}
					foreach (InternalFieldMap ifm in relatedMap.PrimaryKeys)
					{
						delString += "child_"+ifm.FieldName + ",";
                        valueString += CreateParameterName("child_" + ifm.FieldName) + ",";
					}
					delString = delString.Substring(0, delString.Length - 1) + ") " + valueString.Substring(0, valueString.Length - 1) + ")";
					ret.Add(delString,new List<List<IDbDataParameter>>());
					foreach (Table t in values)
					{
						foreach (InternalFieldMap ifm in relatedMap.PrimaryKeys)
						{
							for (int x = 0; x < pars.Count; x++)
							{
								if (pars[x].ParameterName == CreateParameterName("child_" + ifm.FieldName))
								{
									pars.RemoveAt(x);
									break;
								}
							}
                            object val = null;
                            if (relatedMap.GetClassFieldName(ifm) == null)
                                val = LocateFieldValue(t, relatedMap, ifm.FieldName,_pool);
                            else
                                val = t.GetField(relatedMap.GetClassFieldName(ifm));
							pars.Add(conn.CreateParameter(CreateParameterName("child_"+ifm.FieldName), val,ifm.FieldType,ifm.FieldLength));
						}
                        List<IDbDataParameter> tmp = new List<IDbDataParameter>();
                        tmp.AddRange(pars.ToArray());
                        ret[delString].Add(tmp);
					}
				}
			}catch (Exception e)
			{
				Logger.LogLine(e.Message);
				return null;
			}
			return ret;
		}

        internal static object LocateFieldValue(Table table, TableMap relatedMap,string fieldName,ConnectionPool pool)
        {
            if (relatedMap.GetClassFieldName(fieldName) != null)
                return table.GetField(relatedMap.GetClassFieldName(fieldName));
            foreach (Type t in relatedMap.ForeignTables)
            {
                foreach (ExternalFieldMap efm in relatedMap.GetFieldInfoForForeignTable(t))
                {
                    if (fieldName.StartsWith(efm.AddOnName + "_"))
                    {
                        TableMap etm = ClassMapper.GetTableMap(efm.ObjectType);
                        foreach (InternalFieldMap ifm in etm.Fields)
                        {
                            if (pool.CorrectName(efm.AddOnName + "_" + ifm.FieldName) == fieldName)
                            {
                                if (etm.GetClassFieldName(ifm) != null)
                                    return ((Table)table.GetField(relatedMap.GetClassFieldName(efm))).GetField(etm.GetClassFieldName(ifm));
                                else
                                {
                                    object obj = LocateFieldValue((Table)table.GetField(relatedMap.GetClassFieldName(efm)), etm, ifm.FieldName,pool);
                                    if (obj != null)
                                        return obj;
                                }
                            }
                        }
                    }
                }
            }
            return null;
        }
		
		internal string Update(Table table,out List<IDbDataParameter> queryParameters)
		{
			queryParameters = new List<IDbDataParameter>();
            if ((table.ChangedFields == null) || (table.ChangedFields.Count == 0))
                return "";
			try{
				TableMap map = ClassMapper.GetTableMap(table.GetType());
				string fields = "";
				string conditions = "";
                bool addedAutogenCorrection = false;
				List<string> changedFields=table.ChangedFields;
                if (changedFields == null)
                {
                    changedFields = new List<string>();
                    foreach (FieldNamePair fnp in map.FieldNamePairs)
                    {
                        changedFields.Add(fnp.ClassFieldName);
                    }
                }
                if (changedFields.Count == 0)
                    return "";
				foreach (FieldNamePair fnp in map.FieldNamePairs)
				{
                    if (changedFields.Contains(fnp.ClassFieldName))
                    {
                        if (map[fnp].PrimaryKey && !map[fnp].AutoGen && !addedAutogenCorrection && map.HasComplexNumberAutogen)
                        {
                            foreach (InternalFieldMap ifm in map.PrimaryKeys)
                            {
                                if (ifm.AutoGen)
                                {
                                    fields += pool.CorrectName(ifm.FieldName) + " = (SELECT (CASE WHEN MAX(" + pool.CorrectName(ifm.FieldName) + ") IS NULL THEN 0 ELSE MAX(" + pool.CorrectName(ifm.FieldName) + ") END)+1 FROM " + pool.CorrectName(map.Name) + " WHERE ";
                                    int cnt = 0;
                                    foreach (InternalFieldMap i in map.PrimaryKeys)
                                    {
                                        object val = LocateFieldValue(table, map, ifm.FieldName, pool);
                                        if (val == null)
                                            fields += pool.CorrectName(ifm.FieldName) + " IS NULL AND ";
                                        else
                                        {
                                            fields += pool.CorrectName(ifm.FieldName) + " = " + CreateParameterName(ifm.FieldName + "_" + cnt.ToString());
                                            queryParameters.Add(conn.CreateParameter(CreateParameterName(ifm.FieldName + "_" + cnt.ToString()), val));
                                        }
                                    }
                                    fields = fields.Substring(0, fields.Length - 4);
                                    fields += "), ";
                                    break;
                                }
                            }
                            addedAutogenCorrection = true;
                        }
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
                                        fields += conn.Pool.CorrectName(efm.AddOnName + "_" + relatedTableMap.GetTableFieldName(fm)) + " = " + CreateParameterName(conn.Pool.CorrectName(efm.AddOnName + "_" + relatedTableMap.GetTableFieldName(fm))) + ", ";
                                        queryParameters.Add(conn.CreateParameter(CreateParameterName(conn.Pool.CorrectName(efm.AddOnName + "_" + relatedTableMap.GetTableFieldName(fm))), null));
                                    }
                                }
                                else
                                {
                                    Table relatedTable = (Table)table.GetField(fnp.ClassFieldName);
                                    foreach (FieldMap fm in relatedTableMap.PrimaryKeys)
                                    {
                                        fields += conn.Pool.CorrectName(efm.AddOnName + "_" + relatedTableMap.GetTableFieldName(fm)) + " = " + CreateParameterName(conn.Pool.CorrectName(efm.AddOnName + "_" + relatedTableMap.GetTableFieldName(fm))) + ", ";
                                        queryParameters.Add(conn.CreateParameter(CreateParameterName(conn.Pool.CorrectName(efm.AddOnName + "_" + relatedTableMap.GetTableFieldName(fm))), relatedTable.GetField(relatedTableMap.GetClassFieldName(fm)), ((InternalFieldMap)fm).FieldType, ((InternalFieldMap)fm).FieldLength));
                                    }
                                }
                            }
                        }
                        else if (!map[fnp].IsArray)
                        {
                            fields += fnp.TableFieldName + " = " + CreateParameterName(fnp.TableFieldName) + ", ";
                            if (table.GetField(fnp.ClassFieldName) == null)
                            {
                                queryParameters.Add(conn.CreateParameter(CreateParameterName(fnp.TableFieldName), null));
                            }
                            else
                            {
                                queryParameters.Add(conn.CreateParameter(CreateParameterName(fnp.TableFieldName), table.GetField(fnp.ClassFieldName), ((InternalFieldMap)map[fnp]).FieldType, ((InternalFieldMap)map[fnp]).FieldLength));
                            }
                        }
                    }
				}
                foreach (FieldNamePair fnp in map.FieldNamePairs)
                {
                    if (map[fnp].PrimaryKey || !map.HasPrimaryKeys)
                    {
                        if (map[fnp] is ExternalFieldMap)
                        {
                            Table obj = (Table)table.GetInitialPrimaryValue(fnp.ClassFieldName);
                            ExternalFieldMap efm = (ExternalFieldMap)map[fnp];
                            TableMap emap = ClassMapper.GetTableMap(efm.ObjectType);
                            if (obj == null)
                            {
                                foreach (FieldMap fm in emap.PrimaryKeys)
                                {
                                    conditions += conn.Pool.CorrectName(efm.AddOnName + "_" + emap.GetTableFieldName(fm)) + " IS NULL AND ";
                                }
                            }
                            else
                            {
                                foreach (FieldMap fm in emap.PrimaryKeys)
                                {
                                    conditions += conn.Pool.CorrectName(efm.AddOnName + "_" + emap.GetTableFieldName(fm)) + " = " + CreateParameterName(conn.Pool.CorrectName("init_" + efm.AddOnName + "_" + emap.GetTableFieldName(fm)))+" AND ";
                                    queryParameters.Add(conn.CreateParameter(CreateParameterName(conn.Pool.CorrectName("init_"+efm.AddOnName + "_" + emap.GetTableFieldName(fm))), obj.GetField(emap.GetClassFieldName(fm)), ((InternalFieldMap)fm).FieldType, ((InternalFieldMap)fm).FieldLength));
                                }
                            }
                        }
                        else
                        {
                            if (table.GetInitialPrimaryValue(fnp.ClassFieldName) == null)
                            {
                                conditions += fnp.TableFieldName + " IS NULL AND ";
                            }
                            else
                            {
                                conditions += fnp.TableFieldName + " = " + CreateParameterName("init_" + fnp.TableFieldName) + " AND ";
                                queryParameters.Add(conn.CreateParameter(CreateParameterName("init_" + fnp.TableFieldName), table.GetInitialPrimaryValue(fnp.ClassFieldName), ((InternalFieldMap)map[fnp]).FieldType, ((InternalFieldMap)map[fnp]).FieldLength));
                            }
                        }
                    }
                }
                if (fields.Length == 0)
                    return "";
				fields = fields.Substring(0,fields.Length-2);
				if (conditions.Length>0)
				{
					return String.Format(UpdateWithConditions,map.Name,fields,conditions.Substring(0,conditions.Length-4));
				}else
					return String.Format(UpdateWithoutConditions,map.Name,fields);
			}catch (Exception e)
			{
				Logger.LogLine(e.Message);
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
            int origCount = count;
            string tables = map.Name + " table_" + origCount.ToString() + ", ";
            count++;
            tables += GetSubqueryTable(parentMap, map.ParentType, ref count);
            tables+=" table_" + ((int)(count + 1)).ToString();
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
							fields+="table_"+origCount.ToString()+"."+pool.CorrectName(efm.AddOnName+"_"+ifm.FieldName)+",";
						}
					}
				}else{
					fields+="table_"+origCount.ToString()+"."+fnp.TableFieldName+",";
				}
			}
			foreach (InternalFieldMap ifm in parentMap.PrimaryKeys)
			{
                if (!fields.Contains("table_"+origCount.ToString()+"."+ifm.FieldName+","))
                    fields+="table_"+origCount.ToString()+"."+ifm.FieldName+",";
				where+=" table_"+origCount.ToString()+"."+ifm.FieldName+" = table_"+(count+1).ToString()+"."+ifm.FieldName+" AND";
			}
			count++;
			foreach (string str in map.ParentDatabaseFieldNames)
			{
				fields+="table_"+count.ToString()+"."+str+",";
			}
			fields=fields.Substring(0,fields.Length-1);
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
					where = where.Substring(0,where.Length-4);
				}
			}catch (Exception e)
			{
				return false;
			}
			return true;
		}
		
		internal string SelectAll(System.Type type,string[] OrderByFields)
		{
			List<IDbDataParameter> pars = new List<IDbDataParameter>();
			return Select(type,new SelectParameter[0],out pars,OrderByFields);
		}
		
		internal string SelectMax(System.Type type,string maxField,List<SelectParameter> parameters,out List<IDbDataParameter> queryParameters)
		{
			if (parameters==null)
				return SelectMax(type,maxField,new SelectParameter[0],out queryParameters);
			else
				return SelectMax(type,maxField,parameters.ToArray(),out queryParameters);
		}
		
		internal string SelectMax(System.Type type,string maxField,SelectParameter[] parameters,out List<IDbDataParameter> queryParameters)
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
				if (map.GetTableFieldName(maxField)!=null)
					fields=map.GetTableFieldName(maxField);
				else
					fields = maxField;
				if ((parameters!=null)&&(parameters.Length>0))
				{
					startAnd=(where.Length>0);
					string appended="";
					int parCount=0;
					foreach (SelectParameter par in parameters)
					{
						appended+="("+par.ConstructString(map,conn,this,ref queryParameters,ref parCount)+") AND ";
					}
					appended=appended.Substring(0,appended.Length-4);
					if (!startAnd)
						where = "("+appended+")";
					else
						where+=" AND ("+appended+")";
				}
				if (where.Length>0)
					return String.Format(SelectMaxWithConditions ,fields,joins+tables,where);
				else
					return String.Format(SelectMaxWithoutConditions,fields,joins+tables);
			}else
				return null;
		}

        internal string SelectMin(System.Type type, string maxField, List<SelectParameter> parameters, out List<IDbDataParameter> queryParameters)
        {
            if (parameters == null)
                return SelectMin(type, maxField, new SelectParameter[0], out queryParameters);
            else
                return SelectMin(type, maxField, parameters.ToArray(), out queryParameters);
        }

        internal string SelectMin(System.Type type, string maxField, SelectParameter[] parameters, out List<IDbDataParameter> queryParameters)
        {
            TableMap map = ClassMapper.GetTableMap(type);
            string fields = "";
            string tables = "";
            string joins = "";
            string where = "";
            bool startAnd = false;
            queryParameters = new List<IDbDataParameter>();
            if (ObtainFieldTableWhereList(out fields, out tables, out joins, out where, type))
            {
                if (map.GetTableFieldName(maxField) != null)
                    fields = map.GetTableFieldName(maxField);
                else
                    fields = maxField;
                if ((parameters != null) && (parameters.Length > 0))
                {
                    startAnd = (where.Length > 0);
                    string appended = "";
                    int parCount = 0;
                    foreach (SelectParameter par in parameters)
                    {
                        appended += "(" + par.ConstructString(map, conn, this, ref queryParameters, ref parCount) + ") AND ";
                    }
                    appended = appended.Substring(0, appended.Length - 4);
                    if (!startAnd)
                        where = "(" + appended + ")";
                    else
                        where += " AND (" + appended + ")";
                }
                if (where.Length > 0)
                    return String.Format(SelectMinWithConditions, fields, joins + tables, where);
                else
                    return String.Format(SelectMinWithoutConditions, fields, joins + tables);
            }
            else
                return null;
        }
		
		internal string Select(System.Type type,List<SelectParameter> parameters,out List<IDbDataParameter> queryParameters,string[] OrderByFields)
		{
			if (parameters==null)
				return Select(type,new SelectParameter[0],out queryParameters,OrderByFields);
			else
				return Select(type,parameters.ToArray(),out queryParameters,OrderByFields);
		}

        internal string Select(System.Type type, SelectParameter[] parameters, out List<IDbDataParameter> queryParameters, string[] OrderByFields)
		{
			TableMap map = ClassMapper.GetTableMap(type);
			string fields="";
			string tables="";
			string joins="";
			string where="";
			bool startAnd=false;
			queryParameters=new List<IDbDataParameter>();
            string order = "";
            if ((OrderByFields != null) && (OrderByFields.Length > 0))
            {
                foreach (string str in OrderByFields)
                {
                    if (map[str] == null)
                        order += str + ",";
                    else
                        order += ((InternalFieldMap)map[str]).FieldName + ",";
                }
                order = order.Substring(0, order.Length - 1);
            }
			if (ObtainFieldTableWhereList(out fields,out tables, out joins,out where,type))
			{
				if ((parameters!=null)&&(parameters.Length>0))
				{
					startAnd=(where.Length>0);
					string appended="";
					int parCount=0;
					foreach (SelectParameter par in parameters)
					{
						appended+="("+par.ConstructString(map,conn,this,ref queryParameters,ref parCount)+") AND ";
					}
					appended=appended.Substring(0,appended.Length-4);
					if (!startAnd)
						where = "("+appended+")";
					else
						where+=" AND ("+appended+")";
				}
                if (order.Length > 0)
                {
                    if (where.Length > 0)
                        return String.Format(OrderBy,String.Format(SelectWithConditions, fields, joins + tables, where),order);
                    else
                        return String.Format(OrderBy,String.Format(SelectWithoutConditions, fields, joins + tables),order);
                }
                else
                {
                    if (where.Length > 0)
                        return String.Format(SelectWithConditions, fields, joins + tables, where);
                    else
                        return String.Format(SelectWithoutConditions, fields, joins + tables);
                }
			}else
				return null;
		}
		
		internal string SelectCount(System.Type type,List<SelectParameter> parameters,out List<IDbDataParameter> queryParameters)
		{
			if (parameters==null)
				return SelectCount(type,new SelectParameter[0],out queryParameters);
			else
				return SelectCount(type,parameters.ToArray(),out queryParameters);
		}
		
		internal string SelectCount(System.Type type,SelectParameter[] parameters,out List<IDbDataParameter> queryParameters)
		{
			string query=Select(type,parameters,out queryParameters,null);
			return String.Format(SelectCountString,query);
		}

        internal string SelectPaged(System.Type type, List<SelectParameter> parameters, out List<IDbDataParameter> queryParameters, ulong? start, ulong? recordCount)
        {
            return SelectPaged(type, parameters, out queryParameters, start, recordCount, null);
        }
		
		internal string SelectPaged(System.Type type,List<SelectParameter> parameters,out List<IDbDataParameter> queryParameters,ulong? start,ulong? recordCount,string[] OrderByFields)
		{
			if (parameters==null)
				return SelectPaged(type,new SelectParameter[0],out queryParameters,start,recordCount,OrderByFields);
			else
				return SelectPaged(type,parameters.ToArray(),out queryParameters,start,recordCount,OrderByFields);
		}

        internal string SelectPaged(System.Type type, SelectParameter[] parameters, out List<IDbDataParameter> queryParameters, ulong? start, ulong? recordCount)
        {
            return SelectPaged(type, parameters, out queryParameters, start, recordCount, null);
        }
		
		internal virtual string SelectPaged(System.Type type,SelectParameter[] parameters,out List<IDbDataParameter> queryParameters,ulong? start,ulong? recordCount,string[] OrderByFields)
		{
			string query = Select(type,parameters,out queryParameters,OrderByFields);
			if (queryParameters==null)
				queryParameters = new List<IDbDataParameter>();
			if (!start.HasValue)
				start=0;
			if (!recordCount.HasValue)
				recordCount=0;
			queryParameters.Add(conn.CreateParameter(CreateParameterName("startIndex"),(long)start.Value));
			queryParameters.Add(conn.CreateParameter(CreateParameterName("rowCount"),(long)recordCount.Value));
			return String.Format(SelectWithPagingIncludeOffset,query,CreateParameterName("startIndex"),CreateParameterName("rowCount"));
		}

        internal string SelectPaged(string baseQuery, TableMap mainMap, ref List<IDbDataParameter> queryParameters, ulong? start, ulong? recordCount)
        {
            return SelectPaged(baseQuery, mainMap, ref queryParameters, start, recordCount, null);
        }

        internal virtual string SelectPaged(string baseQuery,TableMap mainMap, ref List<IDbDataParameter> queryParameters, ulong? start, ulong? recordCount,string[] OrderByFields)
        {
            if (!start.HasValue)
                start = 0;
            if (!recordCount.HasValue)
                recordCount = 0;
            queryParameters.Add(conn.CreateParameter(CreateParameterName("startIndex"), (long)start.Value));
            queryParameters.Add(conn.CreateParameter(CreateParameterName("rowCount"), (long)recordCount.Value));
            return String.Format(SelectWithPagingIncludeOffset, baseQuery, CreateParameterName("startIndex"), CreateParameterName("rowCount"));
        }
		#endregion
	}
}
