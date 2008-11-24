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
		
		#region virtual
		protected virtual string SelectWithConditions
		{
			get{return "SELECT {1} FROM {2} WHERE {3}";}
		}
		
		protected virtual string SelectWithoutConditions
		{
			get{return "SELECT {1} FROM {2}";}
		}
		
		protected virtual string SelectMaxWithConditions
		{
			get{return "SELECT MAX({1}) FROM {2} WHERE {3}";}
		}
		
		protected virtual string SelectMaxWithoutConditions
		{
			get{return "SELECT MAX({1}) FROM {2}";}
		}
		
		protected virtual string OrderBy
		{
			get{return " ORDER BY {1}";}
		}
		
		protected virtual string InsertString
		{
			get{return "INSERT INTO {1}({2}) VALUES {3}";}
		}
		
		protected virtual string DeleteWithConditions
		{
			get{return "DELETE FROM {1} WHERE {2}";}
		}
		
		protected virtual string DeleteWithoutConditions
		{
			get{return "DELETE FROM {1}";}
		}
		
		protected virtual string UpdateWithConditions
		{
			get{return "UPDATE {1} SET {2} WHERE {3}";}
		}
		
		protected virtual string UpdateWithoutConditions
		{
			get{return "UPDATE {1} SET {2}";}
		}
		
		protected virtual string AlterFieldTypeString
		{
			get{return "ALTER TABLE {1} ALTER COLUMN {2} TYPE {3}";}
		}
		
		protected virtual string CreatePrimaryKeyString
		{
			get{return "ALTER TABLE {1} ADD PRIMARY KEY({2})";}
		}
		
		protected virtual string CreateNullConstraintString
		{
			get{return "ALTER TABLE {1} ADD CONSTRAINT nn_{2} {2} NOT NULL";}
		}
		
		protected virtual string CreateForiegnKeyString
		{
			get{return "ALTER TABLE {1} ADD FORIEGN KEY ({2}) REFERENCES {3}({4}) ON UPDATE {5} ON DELETE {6}";}
		}
		
		protected virtual string CreateColumnString
		{
			get{return "ALTER TABLE {1} ADD {2} {3}";}
		}
		
		protected virtual string DropColumnString
		{
			get{return "ALTER TABLE {1} DROP {2}";}
		}
		#endregion
		
		#region TableAlterations
		internal string DropColumn(string table, string field)
		{
			return string.Format(DropColumnString,table,field);
		}
		
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
		
		internal string CreateForiegnKey(string table, List<string> fields, string foriegnTable, List<string> foriegnFields,string UpdateAction,string DeleteAction)
		{
			string field="";
			foreach (string str in fields)
			{
				field+=str+",";
			}
			field=field.Substring(0,field.Length-1);
			string foriegns = "";
			foreach (string str in foriegnFields)
			{
				foriegns+=str+",";
			}
			foriegns=foriegns.Substring(0,foriegns.Length-1);
			return string.Format(CreateForiegnKeyString,table,field,foriegnTable,foriegns,UpdateAction,DeleteAction);
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
									pars.Add(new SelectParameter(relatedTableMap.GetTableFieldName(fm),null));
								}
							}
							else
							{
								/*if (!select.Contains("WHERE"))
								{
									select += " WHERE ";
								}
								select = select.Replace(" WHERE ", ", " + relatedTableMap.Name + " WHERE ");*/
								Table relatedTable = (Table)table.GetType().GetProperty(fnp.ClassFieldName).GetValue(table, new object[0]);
								foreach (FieldMap fm in relatedTableMap.PrimaryKeys)
								{
									pars.Add(new SelectParameter(relatedTableMap.GetTableFieldName(fm),relatedTable.GetType().GetProperty(relatedTableMap.GetClassFieldName(fm)).GetValue(relatedTable, new Object[0])));
									/*									values += relatedTableMap.GetTableFieldName(fm) + ",";
									pars.Add(conn.CreateParameter("@" + relatedTableMap.GetTableFieldName(fm), relatedTable.GetType().GetProperty(relatedTableMap.GetClassFieldName(fm)).GetValue(relatedTable, new Object[0])));
									select += ClassMapper.GetTableMap(table.GetType()).Name + "." + relatedTableMap.GetTableFieldName(fm) + " = " + relatedTableMap.Name + "." + relatedTableMap.GetTableFieldName(fm) + " AND ";
									select += relatedTableMap.Name + "." + relatedTableMap.GetTableFieldName(fm) + " = @" + relatedTableMap.GetTableFieldName(fm) + " AND ";*/
								}
								/*foreach (FieldMap fm in relatedTableMap.Fields)
								{
									fields += "," + relatedTableMap.Name + "." + relatedTableMap.GetTableFieldName(fm);
								}*/
							}
						}
					}
					else
					{
						fields += "," + map.Name + "." + fnp.TableFieldName;
						values += fnp.TableFieldName + ",";
						if (table.IsFieldNull(fnp.ClassFieldName))
						{
							insertParameters.Add(conn.CreateParameter("@" + fnp.TableFieldName,null));
							pars.Add(new SelectParameter(fnp.TableFieldName,null));
						}
						else
						{
							insertParameters.Add(conn.CreateParameter("@" + fnp.TableFieldName, table.GetType().GetProperty(fnp.ClassFieldName).GetValue(table, new object[0])));
							pars.Add(new SelectParameter(fnp.TableFieldName,table.GetType().GetProperty(fnp.ClassFieldName).GetValue(table, new object[0])));
						}
					}
				}
				values=values.Substring(0,values.Length-1);
				foreach (InternalFieldMap ifm in map.Fields)
				{
					if (!ifm.AutoGen)
					{
						if (table.IsFieldNull(map.GetClassFieldName(ifm)))
							pars.Add(new SelectParameter(ifm.FieldName,null));
						else
							pars.Add(new SelectParameter(ifm.FieldName,table.GetType().GetProperty(map.GetClassFieldName(ifm)).GetValue(table, new object[0])));
					}
				}
				select = Select(table.GetType(),pars,out selectParameters,conn);
				foreach (InternalFieldMap f in map.InternalPrimaryKeys)
				{
					if (f.AutoGen)
					{
						select+=" AND "+map.GetTableFieldName(f)+" IN "+SelectMax(table.GetType(),map.GetTableFieldName(f),pars,out selectParameters,conn);
					}
				}
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
		private bool ObtainFieldTableWhereList(out string fields,out string tables,out string joins,out string where,System.Type type)
		{
			fields="";
			tables="";
			joins="";
			where="";
			try{
				TableMap map = ClassMapper.GetTableMap(type);
				joins=map.Name;
				foreach (FieldNamePair fnp in map.FieldNamePairs)
				{
					if (map[fnp] is ExternalFieldMap)
					{
						if (!((ExternalFieldMap)map[fnp]).IsArray)
						{
							TableMap relatedTableMap = ClassMapper.GetTableMap(type.GetProperty(fnp.ClassFieldName).PropertyType);
							if (map.GetFieldInfoForForiegnTable(type.GetProperty(fnp.ClassFieldName).PropertyType).Nullable)
							{
								joins += " LEFT JOIN " + relatedTableMap.Name + " ON ";
								foreach (FieldMap fm in relatedTableMap.PrimaryKeys)
								{
									joins += map.Name + "." + relatedTableMap.GetTableFieldName(fm) + " = " + relatedTableMap.Name + "." + relatedTableMap.GetTableFieldName(fm) + " AND ";
								}
								joins = joins.Substring(0, joins.Length - 4);
							}
							else
							{
								tables += "," + relatedTableMap.Name;
								foreach (FieldMap fm in relatedTableMap.PrimaryKeys)
								{
									where += map.Name + "." + relatedTableMap.GetTableFieldName(fm) + " = " + relatedTableMap.Name + "." + relatedTableMap.GetTableFieldName(fm) + " AND ";
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
						fields += "," + map.Name + "." + fnp.TableFieldName;
					}
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
			queryParameters = new List<IDbDataParameter>();
			if (ObtainFieldTableWhereList(out fields,out tables, out joins,out where, type))
			{
				fields = maxField;
				if ((parameters!=null)&&(parameters.Count>0))
				{
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
			queryParameters=new List<IDbDataParameter>();
			if (ObtainFieldTableWhereList(out fields,out tables, out joins,out where,type))
			{
				if ((parameters!=null)&&(parameters.Count>0))
				{
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
