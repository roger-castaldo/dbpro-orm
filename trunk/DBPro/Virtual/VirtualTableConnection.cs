using System;
using System.Collections.Generic;
using System.Text;
using Org.Reddragonit.Dbpro.Virtual.Attributes;
using Org.Reddragonit.Dbpro.Connections;
using System.Reflection;
using Org.Reddragonit.Dbpro.Structure.Mapping;

namespace Org.Reddragonit.Dbpro.Virtual
{
	public class VirtualTableConnection
	{
		private struct ExtractedVirtualField
		{

			public bool IsInternal{
				get{return !_fieldName.Contains(".");}
			}
			
			public string TablePath{
				get{
					if (IsInternal)
						return "";
					else{
						return "main_"+_fieldName.Substring(0,_fieldName.LastIndexOf(".")).Replace(".","_");
					}
				}
			}
			
			private string _fieldName;
			public string FieldName
			{
				get {
					if (IsInternal)
						return _fieldName;
					else
						return _fieldName.Substring(_fieldName.LastIndexOf(".")+1);
				}
			}
			
			public string FullName{
				get{return _fieldName;}
			}

			private string _classFieldName;
			public string ClassFieldName
			{
				get { return _classFieldName; }
			}

			public ExtractedVirtualField(VirtualField field, string classFieldName)
			{
				_fieldName = field.FieldName;
				_classFieldName = classFieldName;
			}
		}
		
		private struct TablePath{
			private string _endTable;
			public string EndTable{
				get{return _endTable;}
			}
			
			private string _path;
			public string Path{
				get{return _path;}
			}
			
			private Type _endType;
			public Type EndType{
				get{return _endType;}
			}
			
			public TablePath(string endTable,string path,Type endType)
			{
				_endTable=endTable;
				_path=path;
				_endType=endType;
			}
		}

		private List<ExtractedVirtualField> ExtractFieldFromType(Type type)
		{
			List<ExtractedVirtualField> ret = new List<ExtractedVirtualField>();
			foreach (PropertyInfo pi in type.GetProperties())
			{
				if (pi.GetCustomAttributes(typeof(VirtualField), true).Length > 0)
				{
					ret.Add(new ExtractedVirtualField((VirtualField)pi.GetCustomAttributes(typeof(VirtualField),true)[0],pi.Name));
				}
			}
			return ret;
		}

		public List<object> SelectVirtualTable(Type type)
		{
			if (type.GetCustomAttributes(typeof(VirtualTableAttribute), true).Length == 0)
				throw new Exception("Unable to execute a Virtual Table Query from a class that does not have a VirtualTableAttribute attached to it.");
			List<object> ret = new List<object>();
			Type mainTable = VirtualTableAttribute.GetMainTableTypeForVirtualTable(type);
			Connection conn = ConnectionPoolManager.GetConnection(mainTable).getConnection();
			TableMap mainMap = ClassMapper.GetTableMap(mainTable);
			string originalQuery = conn.queryBuilder.SelectAll(mainTable);
			string fieldString = "";
			List<ExtractedVirtualField> fields = ExtractFieldFromType(type);
			List<ExtractedVirtualField> fieldsUsed = new List<ExtractedVirtualField>();
			List<TablePath> paths = new List<TablePath>();
			foreach (TableMap.FieldNamePair fnp in mainMap.FieldNamePairs){
				if (mainMap[fnp] is ExternalFieldMap){
					ExternalFieldMap efm = (ExternalFieldMap)mainMap[fnp];
					TableMap extMap = ClassMapper.GetTableMap(efm.Type);
					string innerJoin = " INNER JOIN ";
					if (efm.Nullable)
						innerJoin=" LEFT JOIN ";
					if (efm.IsArray){
						innerJoin+=conn.Pool.CorrectName(mainMap.Name+"_"+extMap.Name)+" main_intermediate_"+fnp.ClassFieldName +" ON ";
						foreach (InternalFieldMap ifm in mainMap.PrimaryKeys)
							innerJoin+=" virtualTable."+conn.Pool.CorrectName(ifm.FieldName)+" = main_intermediate_"+fnp.ClassFieldName +"."+conn.Pool.CorrectName("PARENT_"+ifm.FieldName)+" AND ";
						innerJoin=innerJoin.Substring(0,innerJoin.Length-5);
						innerJoin+=" INNER JOIN "+conn.Pool.CorrectName(extMap.Name)+" main_"+fnp.ClassFieldName+" ON ";
						foreach (InternalFieldMap ifm in extMap.PrimaryKeys)
							innerJoin+=" main_intermediate_"+fnp.ClassFieldName+"."+conn.Pool.CorrectName("CHILD_"+ifm.FieldName)+" = main_"+fnp.ClassFieldName+"."+conn.Pool.CorrectName(ifm.FieldName)+" AND ";
						innerJoin=innerJoin.Substring(0,innerJoin.Length-5);
					}else{
						innerJoin+=conn.Pool.CorrectName(extMap.Name)+" main_"+fnp.ClassFieldName+" ON ";
						foreach (InternalFieldMap ifm in extMap.PrimaryKeys)
							innerJoin+=" virtualTable."+conn.Pool.CorrectName(efm.AddOnName+"_"+ifm.FieldName)+" = main_"+fnp.ClassFieldName+"."+conn.Pool.CorrectName(ifm.FieldName)+" AND ";
						innerJoin=innerJoin.Substring(0,innerJoin.Length-5);
					}
					paths.Add(new TablePath("main_"+fnp.ClassFieldName,innerJoin,efm.Type));
				}
			}
			RecurExtractPaths(ref paths,conn);
			string appendedJoins = "";
			foreach (ExtractedVirtualField field in fields)
			{
				if (field.IsInternal){
					fieldString += ", virtualTable." + mainMap.GetTableFieldName(field.FieldName) + " AS " + field.ClassFieldName;
					fieldsUsed.Add(field);
				}
				else{
					foreach (TablePath tp in paths){
						if (tp.EndTable == field.TablePath){
							if (!appendedJoins.Contains(tp.Path))
								appendedJoins+=" "+tp.Path;
							fieldString+=", "+field.TablePath+"."+conn.Pool.CorrectName(ClassMapper.GetTableMap(tp.EndType).GetTableFieldName(field.FieldName))+" AS "+field.ClassFieldName;
							break;
						}
					}
					if (!appendedJoins.Contains(field.TablePath))
						throw new Exception("Unable to tie relation from "+mainMap.Name+" through field "+field.FullName);
				}
			}
			fieldString = "SELECT " + fieldString.Substring(1) + " FROM (" + originalQuery+") virtualTable "+appendedJoins;
			conn.ExecuteQuery(fieldString);
			while (conn.Read()){
				object obj = type.GetConstructor(Type.EmptyTypes).Invoke(new object[0]);
				for (int x=0;x<conn.FieldCount;x++){
					PropertyInfo pi = type.GetProperty(fields[x].ClassFieldName);
					if (conn.IsDBNull(x))
						pi.SetValue(obj,null,new object[0]);
					else
						pi.SetValue(obj,conn.GetValue(x),new object[0]);
				}
				ret.Add(obj);
			}
			conn.CloseConnection();
			return ret;
		}
		
		private void RecurExtractPaths(ref List<TablePath> paths,Connection conn){
			bool changed=true;
			while (changed){
				changed=false;
				for (int x=0;x<paths.Count;x++){
					TablePath tp = paths[x];
					TableMap extMap = ClassMapper.GetTableMap(tp.EndType);
					bool contains=false;
					foreach (TableMap.FieldNamePair fnp in extMap.FieldNamePairs){
						if (extMap[fnp] is ExternalFieldMap){
							if (((ExternalFieldMap)extMap[fnp]).IsSelfRelated){
								if (tp.EndTable.EndsWith("_"+fnp.ClassFieldName))
								{
									contains=true;
									break;
								}else{
									foreach (TablePath t in paths){
										if (t.EndTable.StartsWith(tp.EndTable)&&t.EndTable.EndsWith("_"+fnp.ClassFieldName)){
											contains=true;
											break;
										}
									}
								}
							}else{
								foreach (TablePath t in paths){
									if (t.EndTable==tp.EndTable+"_"+fnp.ClassFieldName){
										contains=true;
										break;
									}
								}
							}
						}
					}
					if (contains)
						break;
					else if (extMap.ExternalFieldMaps.Count>0){
						changed=true;
						foreach (TableMap.FieldNamePair fnp in extMap.FieldNamePairs){
							if (extMap[fnp] is ExternalFieldMap){
								ExternalFieldMap efm = (ExternalFieldMap)extMap[fnp];
								TableMap eMap = ClassMapper.GetTableMap(efm.Type);
								string innerJoin = " INNER JOIN ";
								if (efm.Nullable)
									innerJoin=" LEFT JOIN ";
								if (efm.IsArray){
									innerJoin+=conn.Pool.CorrectName(eMap.Name+"_"+eMap.Name)+" "+tp.EndTable+"_intermediate_"+fnp.ClassFieldName +" ON ";
									foreach (InternalFieldMap ifm in extMap.PrimaryKeys)
										innerJoin+=" "+tp.EndTable+"."+conn.Pool.CorrectName(ifm.FieldName)+" = "+tp.EndTable+"_intermediate_"+fnp.ClassFieldName +"."+conn.Pool.CorrectName("PARENT_"+ifm.FieldName)+" AND ";
									innerJoin=innerJoin.Substring(0,innerJoin.Length-5);
									innerJoin+=" INNER JOIN "+conn.Pool.CorrectName(eMap.Name)+" "+tp.EndTable+"_"+fnp.ClassFieldName+" ON ";
									foreach (InternalFieldMap ifm in eMap.PrimaryKeys)
										innerJoin+=" "+tp.EndTable+"_intermediate_"+fnp.ClassFieldName+"."+conn.Pool.CorrectName("CHILD_"+ifm.FieldName)+" = "+tp.EndTable+"_"+fnp.ClassFieldName+"."+conn.Pool.CorrectName(ifm.FieldName)+" AND ";
									innerJoin=innerJoin.Substring(0,innerJoin.Length-5);
								}else{
									innerJoin+=conn.Pool.CorrectName(eMap.Name)+" "+tp.EndTable+"_"+fnp.ClassFieldName+" ON ";
									foreach (InternalFieldMap ifm in eMap.PrimaryKeys)
										innerJoin+=" "+tp.EndTable+"."+conn.Pool.CorrectName(efm.AddOnName+"_"+ifm.FieldName)+" = "+tp.EndTable+"_"+fnp.ClassFieldName+"."+conn.Pool.CorrectName(ifm.FieldName)+" AND ";
									innerJoin=innerJoin.Substring(0,innerJoin.Length-5);
								}
								paths.Add(new TablePath(tp.EndTable+"_"+fnp.ClassFieldName,tp.Path+innerJoin,efm.Type));
							}
						}
					}
				}
			}
		}
	}
}
