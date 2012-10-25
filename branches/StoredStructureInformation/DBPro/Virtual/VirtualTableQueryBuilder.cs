using System;
using System.Collections.Generic;
using System.Text;
using Org.Reddragonit.Dbpro.Virtual.Attributes;
using Org.Reddragonit.Dbpro.Connections;
using System.Reflection;
using System.Data;
using Org.Reddragonit.Dbpro.Connections.PoolComponents;
using Org.Reddragonit.Dbpro.Structure.Attributes;

namespace Org.Reddragonit.Dbpro.Virtual
{
	internal class VirtualTableQueryBuilder
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

		private static List<ExtractedVirtualField> ExtractFieldFromType(Type type)
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

        public static string ConstructQuery(Type type, Connection conn)
        {
            if (type.GetCustomAttributes(typeof(VirtualTableAttribute), true).Length == 0)
                throw new Exception("Unable to execute a Virtual Table Query from a class that does not have a VirtualTableAttribute attached to it.");
            Type mainTable = VirtualTableAttribute.GetMainTableTypeForVirtualTable(type);
            sTable mainMap = conn.Pool.Mapping[mainTable];
            string originalQuery = conn.queryBuilder.SelectAll(mainTable,null);
            string fieldString = "";
            List<ExtractedVirtualField> fields = ExtractFieldFromType(type);
            List<ExtractedVirtualField> fieldsUsed = new List<ExtractedVirtualField>();
            List<TablePath> paths = new List<TablePath>();
            foreach (string prop in mainMap.ForeignTableProperties)
            {
                PropertyInfo pi = mainTable.GetProperty(prop, Utility._BINDING_FLAGS);
                sTable extMap = conn.Pool.Mapping[(pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType)];
                bool nullable = false;
                foreach (object obj in pi.GetCustomAttributes(false))
                {
                    if (obj is INullable)
                    {
                        nullable = ((INullable)obj).Nullable;
                        break;
                    }
                }
                string innerJoin = " INNER JOIN ";
                if (nullable)
                    innerJoin = " LEFT JOIN ";
                if (pi.PropertyType.IsArray)
                {
                    sTable iMap = conn.Pool.Mapping[mainTable, prop];
                    innerJoin += iMap.Name + " main_intermediate_" + prop + " ON ";
                    foreach (sTableField f in iMap.Fields)
                    {
                        if (f.ClassProperty != null)
                            innerJoin += " virtualTable." + f.ExternalField + " = main_intermediate_" + prop + "." + f.Name + " AND ";
                    }
                    innerJoin = innerJoin.Substring(0, innerJoin.Length - 5);
                    innerJoin += " INNER JOIN " + extMap.Name + " main_" + prop + " ON ";
                    foreach (sTableField f in iMap.Fields)
                    {
                        if (f.ClassProperty == null && f.ExternalField != null)
                            innerJoin += " main_intermediate_" + prop + "." + f.Name + " = main_" + prop + "." + f.ExternalField + " AND ";
                    }
                    innerJoin = innerJoin.Substring(0, innerJoin.Length - 5);
                }
                else
                {
                    innerJoin += extMap.Name + " main_" + prop + " ON ";
                    foreach (sTableField f in mainMap[prop])
                        innerJoin += " virtualTable." + f.Name + " = main_" + prop + "." + f.ExternalField + " AND ";
                    innerJoin = innerJoin.Substring(0, innerJoin.Length - 5);
                }
                paths.Add(new TablePath("main_" + prop, innerJoin, (pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType)));
            }
            RecurExtractPaths(ref paths, conn);
            string appendedJoins = "";
            foreach (ExtractedVirtualField field in fields)
            {
                if (field.IsInternal)
                {
                    fieldString += ", virtualTable." + mainMap[field.ClassFieldName][0].Name + " AS " + conn.Pool.CorrectName(field.ClassFieldName);
                    fieldsUsed.Add(field);
                }
                else
                {
                    foreach (TablePath tp in paths)
                    {
                        if (tp.EndTable == field.TablePath)
                        {
                            if (!appendedJoins.Contains(tp.Path))
                                appendedJoins += " " + tp.Path;
                            fieldString += ", " + field.TablePath + "." + conn.Pool.Mapping[tp.EndType][field.FieldName][0].Name + " AS " + conn.Pool.CorrectName(field.ClassFieldName);
                            break;
                        }
                    }
                    if (!appendedJoins.Contains(field.TablePath))
                        throw new Exception("Unable to tie relation from " + mainMap.Name + " through field " + field.FullName);
                }
            }
            return "SELECT " + fieldString.Substring(1) + " FROM (" + originalQuery + ") virtualTable " + appendedJoins;
        }
		
		private static void RecurExtractPaths(ref List<TablePath> paths,Connection conn){
			bool changed=true;
			while (changed){
				changed=false;
				for (int x=0;x<paths.Count;x++){
					TablePath tp = paths[x];
					sTable extMap = conn.Pool.Mapping[tp.EndType];
					bool contains=false;
                    foreach (string prop in extMap.ForeignTableProperties){
                        PropertyInfo pi = tp.EndType.GetProperty(prop, Utility._BINDING_FLAGS);
						if (conn.Pool.Mapping.IsMappableType((pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType))){
                            sTableRelation rel = extMap.GetRelationForProperty(prop).Value;
                            if ((pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType).Equals(tp.EndType))
                            {
                                if (tp.EndTable.EndsWith("_" + prop))
                                {
                                    contains = true;
                                    break;
                                }else{
									foreach (TablePath t in paths){
                                        if (t.EndTable.StartsWith(tp.EndTable) && t.EndTable.EndsWith("_" + prop))
                                        {
											contains=true;
											break;
										}
									}
								}
							}else{
								foreach (TablePath t in paths){
                                    if (t.EndTable == tp.EndTable + "_" + prop)
                                    {
										contains=true;
										break;
									}
								}
							}
						}
					}
					if (contains)
						break;
                    else if (extMap.ForeignTableProperties.Length > 0)
                    {
                        changed = true;
                        foreach (string prop in extMap.ForeignTableProperties)
                        {
                            PropertyInfo pi = tp.EndType.GetProperty(prop, Utility._BINDING_FLAGS);
                            sTable pMap = conn.Pool.Mapping[(pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType)];
                            bool nullable = false;
                            foreach (object obj in pi.GetCustomAttributes(false))
                            {
                                if (obj is INullable)
                                {
                                    nullable = ((INullable)obj).Nullable;
                                    break;
                                }
                            }
                            string innerJoin = " INNER JOIN ";
                            if (nullable)
                                innerJoin = " LEFT JOIN ";
                            if (pi.PropertyType.IsArray)
                            {
                                sTable iMap = conn.Pool.Mapping[tp.EndType, prop];
                                innerJoin += iMap.Name + " " + tp.EndTable + "_intermediate_" + prop + " ON ";
                                foreach (sTableField f in iMap.Fields)
                                {
                                    if (f.ClassProperty != null)
                                        innerJoin += " " + tp.EndTable + "." + f.ExternalField + " = " + tp.EndTable + "_intermediate_" + prop + "." + f.Name + " AND ";
                                }
                                innerJoin = innerJoin.Substring(0, innerJoin.Length - 5);
                                innerJoin += " INNER JOIN " + pMap.Name + " " + tp.EndTable + "_" + prop + " ON ";
                                foreach (sTableField f in iMap.Fields)
                                {
                                    if (f.ClassProperty == null && f.ExternalField != null)
                                        innerJoin += " " + tp.EndTable + "_intermediate_" + prop + "." + f.Name + " = " + tp.EndTable + "_" + prop + "." + f.ExternalField + " AND ";
                                }
                                innerJoin = innerJoin.Substring(0, innerJoin.Length - 5);
                            }
                            else
                            {
                                innerJoin += pMap.Name + " " + tp.EndTable + "_" + prop + " ON ";
                                foreach (sTableField f in extMap[prop])
                                    innerJoin += " " + tp.EndTable + "." + f.Name + " = " + tp.EndTable + "_" + prop + "." + f.ExternalField + " AND ";
                                innerJoin = innerJoin.Substring(0, innerJoin.Length - 5);
                            }
                            paths.Add(new TablePath(tp.EndTable + "_" + prop, tp.Path + innerJoin,(pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType)));
                        }
                    }
				}
			}
		}
	}
}
