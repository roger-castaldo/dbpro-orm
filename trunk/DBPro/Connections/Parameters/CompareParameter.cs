/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 05/04/2009
 * Time: 7:09 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using Org.Reddragonit.Dbpro.Structure.Attributes;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using Org.Reddragonit.Dbpro.Structure.Mapping;
using FieldNamePair = Org.Reddragonit.Dbpro.Structure.Mapping.TableMap.FieldNamePair;

namespace Org.Reddragonit.Dbpro.Connections.Parameters
{
	/// <summary>
	/// Description of CompareParameter.
	/// </summary>
	public abstract class CompareParameter : SelectParameter
	{
		private string _fieldName;
		private object _fieldValue;
		
		public CompareParameter(string fieldName, object fieldValue)
		{
			this._fieldName = fieldName;
			this._fieldValue = fieldValue;
		}
		
		public string FieldName {
			get { return _fieldName; }
		}
		
		public object FieldValue {
			get { return _fieldValue; }
		}
		
		protected abstract string ComparatorString{
			get;
		}
		
		protected virtual bool SupportsList{
			get{return false;}
		}

        private FieldNamePair? LocateFieldNamePair(string fieldName,TableMap map,out bool ClassBased,out bool isExternal,out TableMap newMap,out string alias)
        {
            FieldNamePair? ret = null;
            ClassBased = false;
            isExternal = false;
            newMap = null;
            alias = null;
            if (fieldName.Contains("."))
            {
                string newField = fieldName.Substring(0, fieldName.IndexOf("."));
                foreach (FieldNamePair f in map.FieldNamePairs)
                {
                    if (f.ClassFieldName == newField)
                    {
                        ret = LocateFieldNamePair(fieldName.Substring(fieldName.IndexOf(".") + 1), ClassMapper.GetTableMap(((ExternalFieldMap)map[f]).ObjectType), out ClassBased, out isExternal, out newMap, out alias);
                        break;
                    }
                }
                alias = fieldName.Substring(0, fieldName.IndexOf("."))+((alias==null) ? "" : "_"+alias);
            }
            else
            {
                newMap = map;
                ClassBased = false;
                isExternal = false;
                foreach (FieldNamePair f in map.FieldNamePairs)
                {
                    if (f.ClassFieldName == fieldName)
                    {
                        isExternal = map[f] is ExternalFieldMap;
                        ClassBased = true;
                        ret = f;
                        break;
                    }
                    else if (f.TableFieldName == fieldName)
                    {
                        isExternal = map[f] is ExternalFieldMap;
                        ret = f;
                        break;
                    }
                }
                if (!ret.HasValue)
                {
                    if (map.ParentType != null)
                        ret = LocateFieldNamePair(fieldName,ClassMapper.GetTableMap(map.ParentType), out ClassBased, out isExternal,out newMap,out alias);
                }
            }
            return ret;
        }

        internal sealed override List<string> Fields
        {
            get { return new List<string>(new string[]{FieldName}); }
        }

        internal sealed override string ConstructString(TableMap map, Connection conn, QueryBuilder builder, ref List<IDbDataParameter> queryParameters, ref int parCount)
        {
            bool found = false;
            string ret = "";
            FieldType? type = null;
            Type _objType = null;
            int fieldLength = 0;
            bool isExternal = false;
            bool isClassBased=false;
            TableMap newMap;
            string alias = "";
            FieldNamePair? fnp = LocateFieldNamePair(FieldName,map, out isClassBased, out isExternal,out newMap,out alias);
            found = fnp.HasValue;
            if ((alias != null) && (alias.Length > 0))
                alias = "main_table_" + alias + ".";
            if (isExternal)
            {
                if (found)
                {
                    if ((alias == "")||(alias==null))
                        alias = "main_table.";
                    ExternalFieldMap efm = (ExternalFieldMap)newMap[fnp.Value];
                    if (efm == null)
                    {
                        TableMap m = ClassMapper.GetTableMap(map.ParentType);
                        while (m[fnp.Value] == null)
                        {
                            if (m.ParentType != null)
                                m = ClassMapper.GetTableMap(m.ParentType);
                            else
                                throw new Exception("Unable to Locate Parent Field.");
                        }
                        efm = (ExternalFieldMap)m[fnp.Value];
                    }
                    TableMap relatedMap = ClassMapper.GetTableMap(efm.Type);
                    foreach (InternalFieldMap ifm in relatedMap.PrimaryKeys)
                    {
                        ret += " AND " + alias + conn.Pool.CorrectName(efm.AddOnName + "_" + ifm.FieldName) + " " + ComparatorString + " ";
                        type = ifm.FieldType;
                        _objType = ifm.ObjectType;
                        fieldLength = ifm.FieldLength;
                        ret += builder.CreateParameterName("parameter_" + parCount.ToString());
                        string className = ifm.FieldName;
                        if (relatedMap.GetClassFieldName(ifm.FieldName)!=null)
                            className=ifm.FieldName;
                        if (_objType == null)
                        {
                            _objType = ((Org.Reddragonit.Dbpro.Structure.Table)FieldValue).GetField(className,true).GetType();
                        }
                        if ((_objType != null) && _objType.IsEnum)
                        {
                            queryParameters.Add(conn.CreateParameter(builder.CreateParameterName("parameter_" + parCount.ToString()), conn.Pool.GetEnumID(_objType, ((Org.Reddragonit.Dbpro.Structure.Table)FieldValue).GetField(className,true).ToString())));
                        }
                        else if (FieldValue == null)
                        {
                            if (type.HasValue)
                                queryParameters.Add(conn.CreateParameter(builder.CreateParameterName("parameter_"+parCount.ToString()),null,type.Value,fieldLength));
                            else
                                queryParameters.Add(conn.CreateParameter(builder.CreateParameterName("parameter_"+parCount.ToString()),null));
                        }
                        else
                        {
                            object val = ((Org.Reddragonit.Dbpro.Structure.Table)FieldValue).GetField(className, true);
                            if (val == null)
                                val = QueryBuilder.LocateFieldValue((Org.Reddragonit.Dbpro.Structure.Table)FieldValue, relatedMap, ifm.FieldName, conn.Pool);
                            if (type.HasValue)
                                queryParameters.Add(conn.CreateParameter(builder.CreateParameterName("parameter_" + parCount.ToString()), val, type.Value, fieldLength));
                            else
                                queryParameters.Add(conn.CreateParameter(builder.CreateParameterName("parameter_" + parCount.ToString()), val));
                        }
                        parCount++;
                    }
                    ret = ret.Substring(4);
                }else
                    throw new Exception("Unable to handler external fields without specifying class name.");
            }
            else
            {
                if ((alias == "") || (alias == null))
                    alias = "main_table.";
                if (fnp.HasValue)
                {
                    InternalFieldMap ifm = (InternalFieldMap)newMap[fnp.Value];
                    if (ifm == null)
                    {
                        TableMap m = ClassMapper.GetTableMap(newMap.ParentType);
                        while (m[fnp.Value] == null)
                        {
                            if (m.ParentType != null)
                                m = ClassMapper.GetTableMap(m.ParentType);
                            else
                                throw new Exception("Unable to Locate Parent Field.");
                        }
                        ifm = (InternalFieldMap)m[fnp.Value];
                    }
                    ret = alias+conn.Pool.CorrectName(fnp.Value.TableFieldName) + " ";
                    type = ifm.FieldType;
                    _objType = ifm.ObjectType;
                    fieldLength = ifm.FieldLength;
                }
                if (!found)
                {
                    ret = FieldName + " ";
                }
                if (SupportsList)
                {
                    ret += ComparatorString + " (";
                    if (FieldValue.GetType().IsArray || (FieldValue is IEnumerable))
                    {
                        foreach (object obj in (IEnumerable)FieldValue)
                        {
                            if (_objType == null)
                                _objType = obj.GetType();
                            ret += builder.CreateParameterName("parameter_" + parCount.ToString()) + ",";
                            if ((_objType != null) && _objType.IsEnum)
                            {
                                queryParameters.Add(conn.CreateParameter(builder.CreateParameterName("parameter_" + parCount.ToString()), conn.Pool.GetEnumID(_objType, obj.ToString())));
                            }
                            else
                            {
                                if (type.HasValue)
                                    queryParameters.Add(conn.CreateParameter(builder.CreateParameterName("parameter_" + parCount.ToString()), obj, type.Value, fieldLength));
                                else
                                    queryParameters.Add(conn.CreateParameter(builder.CreateParameterName("parameter_" + parCount.ToString()), obj));
                            }
                            parCount++;
                        }
                        ret = ret.Substring(0, ret.Length - 1);
                    }
                    else
                    {
                        ret += builder.CreateParameterName("parameter_" + parCount.ToString());
                        if (_objType == null)
                            _objType = FieldValue.GetType();
                        if ((_objType != null) && _objType.IsEnum)
                        {
                            queryParameters.Add(conn.CreateParameter(builder.CreateParameterName("parameter_" + parCount.ToString()), conn.Pool.GetEnumID(_objType, FieldValue.ToString())));
                        }
                        else
                        {
                            if (type.HasValue)
                                queryParameters.Add(conn.CreateParameter(builder.CreateParameterName("parameter_" + parCount.ToString()), FieldValue, type.Value, fieldLength));
                            else
                                queryParameters.Add(conn.CreateParameter(builder.CreateParameterName("parameter_" + parCount.ToString()), FieldValue));
                        }
                        parCount++;
                    }
                    ret += ")";
                }
                else
                {
                    if (FieldValue == null)
                    {
                        ret += ComparatorString + builder.CreateParameterName("parameter_" + parCount.ToString());
                        queryParameters.Add(conn.CreateParameter(builder.CreateParameterName("parameter_" + parCount.ToString()), FieldValue));
                        parCount++;
                    }
                    else if (FieldValue.GetType().IsArray || (FieldValue is ICollection))
                    {
                        string tmp = ret;
                        tmp += ComparatorString;
                        ret += "( ";
                        foreach (object obj in (IEnumerable)FieldValue)
                        {
                            if (_objType == null)
                                _objType = obj.GetType();
                            ret += tmp + builder.CreateParameterName("parameter_" + parCount.ToString()) + " AND ";
                            if ((_objType != null) && _objType.IsEnum)
                            {
                                queryParameters.Add(conn.CreateParameter(builder.CreateParameterName("parameter_" + parCount.ToString()), conn.Pool.GetEnumID(_objType, obj.ToString())));
                            }
                            else
                            {
                                if (type.HasValue)
                                    queryParameters.Add(conn.CreateParameter(builder.CreateParameterName("parameter_" + parCount.ToString()), obj, type.Value, fieldLength));
                                else
                                    queryParameters.Add(conn.CreateParameter(builder.CreateParameterName("parameter_" + parCount.ToString()), obj));
                            }
                            parCount++;
                        }
                        ret = ret.Substring(0, tmp.Length - 4);
                        ret += " )";
                    }
                    else
                    {
                        ret += ComparatorString;
                        ret += builder.CreateParameterName("parameter_" + parCount.ToString());
                        if (_objType == null)
                            _objType = FieldValue.GetType();
                        if ((_objType != null) && _objType.IsEnum)
                        {
                            queryParameters.Add(conn.CreateParameter(builder.CreateParameterName("parameter_" + parCount.ToString()), conn.Pool.GetEnumID(_objType, FieldValue.ToString())));
                        }
                        else
                        {
                            if (type.HasValue)
                                queryParameters.Add(conn.CreateParameter(builder.CreateParameterName("parameter_" + parCount.ToString()), FieldValue, type.Value, fieldLength));
                            else
                                queryParameters.Add(conn.CreateParameter(builder.CreateParameterName("parameter_" + parCount.ToString()), FieldValue));
                        }
                        parCount++;
                    }
                }
            }
            return ret;
        }
	}
}
