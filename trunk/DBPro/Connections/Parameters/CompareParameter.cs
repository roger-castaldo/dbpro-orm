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

        private FieldNamePair? LocateFieldNamePair(TableMap map,out bool ClassBased,out bool isExternal)
        {
            ClassBased = false;
            isExternal = false;
            FieldNamePair? ret = null;
            foreach (FieldNamePair f in map.FieldNamePairs)
            {
                if (f.ClassFieldName == FieldName)
                {
                    isExternal = map[f] is ExternalFieldMap;
                    ClassBased = true;
                    ret = f;
                    break;
                }
                else if (f.TableFieldName == FieldName)
                {
                    isExternal = map[f] is ExternalFieldMap;
                    ret = f;
                    break;
                }
            }
            if (!ret.HasValue)
            {
                if (map.ParentType != null)
                    ret = LocateFieldNamePair(ClassMapper.GetTableMap(map.ParentType), out ClassBased, out isExternal);
            }
            return ret;
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
            FieldNamePair? fnp = LocateFieldNamePair(map, out isClassBased, out isExternal);
            found = fnp.HasValue;
            if (isExternal)
            {
                if (found)
                {
                    ExternalFieldMap efm = (ExternalFieldMap)map[fnp.Value];
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
                        ret += " AND " + conn.Pool.CorrectName(efm.AddOnName + "_" + ifm.FieldName) + " " + ComparatorString + " ";
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
                        else
                        {
                            object val = ((Org.Reddragonit.Dbpro.Structure.Table)FieldValue).GetField(className,true);
                            if (val==null)
                                val = QueryBuilder.LocateFieldValue((Org.Reddragonit.Dbpro.Structure.Table)FieldValue,relatedMap,ifm.FieldName,conn.Pool);
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
                if (fnp.HasValue)
                {
                    InternalFieldMap ifm = (InternalFieldMap)map[fnp.Value];
                    if (ifm == null)
                    {

                        TableMap m = ClassMapper.GetTableMap(map.ParentType);
                        while (m[fnp.Value] == null)
                        {
                            if (m.ParentType != null)
                                m = ClassMapper.GetTableMap(m.ParentType);
                            else
                                throw new Exception("Unable to Locate Parent Field.");
                        }
                        ifm = (InternalFieldMap)m[fnp.Value];
                    }
                    ret = conn.Pool.CorrectName(fnp.Value.TableFieldName) + " ";
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
