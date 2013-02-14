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
using Org.Reddragonit.Dbpro.Connections.PoolComponents;
using System.Reflection;

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

        protected virtual bool CaseInsensitive
        {
            get { return false; }
        }

        private string LocateTableField(string fieldName,Type tableType,out bool ClassBased,out bool isExternal,out Type newType,out string alias)
        {
            string ret = null;
            ClassBased = false;
            isExternal = false;
            newType = null;
            alias = null;
            if (fieldName.Contains("."))
            {
                string newField = fieldName.Substring(0, fieldName.IndexOf("."));
                PropertyInfo pi = tableType.GetProperty(newField, Utility._BINDING_FLAGS_WITH_INHERITANCE);
                ret = LocateTableField(fieldName.Substring(fieldName.IndexOf(".") + 1), (pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType), out ClassBased, out isExternal, out newType, out alias);
                alias = fieldName.Substring(0, fieldName.IndexOf("."))+((alias==null) ? "" : "_"+alias);
            }
            else
            {
                newType = tableType;
                ClassBased = false;
                isExternal = false;
                ConnectionPool pool = ConnectionPoolManager.GetConnection(tableType);
                sTable map = pool.Mapping[tableType];
                foreach (sTableField fld in map.Fields)
                {
                    if (fld.ClassProperty == fieldName)
                    {
                        if (fld.ExternalField != null && fld.Type!=FieldType.ENUM)
                        {
                            if (new List<string>(map.PrimaryKeyProperties).Contains(fieldName))
                                alias = "";
                            else if ((alias==null ? "" : alias)!="")
                                alias = fieldName+(alias == null ? "" : "_" + alias);
                            ret = fieldName;
                            newType = tableType.GetProperty(fieldName, Utility._BINDING_FLAGS_WITH_INHERITANCE).PropertyType;
                            if (newType.IsArray)
                                newType = newType.GetElementType();
                            isExternal = true;
                            ClassBased = true;
                            break;
                        }
                        else
                        {
                            isExternal = false;
                            ClassBased = true;
                            ret = fld.Name;
                            break;
                        }
                    }
                    else if (fld.Name == fieldName)
                    {
                        ret = fieldName;
                        isExternal = fld.ExternalField != null && fld.Type != FieldType.ENUM;
                        ClassBased = false;
                        break;
                    }
                }
                if (ret==null)
                {
                    if (pool.Mapping.IsMappableType(tableType.BaseType))
                        ret = LocateTableField(fieldName,tableType.BaseType, out ClassBased, out isExternal,out newType,out alias);
                }
            }
            return ret;
        }

        internal sealed override List<string> Fields
        {
            get { return new List<string>(new string[]{FieldName}); }
        }

        internal sealed override string ConstructString(Type tableType, Connection conn, QueryBuilder builder, ref List<IDbDataParameter> queryParameters, ref int parCount)
        {
            bool found = false;
            string ret = "";
            FieldType? type=null;
            Type _objType = null;
            int fieldLength = 0;
            bool isExternal = false;
            bool isClassBased=false;
            Type newType;
            string alias = "";
            string fldName = LocateTableField(FieldName,tableType, out isClassBased, out isExternal,out newType,out alias);
            found = fldName != null;
            if ((alias != null) && (alias.Length > 0))
                alias = "main_table_" + alias + ".";
            if (isExternal)
            {
                if (found)
                {
                    if ((alias == "")||(alias==null))
                        alias = "main_table.";
                    sTable relatedMap = conn.Pool.Mapping[newType];
                    if (isClassBased)
                    {
                        sTable map = conn.Pool.Mapping[tableType];
                        if (FieldValue != null)
                        {
                            foreach (string prop in relatedMap.PrimaryKeyProperties)
                            {
                                foreach (sTableField fld in relatedMap[prop])
                                {
                                    foreach (sTableField f in map[fldName])
                                    {
                                        if (f.ExternalField == fld.Name)
                                        {
                                            ret += " AND " + (this.CaseInsensitive ? "UPPER(" : "") + alias + f.Name + (this.CaseInsensitive ? ")" : "") + " " + ComparatorString + " " + builder.CreateParameterName("parameter_" + parCount.ToString());
                                            object val = QueryBuilder.LocateFieldValue((Org.Reddragonit.Dbpro.Structure.Table)FieldValue, fld, conn.Pool);
                                            queryParameters.Add(conn.CreateParameter(builder.CreateParameterName("parameter_" + parCount.ToString()), val, f.Type, f.Length));
                                            parCount++;
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        else
                        {
                            foreach (sTableField f in map[fldName])
                            {
                                ret += " AND " + (this.CaseInsensitive ? "UPPER(" : "") + alias + f.Name + (this.CaseInsensitive ? ")" : "") + " " + ComparatorString + " " + builder.CreateParameterName("parameter_" + parCount.ToString());
                                queryParameters.Add(conn.CreateParameter(builder.CreateParameterName("parameter_" + parCount.ToString()), null, f.Type, f.Length));
                                parCount++;
                            }
                        }
                    }
                    else
                    {
                        foreach (string prop in relatedMap.PrimaryKeyProperties)
                        {
                            sTableField[] flds = relatedMap[prop];
                            if (flds[0].ExternalField != null)
                            {
                                Org.Reddragonit.Dbpro.Structure.Table tbl = null;
                                if (FieldValue != null)
                                    tbl = (Org.Reddragonit.Dbpro.Structure.Table)QueryBuilder.LocateFieldValue((Org.Reddragonit.Dbpro.Structure.Table)FieldValue, flds[0], conn.Pool);
                                if (tbl != null)
                                {
                                    sTable relMap = conn.Pool.Mapping[tbl.GetType()];
                                    foreach (sTableField fld in relMap.Fields)
                                    {
                                        foreach (sTableField f in flds)
                                        {
                                            if (fld.Name == f.ExternalField)
                                            {
                                                ret += " AND " + (this.CaseInsensitive ? "UPPER(" : "") + alias + flds[0].Name + (this.CaseInsensitive ? ")" : "") + " " + ComparatorString + " " + builder.CreateParameterName("parameter_" + parCount.ToString());
                                                object val = QueryBuilder.LocateFieldValue(tbl, fld, conn.Pool);
                                                queryParameters.Add(conn.CreateParameter(builder.CreateParameterName("parameter_" + parCount.ToString()), val, fld.Type, fld.Length));
                                                parCount++;
                                                break;
                                            }
                                        }
                                    }
                                }
                                else
                                {
                                    foreach (sTableField fld in flds)
                                    {
                                        ret += " AND " + (this.CaseInsensitive ? "UPPER(" : "") + alias + flds[0].Name + (this.CaseInsensitive ? ")" : "") + " " + ComparatorString + " " + builder.CreateParameterName("parameter_" + parCount.ToString());
                                        queryParameters.Add(conn.CreateParameter(builder.CreateParameterName("parameter_" + parCount.ToString()), null, fld.Type, fld.Length));
                                        parCount++;
                                    }
                                }
                            }
                            else
                            {
                                type = flds[0].Type;
                                fieldLength = flds[0].Length;
                                ret += " AND " + (this.CaseInsensitive ? "UPPER(" : "") + alias + flds[0].Name + (this.CaseInsensitive ? ")" : "") + " " + ComparatorString + " " + builder.CreateParameterName("parameter_" + parCount.ToString());
                                if (type == FieldType.ENUM)
                                    queryParameters.Add(conn.CreateParameter(builder.CreateParameterName("parameter_" + parCount.ToString()), conn.Pool.GetEnumID(newType.GetProperty(prop, Utility._BINDING_FLAGS).PropertyType, ((Org.Reddragonit.Dbpro.Structure.Table)FieldValue).GetField(prop).ToString())));
                                else if (FieldValue == null)
                                    queryParameters.Add(conn.CreateParameter(builder.CreateParameterName("parameter_" + parCount.ToString()), null, type.Value, fieldLength));
                                else
                                {
                                    object val = ((Org.Reddragonit.Dbpro.Structure.Table)FieldValue).GetField(prop);
                                    if (val == null)
                                        val = QueryBuilder.LocateFieldValue((Org.Reddragonit.Dbpro.Structure.Table)FieldValue, flds[0], conn.Pool);
                                    queryParameters.Add(conn.CreateParameter(builder.CreateParameterName("parameter_" + parCount.ToString()), val, type.Value, fieldLength));
                                }
                                parCount++;
                            }
                        }
                    }
                    ret = ret.Substring(4);
                }else
                    throw new Exception("Unable to handler external fields without specifying class name.");
            }
            else
            {
                if ((alias == "") || (alias == null))
                    alias = "main_table.";
                if (fldName != null)
                {
                    ret = (this.CaseInsensitive ? "UPPER(" : "") + alias + fldName + " " + (this.CaseInsensitive ? ")" : "");
                    foreach (sTableField fld in conn.Pool.Mapping[newType].Fields)
                    {
                        if (fld.Name == fldName)
                        {
                            type = fld.Type;
                            fieldLength = fld.Length;
                            break;
                        }
                    }
                }
                if (!found)
                    ret = FieldName + " ";
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
                            queryParameters.Add(conn.CreateParameter(builder.CreateParameterName("parameter_" + parCount.ToString()), conn.Pool.GetEnumID(_objType, FieldValue.ToString())));
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
                        ret += ComparatorString +" "+ builder.CreateParameterName("parameter_" + parCount.ToString());
                        queryParameters.Add(conn.CreateParameter(builder.CreateParameterName("parameter_" + parCount.ToString()), FieldValue));
                        parCount++;
                    }
                    else if (FieldValue.GetType().IsArray || (FieldValue is ICollection))
                    {
                        string tmp = ret;
                        tmp += ComparatorString+ " ";
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
                        ret += ComparatorString+" ";
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
