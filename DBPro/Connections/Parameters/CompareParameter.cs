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
		
		internal sealed override string ConstructString(TableMap map,Connection conn,QueryBuilder builder,ref List<IDbDataParameter> queryParameters,ref int parCount)
		{
			bool found=false;
			string ret="";
			FieldType? type=null;
			Type _objType=null;
			int fieldLength=0;
			foreach (FieldNamePair f in map.FieldNamePairs)
			{
				if (FieldName==f.ClassFieldName)
				{
					ret=f.TableFieldName+" ";
					type=((InternalFieldMap)map[f]).FieldType;
					_objType = map[f].ObjectType;
					fieldLength=((InternalFieldMap)map[f]).FieldLength;
					found=true;
					break;
				}else if (FieldName==f.TableFieldName)
				{
					ret=f.TableFieldName+" ";
					type=((InternalFieldMap)map[f]).FieldType;
					_objType = map[f].ObjectType;
					fieldLength=((InternalFieldMap)map[f]).FieldLength;
					found=true;
					break;
				}
			}
			if (!found)
			{
				ret=FieldName+" ";
			}
			if (SupportsList)
			{
				ret+=ComparatorString+" (";
				if (FieldValue.GetType().IsArray||(FieldValue is IEnumerable)){
					foreach (object obj in (IEnumerable)FieldValue)
					{
						if (_objType==null)
							_objType=obj.GetType();
						ret+=builder.CreateParameterName("parameter_"+parCount.ToString())+",";
						if ((_objType!=null)&&_objType.IsEnum)
						{
							queryParameters.Add(conn.CreateParameter(builder.CreateParameterName("parameter_"+parCount.ToString()),conn.Pool.GetEnumID(_objType,obj.ToString())));
						}else{
							if (type.HasValue)
								queryParameters.Add(conn.CreateParameter(builder.CreateParameterName("parameter_"+parCount.ToString()),obj,type.Value,fieldLength));
							else
								queryParameters.Add(conn.CreateParameter(builder.CreateParameterName("parameter_"+parCount.ToString()),obj));
						}
						parCount++;
					}
					ret = ret.Substring(0,ret.Length-1);
				}else{
					ret+=builder.CreateParameterName("parameter_"+parCount.ToString());
					if (_objType==null)
							_objType=FieldValue.GetType();
					if ((_objType!=null)&&_objType.IsEnum)
					{
						queryParameters.Add(conn.CreateParameter(builder.CreateParameterName("parameter_"+parCount.ToString()),conn.Pool.GetEnumID(_objType,FieldValue.ToString())));
					}else{
						if (type.HasValue)
							queryParameters.Add(conn.CreateParameter(builder.CreateParameterName("parameter_"+parCount.ToString()),FieldValue,type.Value,fieldLength));
						else
							queryParameters.Add(conn.CreateParameter(builder.CreateParameterName("parameter_"+parCount.ToString()),FieldValue));
					}
					parCount++;
				}
				ret+=")";
			}else{
				if (FieldValue.GetType().IsArray||(FieldValue is ICollection)){
					string tmp = ret;
					tmp+=ComparatorString;
					ret+= "( ";
					foreach (object obj in (IEnumerable)FieldValue)
					{
						if (_objType==null)
							_objType=obj.GetType();
						ret+=tmp+builder.CreateParameterName("parameter_"+parCount.ToString())+" AND ";
						if ((_objType!=null)&&_objType.IsEnum)
						{
							queryParameters.Add(conn.CreateParameter(builder.CreateParameterName("parameter_"+parCount.ToString()),conn.Pool.GetEnumID(_objType,obj.ToString())));
						}else{
							if (type.HasValue)
								queryParameters.Add(conn.CreateParameter(builder.CreateParameterName("parameter_"+parCount.ToString()),obj,type.Value,fieldLength));
							else
								queryParameters.Add(conn.CreateParameter(builder.CreateParameterName("parameter_"+parCount.ToString()),obj));
						}
						parCount++;
					}
					ret = ret.Substring(0,tmp.Length-4);
					ret+=" )";
				}else{
					ret+=ComparatorString;
					ret+=builder.CreateParameterName("parameter_"+parCount.ToString());
					if (_objType==null)
							_objType=FieldValue.GetType();
					if ((_objType!=null)&&_objType.IsEnum)
					{
						queryParameters.Add(conn.CreateParameter(builder.CreateParameterName("parameter_"+parCount.ToString()),conn.Pool.GetEnumID(_objType,FieldValue.ToString())));
					}else{
						if (type.HasValue)
							queryParameters.Add(conn.CreateParameter(builder.CreateParameterName("parameter_"+parCount.ToString()),FieldValue,type.Value,fieldLength));
						else
							queryParameters.Add(conn.CreateParameter(builder.CreateParameterName("parameter_"+parCount.ToString()),FieldValue));
					}
					parCount++;
				}
			}
			return ret;
		}
	}
}
