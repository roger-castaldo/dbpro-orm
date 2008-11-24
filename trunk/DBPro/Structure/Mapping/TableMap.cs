/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 19/03/2008
 * Time: 9:40 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */

using System;
using System.Collections.Generic;
using System.Reflection;
using Org.Reddragonit.Dbpro.Structure.Attributes;

namespace Org.Reddragonit.Dbpro.Structure.Mapping
{
	internal class TableMap
	{
		
		public struct FieldNamePair{
			public string ClassFieldName;
			public string TableFieldName;
			
			public FieldNamePair(string classFieldName,string tableFieldName)
			{
				ClassFieldName=classFieldName;
				TableFieldName=tableFieldName;
			}
		}
		
		private string _tableName;
		private Dictionary<string,FieldMap> _fields;
		
		private VersionField.VersionTypes? _versionType=null;
		public VersionField.VersionTypes? VersionType
		{
			get{
				return _versionType;
			}
		}
		
		public TableMap(System.Type type,Assembly asm,MemberInfo[] info,ref Dictionary<System.Type,TableMap> map) : this(null,type,asm,info,ref map)
		{}
		
		public TableMap(string TableName,System.Type type,Assembly asm,MemberInfo[] info,ref Dictionary<System.Type,TableMap> map)
		{
			if (TableName ==null)
			{
				foreach (object obj in type.GetCustomAttributes(true))
				{
					if (obj is Org.Reddragonit.Dbpro.Structure.Attributes.Table)
					{
						Org.Reddragonit.Dbpro.Structure.Attributes.Table t = (Org.Reddragonit.Dbpro.Structure.Attributes.Table)obj;
						TableName=t.TableName;
					}
				}
			}
			_tableName=TableName;
			_fields = new Dictionary<string,FieldMap>();
			foreach (MemberInfo mi in info)
			{
				foreach (object obj in mi.GetCustomAttributes(true))
				{
					if (obj is IField)
					{
						_fields.Add(mi.Name,new InternalFieldMap(mi));
					}else if  (obj is IForiegnField)
					{
						System.Type ty = asm.GetType(mi.ToString().Substring(0,mi.ToString().IndexOf(" ")).Replace("[]",""),false);
						TableMap t;
						if (!map.ContainsKey(ty)){
							t= new TableMap(ty,asm,ty.GetMembers(BindingFlags.Public |      //Get public members
							                                     BindingFlags.NonPublic |   //Get private/protected/internal members
							                                     BindingFlags.Static |      //Get static members
							                                     BindingFlags.Instance |    //Get instance members
							                                     BindingFlags.DeclaredOnly ),ref map);
							map.Add(ty,t);
						}else
						{
							t=map[ty];
						}
						_fields.Add(mi.Name,new ExternalFieldMap(ty,mi));
					}
					if (obj is IVersionField)
					{
						if (!_versionType.HasValue)
							_versionType=((IVersionField)obj).VersionType;
						else if (_versionType.Value!=((IVersionField)obj).VersionType)
							throw new Exception("Cannot use two different version  types in the same table.");
					}
				}
			}
			int autoGenCount=0;
			foreach (InternalFieldMap  pkf in PrimaryKeys)
			{
				if (pkf.AutoGen)
				{
					autoGenCount++;
				}
			}
			if (autoGenCount > 1)
			{
				throw new Exception("Unable to produce database map due to invalid content.  You cannot have more than one autogen primary key field in a table. Class=" + type.Name);
			}
		}
		
		public List<InternalFieldMap> PrimaryKeys{
			get{
				List<InternalFieldMap> ret = new List<InternalFieldMap>();
				foreach (InternalFieldMap f in Fields)
				{
					if (f.PrimaryKey)
					{
						ret.Add(f);
					}
				}
				return ret;
			}
		}

		public List<InternalFieldMap> InternalPrimaryKeys
		{
			get
			{
				List<InternalFieldMap> ret = new List<InternalFieldMap>();
				foreach (FieldMap f in _fields.Values)
				{
					if (f.PrimaryKey   && !(f is ExternalFieldMap))
					{
						ret.Add((InternalFieldMap)f);
					}
				}
				return ret;
			}
		}

		public bool HasPrimaryKeys
		{
			get
			{
				return PrimaryKeys.Count > 0;
			}
		}
		
		public ExternalFieldMap GetFieldInfoForForiegnTable(System.Type table)
		{
			foreach (FieldMap f in _fields.Values )
			{
				if ((f is ExternalFieldMap)&&(((ExternalFieldMap)f).Type==table))
				{
					return (ExternalFieldMap)f;
				}
			}
			return null;
		}
		
		public List<System.Type> ForiegnTables{
			get{
				List<System.Type> ret = new List<System.Type>();
				foreach (FieldMap f in _fields.Values)
				{
					if (f is ExternalFieldMap && !((ExternalFieldMap)f).IsArray )
					{
						ExternalFieldMap efm = (ExternalFieldMap)f;
						ret.Add(efm.Type);
					}
				}
				return ret;
			}
		}

		public List<System.Type> ForiegnTablesCreate
		{
			get
			{
				List<System.Type> ret = new List<System.Type>();
				foreach (FieldMap f in _fields.Values)
				{
					if (f is ExternalFieldMap)
					{
						ExternalFieldMap efm = (ExternalFieldMap)f;
						ret.Add(efm.Type);
					}
				}
				return ret;
			}
		}

		public List<ExternalFieldMap> ExternalFieldMapArrays
		{
			get
			{
				List<ExternalFieldMap> ret = new List<ExternalFieldMap>();
				foreach (FieldMap f in _fields.Values)
				{
					if (f is ExternalFieldMap)
					{
						if (((ExternalFieldMap)f).IsArray)
						{
							ret.Add((ExternalFieldMap)f);
						}
					}
				}
				return ret;
			}
		}
		
		public List<FieldNamePair> FieldNamePairs{
			get{
				List<FieldNamePair> ret = new List<FieldNamePair>();
				foreach (string str in _fields.Keys)
				{
					if (_fields[str] is InternalFieldMap)
					{
						ret.Add(new FieldNamePair(str, ((InternalFieldMap)_fields[str]).FieldName));
					}
					else
					{
						ret.Add(new FieldNamePair(str, str));
					}
				}
				return ret;
			}
		}

		public FieldMap this[string ClassFieldName]
		{
			get
			{
				if (_fields.ContainsKey(ClassFieldName) )
				{
					return _fields[ClassFieldName];
				}
				return null;
			}
		}

		public FieldMap this[FieldNamePair pair]
		{
			get
			{
				return this[pair.ClassFieldName];
			}
		}
		
		public string GetClassFieldName(string TableFieldName)
		{
			foreach (FieldNamePair fnp in FieldNamePairs)
			{
				if (fnp.TableFieldName==TableFieldName)
				{
					return fnp.ClassFieldName;
				}
			}
			return null;
		}

		public string GetTableFieldName(string ClassFieldName)
		{
			foreach (FieldNamePair fnp in FieldNamePairs)
			{
				if (fnp.ClassFieldName == ClassFieldName)
				{
					return fnp.TableFieldName;
				}
			}
			return null;
		}

		public string GetTableFieldName(FieldMap fm)
		{
			return GetTableFieldName(GetClassPropertyName(fm));
		}

		public string GetClassFieldName(FieldMap fm)
		{
			return GetClassFieldName(GetTableFieldName(fm));
		}

		public string GetClassPropertyName(FieldMap fi)
		{
			foreach (string str in _fields.Keys)
			{
				if (_fields[str].Equals(fi))
				{
					return str;
				}
			}
			return null;
		}
		
		public List<InternalFieldMap> Fields{
			get{
				List<InternalFieldMap> ret = new List<InternalFieldMap>();
				foreach (FieldMap f in _fields.Values)
				{
					if (f is ExternalFieldMap)
					{
						if (!((ExternalFieldMap)f).IsArray)
						{
							ExternalFieldMap efm = (ExternalFieldMap)f;
							foreach (InternalFieldMap fm in ClassMapper.GetTableMap(((ExternalFieldMap)f).Type).PrimaryKeys)
							{
								ret.Add(new InternalFieldMap(fm.FieldLength, fm.FieldName, fm.FieldType, efm.PrimaryKey, false, efm.Nullable,efm.Versionable));
							}
						}
					}else{
						ret.Add((InternalFieldMap)f);
					}
				}
				return ret;
			}
		}
		
		public string Name{
			get{
				return _tableName;
			}
		}
	}
}
