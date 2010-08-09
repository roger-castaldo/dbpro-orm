using System;
using System.Reflection;
using System.Collections.Generic;
using System.Threading;
using Org.Reddragonit.Dbpro.Connections;
using Org.Reddragonit.Dbpro.Structure.Mapping;
using FieldNamePair = Org.Reddragonit.Dbpro.Structure.Mapping.TableMap.FieldNamePair;

namespace Org.Reddragonit.Dbpro.Structure
{
	internal enum LoadStatus{
		Complete,
		Partial,
		NotLoaded
	}
	
	public abstract class Table : MarshalByRefObject,IConvertible
	{
		private bool _isSaved = false;
		private LoadStatus _loadStatus=LoadStatus.NotLoaded;
		private Dictionary<string, object> _initialPrimaryKeys = new Dictionary<string, object>();

		public Table()
		{
			InitPrimaryKeys();
		}
		
		internal LoadStatus LoadStatus{
			get{return _loadStatus;}
			set{_loadStatus=value;}
		}
		
		private void InitPrimaryKeys()
		{
			_initialPrimaryKeys.Clear();
			TableMap map = ClassMapper.GetTableMap(this.GetType());
			foreach (FieldNamePair fnp in map.FieldNamePairs)
			{
				if (map[fnp].PrimaryKey)
				{
					_initialPrimaryKeys.Add(fnp.ClassFieldName,this.GetType().GetProperty(fnp.ClassFieldName).GetValue(this,new object[0]));
				}
			}
		}
		
		internal object GetInitialPrimaryValue(string ClassFieldName)
		{
			if ((_initialPrimaryKeys!=null)&&(_initialPrimaryKeys.ContainsKey(ClassFieldName)))
				return _initialPrimaryKeys[ClassFieldName];
			return null;
		}
		
		internal void CopyValuesFrom(Table table)
		{
			foreach (PropertyInfo pi in table.GetType().GetProperties(BindingFlags.Public |      //Get public members
			                                                          BindingFlags.NonPublic |   //Get private/protected/internal members
			                                                          BindingFlags.Static |      //Get static members
			                                                          BindingFlags.Instance |    //Get instance members
			                                                          BindingFlags.DeclaredOnly))
			{
				try{
					pi.SetValue(this,pi.GetValue(table,new object[0]),new object[0]);
				}catch (Exception e)
				{
					
				}
			}
			InitPrimaryKeys();
		}
		
		internal string ConnectionName
		{
			get{
				foreach (Attribute att in this.GetType().GetCustomAttributes(true))
				{
					if (att.GetType()==typeof(Org.Reddragonit.Dbpro.Structure.Attributes.Table))
					{
						return ((Org.Reddragonit.Dbpro.Structure.Attributes.Table)att).ConnectionName;
					}
				}
				return Connections.ConnectionPoolManager.DEFAULT_CONNECTION_NAME;
			}
		}

		internal bool IsSaved
		{
			get
			{
				return _isSaved;
			}
		}

		internal void SetValues(Connection conn)
		{
			_initialPrimaryKeys.Clear();
			TableMap map = ClassMapper.GetTableMap(this.GetType());
			RecurSetValues(map,conn);
			_isSaved = true;
		}
		
		private void RecurSetValues(TableMap map,Connection conn)
		{
			foreach (FieldNamePair fnp in map.FieldNamePairs)
			{
				if (map[fnp] is ExternalFieldMap)
				{
					if (!((ExternalFieldMap)map[fnp]).IsArray)
					{
						ExternalFieldMap efm = (ExternalFieldMap)map[fnp];
						Table t = (Table)this.GetType().GetProperty(fnp.ClassFieldName).PropertyType.GetConstructor(Type.EmptyTypes).Invoke(new object[0]);
						foreach (InternalFieldMap ifm in ClassMapper.GetTableMap(t.GetType()).PrimaryKeys)
						{
							if (conn.ContainsField(conn.Pool.CorrectName(efm.AddOnName+"_"+ifm.FieldName))&&!conn.IsDBNull(conn.GetOrdinal(conn.Pool.CorrectName(efm.AddOnName+"_"+ifm.FieldName))))
							{
								t.GetType().GetProperty(map.GetClassFieldName(ifm.FieldName)).SetValue(t,conn[conn.Pool.CorrectName(efm.AddOnName+"_"+ifm.FieldName)],new object[0]);
							}
						}
						t._loadStatus= LoadStatus.Partial;
						if (!t.AllFieldsNull)
						{
							t.InitPrimaryKeys();
							this.GetType().GetProperty(fnp.ClassFieldName).SetValue(this, t, new object[0]);
						}
					}
				}
				else
				{
					if (conn.ContainsField(fnp.TableFieldName))
					{
						if (conn.IsDBNull(conn.GetOrdinal(fnp.TableFieldName)))
						{
							try{
								this.GetType().GetProperty(fnp.ClassFieldName).SetValue(this,null,new object[0]);
							}catch(Exception e){}
						}
						else
						{
							this.GetType().GetProperty(fnp.ClassFieldName).SetValue(this, conn[fnp.TableFieldName], new object[0]);
						}
					}
				}
				if ((map[fnp].PrimaryKey)&&!_initialPrimaryKeys.ContainsKey(fnp.ClassFieldName))
				{
					_initialPrimaryKeys.Add(fnp.ClassFieldName,this.GetType().GetProperty(fnp.ClassFieldName).GetValue(this,new object[0]));
				}
			}
			if (map.ParentType!=null)
			{
				RecurSetValues(ClassMapper.GetTableMap(map.ParentType),conn);
			}
		}

		internal bool AllFieldsNull
		{
			get
			{
				foreach (FieldNamePair fnp in ClassMapper.GetTableMap(this.GetType()).FieldNamePairs)
				{
					if (!IsFieldNull(fnp.ClassFieldName))
					{
						return false;
					}
				}
				return true;
			}
		}
		
		internal object GetField(string FieldName)
		{
			if (IsFieldNull(FieldName))
			{
				return null;
			}else
			{
				return this.GetType().GetProperty(FieldName).GetValue(this,new object[0]);
			}
		}
		
		internal bool IsFieldNull(string FieldName)
		{
			PropertyInfo pi = this.GetType().GetProperty(FieldName);
			object cur = pi.GetValue(this,new object[0]);
			return equalObjects(cur,ClassMapper.InitialValueForClassField(this.GetType(),FieldName));
		}

		private bool equalObjects(object obj1, object obj2)
		{
			if (obj1 == null)
			{
				if (obj2 == null)
				{
					return true;
				}
				return false;
			}
			else
			{
				if (obj2 == null)
				{
					return false;
				}
				if (!(obj2.GetType().Equals(obj1.GetType())))
				{
					return false;
				}
				try
				{
					return obj1.Equals(obj2);
				}
				catch (Exception e)
				{
					return false;
				}
			}
		}		
		
		public TypeCode GetTypeCode()
		{
			return TypeCode.Object;
		}
		
		public bool ToBoolean(IFormatProvider provider)
		{
			throw new NotImplementedException();
		}
		
		public char ToChar(IFormatProvider provider)
		{
			throw new NotImplementedException();
		}
		
		public sbyte ToSByte(IFormatProvider provider)
		{
			throw new NotImplementedException();
		}
		
		public byte ToByte(IFormatProvider provider)
		{
			throw new NotImplementedException();
		}
		
		public short ToInt16(IFormatProvider provider)
		{
			throw new NotImplementedException();
		}
		
		public ushort ToUInt16(IFormatProvider provider)
		{
			throw new NotImplementedException();
		}
		
		public int ToInt32(IFormatProvider provider)
		{
			throw new NotImplementedException();
		}
		
		public uint ToUInt32(IFormatProvider provider)
		{
			throw new NotImplementedException();
		}
		
		public long ToInt64(IFormatProvider provider)
		{
			throw new NotImplementedException();
		}
		
		public ulong ToUInt64(IFormatProvider provider)
		{
			throw new NotImplementedException();
		}
		
		public float ToSingle(IFormatProvider provider)
		{
			throw new NotImplementedException();
		}
		
		public double ToDouble(IFormatProvider provider)
		{
			throw new NotImplementedException();
		}
		
		public decimal ToDecimal(IFormatProvider provider)
		{
			throw new NotImplementedException();
		}
		
		public DateTime ToDateTime(IFormatProvider provider)
		{
			throw new NotImplementedException();
		}
		
		public string ToString(IFormatProvider provider)
		{
			throw new NotImplementedException();
		}
		
		public object ToType(Type conversionType, IFormatProvider provider)
		{
			if (!conversionType.IsSubclassOf(typeof(Table)))
				throw new Exception("Cannot convert object to type that does not inherit table.");
			if (!this.GetType().IsSubclassOf(conversionType))
				throw new Exception("Cannot convert object to type that is not a parent of the current class.");
			object ret = conversionType.GetConstructor(Type.EmptyTypes).Invoke(new object[0]);
			foreach (PropertyInfo pi in conversionType.GetProperties(BindingFlags.Public |      //Get public members
			                                                         BindingFlags.NonPublic |   //Get private/protected/internal members
			                                                         BindingFlags.Static |      //Get static members
			                                                         BindingFlags.Instance |    //Get instance members
			                                                         BindingFlags.DeclaredOnly))
			{
				if (!this.IsFieldNull(pi.Name))
					pi.SetValue(ret,pi.GetValue(this,new object[0]),new object[0]);
			}
			((Table)ret).InitPrimaryKeys();
			return ret;
		}
	}
}