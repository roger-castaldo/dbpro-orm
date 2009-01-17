using System;
using System.Reflection;
using System.Collections.Generic;
using System.Threading;
using Org.Reddragonit.Dbpro.Structure.Mapping;
using FieldNamePair = Org.Reddragonit.Dbpro.Structure.Mapping.TableMap.FieldNamePair;

namespace Org.Reddragonit.Dbpro.Structure
{
	public abstract class Table : IConvertible
	{
		private List<string> _nullFields=null;
		internal bool _isSaved = false;
		private Dictionary<string, object> _initialPrimaryValues = null;
		private Dictionary<string, object> _initialValues = null;

		public Table()
		{
            TableMap map = ClassMapper.GetTableMap(this.GetType());
			_nullFields = new List<String>();
			_initialPrimaryValues = new Dictionary<string, object>();
			_initialValues = new Dictionary<string, object>();
			foreach (FieldNamePair fnp in map.FieldNamePairs)
			{
				if (map[fnp].PrimaryKey)
				{
					_initialPrimaryValues.Add(fnp.ClassFieldName, this.GetType().GetProperty(fnp.ClassFieldName).GetValue(this, new object[0]));
				}
				_initialValues.Add(fnp.ClassFieldName, this.GetType().GetProperty(fnp.ClassFieldName).GetValue(this, new object[0]));
			}
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

		internal void SetValues(Org.Reddragonit.Dbpro.Connections.Connection conn)
		{
			_initialPrimaryValues = new Dictionary<string, object>();
			TableMap map = ClassMapper.GetTableMap(this.GetType());
			RecurSetValues(map,conn);
			_isSaved = true;
		}
		
		private void RecurSetValues(TableMap map,Org.Reddragonit.Dbpro.Connections.Connection conn)
		{
			foreach (FieldNamePair fnp in map.FieldNamePairs)
			{
				if (map[fnp] is ExternalFieldMap)
				{
					if (!((ExternalFieldMap)map[fnp]).IsArray)
					{
						Table t = (Table)this.GetType().GetProperty(fnp.ClassFieldName).PropertyType.GetConstructor(Type.EmptyTypes).Invoke(new object[0]);
						t.SetValues(conn);
						if (!t.AllFieldsNull)
						{
							this.GetType().GetProperty(fnp.ClassFieldName).SetValue(this, t, new object[0]);
						}
						else
						{
							_nullFields.Add(fnp.ClassFieldName);
						}
					}
				}
				else
				{
					if (conn.ContainsField(fnp.TableFieldName))
					{
						if (conn.IsDBNull(conn.GetOrdinal(fnp.TableFieldName)))
						{
							if (_nullFields == null)
							{
								_nullFields = new List<string>();
							}
							_nullFields.Add(fnp.ClassFieldName);
						}
						else
						{
							this.GetType().GetProperty(fnp.ClassFieldName).SetValue(this, conn[fnp.TableFieldName], new object[0]);
						}
					}
				}
				if ((map[fnp].PrimaryKey || !map.HasPrimaryKeys)&&(!_initialPrimaryValues.ContainsKey(fnp.ClassFieldName)))
				{
					_initialPrimaryValues.Add(fnp.ClassFieldName, GetField(fnp.ClassFieldName));
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
		
		protected object GetField(string FieldName)
		{
			if (IsFieldNull(FieldName))
			{
				return null;
			}else
			{
				return this.GetType().GetProperty(FieldName).GetValue(this,new object[0]);
			}
		}

		internal object GetInitialPrimaryValue(FieldNamePair pair)
		{
			if (_initialPrimaryValues.ContainsKey(pair.ClassFieldName))
			{
				return _initialPrimaryValues[pair.ClassFieldName];
			}
			return null;
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

		internal bool IsFieldNull(string FieldName)
		{
			return equalObjects(_initialValues[FieldName], this.GetType().GetProperty(FieldName).GetValue(this, new object[0]));
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
				pi.SetValue(ret,pi.GetValue(this,new object[0]),new object[0]);
			}
			return ret;
		}
	}
}
