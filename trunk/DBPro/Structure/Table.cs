using System;
using System.Reflection;
using System.Collections.Generic;
using System.Threading;
using Org.Reddragonit.Dbpro.Connections;
using Org.Reddragonit.Dbpro.Structure.Mapping;
using FieldNamePair = Org.Reddragonit.Dbpro.Structure.Mapping.TableMap.FieldNamePair;
using FieldType = Org.Reddragonit.Dbpro.Structure.Attributes.FieldType;

namespace Org.Reddragonit.Dbpro.Structure
{
    /*
     * Houses the different loaded states a table can be in for the Lazy proxy to handle special calls.
     */
	internal enum LoadStatus{
		Complete,
		Partial,
		NotLoaded
	}
	
	public abstract class Table : MarshalByRefObject,IConvertible
	{
        //Whether or not the table has been saved in the database.
		internal bool _isSaved = false;
        //Houses the current load state for the table object.
		private LoadStatus _loadStatus=LoadStatus.NotLoaded;
        //Houses the initial values for the primary keys prior to editing them.
		private Dictionary<string, object> _initialPrimaryKeys = new Dictionary<string, object>();

		protected Table()
		{
			InitPrimaryKeys();
		}
		
        //Creates a new instance of a table object wrapping it in the proxy class.
		protected static Table Instance(Type type)
		{
			if (!type.IsSubclassOf(typeof(Table)))
				throw new Exception("Cannot create instance of a class that is not a table.");
			return (Table)LazyProxy.Instance(type.GetConstructor(Type.EmptyTypes).Invoke(new object[0]));
		}
		
        //access the load status of the current table object.
		internal LoadStatus LoadStatus{
			get{return _loadStatus;}
			set{_loadStatus=value;
				if ((_loadStatus== LoadStatus.Complete)||(_loadStatus== LoadStatus.Partial))
					_isSaved=true;
			}
		}
		
        /*virtual function doesn't do anything, calls are caught by the proxy to 
         * to return the changed fields according to the proxy.
        */
		internal List<string> ChangedFields{
			get{return null;}
		}
		
        //Load the initial primary keys into the table object for later comparison.
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
		
        //called to get the initial value of a primary key field
		internal object GetInitialPrimaryValue(string ClassFieldName)
		{
			if ((_initialPrimaryKeys!=null)&&(_initialPrimaryKeys.ContainsKey(ClassFieldName)))
				return _initialPrimaryKeys[ClassFieldName];
			return null;
		}
		
        //called to copy values from an existing table object, this is done for table inheritance
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
		
        //returns the connection name that this table is linked to
		internal string ConnectionName
		{
			get{
				string ret = null;
				foreach (Attribute att in this.GetType().GetCustomAttributes(true))
				{
					if (att.GetType()==typeof(Org.Reddragonit.Dbpro.Structure.Attributes.Table))
					{
						ret = ((Org.Reddragonit.Dbpro.Structure.Attributes.Table)att).ConnectionName;
					}
				}
				if (!ConnectionPoolManager.ConnectionExists(ret))
					ret=ConnectionPoolManager.DEFAULT_CONNECTION_NAME;
				return ret;
			}
		}

        //returns if the table object has been saved in the database.
		internal bool IsSaved
		{
			get
			{
				return _isSaved;
			}
		}

        //called by a connection to set the values in the table object from the generated query.
		internal void SetValues(Connection conn)
		{
			_initialPrimaryKeys.Clear();
			TableMap map = ClassMapper.GetTableMap(this.GetType());
			RecurSetValues(map,conn);
			_isSaved = true;
		}

        //called to set values onto a table that is externally mapped to this current table
        //this is used through lazy loading proxies by only setting the primary key fields.
        private Table SetExternalValues(TableMap map, Connection conn, string additionalAddOnName, out bool setValue,Table table)
        {
            setValue = false;
            foreach (InternalFieldMap ifm in map.PrimaryKeys)
            {
                if (conn.ContainsField(conn.Pool.CorrectName(additionalAddOnName + "_" + ifm.FieldName)) && !conn.IsDBNull(conn.GetOrdinal(conn.Pool.CorrectName(additionalAddOnName + "_" + ifm.FieldName))))
                {
                    table.SetField(map.GetClassFieldName(ifm.FieldName), conn[conn.Pool.CorrectName(additionalAddOnName + "_" + ifm.FieldName)]);
                    setValue = true;
                }
            }
            foreach (FieldNamePair fnp in map.FieldNamePairs)
            {
                if ((map[fnp] is ExternalFieldMap)&&map[fnp].PrimaryKey)
                {
                    if (!((ExternalFieldMap)map[fnp]).IsArray)
                    {
                        ExternalFieldMap efm = (ExternalFieldMap)map[fnp];
                        Table t = (Table)LazyProxy.Instance(efm.Type.GetConstructor(Type.EmptyTypes).Invoke(new object[0]));
                        bool sValue = false;
                        t = SetExternalValues(ClassMapper.GetTableMap(t.GetType()), conn, additionalAddOnName + "_" + efm.AddOnName, out sValue,t);
                        if (sValue)
                            setValue = true;
                        if (!t.AllFieldsNull && sValue)
                        {
                            t.InitPrimaryKeys();
                            table.SetField(fnp.ClassFieldName, t);
                        }
                    }
                }
            }
            table.LoadStatus = LoadStatus.Partial;
            return table;
        }
		
        //recursively sets values 
		private void RecurSetValues(TableMap map,Connection conn)
		{
			foreach (FieldNamePair fnp in map.FieldNamePairs)
			{
				if (map[fnp] is ExternalFieldMap)
				{
					if (!((ExternalFieldMap)map[fnp]).IsArray)
					{
						ExternalFieldMap efm = (ExternalFieldMap)map[fnp];
                        Table t = (Table)LazyProxy.Instance(efm.Type.GetConstructor(Type.EmptyTypes).Invoke(new object[0]));
                        bool setValue = false;
                        t = SetExternalValues(ClassMapper.GetTableMap(efm.Type), conn, efm.AddOnName, out setValue,t);
						if (!t.AllFieldsNull&&setValue)
						{
							t.InitPrimaryKeys();
							this.SetField(fnp.ClassFieldName,t);
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
								this.SetField(fnp.ClassFieldName,null);
							}catch(Exception e){}
						}
						else
						{
							if (((InternalFieldMap)map[fnp]).FieldType==FieldType.ENUM)
								this.SetField(fnp.ClassFieldName, conn.Pool.GetEnumValue(map[fnp].ObjectType,(int)conn[fnp.TableFieldName]));
							else
								this.SetField(fnp.ClassFieldName, conn[fnp.TableFieldName]);
						}
					}
				}
				if ((map[fnp].PrimaryKey)&&!_initialPrimaryKeys.ContainsKey(fnp.ClassFieldName))
				{
					_initialPrimaryKeys.Add(fnp.ClassFieldName,this.GetField(fnp.ClassFieldName));
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
				PropertyInfo pi = this.GetType().GetProperty(FieldName);
				if (pi==null)
				{
					foreach (PropertyInfo p in this.GetType().GetProperties(BindingFlags.Public |      //Get public members
					                                                        BindingFlags.NonPublic |   //Get private/protected/internal members
					                                                        BindingFlags.Static |      //Get static members
					                                                        BindingFlags.Instance |    //Get instance members
					                                                        BindingFlags.DeclaredOnly  ))
					{
						if (p.Name==FieldName)
						{
							pi=p;
							break;
						}
					}
				}
				return pi.GetValue(this,new object[0]);
			}
		}
		
		internal void SetField(string FieldName,object value)
		{
            if (FieldName == null)
                return;
			if (value==null)
				value = ClassMapper.InitialValueForClassField(this.GetType(),FieldName);
			PropertyInfo pi = this.GetType().GetProperty(FieldName);
			if (pi==null)
			{
				foreach (PropertyInfo p in this.GetType().GetProperties(BindingFlags.Public |      //Get public members
				                                                        BindingFlags.NonPublic |   //Get private/protected/internal members
				                                                        BindingFlags.Static |      //Get static members
				                                                        BindingFlags.Instance |    //Get instance members
				                                                        BindingFlags.DeclaredOnly  ))
				{
					if (p.Name==FieldName)
					{
						pi=p;
						break;
					}
				}
			}
			if (pi.PropertyType.Equals(typeof(bool))&&!(value.GetType().Equals(typeof(bool))))
			{
				if (value.GetType().Equals(typeof(int)))
				{
					if ((int)value==0)
						pi.SetValue(this,false,new object[0]);
					else
						pi.SetValue(this,true,new object[0]);
				}else if (value.GetType().Equals(typeof(string)))
				{
					if (((string)value).Length==1)
					{
						if ((string)value=="F")
							value="False";
						else
							value="True";
					}
					pi.SetValue(this,bool.Parse((string)value),new object[0]);
				}else if (value.GetType().Equals(typeof(char)))
				{
					if ((char)value=='F')
						pi.SetValue(this,false,new object[0]);
					else
						pi.SetValue(this,true,new object[0]);
				}
			}else 
				pi.SetValue(this,value,new object[0]);
		}
		
		internal bool IsFieldNull(string FieldName)
		{
            if (FieldName == null)
                return true;
			PropertyInfo pi = this.GetType().GetProperty(FieldName);
			if (pi==null)
			{
				foreach (PropertyInfo p in this.GetType().GetProperties(BindingFlags.Public |      //Get public members
				                                                        BindingFlags.NonPublic |   //Get private/protected/internal members
				                                                        BindingFlags.Static |      //Get static members
				                                                        BindingFlags.Instance |    //Get instance members
				                                                        BindingFlags.DeclaredOnly  ))
				{
					if (p.Name==FieldName)
					{
						pi=p;
						break;
					}
				}
			}
			object cur = pi.GetValue(this,new object[0]);
            if (ClassMapper.GetTableMap(this.GetType())[FieldName] != null)
            {
                if (((pi.PropertyType.Equals(typeof(bool))||pi.PropertyType.IsEnum)
                    && !ClassMapper.GetTableMap(this.GetType())[FieldName].Nullable) ||
                    (ClassMapper.GetTableMap(this.GetType())[FieldName].PrimaryKey && this.IsSaved)||
                    !ClassMapper.GetTableMap(this.GetType())[FieldName].Nullable)
                    return false;
            }
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
                if (pi.CanWrite)
                {
                    if (!this.IsFieldNull(pi.Name))
                        pi.SetValue(ret, pi.GetValue(this, new object[0]), new object[0]);
                }
			}
			((Table)ret).InitPrimaryKeys();
			return ret;
		}
	}
}
