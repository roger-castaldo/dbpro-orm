using System;
using System.Reflection;
using System.Collections.Generic;
using System.Threading;
using Org.Reddragonit.Dbpro.Connections;
using FieldType = Org.Reddragonit.Dbpro.Structure.Attributes.FieldType;
using Org.Reddragonit.Dbpro.Connections.Parameters;
using Org.Reddragonit.Dbpro.Connections.PoolComponents;
using Org.Reddragonit.Dbpro.Structure.Attributes;
using System.ComponentModel;

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


        internal List<string> _changedFields = null;
        /*virtual function doesn't do anything, calls are caught by the proxy to 
         * to return the changed fields according to the proxy.
        */
		internal List<string> ChangedFields{
			get{return _changedFields;}
		}
		
        //Load the initial primary keys into the table object for later comparison.
        private void InitPrimaryKeys()
        {
            _initialPrimaryKeys.Clear();
            sTable map = ConnectionPoolManager.GetConnection(this.GetType()).Mapping[this.GetType()];
            List<string> props = new List<string>(map.PrimaryKeyProperties);
            if (props.Count == 0)
                props.AddRange(map.PrimaryKeyProperties);
            foreach (string prop in props)
            {
                object obj = GetField(prop);
                if (obj!=null)
                    _initialPrimaryKeys.Add(prop,obj);
            }
        }

        //used to load the original data to be used for update triggers
        internal Table LoadCopyOfOriginal(Connection conn)
        {
            List<SelectParameter> pars = new List<SelectParameter>();
            foreach (string str in _initialPrimaryKeys.Keys)
                pars.Add(new EqualParameter(str, _initialPrimaryKeys[str]));
            List<Org.Reddragonit.Dbpro.Structure.Table> tmp = conn.Select(this.GetType(),
                pars.ToArray());
            if (tmp.Count > 0)
                return tmp[0];
            return this;
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
            ConnectionPool pool = ConnectionPoolManager.GetConnection(table.GetType());
            sTable mp = pool.Mapping[table.GetType()];
			foreach (string prop in mp.Properties)
			{
				try{
                    this.SetField(prop, table.GetField(prop));
				}catch (Exception e)
				{
					
				}
			}
            Type t = table.GetType().BaseType;
            while (pool.Mapping.IsMappableType(t))
            {
                mp = pool.Mapping[t];
                foreach (string prop in mp.Properties)
                {
                    try
                    {
                        this.SetField(prop, table.GetField(prop));
                    }
                    catch (Exception e)
                    {

                    }
                }
                t = t.BaseType;
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
            Logger.LogLine("Obtaining table map for " + this.GetType().FullName + " to allow setting of values from query");
            sTable map = conn.Pool.Mapping[this.GetType()];
            Logger.LogLine("Recursively setting values from query for " + this.GetType().FullName);
            RecurSetValues(map, conn);
            _isSaved = true;
        }

        //called to set values onto a table that is externally mapped to this current table
        //this is used through lazy loading proxies by only setting the primary key fields.
        private Table SetExternalValues(sTable map,string propertyName, Connection conn, out bool setValue,Table table)
        {
            setValue = false;
            sTable eMap = conn.Pool.Mapping[table.GetType()];
            sTableField[] flds = map[propertyName];
            List<string> fProps = new List<string>(eMap.ForeignTableProperties);
            Type ty = table.GetType().BaseType;
            while (conn.Pool.Mapping.IsMappableType(ty))
            {
                foreach (string str in conn.Pool.Mapping[ty].ForeignTableProperties)
                {
                    if (!fProps.Contains(str))
                        fProps.Add(str);
                }
                ty = ty.BaseType;
            }
            foreach (string prop in eMap.PrimaryKeyProperties)
            {
                if (fProps.Contains(prop))
                {
                    PropertyInfo pi = table.GetType().GetProperty(prop, Utility._BINDING_FLAGS);
                    if (pi == null)
                        pi = table.GetType().GetProperty(prop, Utility._BINDING_FLAGS_WITH_INHERITANCE);
                    Table t = (Table)pi.PropertyType.GetConstructor(Type.EmptyTypes).Invoke(new object[0]);
                    t._loadStatus = LoadStatus.Partial;
                    t = (Table)LazyProxy.Instance(t);
                    foreach (sTableField f in eMap[prop])
                    {
                        foreach (sTableField fld in flds)
                        {
                            if (fld.ExternalField == f.Name)
                            {
                                if (conn.ContainsField(fld.Name) && !conn.IsDBNull(conn.GetOrdinal(fld.Name)))
                                {
                                    RecurSetPropertyValue(f.ExternalField, conn, fld.Name, t);
                                }
                                break;
                            }
                        }
                    }
                    if (!t.AllFieldsNull)
                    {
                        t.InitPrimaryKeys();
                        table.SetField(prop, t);
                        setValue = true;
                    }
                }
                else
                {
                    foreach (sTableField f in eMap[prop])
                    {
                        foreach (sTableField fld in flds)
                        {
                            if (fld.ExternalField == f.Name)
                            {
                                if (conn.ContainsField(fld.Name)&&!conn.IsDBNull(conn.GetOrdinal(fld.Name))){
                                    table.SetField(f.ClassProperty, conn[fld.Name]);
                                    setValue = true;
                                }
                                break;
                            }
                        }
                    }
                }
            }
            return table;
        }

        internal void RecurSetPropertyValue(string internalFieldName, Connection conn, string queryFieldName, Table table)
        {
            sTable map = conn.Pool.Mapping[table.GetType()];
            foreach (sTableField fld in map.Fields)
            {
                if (fld.Name == internalFieldName)
                {
                    if (fld.ExternalField == null)
                        table.SetField(fld.ClassProperty, conn[queryFieldName]);
                    else
                    {
                        if (table.GetField(fld.ClassProperty) == null)
                        {
                            PropertyInfo pi = table.GetType().GetProperty(fld.ClassProperty, Utility._BINDING_FLAGS);
                            if (pi == null)
                                pi = table.GetType().GetProperty(fld.ClassProperty, Utility._BINDING_FLAGS_WITH_INHERITANCE);
                            Table t = (Table)pi.PropertyType.GetConstructor(Type.EmptyTypes).Invoke(new object[0]);
                            t._loadStatus = LoadStatus.Partial;
                            t = (Table)LazyProxy.Instance(t);
                            table.SetField(fld.Name,t);
                        }
                        RecurSetPropertyValue(fld.ExternalField, conn, queryFieldName, (Table)table.GetField(fld.Name));
                    }
                    break;
                }
            }
        }
		
        //recursively sets values 
		private void RecurSetValues(sTable map,Connection conn)
		{
            Type ty = conn.Pool.Mapping[map.Name];
            List<string> extFields = new List<string>(map.ForeignTableProperties);
			foreach (string prop in map.Properties)
			{
                PropertyInfo pi = ty.GetProperty(prop, Utility._BINDING_FLAGS);
                if (pi != null)
                {
                    if (extFields.Contains(prop) && !pi.PropertyType.IsEnum)
                    {
                        if (!pi.PropertyType.IsArray)
                        {
                            Table t = (Table)pi.PropertyType.GetConstructor(Type.EmptyTypes).Invoke(new object[0]);
                            t._loadStatus = LoadStatus.Partial;
                            t = (Table)LazyProxy.Instance(t);
                            bool setValue = false;
                            t = SetExternalValues(map, prop, conn, out setValue, t);
                            if (!t.AllFieldsNull && setValue)
                            {
                                t.InitPrimaryKeys();
                                this.SetField(prop, t);
                            }
                        }
                    }
                    else
                    {
                        if (!pi.PropertyType.IsArray)
                        {
                            sTableField fld = map[prop][0];
                            if (conn.ContainsField(fld.Name))
                            {
                                if (conn.IsDBNull(conn.GetOrdinal(fld.Name)))
                                {
                                    try
                                    {
                                        this.SetField(prop, null);
                                    }
                                    catch (Exception e) { }
                                }
                                else
                                {
                                    if (fld.Type == FieldType.ENUM)
                                        this.SetField(prop, conn.Pool.GetEnumValue(pi.PropertyType, (int)conn[fld.Name]));
                                    else
                                        this.SetField(prop, conn[fld.Name]);
                                }
                            }
                        }
                    }
                }
			}
			if (conn.Pool.Mapping.IsMappableType(ty.BaseType))
			{
				RecurSetValues(conn.Pool.Mapping[ty.BaseType],conn);
			}
            this.InitPrimaryKeys();
		}

		internal bool AllFieldsNull
		{
			get
			{
				foreach (string prop in ConnectionPoolManager.GetConnection(GetType()).Mapping[GetType()].Properties)
				{
					if (!IsFieldNull(prop))
					{
						return false;
					}
				}
				return true;
			}
		}

        internal PropertyInfo LocatePropertyInfo(string FieldName)
        {
            ConnectionPool pool = ConnectionPoolManager.GetConnection(this.GetType());
            sTable map = pool.Mapping[this.GetType()];
            PropertyInfo ret = this.GetType().GetProperty(FieldName, Utility._BINDING_FLAGS);
            if (ret == null)
            {
                foreach (PropertyInfo p in this.GetType().GetProperties(Utility._BINDING_FLAGS))
                {
                    if (p.Name == FieldName)
                    {
                        ret = p;
                        break;
                    }
                }
            }
            if (ret == null)
            {
                Type t = this.GetType().BaseType;
                while(pool.Mapping.IsMappableType(t)){
                    foreach (PropertyInfo p in t.GetProperties(Utility._BINDING_FLAGS))
                    {
                        if (p.Name == FieldName){
                            ret = p;
                            break;
                        }
                    }
                    if (ret!=null)
                        break;
                    t=t.BaseType;
                }
            }
            if (ret == null)
            {
                foreach (sTableField fld in map.Fields){
                    if (fld.Name == FieldName){
                        ret = LocatePropertyInfo(fld.ClassProperty);
                        break;
                    }
                }
            }
            return ret;
        }
		
		internal object GetField(string FieldName)
		{
            if (FieldName == null)
                return null;
            PropertyInfo pi = LocatePropertyInfo(FieldName);
            if (pi == null)
                return null;
			if (IsFieldNull(FieldName))
			{
				return null;
			}else
			{
				return pi.GetValue(this,new object[0]);
			}
		}
		
		internal void SetField(string FieldName,object value)
		{
            if (FieldName == null)
                return;
            PropertyInfo pi = LocatePropertyInfo(FieldName);
            if (value == null)
                value = pi.GetValue(this.GetType().GetConstructor(Type.EmptyTypes).Invoke(new object[0]),new object[0]);
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
			}else if (pi.PropertyType.Equals(typeof(uint))||pi.PropertyType.Equals(typeof(UInt32))){
                pi.SetValue(this, BitConverter.ToUInt32(BitConverter.GetBytes(int.Parse(value.ToString())), 0),new object[0]);
            }else if (pi.PropertyType.Equals(typeof(ushort)) || pi.PropertyType.Equals(typeof(UInt16))){
                pi.SetValue(this, BitConverter.ToUInt16(BitConverter.GetBytes(short.Parse(value.ToString())), 0), new object[0]);
            }
            else if (pi.PropertyType.Equals(typeof(ulong)) || pi.PropertyType.Equals(typeof(UInt64)))
            {
                pi.SetValue(this, BitConverter.ToUInt64(BitConverter.GetBytes(long.Parse(value.ToString())), 0), new object[0]);
            }
            else
            {
                if (value != null)
                {
                    if (value.GetType().FullName != pi.PropertyType.FullName)
                    {
                        Type pt = pi.PropertyType;
                                if (pt.IsGenericType && pt.GetGenericTypeDefinition().FullName.StartsWith("System.Nullable"))
                                    pt = pt.GetGenericArguments()[0];
                        if (pt.GetCustomAttributes(typeof(TypeConverterAttribute),false).Length > 0)
                        {
                            if (TypeDescriptor.GetConverter(pt).CanConvertFrom(value.GetType()))
                            {
                                value = TypeDescriptor.GetConverter(pt).ConvertFrom(value);
                            }
                            else
                            {
                                try
                                {
                                    object val = Convert.ChangeType(value, pt);
                                    value = val;
                                }
                                catch (Exception e)
                                {
                                    Logger.LogLine(e);
                                }
                            }
                        }
                        else
                        {
                            try
                            {
                                object val = Convert.ChangeType(value, pt);
                                value = val;
                            }
                            catch (Exception e)
                            {
                                Logger.LogLine(e);
                            }
                        }
                    }
                }
                pi.SetValue(this, value, new object[0]);
            }
		}
		
		internal bool IsFieldNull(string FieldName)
		{
            if (FieldName == null)
                return true;
            PropertyInfo pi = LocatePropertyInfo(FieldName);
			object cur = pi.GetValue(this,new object[0]);
            sTable map = ConnectionPoolManager.GetConnection(this.GetType()).Mapping[this.GetType()];
            if (map[FieldName].Length>0)
            {
                sTableField fld = map[FieldName][0];
                if (((pi.PropertyType.Equals(typeof(bool))||pi.PropertyType.IsEnum)
                    && !fld.Nullable) ||
                    (new List<string>(map.PrimaryKeyProperties).Contains(FieldName) && this.IsSaved)||
                    !fld.Nullable)
                    return false;
            }
            return equalObjects(cur, pi.GetValue(this.GetType().GetConstructor(Type.EmptyTypes).Invoke(new object[0]), new object[0]));
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
			if (!this.GetType().IsSubclassOf(conversionType) && !conversionType.IsSubclassOf(this.GetType()))
				throw new Exception("Cannot convert object to type that is not a parent/child of the current class.");
			Table ret = (Table)conversionType.GetConstructor(Type.EmptyTypes).Invoke(new object[0]);
            sTable map = ConnectionPoolManager.GetConnection(conversionType).Mapping[conversionType];
            if (conversionType.IsSubclassOf(this.GetType()))
                map = ConnectionPoolManager.GetConnection(this.GetType()).Mapping[this.GetType()];
            else
            {
                ret._isSaved = this._isSaved;
                ret._loadStatus = this._loadStatus;
            }
            ((Table)ret)._changedFields = new List<string>();
            foreach (string prop in map.Properties)
            {
                if (!this.IsFieldNull(prop))
                    ((Table)ret).SetField(prop, this.GetField(prop));
                if (this.ChangedFields != null)
                {
                    if (this.ChangedFields.Contains(prop))
                        ((Table)ret)._changedFields.Add(prop);
                }
            }
            ConnectionPool pool = ConnectionPoolManager.GetConnection(ret.GetType());
            Type t = ret.GetType().BaseType;
            while (pool.Mapping.IsMappableType(t))
            {
                map = pool.Mapping[t];
                foreach (string prop in map.Properties)
                {
                    if (!this.IsFieldNull(prop))
                        ((Table)ret).SetField(prop, this.GetField(prop));
                    if (this.ChangedFields != null)
                    {
                        if (this.ChangedFields.Contains(prop))
                            ((Table)ret)._changedFields.Add(prop);
                    }
                }    
                t = t.BaseType;
            }
            ((Table)ret).InitPrimaryKeys();
            foreach (string str in this._initialPrimaryKeys.Keys)
            {
                if (((Table)ret)._initialPrimaryKeys.ContainsKey(str))
                {
                    ((Table)ret)._initialPrimaryKeys.Remove(str);
                    ((Table)ret)._initialPrimaryKeys.Add(str, this._initialPrimaryKeys[str]);
                }
            }
			return ret;
		}

        //Called to delete the instance of the table object.
        public void Delete()
        {
            if (!this.IsSaved)
                throw new Exception("Cannot delete an object from the database if it is not in the database.");
            Connection conn = ConnectionPoolManager.GetConnection(this.GetType()).getConnection();
            conn.Delete(this);
            conn.CloseConnection();
        }

        //Called to update the instance of the table object.
        public void Update()
        {
            if (!this.IsSaved)
                throw new Exception("Cannot update an object that is not in the database.");
            Connection conn = ConnectionPoolManager.GetConnection(this.GetType()).getConnection();
            conn.Save(this);
            conn.CloseConnection();
        }

        //Called to save the instance of the table object into the database
        public void Save()
        {
            if (this.IsSaved)
                throw new Exception("Cannot Save an object to the database when it already exists.");
            Connection conn = ConnectionPoolManager.GetConnection(this.GetType()).getConnection();
            Table tmp = conn.Save(this);
            sTable tbl = conn.Pool.Mapping[this.GetType()];
            conn.CloseConnection();
            if (tmp == null)
                throw new Exception("An error occured attempting to save the table.");
            foreach (string prop in tbl.Properties)
            {
                PropertyInfo pi = this.GetType().GetProperty(prop, Utility._BINDING_FLAGS);
                if (pi == null)
                    pi = this.GetType().GetProperty(prop, Utility._BINDING_FLAGS_WITH_INHERITANCE);
                if (pi.CanWrite)
                    pi.SetValue(this, pi.GetValue(tmp, new object[0]), new object[0]);
            }
            this._isSaved = true;
            this._changedFields = null;
        }

        internal bool IsProxied
        {
            get { return false; }
        }

	}
}
