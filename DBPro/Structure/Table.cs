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
using System.Text.RegularExpressions;
using System.Diagnostics;
using Org.Reddragonit.Dbpro.Exceptions;
using Org.Reddragonit.Dbpro.Validation;

namespace Org.Reddragonit.Dbpro.Structure
{
    /*
     * Houses the different loaded states a table can be in for the Lazy proxy to handle special calls.
     */
	internal enum LoadStatus{
		Complete,
		Partial,
		NotLoaded,
        Loading
	}
	
	public abstract class Table : MarshalByRefObject,IConvertible
	{
        private static readonly Regex _regFunctionCall = new Regex("^(get|set)_(.+)$", RegexOptions.Compiled | RegexOptions.ECMAScript | RegexOptions.IgnoreCase);

        //Whether or not the table has been saved in the database.
        internal bool _isSaved = false;
        //Whether or not the table has been converted from a table loaded in the database
        private bool _isParentSaved = false;
        //Houses the current load state for the table object.
		private LoadStatus _loadStatus=LoadStatus.NotLoaded;
        internal LoadStatus LoadStatus { get { return _loadStatus; } set{ _loadStatus = value; } }
        //Houses the initial values of the table.
        private Dictionary<string, object> _initialValues;
        //Houses the changed property values when set is called
        private Dictionary<string, object> _values;
        internal void WasSaved()
        {
            foreach (string str in _values.Keys)
            {
                _initialValues.Remove(str);
                _initialValues.Add(str, _values[str]);
            }
            _values.Clear();
        }
        //returns all fields that have been modified by the set call
        internal List<string> ChangedFields
        {
            get {
                string[] ret = new string[_values.Count];
                _values.Keys.CopyTo(ret, 0);
                return new List<string>(ret);
            }
        }
        internal Dictionary<string, int> OriginalArrayLengths {
            get {
                Dictionary<string, int> ret = new Dictionary<string, int>();
                foreach (string str in _initialValues.Keys)
                {
                    if (_map.ArrayProperties.Contains(str))
                    {
                        if (_initialValues[str] == null)
                            ret.Add(str, 0);
                        else
                            ret.Add(str, ((Array)_initialValues[str]).Length);
                    }
                }
                return ret;
            }
        }
        private Dictionary<string, List<int>> _replacedArrayIndexes = new Dictionary<string, List<int>>();
        internal Dictionary<string, List<int>> ReplacedArrayIndexes { get { return _replacedArrayIndexes; } }
        private sTable _map;

        protected Table()
		{
            _initialValues = new Dictionary<string, object>();
            _values = new Dictionary<string, object>();
            _map = ConnectionPoolManager.GetPool(this.GetType()).Mapping[this.GetType()];
            _values = new Dictionary<string, object>();
		}
		
        //Creates a new instance of a table object wrapping it in the proxy class.
		internal static Table Instance(Type type)
		{
			if (!type.IsSubclassOf(typeof(Table)))
				throw new Exception("Cannot create instance of a class that is not a table.");
			return (Table)type.GetConstructor(Type.EmptyTypes).Invoke(new object[0]);
		}

        //Hidden call used to access table property values
        private object this[string key]
        {
            get
            {
                if (_values.ContainsKey(key))
                    return _values[key];
                else if (_initialValues.ContainsKey(key))
                    return _initialValues[key];
                return null;
            }
            set
            {
                if (LoadStatus == LoadStatus.Loading)
                {
                    _initialValues.Remove(key);
                    _initialValues.Add(key, value);
                }
                else if (LoadStatus == LoadStatus.Partial && !_initialValues.ContainsKey(key))
                    _initialValues.Add(key, value);
                else
                {
                    if (_values.ContainsKey(key))
                        _values.Remove(key);
                    if (_initialValues.ContainsKey(key))
                    {
                        if (!Utility.IsEqual(_initialValues[key],value))
                            _values.Add(key, value);
                    }
                    else
                        _values.Add(key, value);
                }
            }
        }

        /*Called by implemented classes to get the value of a property
        e.g. public string FirstName{get{return (string)get();}}
        the function will take care of detecting which property you are looking to obtain and 
        attempt to obtain it from the stored data.*/
        protected object get()
        {
            StackTrace st = new StackTrace();
            StackFrame sf = st.GetFrames()[1];
            Match m = _regFunctionCall.Match(sf.GetMethod().Name);
            if (!m.Success)
                throw new Exception("Invalid Get Call");
            if (m.Groups[1].Value.ToLower() != "get")
                throw new Exception("Invalid Get Call");
            object ret = this[m.Groups[2].Value];
            if (ret == null && _loadStatus == LoadStatus.Partial)
            {
                _CompleteLazyLoad();
                ret = this[m.Groups[2].Value];
            }
            if (ret == null)
            {
                MethodInfo mi = (MethodInfo)sf.GetMethod();
                if (Nullable.GetUnderlyingType(mi.ReturnType) == null)
                {
                    if (mi.ReturnType.IsValueType)
                        ret = Activator.CreateInstance(mi.ReturnType);
                }
            }
            return ret;
        }

        /*Called by implemented classes to set the value of a property
        e.g. public string FirstName{set{return set(value);}}
        the function will take care of detecting which property you are looking to set and 
        attempt to set it in the stored data.*/
        protected void set(object value)
        {
            StackTrace st = new StackTrace();
            StackFrame sf = st.GetFrames()[1];
            Match m = _regFunctionCall.Match(sf.GetMethod().Name);
            if (!m.Success)
                throw new Exception("Invalid Set Call");
            if (m.Groups[1].Value.ToLower() != "set")
                throw new Exception("Invalid Set Call");
            string prop = m.Groups[2].Value;
            if (_loadStatus == LoadStatus.Partial)
                _CompleteLazyLoad();
            if (_isSaved)
            {
                if (new List<string>(_map.PrimaryKeyProperties).Contains(prop) && Utility.StringsEqual(prop, _map.AutoGenProperty))
                    throw new AlterPrimaryKeyException(this.GetType().FullName, prop);
                object curVal = this[prop];
                this[prop] = value;
                if (_map.ArrayProperties.Contains(prop))
                {
                    if (curVal != null && value != null)
                    {
                        List<int> indexes = new List<int>();
                        if (_replacedArrayIndexes.ContainsKey(prop))
                        {
                            indexes = _replacedArrayIndexes[prop];
                            _replacedArrayIndexes.Remove(prop);
                        }
                        Array arCur = (Array)curVal;
                        Array arNew = (Array)value;
                        for (int x = 0; x < OriginalArrayLengths[prop]; x++)
                        {
                            if (!indexes.Contains(x) && x < arNew.Length)
                            {
                                if (arCur.GetValue(x) is Table)
                                {
                                    if (!((Table)arNew.GetValue(x)).IsSaved)
                                        indexes.Add(x);
                                    if (((Table)arNew.GetValue(x)).ChangedFields.Count > 0)
                                        indexes.Add(x);
                                    else if (!((Table)arCur.GetValue(x)).PrimaryKeysEqual((Table)arNew.GetValue(x)))
                                        indexes.Add(x);
                                }
                                else if (!arCur.GetValue(x).Equals(arNew.GetValue(x)))
                                    indexes.Add(x);
                            }
                        }
                        for (int x = arCur.Length; x < arNew.Length; x++)
                            indexes.Add(x);
                        _replacedArrayIndexes.Add(prop, indexes);
                    }
                }
            }
            foreach (object obj in sf.GetMethod().GetCustomAttributes(true))
            {
                if (obj is ValidationAttribute)
                {
                    ValidationAttribute va = (ValidationAttribute)obj;
                    if (!va.IsValidValue(value))
                        va.FailValidation(this.GetType().ToString(), prop);
                }
                else if (obj is PropertySetChangesField)
                {
                    foreach (string str in ((PropertySetChangesField)obj).FieldAffected)
                    {
                        PropertyInfo pi = this.GetType().GetProperty(str);
                        this[str] = pi.GetValue(this, new object[0]);
                    }
                }
            }
            this[prop] = value;
        }

        private void _CompleteLazyLoad()
        {
            ConnectionPool pool = ConnectionPoolManager.GetPool(this.GetType());
            List<SelectParameter> pars = new List<SelectParameter>();
            foreach (string prop in _map.PrimaryKeyProperties)
                pars.Add(new EqualParameter(prop, this[prop]));
            Connection conn = pool.GetConnection();
            Table tmp = null;
            try
            {
                tmp = conn.Select(this.GetType(), pars)[0];
            }catch (Exception e)
            {
                Logger.LogLine(e);
            }
            conn.CloseConnection();
            if (tmp == null)
                throw new Exception("Unable to load lazy table.");
            List<string> pkeys = new List<string>(_map.PrimaryKeyProperties);
            foreach (string prop in _map.Properties)
            {
                if ((!pkeys.Contains(prop)) && (!tmp.IsFieldNull(prop)))
                    this.SetField(prop, tmp.GetField(prop));
            }
            Type btype = this.GetType().BaseType;
            while (pool.Mapping.IsMappableType(btype))
            {
                sTable map = pool.Mapping[btype];
                foreach (string prop in map.Properties)
                {
                    if ((!pkeys.Contains(prop)) && (!tmp.IsFieldNull(prop)))
                        this.SetField(prop, tmp.GetField(prop));
                }
                btype = btype.BaseType;
            }
            this._loadStatus = LoadStatus.Complete;
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
            }
            else
            {
                return pi.GetValue(this, new object[0]);
            }
        }

        internal void SetField(string FieldName, object value)
        {
            if (FieldName == null)
                return;
            PropertyInfo pi = LocatePropertyInfo(FieldName);
            if (value == null)
                value = pi.GetValue(this.GetType().GetConstructor(Type.EmptyTypes).Invoke(new object[0]), new object[0]);
            if (pi.PropertyType.Equals(typeof(bool)) && !(value.GetType().Equals(typeof(bool))))
            {
                if (value.GetType().Equals(typeof(int)))
                {
                    if ((int)value == 0)
                        this[pi.Name] = false;
                    else
                        this[pi.Name] = true;
                }
                else if (value.GetType().Equals(typeof(string)))
                {
                    if (((string)value).Length == 1)
                    {
                        if ((string)value == "F")
                            value = "False";
                        else
                            value = "True";
                    }
                    this[pi.Name] = bool.Parse((string)value);
                }
                else if (value.GetType().Equals(typeof(char)))
                {
                    if ((char)value == 'F')
                        this[pi.Name] = false;
                    else
                        this[pi.Name] = true;
                }
            }
            else if (pi.PropertyType.Equals(typeof(uint)) || pi.PropertyType.Equals(typeof(UInt32)))
            {
                this[pi.Name] =  BitConverter.ToUInt32(BitConverter.GetBytes(int.Parse(value.ToString())), 0);
            }
            else if (pi.PropertyType.Equals(typeof(ushort)) || pi.PropertyType.Equals(typeof(UInt16)))
            {
                this[pi.Name] = BitConverter.ToUInt16(BitConverter.GetBytes(short.Parse(value.ToString())), 0);
            }
            else if (pi.PropertyType.Equals(typeof(ulong)) || pi.PropertyType.Equals(typeof(UInt64)))
            {
                this[pi.Name] = BitConverter.ToUInt64(BitConverter.GetBytes(long.Parse(value.ToString())), 0);
            }
            else if ((pi.PropertyType.Equals(typeof(byte)) || pi.PropertyType.Equals(typeof(Byte))) && ((value.GetType().Equals(typeof(string)) && value.ToString().Length == 1) || (value.GetType().Equals(typeof(bool)) || value.GetType().Equals(typeof(Boolean)))))
            {
                this[pi.Name] = ((value.GetType().Equals(typeof(bool)) || value.GetType().Equals(typeof(Boolean))) ? (byte)((bool)value ? 'T' : 'F') : (byte)value.ToString()[0]);
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
                        MethodInfo conMethod = null;
                        foreach (MethodInfo mi in pt.GetMethods(BindingFlags.Static | BindingFlags.Public))
                        {
                            if (mi.Name == "op_Implicit" || mi.Name == "op_Explicit")
                            {
                                if (mi.ReturnType.Equals(pt)
                                    && mi.GetParameters().Length == 1
                                    && mi.GetParameters()[0].ParameterType.Equals(value.GetType()))
                                {
                                    conMethod = mi;
                                    break;
                                }
                            }
                        }
                        if (conMethod != null)
                            value = conMethod.Invoke(null, new object[] { value });
                        else if (pt.GetCustomAttributes(typeof(TypeConverterAttribute), false).Length > 0)
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
                this[pi.Name] = value;
            }
        }

        internal bool IsFieldNull(string FieldName)
        {
            if (FieldName == null)
                return true;
            PropertyInfo pi = LocatePropertyInfo(FieldName);
            object cur = this[pi.Name];
            ConnectionPool pool = ConnectionPoolManager.GetPool(this.GetType());
            if (_map[FieldName].Length > 0)
            {
                sTableField fld = _map[FieldName][0];
                if (((pi.PropertyType.Equals(typeof(bool)) || Utility.IsEnum(pi.PropertyType))
                    && !fld.Nullable) ||
                    (new List<string>(_map.PrimaryKeyProperties).Contains(FieldName) && this.IsSaved) ||
                    (!fld.Nullable && this.IsSaved))
                    return false;
                else if (new List<string>(_map.PrimaryKeyFields).Contains(FieldName) && this.IsParentSaved && _isParentPrimaryKey(FieldName, pool, this.GetType()))
                    return (_initialValues.ContainsKey(pi.Name) ? false : true);
                else if (new List<string>(_map.PrimaryKeyFields).Contains(FieldName)
                    && _initialValues.ContainsKey(pi.Name))
                    return _initialValues[FieldName] == cur && !this.IsSaved && !this.IsParentSaved && _loadStatus == LoadStatus.NotLoaded && !_isParentPrimaryKey(FieldName, pool, this.GetType())
                        && !_IsSavedTable(cur);
                return cur == null;
            }
            return equalObjects(cur, pi.GetValue(this.GetType().GetConstructor(Type.EmptyTypes).Invoke(new object[0]), new object[0]));
        }

        internal PropertyInfo LocatePropertyInfo(string FieldName)
        {
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
                ConnectionPool pool = ConnectionPoolManager.GetPool(this.GetType());
                while (pool.Mapping.IsMappableType(t))
                {
                    foreach (PropertyInfo p in t.GetProperties(Utility._BINDING_FLAGS))
                    {
                        if (p.Name == FieldName)
                        {
                            ret = p;
                            break;
                        }
                    }
                    if (ret != null)
                        break;
                    t = t.BaseType;
                }
            }
            if (ret == null)
            {
                foreach (sTableField fld in _map.Fields)
                {
                    if (fld.Name == FieldName)
                    {
                        ret = LocatePropertyInfo(fld.ClassProperty);
                        break;
                    }
                }
            }
            return ret;
        }

        internal bool PrimaryKeysEqual(Table table)
        {
            foreach (string prop in _map.PrimaryKeyProperties)
            {
                object obj = this.GetField(prop);
                if (obj is Table)
                {
                    if (!((Table)obj).PrimaryKeysEqual((Table)table.GetField(prop)))
                        return false;
                }
                else if (!obj.Equals(table.GetField(prop)))
                    return false;
            }
            return true;
        }

        //used to load the original data to be used for update triggers
        internal Table LoadCopyOfOriginal(Connection conn)
        {
            List<SelectParameter> pars = new List<SelectParameter>();
            foreach (string str in _map.PrimaryKeyProperties)
                pars.Add(new EqualParameter(str, _initialValues[str]));
            List<Org.Reddragonit.Dbpro.Structure.Table> tmp = conn.Select(this.GetType(),
                pars.ToArray());
            if (tmp.Count > 0)
                return tmp[0];
            return this;
        }
		
        //called to get the initial value of a primary key field
		internal object GetInitialPrimaryValue(string ClassFieldName)
		{
			if ((_map.PrimaryKeyProperties!=null)&&new List<string>(_map.PrimaryKeyProperties).Contains(ClassFieldName)&&(_initialValues.ContainsKey(ClassFieldName)))
				return _initialValues[ClassFieldName];
			return null;
		}
		
        //called to copy values from an existing table object, this is done for table inheritance
		internal void CopyValuesFrom(Table table)
		{
            ConnectionPool pool = ConnectionPoolManager.GetPool(table.GetType());
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
            if (table.GetType() == this.GetType().BaseType)
            {
                this._isParentSaved = table.IsSaved;
                if (this._isParentSaved)
                {
                    foreach (string str in _map.PrimaryKeyFields)
                    {
                        if (table[str] != null && _values.ContainsKey(str))
                            _values.Remove(str);
                        if (table[str] != null && !_initialValues.ContainsKey(str))
                            _initialValues.Add(str,table[str]);
                    }
                }
            }
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

        //returnf if the parent table the object was converted from was saved
        internal bool IsParentSaved
        {
            get
            {
                return _isParentSaved;
            }
        }

        //called by a connection to set the values in the table object from the generated query.
        internal void SetValues(Connection conn)
        {
            _loadStatus = LoadStatus.Loading;
            Logger.LogLine("Obtaining table map for " + this.GetType().FullName + " to allow setting of values from query");
            Logger.LogLine("Recursively setting values from query for " + this.GetType().FullName);
            RecurSetValues(_map, conn);
            _values.Clear();
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
                if (fProps.Contains(prop) && !eMap.IsEnumProperty(prop))
                {
                    PropertyInfo pi = table.GetType().GetProperty(prop, Utility._BINDING_FLAGS);
                    if (pi == null)
                        pi = table.GetType().GetProperty(prop, Utility._BINDING_FLAGS_WITH_INHERITANCE);
                    Table t = (Table)pi.PropertyType.GetConstructor(Type.EmptyTypes).Invoke(new object[0]);
                    t._loadStatus = LoadStatus.Partial;
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
                    if (!t.AllPrimaryKeysNull)
                    {
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
                                    if (f.Type == FieldType.ENUM)
                                        table.SetField(prop, conn.Pool.GetEnumValue(table.GetType().GetProperty(f.ClassProperty,Utility._BINDING_FLAGS_WITH_INHERITANCE).PropertyType, (int)conn[fld.Name]));
                                    else
                                        table.SetField(prop, conn[fld.Name]);
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
                    else if (fld.Type == FieldType.ENUM)
                    {
                        PropertyInfo pi = table.GetType().GetProperty(fld.ClassProperty, Utility._BINDING_FLAGS);
                        if (pi == null)
                            pi = table.GetType().GetProperty(fld.ClassProperty, Utility._BINDING_FLAGS_WITH_INHERITANCE);
                        table.SetField(fld.ClassProperty, conn.GetEnum(pi.PropertyType, queryFieldName));
                    }
                    else
                    {
                        if (table.GetField(fld.ClassProperty) == null)
                        {
                            PropertyInfo pi = table.GetType().GetProperty(fld.ClassProperty, Utility._BINDING_FLAGS);
                            if (pi == null)
                                pi = table.GetType().GetProperty(fld.ClassProperty, Utility._BINDING_FLAGS_WITH_INHERITANCE);
                            Table t = (Table)pi.PropertyType.GetConstructor(Type.EmptyTypes).Invoke(new object[0]);
                            t._loadStatus = LoadStatus.Partial;
                            table.SetField(fld.Name, t);
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
                if (!map.ArrayProperties.Contains(prop))
                {
                    PropertyInfo pi = ty.GetProperty(prop, Utility._BINDING_FLAGS);
                    if (pi != null)
                    {
                        if (pi.DeclaringType == ty)
                        {
                            if (extFields.Contains(prop) && !Utility.IsEnum(pi.PropertyType))
                            {
                                Table t = (Table)pi.PropertyType.GetConstructor(Type.EmptyTypes).Invoke(new object[0]);
                                t._loadStatus = LoadStatus.Partial;
                                bool setValue = false;
                                t = SetExternalValues(map, prop, conn, out setValue, t);
                                if (!t.AllPrimaryKeysNull && setValue)
                                {
                                    this.SetField(prop, t);
                                }
                            }
                            else
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
			}
			if (conn.Pool.Mapping.IsMappableType(ty.BaseType))
			{
				RecurSetValues(conn.Pool.Mapping[ty.BaseType],conn);
			}
		}

		internal bool AllPrimaryKeysNull
		{
			get
			{
				foreach (string prop in ConnectionPoolManager.GetPool(GetType()).Mapping[GetType()].PrimaryKeyFields)
				{
					if (!IsFieldNull(prop))
					{
						return false;
					}
				}
				return true;
			}
		}

        private bool _IsSavedTable(object cur)
        {
            if (cur is Table)
                return ((Table)cur)._loadStatus!= LoadStatus.NotLoaded;
            return false;
        }

        private bool _isParentPrimaryKey(string FieldName,ConnectionPool pool,Type type)
        {
            if (pool.Mapping.IsMappableType(type.BaseType))
            {
                if (pool.Mapping[type.BaseType][FieldName].Length > 0)
                    return new List<string>(_map.PrimaryKeyFields).Contains(FieldName);
            }
            return false;
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
            {
                Type btype = this.GetType().BaseType;
                while (btype != typeof(Table))
                {
                    if (conversionType.IsSubclassOf(btype))
                        return ((Table)ToType(btype, null)).ToType(conversionType, null);
                    else
                        btype = btype.BaseType;
                }
                throw new Exception("Cannot convert object to type that is not a parent/child or has no matching inheritance of the current class.");
            }
			Table ret = (Table)conversionType.GetConstructor(Type.EmptyTypes).Invoke(new object[0]);
            sTable map = ConnectionPoolManager.GetPool(conversionType).Mapping[conversionType];
            if (conversionType.IsSubclassOf(this.GetType()))
                map = ConnectionPoolManager.GetPool(this.GetType()).Mapping[this.GetType()];
            else
            {
                ret._isSaved = this._isSaved;
                ret._loadStatus = this._loadStatus;
            }
            foreach (string prop in map.Properties)
            {
                if (_initialValues.ContainsKey(prop))
                    ((Table)ret)._initialValues.Add(prop, _initialValues[prop]);
                if (this.ChangedFields != null)
                {
                    if (this.ChangedFields.Contains(prop))
                        ((Table)ret)._values.Add(prop,this.GetField(prop));
                }
            }
            ConnectionPool pool = ConnectionPoolManager.GetPool(ret.GetType());
            Type t = ret.GetType().BaseType;
            if (conversionType.IsSubclassOf(this.GetType()))
                t = this.GetType().BaseType;
            while (pool.Mapping.IsMappableType(t))
            {
                map = pool.Mapping[t];
                foreach (string prop in map.Properties)
                {
                    if (_initialValues.ContainsKey(prop))
                    {
                        ((Table)ret)._initialValues.Remove(prop);
                        ((Table)ret)._initialValues.Add(prop, _initialValues[prop]);
                    }
                    if (this.ChangedFields != null)
                    {
                        if (this.ChangedFields.Contains(prop))
                        {
                            ((Table)ret)._values.Remove(prop);
                            ((Table)ret)._values.Add(prop, this.GetField(prop));
                        }
                    }
                }
                t = t.BaseType;
            }
            ret._isParentSaved = this.IsParentSaved||(this.IsSaved&&ret.GetType().BaseType==this.GetType());
			return ret;
		}

        //Called to delete the instance of the table object.
        public void Delete()
        {
            if (!this.IsSaved)
                throw new Exception("Cannot delete an object from the database if it is not in the database.");
            Connection conn = ConnectionPoolManager.GetConnection(this.GetType());
            try
            {
                conn.Delete(this);
            }
            catch (Exception e)
            {
                conn.CloseConnection();
                throw e;
            }
            conn.CloseConnection();
        }

        //Called to update the instance of the table object.
        public void Update()
        {
            if (!this.IsSaved)
                throw new Exception("Cannot update an object that is not in the database.");
            Connection conn = ConnectionPoolManager.GetConnection(this.GetType());
            try
            {
                conn.Save(this);
            }
            catch (Exception e)
            {
                conn.CloseConnection();
                throw e;
            }
            conn.CloseConnection();
        }

        //Called to save the instance of the table object into the database
        public void Save()
        {
            if (this.IsSaved)
                throw new Exception("Cannot Save an object to the database when it already exists.");
            Connection conn = ConnectionPoolManager.GetConnection(this.GetType());
            Table tmp = null;
            try
            {
                tmp = conn.Save(this);
            }
            catch (Exception e)
            {
                conn.CloseConnection();
                throw e;
            }
            conn.CloseConnection();
            if (tmp == null)
                throw new Exception("An error occured attempting to save the table.");
            foreach (string prop in _map.Properties)
            {
                PropertyInfo pi = this.GetType().GetProperty(prop, Utility._BINDING_FLAGS);
                if (pi == null)
                    pi = this.GetType().GetProperty(prop, Utility._BINDING_FLAGS_WITH_INHERITANCE);
                if (pi.CanWrite)
                    this[pi.Name] = pi.GetValue(tmp, new object[] { });
            }
            this._isSaved = true;
            foreach(string str in _values.Keys)
            {
                _initialValues.Remove(str);
                _initialValues.Add(str, _values[str]);
            }
            _values.Clear();
        }
    }
}
