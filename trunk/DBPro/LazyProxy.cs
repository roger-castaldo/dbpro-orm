/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 19/01/2009
 * Time: 1:46 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using Org.Reddragonit.Dbpro.Connections.Parameters;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.Remoting.Messaging;
using System.Runtime.Remoting.Proxies;
using Org.Reddragonit.Dbpro.Connections;
using Org.Reddragonit.Dbpro.Exceptions;
using Org.Reddragonit.Dbpro.Structure;
using Org.Reddragonit.Dbpro.Validation;
using Org.Reddragonit.Dbpro.Connections.PoolComponents;

namespace Org.Reddragonit.Dbpro
{
	/// <summary>
	/// This class is designed to produce a virtual proxy around each instance of a table.
    /// The proxy's purpose is to wrap lazy calls into the system to allow partial loading where 
    /// necessary such as with externally related tables.  It does this by loading the minimum
    /// amount of data, primary keys, into the table class and marking it as paritally loaded.
    /// When a call is made to finish the loading, the proxy here detects that a call
    /// was made to the item itself and executes the code to load the particiular table
    /// completely.  There is a tag called CompleteLazyLoadPriorToCall that 
    /// forces the loading to be completed when the attributed funcion is called.
	/// </summary>
	internal class LazyProxy : RealProxy, IDisposable
	{

        private ConnectionPool _pool;
		private sTable _map;
		private bool _allowPrimaryChange=true;
		private sTableField? _mainPrimary=null;
		private List<string> _changedFields = new List<string>();
        private Dictionary<string, int> _originalArrayLengths = new Dictionary<string, int>();
        private Dictionary<string, List<int>> _replacedArrayIndexes = new Dictionary<string, List<int>>();
		
		public void Dispose()
		{
			DetachServer();
		}
		
		public LazyProxy(object subject):base(subject.GetType())
		{
            _pool = ConnectionPoolManager.GetPool(subject.GetType());
            _map = _pool.Mapping[subject.GetType()];
            _allowPrimaryChange = _pool.AllowChangingBasicAutogenField;
            if ((_map.PrimaryKeyFields.Length == 1) && (_map.AutoGenProperty != null))
                _mainPrimary = _map[_map.AutoGenProperty][0];
			AttachServer((MarshalByRefObject)subject);
		}
		
		public static object Instance(Object obj)
		{
			return new LazyProxy(obj).GetTransparentProxy();
		}
		
        //this function is called to convert the called method into a propertyinfo
        //object if it is in fact a property, otherwise it returns a null.  It also
        //inidicates if the function is a get or a set call.
		protected static PropertyInfo GetMethodProperty(MethodInfo methodInfo, object owner, out bool IsGet,Type tableType,ConnectionPool pool)
		{
            foreach (PropertyInfo aProp in owner.GetType().GetProperties(Utility._BINDING_FLAGS))
			{
				MethodInfo mi = null;
				mi = aProp.GetGetMethod(true);
				if( mi != null && (mi.Name==methodInfo.Name))
				{
					IsGet = true;
					return aProp;
				}
				mi = aProp.GetSetMethod(true);
				if( mi != null && (mi.Name==methodInfo.Name))
				{
					IsGet = false;
					return aProp;
				}
			}
            if (pool.Mapping.IsMappableType(tableType.BaseType))
            {
                PropertyInfo ret = GetMethodProperty(methodInfo, Convert.ChangeType(owner, tableType.BaseType), out IsGet, tableType.BaseType,pool);
                if (ret != null)
                    return ret;
            }
			IsGet = false;
			return null;
		}
		
        //called to override the hashcode and get it from the underlying object
		public override int GetHashCode()
		{
			return GetUnwrappedServer().GetHashCode();
		}
		
        //called to override the tostring and get it from the underlying object.
		public override string ToString()
		{
			return GetUnwrappedServer().ToString();
		}
		
        /*
         * This is the main function in the lazy proxy.  Any time a method or property is called
         * within a proxied class this will get called.  The first stage is to check for the 
         * CompleteLazyLoadPriorToCall attribute, if it is there and
         * the load status is only partial and that we are not calling functions to set field
         * values, ie already loading, then establish a select using the 
         * primary keys as parameters to the query, execute it and 
         * load the values into the current table object.  Following this we then check to
         * see if the object is null, if it is simply call the function and return the results.
         * If its not null then attempt to pull the property info, if it is a property and it
         * is a get call, then checks are performed for the object being a lazy loaded object,
         * or the table being lazy loaded and the call not being for a primary key,
         * or it being an array of lazy loaded object, in all cases, complete their
         * loading and set the value of the object, then return the value.  If it is a set call
         * then we need to log if the field has now become set to a null value, or
         * if it has changed at all, in either case mark it for use when update is called
         * on the object.
         * 
         */
		public override IMessage Invoke(System.Runtime.Remoting.Messaging.IMessage msg)
		{
			MethodCallMessageWrapper mc = new MethodCallMessageWrapper((IMethodCallMessage)msg);
			MarshalByRefObject owner = GetUnwrappedServer();
			MethodInfo mi = (MethodInfo)mc.MethodBase;
			
			object outVal=null;
            List<string> fieldsAffected = null;

            foreach (object obj in mi.GetCustomAttributes(true))
            {
                if (obj is CompleteLazyLoadPriorToCall)
                {
                    if ((((Table)owner).LoadStatus == LoadStatus.Partial) && !((mi.Name == "SetField") || (mi.Name == "SetValues")))
                    {
                        List<SelectParameter> pars = new List<SelectParameter>();
                        foreach (string prop in _map.PrimaryKeyProperties)
                            pars.Add(new EqualParameter(prop, ((Table)owner).GetField(prop)));
                        Connection conn = ConnectionPoolManager.GetConnection(owner.GetType());
                        Table tmp = null;
                        try
                        {
                            tmp = conn.Select(owner.GetType(), pars)[0];
                        }
                        catch (Exception e)
                        {
                            Logger.LogLine(e);
                        }
                        conn.CloseConnection();
                        if (tmp == null)
                            throw new Exception("Unable to Load Lazy Porxy Instance of table");
                        List<string> pkeys = new List<string>(_map.PrimaryKeyProperties);
                        foreach (string prop in _map.Properties)
                        {
                            if ((!pkeys.Contains(prop)) && (!tmp.IsFieldNull(prop)))
                                ((Table)owner).SetField(prop, tmp.GetField(prop));
                        }
                        ((Table)owner).LoadStatus = LoadStatus.Complete;
                    }
                }
                else if (obj is MethodInvokeChangesField)
                {
                    if (fieldsAffected == null)
                        fieldsAffected = new List<string>();
                    fieldsAffected.AddRange(((MethodInvokeChangesField)obj).FieldAffected);
                }
            }
			
			if (owner!=null)
			{
				bool isGet=false;
				PropertyInfo pi = GetMethodProperty(mi,owner, out isGet,owner.GetType(),_pool);
                if (pi != null)
                {
                    foreach (object obj in pi.GetCustomAttributes(true))
                    {
                        if (obj is CompleteLazyLoadPriorToCall)
                        {
                            if ((((Table)owner).LoadStatus == LoadStatus.Partial) && !((mi.Name == "SetField") || (mi.Name == "SetValues")))
                            {
                                CompleteLazyLoad(owner);
                            }
                        }
                        else if (obj is PropertySetChangesField)
                        {
                            if (fieldsAffected == null)
                                fieldsAffected = new List<string>();
                            fieldsAffected.AddRange(((PropertySetChangesField)obj).FieldAffected);
                        }
                    }
                }

				if ((pi!=null)&&(_IsParentTableField(pi.Name,owner)||new List<string>(_map.ArrayProperties).Contains(pi.Name)))
				{
					if (pi.Name!="LoadStatus")
					{
						if ((((Table)owner).LoadStatus== LoadStatus.Partial)&&(!new List<string>(_map.PrimaryKeyProperties).Contains(pi.Name)))
						{
                            CompleteLazyLoad(owner);
						}
					}
					if (isGet)
					{
						outVal = mi.Invoke(owner, mc.Args);
                        /*if ((new List<string>(_map.ForeignTableProperties).Contains(pi.Name))&&(outVal!=null)&&!Utility.IsEnum(pi.PropertyType))
                        {
                            if (pi.PropertyType.IsArray)
                            {
                                Table[] vals = (Table[])outVal;
                                sTable map = _pool.Mapping[pi.PropertyType.GetElementType()];
                                Connection conn = ConnectionPoolManager.GetConnection(pi.PropertyType.GetElementType()).getConnection();
                                for (int x=0;x<vals.Length;x++)
                                {
                                    List<SelectParameter> pars = new List<SelectParameter>();
                                    foreach (string prop in map.PrimaryKeyProperties)
                                        pars.Add(new EqualParameter(prop, ((Table)vals[x]).GetField(prop)));
                                    vals[x]=conn.Select(pi.PropertyType.GetElementType(),pars)[0];
                                }
                                pi.SetValue(owner,vals,new object[0]);
                                outVal=vals;
                                conn.CloseConnection();
                            }else{
                                Table t = (Table)outVal;
                                if (t.LoadStatus== LoadStatus.Partial)
                                {
                                    List<SelectParameter> pars = new List<SelectParameter>();
                                    sTable map = _pool.Mapping[pi.PropertyType];
                                    foreach (string prop in map.PrimaryKeyProperties)
                                        pars.Add(new EqualParameter(prop,t.GetField(prop)));
                                    Connection conn = ConnectionPoolManager.GetConnection(pi.PropertyType).getConnection();
                                    t = conn.Select(outVal.GetType(),pars)[0];
                                    pi.SetValue(owner,t,new object[0]);
                                    outVal=t;
                                    conn.CloseConnection();
                                }
                            }
                        }*/
					}else
					{
                        sTable map = ConnectionPoolManager.GetPool(owner.GetType()).Mapping[owner.GetType()];
						if (((Table)owner).IsSaved&&new List<string>(map.PrimaryKeyProperties).Contains(pi.Name)&&Utility.StringsEqual(pi.Name,map.AutoGenProperty))
							throw new AlterPrimaryKeyException(owner.GetType().ToString(),pi.Name);
						if (((Table)owner)._isSaved)
						{
							object curVal = pi.GetValue(owner,new object[0]);
                            if (map.ArrayProperties.Contains(pi.Name))
                            {
                                if (!_originalArrayLengths.ContainsKey(pi.Name))
                                    _originalArrayLengths.Add(pi.Name,(curVal==null ? 0 : ((Array)curVal).Length));
                                if (curVal != null && mc.Args[0] != null)
                                {
                                    List<int> indexes = new List<int>();
                                    if (_replacedArrayIndexes.ContainsKey(pi.Name))
                                    {
                                        indexes = _replacedArrayIndexes[pi.Name];
                                        _replacedArrayIndexes.Remove(pi.Name);
                                    }
                                    Array arCur = (Array)curVal;
                                    Array arNew = (Array)mc.Args[0];
                                    for (int x = 0; x < _originalArrayLengths[pi.Name]; x++)
                                    {
                                        if (!indexes.Contains(x) && x<arNew.Length)
                                        {
                                            if (arCur.GetValue(x) is Table)
                                            {
                                                if (!((Table)arNew.GetValue(x))._isSaved)
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
                                    if (indexes.Count > 0 && !_changedFields.Contains(pi.Name))
                                        _changedFields.Add(pi.Name);
                                    _replacedArrayIndexes.Add(pi.Name, indexes);
                                }
                                else if (!_changedFields.Contains(pi.Name))
                                    _changedFields.Add(pi.Name);
                            }else if (((curVal==null)&&(mc.Args[0]!=null))||
							    ((curVal!=null)&&(mc.Args[0]==null))||
							    ((curVal!=null)&&(mc.Args[0]!=null)&&(!curVal.Equals(mc.Args[0]))))
							{
								if (!_changedFields.Contains(pi.Name))
									_changedFields.Add(pi.Name);
							}
						}
                        foreach (object obj in pi.GetCustomAttributes(true))
                        {
                            if (obj is ValidationAttribute)
                            {
                                ValidationAttribute va = (ValidationAttribute)obj;
                                if (!va.IsValidValue(mc.Args[0]))
                                    va.FailValidation(owner.GetType().ToString(), pi.Name);
                            }
                        }
						outVal = mi.Invoke(owner, mc.Args);
					}
				}else
				{
                    if (mi.Name == "Update")
                        ((Table)owner)._changedFields = _changedFields;
                    if ((pi != null) && (pi.Name == "IsProxied"))
                        outVal = true;
                    if ((pi != null) && (pi.Name == "ChangedFields"))
                        outVal = _changedFields;
                    else if (((pi != null) && (pi.Name == "OriginalArrayLengths")) || mi.Name == "get_OriginalArrayLengths")
                        outVal = _originalArrayLengths;
                    else if (((pi != null) && (pi.Name == "ReplacedArrayIndexes")) || mi.Name == "get_ReplacedArrayIndexes")
                        outVal = _replacedArrayIndexes;
                    else if (mi.Name == "get_ChangedFields")
                        outVal = _changedFields;
                    else if (mi.Name == "GetType")
                        outVal = owner.GetType();
                    else if ((mi.Name == "FieldSetter") && (mc.Args.Length == 3) && (mc.Args[1].ToString().Trim() == "_isSaved"))
                        ((Table)owner)._isSaved = (bool)mc.Args[2];
                    else if ((mi.Name == "FieldGetter") && (mc.Args.Length == 2) && (mc.Args[1].ToString().Trim() == "_isSaved"))
                        outVal = ((Table)owner)._isSaved;
                    else if ((mi.Name == "FieldSetter") && (mc.Args.Length == 3) && (mc.Args[1].ToString().Trim() == "_changedFields"))
                        ((Table)owner)._changedFields = (List<string>)mc.Args[2];
                    else if ((pi != null) && (!isGet) && (fieldsAffected != null) && ((Table)owner)._isSaved)
                    {
                        object curVal = pi.GetValue(owner, new object[0]);
                        if (((curVal == null) && (mc.Args[0] != null)) ||
                            ((curVal != null) && (mc.Args[0] == null)) ||
                            ((curVal != null) && (mc.Args[0] != null) && (!curVal.Equals(mc.Args[0]))))
                        {
                            foreach (string str in fieldsAffected)
                            {
                                if (!_changedFields.Contains(str))
                                    _changedFields.Add(str);
                            }
                        }
                        outVal = mi.Invoke(owner, mc.Args);
                    }
                    else
                    {
                        try
                        {
                            outVal = mi.Invoke(owner, mc.Args);
                            if (fieldsAffected != null)
                            {
                                foreach (string str in fieldsAffected)
                                {
                                    if (!_changedFields.Contains(str))
                                        _changedFields.Add(str);
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            Logger.LogLine("Method Call: " + mc.MethodName);
                            Logger.LogLine(ex.Message);
                            Logger.LogLine(ex.Source);
                            Logger.LogLine(ex.StackTrace);
                            Exception e = ex.InnerException;
                            while (e != null)
                            {
                                Logger.LogLine(e.Message);
                                Logger.LogLine(e.Source);
                                Logger.LogLine(e.StackTrace);
                                e = e.InnerException;
                            }
                        }
                    }
				}
			}

            if (mi.Name == "Update")
            {
                _changedFields = new List<string>();
                ((Table)owner)._changedFields = null;
            }

			return new ReturnMessage(outVal,mc.Args,mc.Args.Length, mc.LogicalCallContext, mc);
		}

        private bool _IsParentTableField(string p,object owner)
        {
            if (_map[p].Length > 0)
                return true;
            else
            {
                Type btype = owner.GetType().BaseType;
                while (_pool.Mapping.IsMappableType(btype))
                {
                    if (_pool.Mapping[btype][p].Length > 0)
                        return true;
                    btype = btype.BaseType;
                }
            }
            return false;
        }

        private void CompleteLazyLoad(object owner)
        {
            List<SelectParameter> pars = new List<SelectParameter>();
            foreach (string prop in _map.PrimaryKeyProperties)
                pars.Add(new EqualParameter(prop, ((Table)owner).GetField(prop)));
            Connection conn = ConnectionPoolManager.GetConnection(owner.GetType());
            Table tmp = null;
            try
            {
                tmp = conn.Select(owner.GetType(), pars)[0];
            }
            catch (Exception e)
            {
                Logger.LogLine(e);
            }
            conn.CloseConnection();
            if (tmp == null)
                throw new Exception("Unable to load lazy proxied table.");
            List<string> pkeys = new List<string>(_map.PrimaryKeyProperties);
            foreach (string prop in _map.Properties)
            {
                if ((!pkeys.Contains(prop)) && (!tmp.IsFieldNull(prop)))
                    ((Table)owner).SetField(prop, tmp.GetField(prop));
            }
            Type btype = owner.GetType().BaseType;
            while (_pool.Mapping.IsMappableType(btype))
            {
                sTable map = _pool.Mapping[btype];
                foreach (string prop in map.Properties)
                {
                    if ((!pkeys.Contains(prop)) && (!tmp.IsFieldNull(prop)))
                        ((Table)owner).SetField(prop, tmp.GetField(prop));
                }
                btype = btype.BaseType;
            }
            ((Table)owner).LoadStatus = LoadStatus.Complete;
        }
	}
}

