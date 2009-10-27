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
using Org.Reddragonit.Dbpro.Structure.Mapping;
using FieldNamePair = Org.Reddragonit.Dbpro.Structure.Mapping.TableMap.FieldNamePair;
using Org.Reddragonit.Dbpro.Validation;

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
		
		private TableMap _map;
		private bool _allowPrimaryChange=true;
		private FieldMap _mainPrimary=null;
		private List<string> _changedFields = new List<string>();
		
		public void Dispose()
		{
			DetachServer();
		}
		
		public LazyProxy(object subject):base(subject.GetType())
		{
			_map = ClassMapper.GetTableMap(subject.GetType());
			_allowPrimaryChange=ConnectionPoolManager.GetConnection(_map.ConnectionName).AllowChangingBasicAutogenField;
			if ((_map.PrimaryKeys.Count==1)&&(_map.PrimaryKeys[0].AutoGen))
				_mainPrimary=_map.PrimaryKeys[0];
			AttachServer((MarshalByRefObject)subject);
		}
		
		public static object Instance(Object obj)
		{
			return new LazyProxy(obj).GetTransparentProxy();
		}
		
        //this function is called to convert the called method into a propertyinfo
        //object if it is in fact a property, otherwise it returns a null.  It also
        //inidicates if the function is a get or a set call.
		protected static PropertyInfo GetMethodProperty(MethodInfo methodInfo, object owner, out bool IsGet)
		{
			foreach(PropertyInfo aProp in owner.GetType().GetProperties(BindingFlags.Public |      //Get public members
			                                                            BindingFlags.NonPublic |   //Get private/protected/internal members
			                                                            BindingFlags.Static |      //Get static members
			                                                            BindingFlags.Instance |    //Get instance members
			                                                            BindingFlags.DeclaredOnly  ))
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

            foreach (object obj in mi.GetCustomAttributes(true))
            {
                if (obj is CompleteLazyLoadPriorToCall)
                {
                    if ((((Table)owner).LoadStatus == LoadStatus.Partial) && !((mi.Name == "SetField") || (mi.Name == "SetValues")))
                    {
                        List<SelectParameter> pars = new List<SelectParameter>();
                        foreach (InternalFieldMap ifm in _map.PrimaryKeys)
                        {
                            if (_map.GetClassFieldName(ifm.FieldName) == null)
                                pars.Add(new EqualParameter(_map.GetExternalClassFieldName(ifm.FieldName), ((Table)owner).GetField(_map.GetExternalClassFieldName(ifm.FieldName))));
                            else
                                pars.Add(new EqualParameter(_map.GetClassFieldName(ifm.FieldName), ((Table)owner).GetField(_map.GetClassFieldName(ifm.FieldName))));
                        }
                        Connection conn = ConnectionPoolManager.GetConnection(_map.ConnectionName).getConnection();
                        Table tmp = conn.Select(owner.GetType(), pars)[0];
                        foreach (FieldNamePair fnp in _map.FieldNamePairs)
                        {
                            if ((!_map[fnp].PrimaryKey) && (!tmp.IsFieldNull(fnp.ClassFieldName)))
                                ((Table)owner).SetField(fnp.ClassFieldName, tmp.GetField(fnp.ClassFieldName));
                        }
                        conn.CloseConnection();
                        ((Table)owner).LoadStatus = LoadStatus.Complete;
                    }
                    break;
                }
            }
			
			if (owner!=null)
			{
				bool isGet=false;
				PropertyInfo pi = GetMethodProperty(mi,owner, out isGet);
				
				if ((pi!=null)&&(_map[pi.Name]!=null))
				{
                    FieldMap fm = _map[pi.Name];
					if (pi.Name!="LoadStatus")
					{
						if ((((Table)owner).LoadStatus== LoadStatus.Partial)&&(!fm.PrimaryKey))
						{
							List<SelectParameter> pars = new List<SelectParameter>();
							foreach (InternalFieldMap ifm in _map.PrimaryKeys)
								pars.Add(new EqualParameter(_map.GetClassFieldName(ifm.FieldName),((Table)owner).GetField(_map.GetClassFieldName(ifm.FieldName))));
							Connection conn = ConnectionPoolManager.GetConnection(_map.ConnectionName).getConnection();
							Table tmp = conn.Select(owner.GetType(),pars)[0];
							foreach (FieldNamePair fnp in _map.FieldNamePairs)
							{
								if ((!_map[fnp].PrimaryKey)&&(!tmp.IsFieldNull(fnp.ClassFieldName)))
								    ((Table)owner).SetField(fnp.ClassFieldName,tmp.GetField(fnp.ClassFieldName));
							}
                            conn.CloseConnection();
                            ((Table)owner).LoadStatus = LoadStatus.Complete;
						}
					}
					if (isGet)
					{
						outVal = mi.Invoke(owner, mc.Args);
						if ((fm is ExternalFieldMap)&&(outVal!=null))
						{
							ExternalFieldMap efm = (ExternalFieldMap)fm;
							if (efm.IsArray)
							{
								Table[] vals = (Table[])outVal;
								TableMap map = ClassMapper.GetTableMap(efm.Type);
								Connection conn = ConnectionPoolManager.GetConnection(_map.ConnectionName).getConnection();
								for (int x=0;x<vals.Length;x++)
								{
									List<SelectParameter> pars = new List<SelectParameter>();
                                    foreach (InternalFieldMap ifm in map.PrimaryKeys)
                                    {
                                        if (map.GetClassFieldName(ifm.FieldName) == null)
                                            pars.Add(new EqualParameter(map.GetExternalClassFieldName(ifm.FieldName), ((Table)vals[x]).GetField(map.GetExternalClassFieldName(ifm.FieldName))));
                                        else
                                            pars.Add(new EqualParameter(map.GetClassFieldName(ifm.FieldName), ((Table)vals[x]).GetField(map.GetClassFieldName(ifm.FieldName))));
                                    }
									vals[x]=conn.Select(efm.Type,pars)[0];
								}
								pi.SetValue(owner,vals,new object[0]);
								outVal=vals;
								conn.CloseConnection();
							}else{
								Table t = (Table)outVal;
								if (t.LoadStatus== LoadStatus.Partial)
								{
									List<SelectParameter> pars = new List<SelectParameter>();
									TableMap map = ClassMapper.GetTableMap(t.GetType());
                                    foreach (InternalFieldMap ifm in map.PrimaryKeys)
                                    {
                                        if (map.GetClassFieldName(ifm.FieldName) == null)
                                            pars.Add(new EqualParameter(map.GetExternalClassFieldName(ifm.FieldName), t.GetField(map.GetExternalClassFieldName(ifm.FieldName))));
                                        else
                                            pars.Add(new EqualParameter(map.GetClassFieldName(ifm.FieldName), t.GetField(map.GetClassFieldName(ifm.FieldName))));
                                    }
									Connection conn = ConnectionPoolManager.GetConnection(_map.ConnectionName).getConnection();
									t = conn.Select(outVal.GetType(),pars)[0];
									pi.SetValue(owner,t,new object[0]);
									outVal=t;
                                    conn.CloseConnection();
								}
							}
						}
					}else
					{
						if (((Table)owner).IsSaved&&fm.PrimaryKey&&fm.AutoGen)
							throw new AlterPrimaryKeyException(owner.GetType().ToString(),pi.Name);
						if (((Table)owner)._isSaved)
						{
							object curVal = pi.GetValue(owner,new object[0]);
							if (((curVal==null)&&(mc.Args[0]!=null))||
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
                    else if (mi.Name == "GetType")
                        outVal = owner.GetType();
                    else if ((mi.Name=="FieldSetter")&&(mc.Args.Length==3)&&(mc.Args[1].ToString().Trim()=="_isSaved"))
                    	((Table)owner)._isSaved=(bool)mc.Args[2];
                    else if ((mi.Name=="FieldGetter")&&(mc.Args.Length==2)&&(mc.Args[1].ToString().Trim()=="_isSaved"))
                    	outVal = ((Table)owner)._isSaved;
                    else
                    {
                        try
                        {
                            outVal = mi.Invoke(owner, mc.Args);
                        }
                        catch (Exception ex)
                        {
                            System.Diagnostics.Debug.WriteLine("Method Call: " + mc.MethodName);
                            System.Diagnostics.Debug.WriteLine(ex.Message);
                            System.Diagnostics.Debug.WriteLine(ex.Source);
                            System.Diagnostics.Debug.WriteLine(ex.StackTrace);
                            Exception e = ex.InnerException;
                            while (e != null)
                            {
                                System.Diagnostics.Debug.WriteLine(e.Message);
                                System.Diagnostics.Debug.WriteLine(e.Source);
                                System.Diagnostics.Debug.WriteLine(e.StackTrace);
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
	}
}

