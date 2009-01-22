/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 19/01/2009
 * Time: 1:46 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using Org.Reddragonit.Dbpro.Connections;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.Remoting.Messaging;
using System.Runtime.Remoting.Proxies;
using Org.Reddragonit.Dbpro.Exceptions;
using Org.Reddragonit.Dbpro.Structure;
using Org.Reddragonit.Dbpro.Structure.Mapping;

namespace Org.Reddragonit.Dbpro
{
	/// <summary>
	/// Description of LazyProxy.
	/// </summary>
	internal class LazyProxy : RealProxy, IDisposable
	{
		
		private TableMap _map;
		
		public void Dispose()
		{
			DetachServer();
		}
		
		public LazyProxy(object subject):base(subject.GetType())
		{
			_map = ClassMapper.GetTableMap(subject.GetType());
			AttachServer((MarshalByRefObject)subject);
		}
		
		public static object Instance(Object obj)
		{
			return new LazyProxy(obj).GetTransparentProxy();
		}
		
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
		
		public override int GetHashCode()
		{
			return GetUnwrappedServer().GetHashCode();
		}
		
		public override string ToString()
		{
			return GetUnwrappedServer().ToString();
		}
		
		public override IMessage Invoke(System.Runtime.Remoting.Messaging.IMessage msg)
		{
			MethodCallMessageWrapper mc = new MethodCallMessageWrapper((IMethodCallMessage)msg);
			MarshalByRefObject owner = GetUnwrappedServer();
			MethodInfo mi = (MethodInfo)mc.MethodBase;
			
			object outVal=null;
			
			if (owner!=null)
			{				
				bool isGet=false;				
				PropertyInfo pi = GetMethodProperty(mi,owner, out isGet);
				
				if ((pi!=null)&&(_map[pi.Name]!=null))
				{
					FieldMap fm = _map[pi.Name];
					if (isGet)
					{
						outVal = mi.Invoke(owner, mc.Args);
						if ((fm is ExternalFieldMap)&&(outVal!=null))
						{
							ExternalFieldMap efm = (ExternalFieldMap)fm;
							Table t = (Table)outVal;
							if (t.LoadStatus== LoadStatus.Partial)
							{
								List<SelectParameter> pars = new List<SelectParameter>();
								TableMap map = ClassMapper.GetTableMap(t.GetType());
								foreach (InternalFieldMap ifm in map.PrimaryKeys)
								{
									pars.Add(new SelectParameter(map.GetClassFieldName(ifm.FieldName),t.GetField(map.GetClassFieldName(ifm.FieldName))));
								}
								Connection conn = ConnectionPoolManager.GetConnection(_map.ConnectionName).getConnection();
								t = conn.Select(outVal.GetType(),pars)[0];
								pi.SetValue(owner,t,new object[0]);
								outVal=t;
							}
						}
					}else
					{
						if (((Table)owner).IsSaved&&fm.PrimaryKey)
							throw new AlterPrimaryKeyException(owner.GetType().ToString(),pi.Name);
						outVal = mi.Invoke(owner, mc.Args);
					}
				}else
					outVal=mi.Invoke(owner,mc.Args);
			}
			
			return new ReturnMessage(outVal,mc.Args,mc.Args.Length, mc.LogicalCallContext, mc);
		}
	}
}
