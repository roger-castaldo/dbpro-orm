/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 19/03/2008
 * Time: 9:35 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */

using System;
using System.Reflection;
using System.Collections.Generic;
using System.Threading;

namespace Org.Reddragonit.Dbpro.Structure.Mapping
{

	internal static class ClassMapper
	{
		private static Dictionary<System.Type,TableMap> map;
		private static Mutex mut = new Mutex(false);
		
		public static TableMap GetTableMap(System.Type type)
		{
			mut.WaitOne();
			TableMap ret = null;
			if ((type!=null)&&map.ContainsKey(type))
			{
				ret=map[type];
			}
			mut.ReleaseMutex();
			return ret;
		}
		
		public static TableMap GetTableMapByTableName(string TableName)
		{
			mut.WaitOne();
			TableMap ret = null;
			foreach (System.Type type in TableTypes)
			{
				if (map[type].Name==TableName)
				{
					ret=map[type];
					break;
				}
			}
			mut.ReleaseMutex();
			return ret;
		}

		public static System.Type[] TableTypes
		{
			get
			{
				System.Diagnostics.Debug.WriteLine("MAPS: "+map.Count.ToString());
				mut.WaitOne();
				System.Type[] ret = new Type[map.Count];
				map.Keys.CopyTo(ret, 0);
				mut.ReleaseMutex();
				return ret;
			}
		}
		
		static ClassMapper()
		{
			mut.WaitOne();
			try{
				map = new Dictionary<System.Type,TableMap>();
				foreach (Assembly asm in AppDomain.CurrentDomain.GetAssemblies())
				{
					foreach (Type ty in asm.GetTypes())
					{
						if (ty.IsSubclassOf(typeof(Org.Reddragonit.Dbpro.Structure.Table)))
						{
							if (!map.ContainsKey(ty))
							{
								System.Diagnostics.Debug.WriteLine("Adding Table Map ("+ty.FullName+")");
								map.Add(ty,new TableMap(ty,asm,ty.GetMembers(BindingFlags.Public |      //Get public members
								                                             BindingFlags.NonPublic |   //Get private/protected/internal members
								                                             BindingFlags.Static |      //Get static members
								                                             BindingFlags.Instance |    //Get instance members
								                                             BindingFlags.DeclaredOnly  ),ref map));
								System.Diagnostics.Debug.WriteLine("Table Map Added");
							}
						}
					}
				}
			}catch (Exception e)
			{
				System.Diagnostics.Debug.WriteLine(e.Message);
				System.Diagnostics.Debug.WriteLine(e.Source);
				System.Diagnostics.Debug.WriteLine(e.StackTrace);
			}
			foreach (TableMap tm in map.Values)
			{
				System.Diagnostics.Debug.WriteLine(tm.Name);
				foreach (InternalFieldMap ifm in tm.Fields)
				{
					System.Diagnostics.Debug.WriteLine("\t"+ifm.FieldName+"("+ifm.GetType().ToString()+")");
				}
			}
			mut.ReleaseMutex();
		}
	}
}
