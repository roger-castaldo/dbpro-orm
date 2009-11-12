/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 19/03/2008
 * Time: 9:35 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */

using Org.Reddragonit.Dbpro.Connections;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using Org.Reddragonit.Dbpro.Validation;

namespace Org.Reddragonit.Dbpro.Structure.Mapping
{

	internal static class ClassMapper
	{
		private static Dictionary<System.Type,TableMap> map=null;
        private static Dictionary<System.Type, Dictionary<string, List<ValidationAttribute>>> _validations = null;
		private static Dictionary<Type, object> constructed = new Dictionary<Type, object>();
		private static Mutex mut = new Mutex(false);
		private static List<Type> types;
		
		internal static void CorrectConnectionNames(string[] connectionNames)
		{
			mut.WaitOne();
			if ((map==null)||(map.Count==0))
				InitMaps();
			Type[] types = new Type[map.Count];
			map.Keys.CopyTo(types,0);
			foreach (Type t in types)
			{
				TableMap tm = map[t];
				map.Remove(t);
				tm.CorrectConnectionName(connectionNames);
				map.Add(t,tm);
			}
			mut.ReleaseMutex();
		}
		
		public static TableMap GetTableMap(System.Type type)
		{
			mut.WaitOne();
			if ((map==null)||(map.Count==0))
				InitMaps();
			TableMap ret = null;
			if ((type!=null)&&map.ContainsKey(type))
			{
				ret=map[type];
			}
			mut.ReleaseMutex();
			return ret;
		}
		
		public static object InitialValueForClassField(Type type, string ClassFieldName)
		{
			mut.WaitOne();
			object ret=null;
			if ((map==null)||(map.Count==0))
				InitMaps();
			if (map.ContainsKey(type))
			{
				if (!constructed.ContainsKey(type))
					constructed.Add(type,type.GetConstructor(Type.EmptyTypes).Invoke(new object[0]));
                PropertyInfo pi = ((Table)constructed[type]).LocatePropertyInfo(ClassFieldName);
                ret = pi.GetValue(constructed[type], new object[0]);
			}
			mut.ReleaseMutex();
			return ret;
		}
		
		public static TableMap GetTableMapByTableName(string TableName)
		{
			mut.WaitOne();
			if ((map==null)||(map.Count==0))
				InitMaps();
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
		
		public static List<System.Type> TableTypesForConnection(string name)
		{
			List<System.Type> ret = new List<Type>();
			mut.WaitOne();
			if ((map==null)||(map.Count==0))
				InitMaps();
			foreach (Type t in map.Keys)
			{
				if (map[t].ConnectionName==name)
					ret.Add(t);
			}
			mut.ReleaseMutex();
			return ret;
		}
		
		public static void CorrectNamesForConnection(ConnectionPool pool)
		{
			List<Type> types = TableTypesForConnection(pool.ConnectionName);
			mut.WaitOne();
			if ((map==null)||(map.Count==0))
				InitMaps();
			foreach (Type t in types)
			{
				TableMap tm = map[t];
				tm.CorrectNames(pool);
				map.Remove(t);
				map.Add(t,tm);
			}
			mut.ReleaseMutex();
		}

		public static System.Type[] TableTypes
		{
			get
			{
				System.Diagnostics.Debug.WriteLine("MAPS: "+map.Count.ToString());
				mut.WaitOne();
				if ((map==null)||(map.Count==0))
					InitMaps();
				System.Type[] ret = new Type[map.Count];
				map.Keys.CopyTo(ret, 0);
				mut.ReleaseMutex();
				return ret;
			}
		}
		
		internal static List<Type> ClassedTypes
		{
			get{return types;}
		}
		
		private static void InitMaps()
		{
			types=new List<Type>();
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
								if (!types.Contains(ty))
									types.Add(ty);
								System.Diagnostics.Debug.WriteLine("Adding Table Map ("+ty.FullName+")");
								map.Add(ty,new TableMap(ty,asm,ty.GetProperties(BindingFlags.Public |      //Get public members
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
				System.Diagnostics.Debug.WriteLine("ERROR ESTABLISHING STRUCTURE!!!!");
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
		}
		
		internal static void ReInit(){
			mut.WaitOne();
			InitMaps();
			mut.ReleaseMutex();
		}
		
		static ClassMapper()
		{
			mut.WaitOne();
			InitMaps();
			mut.ReleaseMutex();
		}
	}
}
