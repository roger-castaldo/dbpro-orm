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
			if (map.ContainsKey(type))
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
                mut.WaitOne();
                System.Type[] ret = new Type[map.Count];
                map.Keys.CopyTo(ret, 0);
                mut.ReleaseMutex();
                return ret;
            }
        }
		
		static ClassMapper()
		{
			try{
				map = new Dictionary<System.Type,TableMap>();
				Assembly asm = Assembly.GetEntryAssembly();
				foreach (Type ty in asm.GetTypes())
				{
					if (ty.IsSubclassOf(typeof(Org.Reddragonit.Dbpro.Structure.Table)))
					{
						if (!map.ContainsKey(ty))
						{
							map.Add(ty,new TableMap(ty,asm,ty.GetMembers(BindingFlags.Public |      //Get public members
							                                             BindingFlags.NonPublic |   //Get private/protected/internal members
							                                             BindingFlags.Static |      //Get static members
							                                             BindingFlags.Instance |    //Get instance members
							                                             BindingFlags.DeclaredOnly ),ref map));
						}
					}
				}
				/*foreach (TableMap t in map.Values)
				{
					System.Diagnostics.Debug.WriteLine(t.Name+"{");
					foreach (FieldMap f in t.Fields)
					{
						if (f is InternalFieldMap)
						{
							InternalFieldMap ifm = (InternalFieldMap)f;
							System.Diagnostics.Debug.Write("\t"+ifm.FieldName+" "+ifm.FieldType);
							if (ifm.FieldLength!=0)
							{
								System.Diagnostics.Debug.Write("("+ifm.FieldLength.ToString()+")");
							}
							System.Diagnostics.Debug.Write(",\n");
						}
					}
					if (t.PrimaryKeys.Count>0)
					{
						System.Diagnostics.Debug.Write("\tPRIMARY KEY(");
						foreach (FieldMap f in t.PrimaryKeys)
						{
							if (f is InternalFieldMap)
							{
								InternalFieldMap ifm = (InternalFieldMap)f;
								System.Diagnostics.Debug.Write(ifm.FieldName+",");
							}
						}
						System.Diagnostics.Debug.Write("),\n");
					}
					if (t.ForiegnTables.Count>0)
					{
						foreach (Type type in t.ForiegnTables)
						{
							System.Diagnostics.Debug.Write("\tFORIEGN KEYS(");
							foreach (InternalFieldMap ifm in map[type].PrimaryKeys)
							{
								System.Diagnostics.Debug.Write(ifm.FieldName+",");
							}
							System.Diagnostics.Debug.Write(")\n\t\tREFERENCES "+map[type].Name+"(");
							foreach (InternalFieldMap ifm in map[type].PrimaryKeys)
							{
								System.Diagnostics.Debug.Write(ifm.FieldName+",");
							}
							System.Diagnostics.Debug.Write(")\n\t\tON UPDATE "+t.GetFieldInfoForForiegnTable(type).OnUpdate.ToString()+"\n");
							System.Diagnostics.Debug.WriteLine("\t\tON DELETE "+t.GetFieldInfoForForiegnTable(type).OnDelete.ToString());
						}
					}
					System.Diagnostics.Debug.WriteLine("}\n");
				}*/
			}catch (Exception e)
			{
				System.Diagnostics.Debug.WriteLine(e.Message);
				System.Diagnostics.Debug.WriteLine(e.Source);
				System.Diagnostics.Debug.WriteLine(e.StackTrace);
			}
		}
	}
}