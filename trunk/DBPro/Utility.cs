/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 15/01/2009
 * Time: 6:08 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using Org.Reddragonit.Dbpro.Connections;
using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Reflection;

namespace Org.Reddragonit.Dbpro
{
	/// <summary>
	/// Description of Utility.
	/// </summary>
	public class Utility
	{
		
		public static Type LocateType(string name)
		{
			Type ret = null;
			try{
				foreach (Assembly asm in AppDomain.CurrentDomain.GetAssemblies())
				{
					ret=asm.GetType(name,false);
					if (ret!=null)
						break;
				}
			}catch (Exception e){}
			return ret;
		}
		
		public static string CorrectName(ConnectionPool pool,string name)
		{
			if (pool==null)
				return name;
			else
				return pool.CorrectName(name);
		}
		
		public static string[] MergeStringArrays(string[] array1,string[] array2)
		{
			List<string> ret = new List<string>();
			ret.AddRange(array1);
			ret.AddRange(array2);
			return ret.ToArray();
		}
		
		public static bool StringsEqual(string str1,string str2)
		{
			if (str1==null)
			{
				if (str2!=null)
					return false;
				return true;
			}else if (str2==null)
			{
				return false;
			}
			return str1.Equals(str2);
		}
		
		public static bool StringsEqualIgnoreCaseWhitespace(string str1,string str2)
		{
			if (str1==null)
			{
				if (str2!=null)
					return false;
				return true;
			}else if (str2==null)
			{
				return false;
			}
			Regex r = new Regex("\\s+");
			return r.Replace(str1.ToUpper(),"").Equals(r.Replace(str2.ToUpper(),""));
		}
		
		public static bool StringsEqualIgnoreWhitespace(string str1,string str2)
		{
			if (str1==null)
			{
				if (str2!=null)
					return false;
				return true;
			}else if (str2==null)
			{
				return false;
			}
			Regex r = new Regex("\\s+");
			return r.Replace(str1,"").Equals(r.Replace(str2,""));
		}
		
		public static void RemoveDuplicateStrings(ref List<string> list,string[] ignores)
		{
			for(int x=0;x<list.Count;x++)
			{
				bool process=true;
				foreach (string str in ignores)
				{
					if (str.Equals(list[x]))
					{
						process=false;
						break;
					}
				}
				if (process)
				{
					for (int y=x+1;y<list.Count;y++)
					{
						if (list[x].Equals(list[y]))
						{
							list.RemoveAt(y);
							y--;
						}
					}
				}
			}
		}
	}
}
