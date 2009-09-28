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
	/// This class is a main utility class used to house functionality called upong by other
    /// sections of code.
	/// </summary>
	public class Utility
	{
		
        //Called to locate a system type by running through all assemblies within the current
        //domain until it is able to locate the requested Type.
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
		
        //Called to access the Connection Pool Correct name function
        //while checking for a null pool so clean up common code.
		public static string CorrectName(ConnectionPool pool,string name)
		{
			if (pool==null)
				return name;
			else
				return pool.CorrectName(name);
		}
		
        //Merges to string arrays into one, commonly called for the keywords
        //declarations within a connection
		public static string[] MergeStringArrays(string[] array1,string[] array2)
		{
			List<string> ret = new List<string>();
			ret.AddRange(array1);
			ret.AddRange(array2);
			return ret.ToArray();
		}
		
        //called to compare to string whill checkign for nulls
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
		
        //called to compare to strings while checking for nulls and ignoring all whitespaces as well
        //as case.  This is typically used to compare stored procedures.
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
		
        //called to compare to string while checking for nulls and ignoring all whitespaces.
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
		
        //called to clean up duplicate strings from a string array.  This
        //is used to clean up the keyword arrays made by the connections.
		public static void RemoveDuplicateStrings(ref List<string> list,string[] ignores)
		{
			for(int x=0;x<list.Count;x++)
			{
				bool process=true;
				foreach (string str in ignores)
				{
                    if (StringsEqualIgnoreWhitespace(list[x],str))
					{
						process=false;
						break;
					}
				}
				if (process)
				{
					for (int y=x+1;y<list.Count;y++)
					{
                        if (StringsEqualIgnoreWhitespace(list[y],list[x]))
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
