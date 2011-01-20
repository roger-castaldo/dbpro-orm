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
using Org.Reddragonit.Dbpro.Structure.Mapping;
using System.Threading;
using System.Data;

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
            if (ignores == null)
                ignores = new string[0];
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

        //called to clean up empty strings from an array.  Used through connection pools
        //running massive updates/inserts/etc
        public static void RemoveEmptyStrings(ref List<string> list)
        {
            for (int x = 0; x < list.Count; x++)
            {
                if (list[x].Trim().Length == 0)
                {
                    list.RemoveAt(x);
                    x--;
                }
            }
        }

        internal static List<int> SortDictionaryKeys(Dictionary<int, int>.KeyCollection keys)
        {
            int[] tmp = new int[keys.Count];
            keys.CopyTo(tmp, 0);
            List<int> ret = new List<int>(tmp);
            ret.Sort();
            return ret;
        }

        internal static List<int> SortDictionaryKeys(Dictionary<int, string>.KeyCollection keys)
        {
            int[] tmp = new int[keys.Count];
            keys.CopyTo(tmp, 0);
            List<int> ret = new List<int>(tmp);
            ret.Sort();
            return ret;
        }

        internal static List<string> SortDictionaryKeys(Dictionary<string, FieldMap>.KeyCollection keys)
        {
            string[] tmp = new string[keys.Count];
            keys.CopyTo(tmp, 0);
            List<string> ret = new List<string>(tmp);
            ret.Sort();
            return ret;
        }

        internal static void WaitOne(object obj)
        {
            Monitor.Enter(obj);
        }

        internal static void WaitOne(object obj, int timeout)
        {
            if (!Monitor.TryEnter(obj, timeout))
                throw new Exception("Timeout expired while waiting for lock to be released.");
        }

        internal static void Release(object obj)
        {
            Monitor.Exit(obj);
        }

        internal static bool IsParameterNull(IDbDataParameter par)
        {
            return (par.Value == null ||
                        ((par is Npgsql.NpgsqlParameter) && (par.Value.ToString().Length == 0))
                        || ((par is FirebirdSql.Data.FirebirdClient.FbParameter) && (par.Value.ToString() == "{}"))
                        ||(DBNull.Value.Equals(par.Value))
                       );
        }

        internal static string StripNullParameter(string outputQuery, string ParameterName)
        {
            outputQuery = outputQuery + " ";
            if (outputQuery.StartsWith("UPDATE"))
            {
                string wheres = outputQuery.Substring(outputQuery.LastIndexOf("WHERE") + 5);
                wheres = wheres.Replace(">= " + ParameterName + " ", "IS NULL ");
                wheres = wheres.Replace("<= " + ParameterName + " ", "IS NULL ");
                wheres = wheres.Replace("= " + ParameterName + " ", "IS NULL ");
                wheres = wheres.Replace(" =" + ParameterName + " ", " IS NULL ");
                wheres = wheres.Replace("<> " + ParameterName + " ", "IS NOT NULL ");
                wheres = wheres.Replace(ParameterName + ",", "NULL,");
                wheres = wheres.Replace(ParameterName + ")", "NULL)");
                wheres = wheres.Replace(" " + ParameterName + " IS NULL", " NULL IS NULL");
                wheres = wheres.Replace("(" + ParameterName + " IS NULL", "(NULL IS NULL");
                wheres= wheres.Replace(" =NULL", " IS NULL");
                outputQuery = outputQuery.Substring(0, outputQuery.LastIndexOf("WHERE") + 5);
                outputQuery = outputQuery.Replace(" = " + ParameterName + ", ", " = NULL, ");
                outputQuery = outputQuery.Replace(" = " + ParameterName + " WHERE", " = NULL WHERE");
                outputQuery += wheres;
            }
            else
            {
                outputQuery = outputQuery.Replace(">= " + ParameterName + " ", "IS NULL ");
                outputQuery = outputQuery.Replace("<= " + ParameterName + " ", "IS NULL ");
                outputQuery = outputQuery.Replace("= " + ParameterName + " ", "IS NULL ");
                outputQuery = outputQuery.Replace(" =" + ParameterName + " ", " IS NULL ");
                outputQuery = outputQuery.Replace("<> " + ParameterName + " ", "IS NOT NULL ");
                outputQuery = outputQuery.Replace(ParameterName + ",", "NULL,");
                outputQuery = outputQuery.Replace(ParameterName + ")", "NULL)");
                outputQuery = outputQuery.Replace(" " + ParameterName + " IS NULL", " NULL IS NULL");
                outputQuery = outputQuery.Replace("(" + ParameterName + " IS NULL", "(NULL IS NULL");
                outputQuery = outputQuery.Replace(" =NULL", " IS NULL");
            }
            return outputQuery.Trim();
        }

	}
}
