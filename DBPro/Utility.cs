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
using System.Threading;
using System.Data;
using System.Text;
using System.Collections;

namespace Org.Reddragonit.Dbpro
{
	/// <summary>
	/// This class is a main utility class used to house functionality called upong by other
    /// sections of code.
	/// </summary>
	internal class Utility
	{

        public static readonly BindingFlags _BINDING_FLAGS = BindingFlags.Public |      //Get public members
                                                                             BindingFlags.NonPublic |   //Get private/protected/internal members
                                                                             BindingFlags.Static |      //Get static members
                                                                             BindingFlags.Instance |    //Get instance members
                                                                             BindingFlags.DeclaredOnly;

        public static readonly BindingFlags _BINDING_FLAGS_WITH_INHERITANCE = BindingFlags.Public |      //Get public members
                                                                             BindingFlags.NonPublic |   //Get private/protected/internal members
                                                                             BindingFlags.Static |      //Get static members
                                                                             BindingFlags.Instance;
		
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

        //Called to locate all child classes of a given parent type
        public static List<Type> LocateTypeInstances(Type parent)
        {
            List<Type> ret = new List<Type>();
            foreach (Assembly ass in AppDomain.CurrentDomain.GetAssemblies())
            {
                try
                {
                    if (ass.GetName().Name != "mscorlib" && !ass.GetName().Name.StartsWith("System.") && ass.GetName().Name != "System" && !ass.GetName().Name.StartsWith("Microsoft"))
                    {
                        foreach (Type t in ass.GetTypes())
                        {
                            if (t.IsSubclassOf(parent) || (parent.IsInterface && new List<Type>(t.GetInterfaces()).Contains(parent)))
                                ret.Add(t);
                        }
                    }
                }
                catch (Exception e)
                {
                    if (e.Message != "The invoked member is not supported in a dynamic assembly."
                        && e.Message!="Unable to load one or more of the requested types. Retrieve the LoaderExceptions property for more information.")
                    {
                        throw e;
                    }
                }
            }
            return ret;
        }

        //Called to locate all Types that have the specified attribute type
        public static List<Type> LocateAllTypesWithAttribute(Type attributeType)
        {
            List<Type> ret = new List<Type>();
            foreach (Assembly ass in AppDomain.CurrentDomain.GetAssemblies())
            {
                try
                {
                    if (ass.GetName().Name!="mscorlib" && !ass.GetName().Name.StartsWith("System.") && ass.GetName().Name!="System" && !ass.GetName().Name.StartsWith("Microsoft"))
                    {
                        foreach (Type t in ass.GetTypes())
                        {
                            if (t.GetCustomAttributes(attributeType, false).Length > 0)
                                ret.Add(t);
                        }
                    }
                }
                catch (Exception e)
                {
                    if (e.Message != "The invoked member is not supported in a dynamic assembly."
                        && e.Message != "Unable to load one or more of the requested types. Retrieve the LoaderExceptions property for more information.")
                    {
                        throw e;
                    }
                }
            }
            return ret;
        }

        internal static bool IsEqual(object lt, object rt)
        {
            if (lt == null && rt == null)
                return true;
            else if (lt == null && rt != null)
                return false;
            else if (lt != null && rt == null)
                return false;
            else if (lt.GetType() != rt.GetType())
                return false;
            else if (lt is IComparable)
                return ((IComparable)lt).CompareTo(rt) == 0;
            return lt.Equals(rt);
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

        //called to compare string while ignoring whitespaces and brackets and case (used to handle mssql computed columns)
        public static bool StringsEqualIgnoreWhitespaceBracketsCase(string str1, string str2)
        {
            if (str1 == null)
            {
                if (str2 != null)
                    return false;
                return true;
            }
            else if (str2 == null)
            {
                return false;
            }
            Regex r = new Regex("(\\s+|\\(|\\)|\\[|\\]|\\{|\\})");
            return r.Replace(str1.ToUpper(), "").Equals(r.Replace(str2.ToUpper(), ""));
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

        //called to clean up mssql db code for duplicate lines
        public static string RemoveDuplicateStrings(string code, string[] ignores)
        {
            List<string> tmp = new List<string>(code.Split('\n'));
            RemoveDuplicateStrings(ref tmp, ignores);
            RemoveEmptyStrings(ref tmp);
            StringBuilder ret = new StringBuilder();
            foreach (string str in tmp)
                ret.AppendLine(str);
            return ret.ToString();
        }

        //called to clean up empty strings from an array.  Used through connection pools
        //running massive updates/inserts/etc
        public static void RemoveEmptyStrings(ref List<string> list)
        {
            for (int x = 0; x < list.Count; x++)
            {
                if (list[x].Trim(new char[] { '\n', '\t', ' ', '\r' }).Length == 0)
                {
                    list.RemoveAt(x);
                    x--;
                }
                else
                    list[x] = list[x].Trim(new char[] { '\n', '\t', ' ', '\r' });
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

        internal static bool IsParameterNull(IDbDataParameter par)
        {
            return (par.Value == null ||
                        ((par.GetType().FullName == "Npgsql.NpgsqlParameter") && (par.Value.ToString().Length == 0))
                        || ((par.GetType().FullName == "FirebirdSql.Data.FirebirdClient.FbParameter") && (par.Value.ToString() == "{}"))
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
                wheres = wheres.Replace("= " + ParameterName + ")", "IS NULL)");
                wheres = wheres.Replace(" =" + ParameterName + ")", " IS NULL)");
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
                outputQuery = outputQuery.Replace("= " + ParameterName + ")", "IS NULL)");
                outputQuery = outputQuery.Replace(" =" + ParameterName + ")", " IS NULL)");
                outputQuery = outputQuery.Replace("<> " + ParameterName + " ", "IS NOT NULL ");
                outputQuery = outputQuery.Replace(ParameterName + ",", "NULL,");
                outputQuery = outputQuery.Replace(ParameterName + ")", "NULL)");
                outputQuery = outputQuery.Replace(" " + ParameterName + " IS NULL", " NULL IS NULL");
                outputQuery = outputQuery.Replace("(" + ParameterName + " IS NULL", "(NULL IS NULL");
                outputQuery = outputQuery.Replace(" =NULL", " IS NULL");
            }
            if (outputQuery.StartsWith("INSERT"))
            {
                outputQuery = outputQuery.Replace("("+ParameterName + ",", "(NULL,");
                outputQuery = outputQuery.Replace("," + ParameterName + ",", ",NULL,");
                outputQuery = outputQuery.Replace("(" + ParameterName + ")", "(NULL)");
                outputQuery = outputQuery.Replace("," + ParameterName + ")", ",NULL)");
            }
            return outputQuery.Trim();
        }


        internal static bool OnlyContains(List<string> alterations, string[] p)
        {
            Regex r = new Regex("\\s+");
            for (int x = 0; x < p.Length; x++)
                p[x] = r.Replace(p[x], "");
            List<string> vals = new List<string>(p);
            foreach (string a in alterations)
            {
                if (!vals.Contains(r.Replace(a, "")))
                    return false;
            }
            return true;
        }

        internal static bool IsEnum(Type t)
        {
            return (t.IsGenericType ? t.IsGenericType &&
                   t.GetGenericTypeDefinition() == typeof(Nullable<>) &&
                   t.GetGenericArguments()[0].IsEnum : (t.IsArray ? IsEnum(t.GetElementType()) : t.IsEnum));
        }

        internal static object ConvertEnumParameter(object value,ConnectionPool pool)
        {
            if ((value.GetType().IsArray) || (value is IEnumerable))
            {
                ArrayList tmp = new ArrayList();
                foreach (Enum en in (IEnumerable)value)
                    tmp.Add(pool.GetEnumID(en.GetType(), en.ToString()));
                return tmp;
            }
            else
                return pool.GetEnumID(value.GetType(), value.ToString());
        }
    }
}
