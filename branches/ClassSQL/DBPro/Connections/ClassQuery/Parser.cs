/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 24/11/2008
 * Time: 1:14 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */

using System;
using System.Reflection;
using System.Collections.Generic;
using Org.Reddragonit.Dbpro.Structure.Mapping;

namespace Org.Reddragonit.Dbpro.Connections.ClassQuery
{
	/// <summary>
	/// Description of Parser.
	/// </summary>
	internal class Parser
	{
		public static string ParseClassQuery(string query,List<QueryParameter> parameters)
		{
			string ret = "";
			Tokenizer tok = new Tokenizer(query);
			Tokenizer.Token? t = null;
			List<Tokenizer.Token> tokens = new List<Tokenizer.Token>();
			int classCount=0;
			while ((t=tok.NextToken())!=null)
			{
				Tokenizer.Token token = t.Value;
				if ((token.Type==Tokenizer.TokenType.GENERAL)&&(token.Value.Contains(".")))
				{
					foreach (Assembly ass in AppDomain.CurrentDomain.GetAssemblies())
					{
						try{
							if ((token.ClassType=ass.GetType(token.Value,false,true))!=null)
							{
								classCount++;
								break;
							}
						}catch (Exception e){}
					}
				}
				tokens.Add(token);
			}
			if (tokens.Count==0)
				throw new Exception("Unable to parse query, invalid string supplied.");
			else if ((tokens[0].Type!=Tokenizer.TokenType.COMMAND)||
			         !tokens[0].Command.HasValue||
			         (tokens[0].Command.Value!=Tokenizer.CommandType.SELECT))
				throw new Exception("Invalid query, must start query with SELECT");
			else if (classCount==0)
				throw new Exception("Invalid query, no classes found in the query");
			Dictionary<Type, string> aliases = new Dictionary<Type, string>();
			bool inSelect=false;
			bool inWhere=false;
			int whereIndex=-1;
			List<Type> requiredtables = new List<Type>();
			List<Type> containedTables = new List<Type>();
			for (int x=0;x<tokens.Count;x++)
			{
				if ((!inSelect)&&(tokens[x].Type==Tokenizer.TokenType.COMMAND)&&(tokens[x].Command.Value==Tokenizer.CommandType.SELECT))
					inSelect=true;
				else if (inSelect&&(tokens[x].Type==Tokenizer.TokenType.COMMAND)&&(tokens[x].Command.Value==Tokenizer.CommandType.FROM))
					inSelect=false;
				if (!inWhere&&(tokens[x].Type==Tokenizer.TokenType.COMMAND)&&(tokens[x].Command.Value==Tokenizer.CommandType.WHERE))
				{
					whereIndex=x;
					inWhere=true;
				}
				if (tokens[x].ClassType!=null)
				{
					containedTables.Add(tokens[x].ClassType);
					if ((x+1<tokens.Count)&&(tokens[x+1].ClassType==null)&&(tokens[x+1].Type==Tokenizer.TokenType.GENERAL)&&(tokens[x+1].Command.Value!=Tokenizer.CommandType.WHERE))
					{
						aliases.Add(tokens[x].ClassType,tokens[x+1].Value);
						x++;
						tokens.RemoveAt(x);
					}
				}else if ((inSelect||inWhere)&&(tokens[x].Type==Tokenizer.TokenType.GENERAL))
				{
					Tokenizer.Token to = tokens[x];
					tokens.RemoveAt(x);
					to.Type=Tokenizer.TokenType.FIELD;
					tokens.Insert(x,to);
				}
			}
			foreach (Tokenizer.Token token in tokens)
			{
				if (token.ClassType!=null)
				{
					if (!requiredtables.Contains(token.ClassType))
						requiredtables.Add(token.ClassType);
				}else if (token.Type==Tokenizer.TokenType.FIELD)
				{
					AddRequiredTables(token.Value,ref requiredtables,aliases);
				}
			}
			foreach (Type tp in requiredtables)
			{
				if (!aliases.ContainsKey(tp))
					aliases.Add(tp,"table_"+((int)(aliases.Count+1)).ToString());
				System.Diagnostics.Debug.WriteLine("Required table: "+tp.FullName);
			}
			for(int x=0;x<tokens.Count;x++)
			{
				Tokenizer.Token token = tokens[x];
				if (token.Type==Tokenizer.TokenType.FIELD)
				{
					string tableName = "";
					string fieldName=token.Value;
					if (token.Value.Contains("."))
					{
						tableName=token.Value.Substring(0,token.Value.IndexOf("."));
						fieldName=fieldName.Substring(fieldName.LastIndexOf(".")+1);
					}
					if (!aliases.ContainsValue(tableName))
					{
						foreach (Type tp in requiredtables)
						{
							TableMap tm = ClassMapper.GetTableMap(tp);
							if (tm[fieldName]!=null)
							{
								tableName=aliases[tp];
								fieldName=tm.GetTableFieldName(fieldName);
								break;
							}
						}
					}else{
						foreach(Type tp in requiredtables)
						{
							if (aliases[tp]==tableName)
							{
								fieldName=ClassMapper.GetTableMap(tp).GetTableFieldName(fieldName);
								break;
							}
						}
					}
					tokens.RemoveAt(x);
					token.Value=tableName+"."+fieldName;
					tokens.Insert(x,token);
				}
			}
			bool addAND = false;
			if (whereIndex!=-1)
			{
				addAND=true;
				tokens.Insert(whereIndex+1,new Tokenizer.Token("(",Tokenizer.TokenType.GENERAL));
				int bracketCount=0;
				bool added=false;
				for(int x=whereIndex+2;x<tokens.Count;x++)
				{
					Tokenizer.Token token = tokens[x];
					if (((bracketCount==0)&&token.Value==")")||(token.Type==Tokenizer.TokenType.COMMAND&&((token.Command.Value==Tokenizer.CommandType.GROUP)||(token.Command.Value==Tokenizer.CommandType.ORDER)||(token.Command.Value==Tokenizer.CommandType.LIMIT))))
					{
						tokens.Insert(x+1,new Tokenizer.Token(")",Tokenizer.TokenType.GENERAL));
						added=true;
						break;
					}
					if (token.Value==")")
						bracketCount--;
					else if (token.Value=="(")
						bracketCount++;
				}
				if (!added)
				{
					tokens.Add(new Tokenizer.Token(")",Tokenizer.TokenType.GENERAL));
				}
			}
			for (int x=0;x<tokens.Count;x++)
			{
				Tokenizer.Token token = tokens[x];
				if ((token.ClassType!=null)&&(requiredtables.Count!=containedTables.Count))
				{
					TableMap map = ClassMapper.GetTableMap(token.ClassType);
					foreach (Type type in requiredtables)
					{
						if ((!containedTables.Contains(type))&&(map.ForiegnTables.Contains(type)))
						{
							TableMap etm = ClassMapper.GetTableMap(type);
							ExternalFieldMap efm = map.GetFieldInfoForForiegnTable(type);
							int pos = x+1;
							int count=1;
							if (efm.Nullable)
							{
								tokens.Insert(pos,new Tokenizer.Token("LEFT JOIN "+etm.Name+" "+aliases[type]+" ON ",Tokenizer.TokenType.GENERAL));
								pos++;
							}else
							{
								if (whereIndex==-1)
								{
									tokens.Insert(pos,new Tokenizer.Token("WHERE",Tokenizer.TokenType.COMMAND,Tokenizer.CommandType.WHERE));
									whereIndex=x+1;
									pos++;
								}
								tokens.Insert(whereIndex,new Tokenizer.Token(", "+etm.Name+" "+aliases[type],Tokenizer.TokenType.GENERAL));
								pos=whereIndex+count+1;
								tokens.Insert(pos,new Tokenizer.Token("(",Tokenizer.TokenType.GENERAL));
								pos++;
							}
							foreach (InternalFieldMap ifm in etm.PrimaryKeys)
							{
								tokens.Insert(pos,new Tokenizer.Token(aliases[token.ClassType]+"."+ifm.FieldName,Tokenizer.TokenType.FIELD));
								tokens.Insert(pos+1,new Tokenizer.Token("=",Tokenizer.TokenType.COMPARATOR));
								tokens.Insert(pos+2,new Tokenizer.Token(aliases[type]+"."+ifm.FieldName,Tokenizer.TokenType.FIELD));
								if (efm.Nullable)
								{
									pos+=3;
									count+=3;
								}else{
									tokens.Insert(pos+3,new Tokenizer.Token("AND",Tokenizer.TokenType.COMMAND,Tokenizer.CommandType.AND));
									pos+=4;
								}
							}
							if (!efm.Nullable)
							{
								pos--;
								tokens.RemoveAt(pos);
								tokens.Insert(pos,new Tokenizer.Token(")",Tokenizer.TokenType.GENERAL));
								if (addAND)
									tokens.Insert(pos+1,new Tokenizer.Token("AND",Tokenizer.TokenType.COMMAND,Tokenizer.CommandType.AND));
								else
									addAND=true;
							}
							whereIndex+=count;
							containedTables.Add(type);
						}
					}
				}
			}
			if ((tokens[1].Type==Tokenizer.TokenType.COMMAND)&&(tokens[1].Command.Value==Tokenizer.CommandType.FROM))
			{
				ret+="SELECT * FROM ";
				tokens.RemoveAt(0);
				tokens.RemoveAt(0);
			}
			foreach (Tokenizer.Token token in tokens)
			{
				if (token.ClassType!=null)
					ret+=" "+ClassMapper.GetTableMap(token.ClassType).Name+" "+aliases[token.ClassType];
				else
					ret+=" "+token.Value;
			}
			return ret;
		}
		
		private static void AddRequiredTables(string name,ref List<Type> requiredtables,Dictionary<Type, string> aliases)
		{
			if (name.Contains("."))
			{
				string table = name.Substring(0,name.IndexOf("."));
				if (!aliases.ContainsValue(name))
				{
					Type subType=null;
					foreach (Type ty in requiredtables)
					{
						if ((subType=ClassMapper.GetTableMap(ty).GetTypeForField(table))!=null)
							break;
					}
					if (subType!=null)
					{
						if (!requiredtables.Contains(subType))
							requiredtables.Add(subType);
						AddRequiredTables(name.Substring(table.Length),ref requiredtables,aliases);
					}
				}
			}
		}
	}
}
