/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 24/11/2008
 * Time: 1:19 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */

using System;

namespace Org.Reddragonit.Dbpro.Connections.ClassQuery
{
	/// <summary>
	/// Description of Tokenizer.
	/// </summary>
	internal class Tokenizer
	{
		
		internal enum TokenType
		{
			COMMAND,
			PARAMETER,
			QUOTED,
			GENERAL,
			COMPARATOR,
			FIELD
		}
		
		internal enum CommandType
		{
			SELECT,
			WHERE,
			AND,
			OR,
			JOIN,
			FROM,
			AS,
			IN,
			LEFT,
			RIGHT,
			INNER
		}
		
		internal struct Token
		{
			private string _value;
			public string Value
			{
				get{return _value;}
				set{_value=value;}
			}
			
			private TokenType _type;
			public TokenType Type
			{
				get{return _type;}
				set{_type=value;}
			}
			
			private CommandType? _command;
			public CommandType? Command
			{
				get{return _command;}
			}
			
			private Type _classType;
			public Type ClassType
			{
				get{return _classType;}
				set{_classType=value;}
			}
			
			public Token(string Value,TokenType Type) : this(Value,Type,null)
			{
			}
			
			public Token(string Value,TokenType Type,CommandType? Command)
			{
				_value=Value;
				_type=Type;
				_command=Command;
				_classType=null;
			}
		}
		
		private const char ESCAPE_CHAR='\\';
		private const char PARAMETER_START='@';
		private const string TOKEN_ENDS=" ,=!<>";
		
		private string _query;
		private int _position;
		
		
		public Tokenizer(string Query)
		{
			_query=Query;
			_position=0;
		}
		
		public Token? NextToken()
		{
			Token? ret = null;
			string value="";
			bool quoted=false;
			char lastChar=' ';
			if ((_position<_query.Length)&&(_position!=0)&&(TOKEN_ENDS.Contains(_query[_position].ToString())))
			{
				_position++;
				if (_query[_position-1]!=' ')
				{
					return new Token(_query[_position-1].ToString(),TokenType.COMPARATOR);
				}
			}
			while ((_position<_query.Length)&&(_query[_position]==' '))
				_position++;
			while (_position<_query.Length)
			{
				if (TOKEN_ENDS.Contains(_query[_position].ToString())&&!quoted)
					break;
				else if ((_query[_position]=='\'')||(_query[_position]=='"'))
				{
					if (quoted)
					{
						if (lastChar!='\\')
							quoted=false;
						else
							value=value.Substring(0,value.Length-1);
					}else
					{
						if (lastChar!='\\')
							quoted=true;
						else
							value=value.Substring(0,value.Length-1);
					}
				}
				value+=_query[_position];
				lastChar=_query[_position];
				_position++;
			}
			if (value!="")
			{
				if (value.StartsWith("\"")||value.StartsWith("'"))
					ret = new Token(value,TokenType.QUOTED);
				else if (value.StartsWith(PARAMETER_START.ToString()))
					ret=new Token(value,TokenType.PARAMETER);
				else
				{
					foreach (CommandType ct in Enum.GetValues(typeof(CommandType)))
					{
						if (value.ToUpper()==ct.ToString())
						{
							ret = new Token(value,TokenType.COMMAND,ct);
							break;
						}
					}
					if (ret==null)
					{
						if (value.Contains("("))
							ret = new Token(value,TokenType.COMMAND,null);
						else
							ret = new Token(value,TokenType.GENERAL);
					}
				}
			}
			if ((ret==null)&&(_position<_query.Length))
				ret = NextToken();
			return ret;
		}
		
	}
}
