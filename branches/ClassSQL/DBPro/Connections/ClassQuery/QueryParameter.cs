/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 24/11/2008
 * Time: 1:15 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */

using System;

namespace Org.Reddragonit.Dbpro.Connections.ClassQuery
{
	/// <summary>
	/// Description of QueryParameter.
	/// </summary>
	public class QueryParameter
	{
		private string _name;
		public string Name
		{
			get{return _name;}
			set{_name=value;}
		}
		
		private object _value;
		public object Value
		{
			get{return _value;}
			set{_value=value;}
		}
		
		public QueryParameter()
		{
		}
		
		public QueryParameter(string Name,object Value)
		{
			_name=Name;
			_value=Value;
		}
	}
}
