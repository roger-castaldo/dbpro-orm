/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 24/11/2008
 * Time: 1:14 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */

using System;

namespace Org.Reddragonit.Dbpro.Connections.ClassQuery
{
	/// <summary>
	/// Description of Query.
	/// </summary>
	public class Query
	{
		private bool _singleClass=false;
		internal bool SingleClass
		{
			get{return _singleClass;}
			set{_singleClass=value;}
		}
		
		private string _string;
		public string String
		{
			get{return _string;}
			set{_string=value;}
		}
		
		public string ParsedString
		{
			get{return Parser.ParseClassQuery(_string,null);}
		}
		
		public Query()
		{
		}
		
		public Query(string QueryString)
		{
			_string=QueryString;
		}
	}
}
