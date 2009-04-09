/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 05/04/2009
 * Time: 7:42 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using System;

namespace Org.Reddragonit.Dbpro.Connections.Parameters
{
	/// <summary>
	/// Description of EqualParameter.
	/// </summary>
	public class EqualParameter : CompareParameter
	{
		public EqualParameter(string fieldName,object fieldValue) : base(fieldName,fieldValue)
		{}
		
		protected override string ComparatorString {
			get {return "=";}
		}
	}
}
