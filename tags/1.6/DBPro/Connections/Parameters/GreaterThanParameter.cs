/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 05/04/2009
 * Time: 8:03 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using System;

namespace Org.Reddragonit.Dbpro.Connections.Parameters
{
	/// <summary>
	/// Description of GreaterThanParameter.
	/// </summary>
	public class GreaterThanParameter : CompareParameter
	{
		public GreaterThanParameter(string fieldName,object fieldValue) : base(fieldName,fieldValue)
		{
		}
		
		protected override string ComparatorString {
			get {
				return ">";
			}
		}
	}
}
