/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 05/04/2009
 * Time: 8:00 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using System;

namespace Org.Reddragonit.Dbpro.Connections.Parameters
{
	/// <summary>
	/// Description of InParameter.
	/// </summary>
	public class InParameter : CompareParameter 
	{
		public InParameter(string fieldName,object fieldValue) : base(fieldName,fieldValue)
		{
		}
		
		protected override string ComparatorString {
			get {
				return "IN";
			}
		}
		protected override bool SupportsList {
			get { return true; }
		}
	}
}
