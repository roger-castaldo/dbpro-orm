/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 05/04/2009
 * Time: 7:48 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using System;

namespace Org.Reddragonit.Dbpro.Connections.Parameters
{
	/// <summary>
	/// Description of LikeParameter.
	/// </summary>
	public class LikeParameter : CompareParameter
	{
		public LikeParameter(string fieldName,string fieldValue) : base(fieldName,"'%"+fieldValue.ToString()+"%'")
		{
		}
		
		protected override string ComparatorString {
			get {return "LIKE";}
		}
	}
}
