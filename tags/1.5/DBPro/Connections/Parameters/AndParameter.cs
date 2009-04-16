/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 05/04/2009
 * Time: 7:56 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using System;

namespace Org.Reddragonit.Dbpro.Connections.Parameters
{
	/// <summary>
	/// Description of AndParameter.
	/// </summary>
	public class AndParameter : JoinParameter 
	{
		public AndParameter(SelectParameter leftPar,SelectParameter rightPar) : base(leftPar,rightPar)
		{
		}
		
		protected override string JoinString {
			get {return "AND";}
		}
	}
}
