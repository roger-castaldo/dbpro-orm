/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 15/09/2008
 * Time: 9:13 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */

using System;

namespace Org.Reddragonit.Dbpro.Structure.Attributes
{
	/// <summary>
	/// Description of IVersionField.
	/// </summary>
	public interface IVersionField
	{
		
		VersionField.VersionTypes VersionType
		{
			get;
		}
	}
}
