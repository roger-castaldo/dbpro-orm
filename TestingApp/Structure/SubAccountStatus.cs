/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 29/11/2008
 * Time: 9:54 AM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */

using System;
using Org.Reddragonit.Dbpro.Structure;
using FieldType = Org.Reddragonit.Dbpro.Structure.Attributes.Field.FieldType;

namespace TestingApp.Structure
{
	/// <summary>
	/// Description of SubAccountStatus.
	/// </summary>
	[Org.Reddragonit.Dbpro.Structure.Attributes.Table("SUB_ACCOUNT_STATUS")]
	public class SubAccountStatus : AccountStatus 
	{
		public SubAccountStatus()
		{
		}
		
		
		[Org.Reddragonit.Dbpro.Structure.Attributes.Field("SUB_STATUS_NAME", FieldType.STRING, false, 50)]
        public string SubName
        {
            get
            {
                return StatusName;
            }
            set
            {
                StatusName = value;
            }
        }
	}
}
