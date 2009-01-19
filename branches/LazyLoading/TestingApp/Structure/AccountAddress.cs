/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 04/07/2008
 * Time: 9:24 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */

using System;
using Org.Reddragonit.Dbpro.Structure;
using FieldType = Org.Reddragonit.Dbpro.Structure.Attributes.FieldType;
using UpdateDeleteAction = Org.Reddragonit.Dbpro.Structure.Attributes.ForeignField.UpdateDeleteAction;

namespace TestingApp.Structure
{
	/// <summary>
	/// Description of AccountAddress.
	/// </summary>
	[Org.Reddragonit.Dbpro.Structure.Attributes.Table("ACCOUNT_ADDRESS")]
	public class AccountAddress : Table
	{
		private string _streetName;
		private string _city;
		//private DateTime _startDate;
		private AccountTable _account;
		
		public AccountAddress()
		{
		}
		
		[Org.Reddragonit.Dbpro.Structure.Attributes.VersionField("STREET_NAME", FieldType.STRING, false,250)]
		public string StreetName
		{
			get{return _streetName;}
			set{_streetName=value;}
		}
		
		[Org.Reddragonit.Dbpro.Structure.Attributes.VersionField("CITY", FieldType.STRING, false, 100)]
		public string City
		{
			get{return _city;}
			set{_city=value;}
		}
		
//		[Org.Reddragonit.Dbpro.Structure.Attributes.PrimaryKeyField("START_DATE",FieldType.DATETIME,false,true)]
//		public DateTime StartDate
//		{
//			get{return _startDate;}
//			set{_startDate=value;}
//		}
		
		[Org.Reddragonit.Dbpro.Structure.Attributes.ForeignPrimaryKeyField(UpdateDeleteAction.CASCADE,UpdateDeleteAction.CASCADE)]
		public AccountTable Account
		{
			get{return _account;}
			set{_account=value;}
		}
	}
}
