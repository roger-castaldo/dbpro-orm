/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 02/09/2009
 * Time: 3:10 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using System;
using Org.Reddragonit.Dbpro.Virtual.Attributes;
using TestingApp.Structure;

namespace TestingApp
{
	/// <summary>
	/// Description of UserGroupList.
	/// </summary>
	[VirtualTableAttribute(typeof(User))]
	public class UserGroupList
	{
		public UserGroupList()
		{
		}
		
		private string _firstName;
		[VirtualField(typeof(User),"FirstName")]
		public string FirstName{
			get{return _firstName;}
			set{_firstName=value;}
		}
		
		private string _lastName;
		[VirtualField(typeof(User),"LastName")]
		public string LastName{
			get{return _lastName;}
			set{_lastName=value;}
		}
		
		private string _groupName;
		[VirtualField(typeof(Group),"UserGroup.Name")]
		public string GroupName{
			get{return _groupName;}
			set{_groupName=value;}
		}
	}
}
