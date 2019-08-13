/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 02/09/2009
 * Time: 3:10 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using System;
using TestingApp.Structure;
using Org.Reddragonit.Dbpro.Virtual;

namespace TestingApp
{
	/// <summary>
	/// Description of UserGroupList.
	/// </summary>
    [ClassViewAttribute("TestingApp.Structure", "SELECT u.FirstName,u.LastName,u.UserGroup.Name as \"GroupName\" FROM User u")]
	public class UserGroupList : IClassView
	{
		public UserGroupList()
		{
		}
		
		private string _firstName;
		public string FirstName{
			get{return _firstName;}
		}
		
		private string _lastName;
		public string LastName{
			get{return _lastName;}
		}
		
		private string _groupName;
		public string GroupName{
			get{return _groupName;}
		}

        #region IClassView Members

        public void LoadFromRow(ViewResultRow row)
        {
            _firstName = row["FirstName"].ToString();
            _lastName = row["LastName"].ToString();
            _groupName = row["GroupName"].ToString();
        }

        #endregion
    }
}
