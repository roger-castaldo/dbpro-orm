/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 22/01/2009
 * Time: 12:56 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using System;
using System.Collections.Generic;
using Org.Reddragonit.Dbpro.Connections;
using Org.Reddragonit.Dbpro.Structure.Attributes;

namespace TestingApp.Structure
{
	/// <summary>
	/// Description of Group.
	/// </summary>
    [Table()]
	public class Group : Org.Reddragonit.Dbpro.Structure.Table
	{
		private long _Id;
		[PrimaryKeyField(true)]
		public long ID
		{
			get{return _Id;}
			set{_Id=value;}
		}
		
		private string _Name;
		[Field(150,false)]
		public string Name
		{
			get{return _Name;}
			set{_Name=value;}
		}
		
		private Group _parent;
		[ForeignField(true,ForeignField.UpdateDeleteAction.NO_ACTION,ForeignField.UpdateDeleteAction.NO_ACTION)]
		public Group ParentGroup
		{
			get{return _parent;}
			set{_parent=value;}
		}
		
		private bool _inheritParentRights;
		[Field(false)]
		public bool InheritParentRights
		{
			get{return _inheritParentRights;}
			set{_inheritParentRights=value;}
		}
		
		private SecurityRight[] _rights;
		[ForeignField(ForeignField.UpdateDeleteAction.CASCADE,ForeignField.UpdateDeleteAction.CASCADE)]
		public SecurityRight[] Rights
		{
			get{return _rights;}
			set{_rights=value;}
		}
		
		public bool HasRight(SecurityRight right)
		{
			return HasRight(right.Name);
		}
		
		public bool HasRight(string rightName)
		{
			if (Rights!=null)
			{
				foreach (SecurityRight right in Rights)
				{
					if (right.Name==rightName)
						return true;
				}
			}
			if ((ParentGroup!=null)&&(InheritParentRights))
				return ParentGroup.HasRight(rightName);
			return false;
		}
		
		private static Connection conn
		{
			get{
				return ConnectionPoolManager.GetConnection("Security");
			}
		}
		
		public static Group Save(Group group)
		{
			Connection c = conn;
			Group ret = (Group)c.Save((Org.Reddragonit.Dbpro.Structure.Table)group);
			c.Commit();
			c.CloseConnection();
			return ret;
		}
		
		public static List<Group> LoadAllGroups()
		{
			List<Group> ret = new List<Group>();
			Connection c = conn;
			foreach (Org.Reddragonit.Dbpro.Structure.Table tbl in c.SelectAll(typeof(Group)))
				ret.Add((Group)tbl);
			c.CloseConnection();
			return ret;
		}
		
		public Group()
		{
		}
		
		public static Group Instance()
		{
			return (Group)Instance(typeof(Group));
		}
	}
}
