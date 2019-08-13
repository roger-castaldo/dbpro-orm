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
        [PrimaryKeyField(true)]
        public long ID
        {
            get { return (long)get(); }
            set { set(value); }
        }

        [Field(150, false)]
        public string Name
        {
            get { return (string)get(); }
            set { set(value); }
        }

        [ForeignField(true, ForeignField.UpdateDeleteAction.NO_ACTION, ForeignField.UpdateDeleteAction.NO_ACTION)]
        public Group ParentGroup
        {
            get { return (Group)get(); }
            set { set(value); }
        }

        [Field(false)]
        public bool InheritParentRights
        {
            get { return (bool)get(); }
            set { set(value); }
        }

        [ForeignField(ForeignField.UpdateDeleteAction.CASCADE, ForeignField.UpdateDeleteAction.CASCADE)]
        public SecurityRight[] Rights
        {
            get { return (SecurityRight[])get(); }
            set { set(value); }
        }

        public bool HasRight(SecurityRight right)
        {
            return HasRight(right.Name);
        }

        public bool HasRight(string rightName)
        {
            if (Rights != null)
            {
                foreach (SecurityRight right in Rights)
                {
                    if (right.Name == rightName)
                        return true;
                }
            }
            if ((ParentGroup != null) && (InheritParentRights))
                return ParentGroup.HasRight(rightName);
            return false;
        }

        private static Connection conn
        {
            get
            {
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
            return new Group();
        }
    }
}
