/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 22/01/2009
 * Time: 12:55 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using Org.Reddragonit.Dbpro.Connections.Parameters;
using System;
using System.Collections.Generic;
using Org.Reddragonit.Dbpro.Connections;
using Org.Reddragonit.Dbpro.Structure.Attributes;

namespace TestingApp.Structure
{
	/// <summary>
	/// Description of SecurityRight.
	/// </summary>
	[Table("SECURITY_RIGHTS","Security")]
	public class SecurityRight : Org.Reddragonit.Dbpro.Structure.Table
	{
		
		private long _id;
		[PrimaryKeyField(true,false)]
		public long ID
		{
			get{return _id;}
			set{_id=value;}
		}
		
		private string _name;
		[Field(150,false)]
		public string Name
		{
			get{return _name;}
			set{_name=value;}
		}
		
		private static Connection conn
		{
			get{
				return ConnectionPoolManager.GetConnection("Security").getConnection();
			}
		}
		
		public static SecurityRight Save(SecurityRight right)
		{
			Connection c = conn;
			SecurityRight ret = (SecurityRight)c.Save((Org.Reddragonit.Dbpro.Structure.Table)right);
			c.Commit();
			c.CloseConnection();
			return ret;
		}
		
		public static List<SecurityRight> LoadAll()
		{
			List<SecurityRight> ret = new List<SecurityRight>();
			Connection c = conn;
			ret.AddRange((SecurityRight[])c.SelectAll(typeof(SecurityRight)).ToArray());
			c.CloseConnection();
			return ret;
		}
		
		protected SecurityRight(){}
		
		public static SecurityRight Instance()
		{
			return (SecurityRight)Instance(typeof(SecurityRight));
		}
		
		public static SecurityRight CreateSecurityRight(string Name)
		{
			SecurityRight sr = new SecurityRight();
			sr.Name=Name;
			List<SelectParameter> pars = new List<SelectParameter>();
			pars.Add(new EqualParameter("Name",Name));
			Connection c = conn;
			List<Org.Reddragonit.Dbpro.Structure.Table> rights = c.Select(sr.GetType(),pars);
			if (rights.Count>0)
				sr=(SecurityRight)rights[0];
			else{
				sr=Save(sr);
			}
			c.CloseConnection();
			return sr;
		}
	}
}
