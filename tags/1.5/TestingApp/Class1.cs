
using Org.Reddragonit.Dbpro.Structure;
using System;
using System.Collections.Generic;
using System.Data;
using Org.Reddragonit.Dbpro.Connections;
using TestingApp.Structure;

namespace TestingApp
{
	/// <summary>
	/// Summary description for Class1.
	/// </summary>
	class Class1
	{
		/// <summary>
		/// The main entry point for the application.
		/// </summary>
		[STAThread]
		static void Main(string[] args)
		{
			ConnectionPool pool = ConnectionPoolManager.GetConnection("Security");
			Connection c = pool.getConnection();
			Group g = new Group();
			g.InheritParentRights=false;
			g.Name="Admin";
			g=Group.Save(g);
			User u= new User();
			u.FirstName="Roger";
			u.LastName="Castaldo";
			u.UserGroup=g;
			u.UserName="rcastaldo";
			u.Password="copperbed1";
			u.Type=UserTypes.Admin;
			u = User.Save(u);
			
			u = User.LoginUser("rcastaldo","copperbed1");
			if (u==null)
				Console.WriteLine("Unable to login user.");
			else
				Console.WriteLine("User logged in.");
			
			PagedTableList lst = new PagedTableList(typeof(User),null,null);
			PagedTableListEnumerator e = lst.GetEnumerator();
			while (e.MoveNext())
			{
				Console.WriteLine(((User)e.Current).UserName);
			}
			
            pool.ClosePool();
            Console.ReadLine();
		}
	}
}
