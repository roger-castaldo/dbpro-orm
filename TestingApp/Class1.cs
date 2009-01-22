
using System;
using System.Collections.Generic;
using TestingApp.Structure;
using Org.Reddragonit.Dbpro.Connections;
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
			/*Group g = new Group();
			g.InheritParentRights=false;
			g.Name="Admin";
			g=Group.Save(g);
			User u= new User();
			u.FirstName="Roger";
			u.LastName="Castaldo";
			u.UserGroup=g;
			u.UserName="rcastaldo";
			u.Password="copperbed1";
			u = User.Save(u);*/
			
			User u = User.LoginUser("rcastaldo","copperbed1");
			if (u==null)
				Console.WriteLine("Unable to login user.");
			else
				Console.WriteLine("User logged in.");
			
            pool.ClosePool();
            Console.ReadLine();
		}
	}
}
