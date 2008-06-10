using System;
using System.Collections.Generic;
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
            AccountStatus acs = new AccountStatus();
            AccountTable a = new AccountTable();
            //Org.Reddragonit.Dbpro.Connections.Firebird.FBConnectionPool pool = new Org.Reddragonit.Dbpro.Connections.Firebird.FBConnectionPool("sysdba", "masterkey", "C:\\Documents and Settings\\rcastaldo\\My Documents\\Firebird\\TESTING.FDB", "localhost", 3050);
            Org.Reddragonit.Dbpro.Connections.Firebird.FBConnectionPool pool = new Org.Reddragonit.Dbpro.Connections.Firebird.FBConnectionPool("sysdba", "copperbed1", "G:\\BillingPro\\database\\BILLINGPRO.FDB", "localhost", 3050);
            acs.StatusId = 1;
            acs.StatusName = "Active";
            acs.Data = System.Text.ASCIIEncoding.ASCII.GetBytes("Hello Joe");
            a.FirstName = "Roger";
            a.LastName = "Castaldo";
            a.Status = new AccountStatus[] { acs };
            Org.Reddragonit.Dbpro.Connections.Connection conn = pool.getConnection();
            //conn.CreateTable(acs,true);
            //conn.CreateTable(a,true);
            /*a=(AccountTable)conn.Save(a);
            a.FirstName = "George";
            a = (AccountTable)conn.Save(a);
            Console.WriteLine(conn.SelectAll(typeof(AccountTable)).Count);
            List<Org.Reddragonit.Dbpro.Connections.SelectParameter> pars = new List<Org.Reddragonit.Dbpro.Connections.SelectParameter>();
            pars.Add(new Org.Reddragonit.Dbpro.Connections.SelectParameter("StatusId",a.Status[0].StatusId ));
            Console.WriteLine(((AccountStatus)conn.Select(typeof(AccountStatus),pars)[0]).StatusId );
            conn.CloseConnection();*/
            pool.ClosePool();
            Console.ReadLine();
		}
	}
}
