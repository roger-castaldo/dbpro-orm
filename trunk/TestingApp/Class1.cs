
using Org.Reddragonit.Dbpro.Virtual;
using System;
using System.Collections.Generic;
using System.Data;
using Org.Reddragonit.Dbpro.Connections;
using Org.Reddragonit.Dbpro.Structure;
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
			Console.WriteLine("Selecting from a Virtual Table...");
			VirtualTableConnection vtb = new VirtualTableConnection();
			vtb.SelectVirtualTable(typeof(UserGroupList));
			Console.WriteLine("Examine Diagnostics messages.");
			Console.ReadLine();
		}
	}
}
