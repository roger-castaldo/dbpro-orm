
using Org.Reddragonit.Dbpro.Connections.ClassSQL;
using System;
using System.Collections.Generic;
using System.Data;
using Org.Reddragonit.Dbpro.Connections;
using Org.Reddragonit.Dbpro.Structure;
using Org.Reddragonit.Dbpro.Virtual;
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
			/*Console.WriteLine("Selecting from a Virtual Table...");
			User u = User.Instance();
			u.Active=true;
			u.FirstName="Roger";
			u.LastName="Castaldo";
			u.Password="testing";
			u.Type=UserTypes.Normal;
			u.UserGroup=Group.LoadAllGroups()[0];
			u.UserName="rcastaldo";
			u=User.Save(u);
			VirtualTableConnection vtb = new VirtualTableConnection();
			List<object> tmp =vtb.SelectVirtualTable(typeof(UserGroupList));
			foreach (UserGroupList ugl in tmp){
				Console.WriteLine("FirstName: "+ugl.FirstName+"\tLastName: "+ugl.LastName+"\tGroup: "+ugl.GroupName);
			}*/
			ClassQuery cq =new ClassQuery("TestingApp.Structure","SELECT u.FirstName||' '||u.LastName AS PersonName,u.UserGroup FROM User u WHERE u.UserGroup.Rights.Name='Roger'");
            cq.Execute();
            while (cq.Read()){
                Group grp = (Group)cq[1];
                Console.WriteLine("Person: " + cq[0].ToString() + " in Group: " + grp.Name);
            }
			Console.WriteLine("Examine Diagnostics messages.");
			Console.ReadLine();
		}
	}
}
