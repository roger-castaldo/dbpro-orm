
using Org.Reddragonit.Dbpro.Connections.ClassSQL;
using System;
using System.Collections.Generic;
using System.Data;
using Org.Reddragonit.Dbpro.Connections;
using Org.Reddragonit.Dbpro.Structure;
using Org.Reddragonit.Dbpro.Virtual;
using TestingApp.Structure;
using Org.Reddragonit.Dbpro.Backup;
using System.Xml;
using Org.Reddragonit.Dbpro;
using System.IO;
using System.Reflection;

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
            Group grp = new Group();
            grp.Name = "test";
            Group.Save(grp);
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
			}
			ClassQuery cq =new ClassQuery("TestingApp.Structure","SELECT u.FirstName||' '||u.LastName AS PersonName,u.UserGroup FROM User u WHERE u.UserGroup.Rights.Name='Roger'");
            cq.Execute();
            while (cq.Read()){
                grp = (Group)cq[1];
                Console.WriteLine("Person: " + cq[0].ToString() + " in Group: " + grp.Name);
            }
            //XmlDocument doc = new XmlDocument();
            //string xml = ReadEmbeddedResource("TestingApp.CompressionTester.xml");
            //System.Diagnostics.Debug.WriteLine("Uncompressed Size=" + xml.Length.ToString());
            //doc.LoadXml(xml);
            //byte[] tmp = XMLCompressor.CompressXMLDocument(doc);
            //System.Diagnostics.Debug.WriteLine("Compressed size=" + tmp.Length.ToString());
            //System.Diagnostics.Debug.WriteLine(System.Text.ASCIIEncoding.ASCII.GetString(tmp));
            //doc = XMLCompressor.DecompressXMLDocument(new MemoryStream(tmp));
            //XmlTextWriter xtw = new XmlTextWriter(new FileStream(".\\results.xml", FileMode.Create, FileAccess.Write, FileShare.None), System.Text.Encoding.ASCII); 
            //doc.WriteContentTo(xtw);
            //xtw.Flush();
            //xtw.Close();
             */
            Console.WriteLine("Attempting to backup database...");
            ConnectionPool pool = ConnectionPoolManager.GetConnection("Security");
            Stream fs = new FileStream(".\\backuptesting.zip", FileMode.Create, FileAccess.Write, FileShare.None);
            BackupManager.BackupDataToStream(pool, ref fs);
            Console.WriteLine("Backup attempt completed successfully...");
			Console.WriteLine("Examine Diagnostics messages.");
			Console.ReadLine();
		}

        public static Stream LocateEmbededResource(string name)
        {
            Stream ret = typeof(Utility).Assembly.GetManifestResourceStream(name);
            if (ret == null)
            {
                foreach (Assembly ass in AppDomain.CurrentDomain.GetAssemblies())
                {
                    try
                    {
                        if (!ass.GetName().Name.Contains("mscorlib") && !ass.GetName().Name.StartsWith("System") && !ass.GetName().Name.StartsWith("Microsoft"))
                        {
                            ret = ass.GetManifestResourceStream(name);
                            if (ret != null)
                                break;
                        }
                    }
                    catch (Exception e)
                    {
                        if (e.Message != "The invoked member is not supported in a dynamic assembly.")
                        {
                            throw e;
                        }
                    }
                }
            }
            return ret;
        }

        public static string ReadEmbeddedResource(string name)
        {
            Stream s = LocateEmbededResource(name);
            string ret = "";
            if (s != null)
            {
                TextReader tr = new StreamReader(s);
                ret = tr.ReadToEnd();
                tr.Close();
            }
            return ret;
        }
	}
}
