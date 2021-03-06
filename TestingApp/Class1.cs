
using System;
using System.Collections.Generic;
using System.Data;
using Org.Reddragonit.Dbpro.Connections;
using Org.Reddragonit.Dbpro.Structure;
using TestingApp.Structure;
using System.IO;
using System.Reflection;
using System.Threading;

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
            Connection conn = ConnectionPoolManager.GetConnection(typeof(Group));
            conn.SelectAll(typeof(Group));
            _RunThreadTests();
            IDbDataParameter[] pars = new IDbDataParameter[]{
                    conn.CreateParameter("@Name","Test2"),
                    conn.CreateParameter("@Inherit",false),
                    conn.CreateParameter("@Parent",null),
                    conn.CreateParameter("@ID",(long?)null)
                };
            pars[pars.Length - 1].Direction = ParameterDirection.ReturnValue;
            conn.ExecuteNonQuery("INSERT INTO RES_GROUP(NAME,INHERIT_PARENT_RIGHTS,PARENT_GROUP_ID) VALUES(@Name,@Inherit,@Parent) RETURNING ID",
                pars);
            Console.WriteLine(pars[pars.Length - 1].Value);
            Group grp = new Group();
            grp.Name = "Testing";
            grp.ParentGroup = null;
            grp.Rights = SecurityRight.LoadAll().ToArray();
            grp.Save();
			Console.WriteLine("Selecting from a Virtual Table...");
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
            conn = ConnectionPoolManager.GetConnection(typeof(User));
            foreach (UserGroupList ugl in conn.SelectClassView(typeof(UserGroupList))){
                Console.WriteLine("FirstName: "+ugl.FirstName+"\tLastName: "+ugl.LastName+"\tGroup: "+ugl.GroupName);
            }
			/*ClassQuery cq =new ClassQuery("TestingApp.Structure","SELECT u.FirstName||' '||u.LastName AS PersonName,u.UserGroup FROM User u WHERE u.UserGroup IN @Groups");
            cq.Execute(new IDbDataParameter[]{cq.CreateParameter("@Groups",Group.LoadAllGroups())});
            while (cq.Read()){
                grp = (Group)cq[1];
                Console.WriteLine("Person: " + cq[0].ToString() + " in Group: " + grp.Name);
            }*/
            
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

            //Thread t = new Thread(new ThreadStart(SecondaryThreadStart));
            //t.Start();
            /*ConnectionPool pool = ConnectionPoolManager.GetConnection("Security");
            Console.WriteLine("Attempting to backup database...");
            Stream fs= new FileStream(".\\backuptesting.zip", FileMode.Create, FileAccess.Write, FileShare.None);
            BackupManager.BackupDataToStream(pool, ref fs);
            Console.WriteLine("Backup attempt completed successfully...");
            /*fs = new FileStream(".\\backuptesting.zip", FileMode.Open, FileAccess.Read, FileShare.None);
            Console.WriteLine("Attempting to restore database...");
            BackupManager.RestoreDataFromStream(pool, ref fs);*/
            //Console.WriteLine("Waiting on secondary thread to complete...");
            //t.Join();*/
			Console.WriteLine("Examine Diagnostics messages.");
			Console.ReadLine();
		}

        private static void _RunThreadTests()
        {
            List<Thread> threads = new List<Thread>();
            for (int x = 0; x < 50; x++)
                threads.Add(new Thread(new ThreadStart(SecondaryThreadStart)));
            for (int x = 0; x < 50; x++)
                threads[x].Start();
            threads[threads.Count - 1].Join();
        }

        private static void SecondaryThreadStart()
        {
            Connection conn = ConnectionPoolManager.GetConnection(typeof(Group));
            Console.WriteLine("Obtained Connection ID:" + conn.ID);
            conn.CloseConnection();
            Console.WriteLine("Closed Connection ID:" + conn.ID);
        }

        public static Stream LocateEmbededResource(string name)
        {
            Stream ret = typeof(Table).Assembly.GetManifestResourceStream(name);
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
