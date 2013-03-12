/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 16/03/2009
 * Time: 8:47 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using System;
using System.Reflection;
using System.Xml;

namespace Org.Reddragonit.Dbpro.Connections.MySql
{
	/// <summary>
	/// Description of MySqlConnectionPool.
	/// </summary>
	public class MySqlConnectionPool : ConnectionPool 
	{

        private string _connectionString;
        protected override string connectionString
        {
            get { return _connectionString; }
        }
				
		private string _dbName=null;
		internal string DbName{
			get{return _dbName;}
		}
		
        public MySqlConnectionPool(XmlElement elem)
            : base(elem)
        {
            int port=3306;
            string databaseServer = null;
            string database = null;
            string username = null;
            string password = null;
            foreach (XmlNode node in elem.ChildNodes)
            {
                if (node.Name == "ConnectionParameter")
                {
                    switch(node.Attributes["parameter_name"].Value){
                        case "databaseServer":
                            databaseServer=node.Attributes["parameter_value"].Value;
                            break;
                        case "port":
                            port = int.Parse(node.Attributes["parameter_value"].Value);
                            break;
                        case "username":
                            username = node.Attributes["parameter_value"].Value;
                            break;
                        case "password":
                            password = node.Attributes["parameter_value"].Value;
                            break;
                        case "database":
                            database = node.Attributes["parameter_value"].Value;
                            break;
                    }
                }
            }
            _connectionString = "Uid=" + username + ";" +
               "Pwd=" + password + ";" +
               "Database=" + database + ";" +
               "Server=" + databaseServer + ";" +
               "Port=" + port.ToString() + ";";
            _dbName = database;
        }

        protected override Connection CreateConnection(bool exclusiveLock)
		{
			return new MySqlConnection(this,connectionString,_readonly,exclusiveLock);
		}

        private static Version _version;
        internal static Version AssemblyVersion
        {
            get { return _version; }
        }

        protected override void _InitClass()
        {
            if (Utility.LocateType(MySqlConnection._PARAMETER_NAME) == null)
                Assembly.Load(MySqlConnection._ASSEMBLY_NAME);
            _version = Assembly.Load(MySqlConnection._ASSEMBLY_NAME).GetName().Version;
        }

        protected override bool _IsCoreStoredProcedure(StoredProcedure storedProcedure)
        {
            return false;
        }
	}
}
