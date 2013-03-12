/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 03/04/2009
 * Time: 10:35 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using System;
using System.Reflection;
using System.Xml;

namespace Org.Reddragonit.Dbpro.Connections.PgSql
{
	/// <summary>
	/// Description of PgSqlConnectionPool.
	/// </summary>
	public class PgSqlConnectionPool : ConnectionPool 
	{
		private string _dbName=null;
		internal string DbName{
			get{return _dbName;}
		}

        private string _connectionString;
        protected override string connectionString
        {
            get { return _connectionString; }
        }

        public PgSqlConnectionPool(XmlElement elem)
            : base(elem)
        {
            int port = 5432;
            string databaseServer = null;
            string database = null;
            string username = null;
            string password = null;
            foreach (XmlNode node in elem.ChildNodes)
            {
                if (node.Name == "ConnectionParameter")
                {
                    switch (node.Attributes["parameter_name"].Value)
                    {
                        case "databaseServer":
                            databaseServer = node.Attributes["parameter_value"].Value;
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
            _connectionString = "User Id=" + username + ";" +
                   "Password=" + password + ";" +
                   "Database=" + database + ";" +
                   "Server=" + databaseServer + ";" +
                   "Port=" + port.ToString() + ";";
            _dbName = database;
        }
		
		protected override Connection CreateConnection(bool exclusiveLock)
		{
			return new PgSqlConnection(this,connectionString,this._readonly,exclusiveLock);
		}

        protected override void _InitClass()
        {
            if (Utility.LocateType(PgSqlConnection._PARAMETER_TYPE_NAME) == null)
                Assembly.Load(PgSqlConnection._ASSEMBLY_NAME);
        }

        protected override bool _IsCoreStoredProcedure(StoredProcedure storedProcedure)
        {
            return false;
        }
	}
}
