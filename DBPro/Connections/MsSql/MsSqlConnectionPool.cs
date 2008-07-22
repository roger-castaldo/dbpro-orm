using System;
using System.Collections.Generic;
using System.Text;

namespace Org.Reddragonit.Dbpro.Connections.MsSql
{
    class MsSqlConnectionPool : ConnectionPool 
    {
        public MsSqlConnectionPool(string username, string password, string database, string databaseServer, int minPoolSize, int maxPoolSize, long maxKeepAlive, bool UpdateStructureDebugMode,string connectionName)
            : this(username, password, database, databaseServer, 1433, minPoolSize, maxPoolSize, maxKeepAlive, UpdateStructureDebugMode,connectionName)
        { }

        public MsSqlConnectionPool(string username, string password, string database, string databaseServer, int port, int minPoolSize, int maxPoolSize, long maxKeepAlive, bool UpdateStructureDebugMode,string connectionName)
            : base("Server="+databaseServer+":"+port.ToString()+
                    ";Database="+database+";"+
                    "User ID="+username+";"+
                    "Password="+password+";", minPoolSize, maxPoolSize, maxKeepAlive,UpdateStructureDebugMode,connectionName)
		{}

        public MsSqlConnectionPool(string username, string password, string database, string databaseServer, bool UpdateStructureDebugMode,string connectionName)
            : this(username, password, database, databaseServer, 1433, UpdateStructureDebugMode,connectionName)
        {}

        public MsSqlConnectionPool(string username, string password, string database, string databaseServer, int port, bool UpdateStructureDebugMode,string connectionName)
            : this(username, password, database, databaseServer, port, 5, 10, 600, UpdateStructureDebugMode,connectionName)
		{
			
		}
		
		protected override Connection CreateConnection()
		{
			return new MsSqlConnection(this,connectionString);
		}
    }
}
