using System;
using System.Collections.Generic;
using System.Text;

namespace Org.Reddragonit.Dbpro.Connections.MsSql
{
    class MsSqlConnectionPool : ConnectionPool 
    {
        public MsSqlConnectionPool(string username, string password, string database, string databaseServer, int minPoolSize, int maxPoolSize, long maxKeepAlive, bool UpdateStructureDebugMode)
            : this(username, password, database, databaseServer, 1433, minPoolSize, maxPoolSize, maxKeepAlive, UpdateStructureDebugMode)
        { }

        public MsSqlConnectionPool(string username, string password, string database, string databaseServer, int port, int minPoolSize, int maxPoolSize, long maxKeepAlive, bool UpdateStructureDebugMode)
            : base("Server="+databaseServer+":"+port.ToString()+
                    ";Database="+database+";"+
                    "User ID="+username+";"+
                    "Password="+password+";", minPoolSize, maxPoolSize, maxKeepAlive,UpdateStructureDebugMode)
		{}

        public MsSqlConnectionPool(string username, string password, string database, string databaseServer, bool UpdateStructureDebugMode)
            : this(username, password, database, databaseServer, 1433, UpdateStructureDebugMode)
        {}

        public MsSqlConnectionPool(string username, string password, string database, string databaseServer, int port, bool UpdateStructureDebugMode)
            : this(username, password, database, databaseServer, port, 5, 10, 600, UpdateStructureDebugMode)
		{
			
		}
		
		protected override Connection CreateConnection()
		{
			return new MsSqlConnection(this,connectionString);
		}
    }
}
