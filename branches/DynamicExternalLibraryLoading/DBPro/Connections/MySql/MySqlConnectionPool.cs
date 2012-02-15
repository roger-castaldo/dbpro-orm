/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 16/03/2009
 * Time: 8:47 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using System;

namespace Org.Reddragonit.Dbpro.Connections.MySql
{
	/// <summary>
	/// Description of MySqlConnectionPool.
	/// </summary>
	public class MySqlConnectionPool : ConnectionPool 
	{
		
				
		private string _dbName=null;
		internal string DbName{
			get{return _dbName;}
		}
		
		public MySqlConnectionPool(string username, string password, string database, string databaseServer, int minPoolSize, int maxPoolSize, long maxKeepAlive,bool UpdateStructureDebugMode,string connectionName) 
			: this(username,password,database,databaseServer,3306,minPoolSize,maxPoolSize,maxKeepAlive,UpdateStructureDebugMode,connectionName,true,false)
		{ }
		
		public MySqlConnectionPool(string username, string password, string database, string databaseServer, int minPoolSize, int maxPoolSize, long maxKeepAlive,bool UpdateStructureDebugMode,string connectionName,bool allowTableDeletions)
            : this(username, password, database, databaseServer, 3306, minPoolSize, maxPoolSize, maxKeepAlive, UpdateStructureDebugMode, connectionName, allowTableDeletions, false)
		{ }

		public MySqlConnectionPool(string username, string password, string database, string databaseServer, bool UpdateStructureDebugMode,string connectionName)
			: this(username, password, database, databaseServer, 3306,UpdateStructureDebugMode,connectionName,true)
		{}

		public MySqlConnectionPool(string username, string password, string database, string databaseServer, int port, bool UpdateStructureDebugMode,string connectionName)
            : this(username, password, database, databaseServer, port, 5, 10, 600, UpdateStructureDebugMode, connectionName, true, false)
		{
			
		}
		
		public MySqlConnectionPool(string username, string password, string database, string databaseServer, bool UpdateStructureDebugMode,string connectionName,bool allowTableDeletions)
			: this(username, password, database, databaseServer, 3306,UpdateStructureDebugMode,connectionName,allowTableDeletions)
		{}

		public MySqlConnectionPool(string username, string password, string database, string databaseServer, int port, bool UpdateStructureDebugMode,string connectionName,bool allowTableDeletions)
            : this(username, password, database, databaseServer, port, 5, 10, 600, UpdateStructureDebugMode, connectionName, allowTableDeletions, false)
		{
			
		}

        public MySqlConnectionPool(string username, string password, string database, string databaseServer, int minPoolSize, int maxPoolSize, long maxKeepAlive, bool UpdateStructureDebugMode, string connectionName,int readTimeout)
            : this(username, password, database, databaseServer, 3306, minPoolSize, maxPoolSize, maxKeepAlive, UpdateStructureDebugMode, connectionName, true, readTimeout, false)
        { }

        public MySqlConnectionPool(string username, string password, string database, string databaseServer, int minPoolSize, int maxPoolSize, long maxKeepAlive, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions, int readTimeout)
            : this(username, password, database, databaseServer, 3306, minPoolSize, maxPoolSize, maxKeepAlive, UpdateStructureDebugMode, connectionName, allowTableDeletions, readTimeout, false)
        { }

        public MySqlConnectionPool(string username, string password, string database, string databaseServer, bool UpdateStructureDebugMode, string connectionName, int readTimeout)
            : this(username, password, database, databaseServer, 3306, UpdateStructureDebugMode, connectionName, true,readTimeout)
        { }

        public MySqlConnectionPool(string username, string password, string database, string databaseServer, int port, bool UpdateStructureDebugMode, string connectionName, int readTimeout)
            : this(username, password, database, databaseServer, port, 5, 10, 600, UpdateStructureDebugMode, connectionName, true, readTimeout, false)
        {

        }

        public MySqlConnectionPool(string username, string password, string database, string databaseServer, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions, int readTimeout)
            : this(username, password, database, databaseServer, 3306, UpdateStructureDebugMode, connectionName, allowTableDeletions,readTimeout)
        { }

        public MySqlConnectionPool(string username, string password, string database, string databaseServer, int port, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions,int readTimeout)
            : this(username, password, database, databaseServer, port, 5, 10, 600, UpdateStructureDebugMode, connectionName, allowTableDeletions, readTimeout, false)
        {

        }

        public MySqlConnectionPool(string username, string password, string database, string databaseServer, int minPoolSize, int maxPoolSize, long maxKeepAlive, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions, bool Readonly)
            : this(username, password, database, databaseServer, 3306, minPoolSize, maxPoolSize, maxKeepAlive, UpdateStructureDebugMode, connectionName, allowTableDeletions, Readonly)
        { }

        public MySqlConnectionPool(string username, string password, string database, string databaseServer, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions, bool Readonly)
            : this(username, password, database, databaseServer, 3306, UpdateStructureDebugMode, connectionName, allowTableDeletions,Readonly)
        { }

        public MySqlConnectionPool(string username, string password, string database, string databaseServer, int port, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions, bool Readonly)
            : this(username, password, database, databaseServer, port, 5, 10, 600, UpdateStructureDebugMode, connectionName, allowTableDeletions, Readonly)
        {

        }

        public MySqlConnectionPool(string username, string password, string database, string databaseServer, int minPoolSize, int maxPoolSize, long maxKeepAlive, bool UpdateStructureDebugMode, string connectionName, int readTimeout, bool Readonly)
            : this(username, password, database, databaseServer, 3306, minPoolSize, maxPoolSize, maxKeepAlive, UpdateStructureDebugMode, connectionName, true, readTimeout, Readonly)
        { }

        public MySqlConnectionPool(string username, string password, string database, string databaseServer, int minPoolSize, int maxPoolSize, long maxKeepAlive, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions, int readTimeout, bool Readonly)
            : this(username, password, database, databaseServer, 3306, minPoolSize, maxPoolSize, maxKeepAlive, UpdateStructureDebugMode, connectionName, allowTableDeletions, readTimeout, Readonly)
        { }

        public MySqlConnectionPool(string username, string password, string database, string databaseServer, bool UpdateStructureDebugMode, string connectionName, int readTimeout, bool Readonly)
            : this(username, password, database, databaseServer, 3306, UpdateStructureDebugMode, connectionName, true, readTimeout,Readonly)
        { }

        public MySqlConnectionPool(string username, string password, string database, string databaseServer, int port, bool UpdateStructureDebugMode, string connectionName, int readTimeout, bool Readonly)
            : this(username, password, database, databaseServer, port, 5, 10, 600, UpdateStructureDebugMode, connectionName, true, readTimeout, Readonly)
        {

        }

        public MySqlConnectionPool(string username, string password, string database, string databaseServer, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions, int readTimeout, bool Readonly)
            : this(username, password, database, databaseServer, 3306, UpdateStructureDebugMode, connectionName, allowTableDeletions, readTimeout,Readonly)
        { }

        public MySqlConnectionPool(string username, string password, string database, string databaseServer, int port, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions, int readTimeout, bool Readonly)
            : this(username, password, database, databaseServer, port, 5, 10, 600, UpdateStructureDebugMode, connectionName, allowTableDeletions, readTimeout, Readonly)
        {

        }

        public MySqlConnectionPool(string username, string password, string database, string databaseServer, int port, int minPoolSize, int maxPoolSize, long maxKeepAlive, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions,bool Readonly)
            : base("Uid=" + username + ";" +
                   "Pwd=" + password + ";" +
                   "Database=" + database + ";" +
                   "Server=" + databaseServer + ";" +
                   "Port=" + port.ToString() + ";", minPoolSize, maxPoolSize, maxKeepAlive, UpdateStructureDebugMode, connectionName, allowTableDeletions,Readonly)
        {
            _dbName = database;
        }

        public MySqlConnectionPool(string username, string password, string database, string databaseServer, int port, int minPoolSize, int maxPoolSize, long maxKeepAlive, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions, int readTimeout, bool Readonly)
            : base("Uid=" + username + ";" +
                   "Pwd=" + password + ";" +
                   "Database=" + database + ";" +
                   "Server=" + databaseServer + ";" +
                   "Port=" + port.ToString() + ";", minPoolSize, maxPoolSize, maxKeepAlive, UpdateStructureDebugMode, connectionName, allowTableDeletions, readTimeout, Readonly)
        {
            _dbName = database;
        }

		
		protected override Connection CreateConnection(bool exclusiveLock)
		{
			return new MySqlConnection(this,connectionString,_readonly,exclusiveLock);
		}
	}
}
