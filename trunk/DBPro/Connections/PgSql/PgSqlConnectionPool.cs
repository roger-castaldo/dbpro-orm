/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 03/04/2009
 * Time: 10:35 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using System;

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
		
		public PgSqlConnectionPool(string username, string password, string database, string databaseServer, int minPoolSize, int maxPoolSize, long maxKeepAlive,bool UpdateStructureDebugMode,string connectionName) 
			: this(username,password,database,databaseServer,5432,minPoolSize,maxPoolSize,maxKeepAlive,UpdateStructureDebugMode,connectionName,true,false)
		{ }
		
		public PgSqlConnectionPool(string username, string password, string database, string databaseServer, int minPoolSize, int maxPoolSize, long maxKeepAlive,bool UpdateStructureDebugMode,string connectionName,bool allowTableDeletions)
            : this(username, password, database, databaseServer, 5432, minPoolSize, maxPoolSize, maxKeepAlive, UpdateStructureDebugMode, connectionName, allowTableDeletions, false)
		{ }

		public PgSqlConnectionPool(string username, string password, string database, string databaseServer, bool UpdateStructureDebugMode,string connectionName)
            : this(username, password, database, databaseServer, 5432, UpdateStructureDebugMode, connectionName, false)
		{}

		public PgSqlConnectionPool(string username, string password, string database, string databaseServer, int port, bool UpdateStructureDebugMode,string connectionName)
            : this(username, password, database, databaseServer, port, 5, 10, 600, UpdateStructureDebugMode, connectionName, true, false)
		{
			
		}
		
		public PgSqlConnectionPool(string username, string password, string database, string databaseServer, bool UpdateStructureDebugMode,string connectionName,bool allowTableDeletions)
            : this(username, password, database, databaseServer, 5432, UpdateStructureDebugMode, connectionName, allowTableDeletions, false)
		{}

		public PgSqlConnectionPool(string username, string password, string database, string databaseServer, int port, bool UpdateStructureDebugMode,string connectionName,bool allowTableDeletions)
            : this(username, password, database, databaseServer, port, 5, 10, 600, UpdateStructureDebugMode, connectionName, allowTableDeletions, false)
		{
			
		}

        public PgSqlConnectionPool(string username, string password, string database, string databaseServer, int minPoolSize, int maxPoolSize, long maxKeepAlive, bool UpdateStructureDebugMode, string connectionName,int readTimeout)
            : this(username, password, database, databaseServer, 5432, minPoolSize, maxPoolSize, maxKeepAlive, UpdateStructureDebugMode, connectionName, true, readTimeout, false)
        { }

        public PgSqlConnectionPool(string username, string password, string database, string databaseServer, int minPoolSize, int maxPoolSize, long maxKeepAlive, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions, int readTimeout)
            : this(username, password, database, databaseServer, 5432, minPoolSize, maxPoolSize, maxKeepAlive, UpdateStructureDebugMode, connectionName, allowTableDeletions, readTimeout, false)
        { }

        public PgSqlConnectionPool(string username, string password, string database, string databaseServer, bool UpdateStructureDebugMode, string connectionName, int readTimeout)
            : this(username, password, database, databaseServer, 5432, UpdateStructureDebugMode, connectionName, readTimeout, false)
        { }

        public PgSqlConnectionPool(string username, string password, string database, string databaseServer, int port, bool UpdateStructureDebugMode, string connectionName, int readTimeout)
            : this(username, password, database, databaseServer, port, 5, 10, 600, UpdateStructureDebugMode, connectionName, true, readTimeout, false)
        {

        }

        public PgSqlConnectionPool(string username, string password, string database, string databaseServer, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions, int readTimeout)
            : this(username, password, database, databaseServer, 5432, UpdateStructureDebugMode, connectionName, allowTableDeletions, readTimeout, false)
        { }

        public PgSqlConnectionPool(string username, string password, string database, string databaseServer, int port, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions, int readTimeout)
            : this(username, password, database, databaseServer, port, 5, 10, 600, UpdateStructureDebugMode, connectionName, allowTableDeletions, readTimeout, false)
        {

        }

        public PgSqlConnectionPool(string username, string password, string database, string databaseServer, int minPoolSize, int maxPoolSize, long maxKeepAlive, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions, bool Readonly)
            : this(username, password, database, databaseServer, 5432, minPoolSize, maxPoolSize, maxKeepAlive, UpdateStructureDebugMode, connectionName, allowTableDeletions,Readonly)
        { }

        public PgSqlConnectionPool(string username, string password, string database, string databaseServer, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions, bool Readonly)
            : this(username, password, database, databaseServer, 5432, UpdateStructureDebugMode, connectionName, allowTableDeletions,Readonly)
        { }

        public PgSqlConnectionPool(string username, string password, string database, string databaseServer, int port, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions, bool Readonly)
            : this(username, password, database, databaseServer, port, 5, 10, 600, UpdateStructureDebugMode, connectionName, allowTableDeletions,Readonly)
        {

        }

        public PgSqlConnectionPool(string username, string password, string database, string databaseServer, int minPoolSize, int maxPoolSize, long maxKeepAlive, bool UpdateStructureDebugMode, string connectionName, int readTimeout, bool Readonly)
            : this(username, password, database, databaseServer, 5432, minPoolSize, maxPoolSize, maxKeepAlive, UpdateStructureDebugMode, connectionName, true, readTimeout,Readonly)
        { }

        public PgSqlConnectionPool(string username, string password, string database, string databaseServer, int minPoolSize, int maxPoolSize, long maxKeepAlive, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions, int readTimeout, bool Readonly)
            : this(username, password, database, databaseServer, 5432, minPoolSize, maxPoolSize, maxKeepAlive, UpdateStructureDebugMode, connectionName, allowTableDeletions, readTimeout,Readonly)
        { }

        public PgSqlConnectionPool(string username, string password, string database, string databaseServer, bool UpdateStructureDebugMode, string connectionName, int readTimeout, bool Readonly)
            : this(username, password, database, databaseServer, 5432, UpdateStructureDebugMode, connectionName, readTimeout,Readonly)
        { }

        public PgSqlConnectionPool(string username, string password, string database, string databaseServer, int port, bool UpdateStructureDebugMode, string connectionName, int readTimeout, bool Readonly)
            : this(username, password, database, databaseServer, port, 5, 10, 600, UpdateStructureDebugMode, connectionName, true, readTimeout,Readonly)
        {

        }

        public PgSqlConnectionPool(string username, string password, string database, string databaseServer, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions, int readTimeout, bool Readonly)
            : this(username, password, database, databaseServer, 5432, UpdateStructureDebugMode, connectionName, allowTableDeletions, readTimeout,Readonly)
        { }

        public PgSqlConnectionPool(string username, string password, string database, string databaseServer, int port, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions, int readTimeout, bool Readonly)
            : this(username, password, database, databaseServer, port, 5, 10, 600, UpdateStructureDebugMode, connectionName, allowTableDeletions, readTimeout,Readonly)
        {

        }

        public PgSqlConnectionPool(string username, string password, string database, string databaseServer, int port, int minPoolSize, int maxPoolSize, long maxKeepAlive, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions,bool Readonly)
            : base("User Id=" + username + ";" +
                   "Password=" + password + ";" +
                   "Database=" + database + ";" +
                   "Server=" + databaseServer + ";" +
                   "Port=" + port.ToString() + ";", minPoolSize, maxPoolSize, maxKeepAlive, UpdateStructureDebugMode, connectionName, allowTableDeletions,Readonly)
        {
            _dbName = database;
        }

        public PgSqlConnectionPool(string username, string password, string database, string databaseServer, int port, int minPoolSize, int maxPoolSize, long maxKeepAlive, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions, int readTimeout,bool Readonly)
            : base("User Id=" + username + ";" +
                   "Password=" + password + ";" +
                   "Database=" + database + ";" +
                   "Server=" + databaseServer + ";" +
                   "Port=" + port.ToString() + ";", minPoolSize, maxPoolSize, maxKeepAlive, UpdateStructureDebugMode, connectionName, allowTableDeletions,readTimeout,Readonly)
        {
            _dbName = database;
        }
		
		protected override Connection CreateConnection(bool exclusiveLock)
		{
			return new PgSqlConnection(this,connectionString,this._readonly,exclusiveLock);
		}
	}
}
