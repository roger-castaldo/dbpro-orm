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
			: this(username,password,database,databaseServer,5432,minPoolSize,maxPoolSize,maxKeepAlive,UpdateStructureDebugMode,connectionName)
		{ }

		public PgSqlConnectionPool(string username, string password, string database, string databaseServer, int port, int minPoolSize, int maxPoolSize, long maxKeepAlive, bool UpdateStructureDebugMode,string connectionName)
			: base("User Id="+username+";" +
			       "Password="+password+";" +
			       "Database="+database+";" +
			       "Server="+databaseServer+";" +
			       "Port="+port.ToString()+";",minPoolSize,maxPoolSize,maxKeepAlive,UpdateStructureDebugMode,connectionName)
		{
			_dbName=database;
		}

		public PgSqlConnectionPool(string username, string password, string database, string databaseServer, bool UpdateStructureDebugMode,string connectionName)
			: this(username, password, database, databaseServer, 5432,UpdateStructureDebugMode,connectionName)
		{}

		public PgSqlConnectionPool(string username, string password, string database, string databaseServer, int port, bool UpdateStructureDebugMode,string connectionName)
			: this(username,password,database,databaseServer,port,5,10,600,UpdateStructureDebugMode,connectionName)
		{
			
		}
		
		protected override Connection CreateConnection()
		{
			return new PgSqlConnection(this,connectionString);
		}
	}
}
