/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 23/03/2008
 * Time: 9:30 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */

using System;
using Org.Reddragonit.Dbpro.Connections;

namespace Org.Reddragonit.Dbpro.Connections.Firebird
{
	/// <summary>
	/// Description of FBConnectionPool.
	/// </summary>
	public class FBConnectionPool : ConnectionPool
	{
        public FBConnectionPool(string username, string password, string databasePath, string databaseServer, int minPoolSize, int maxPoolSize, long maxKeepAlive,bool UpdateStructureDebugMode) : this(username,password,databasePath,databaseServer,3050,minPoolSize,maxPoolSize,maxKeepAlive,UpdateStructureDebugMode)
        { }

        public FBConnectionPool(string username, string password, string databasePath, string databaseServer, int port, int minPoolSize, int maxPoolSize, long maxKeepAlive, bool UpdateStructureDebugMode) 
			: base("User="+username+";" +
				"Password="+password+";" +
				"Database="+databasePath+";" +
				"DataSource="+databaseServer+";" +
				"Port="+port.ToString()+";",minPoolSize,maxPoolSize,maxKeepAlive,UpdateStructureDebugMode)
		{}

        public FBConnectionPool(string username, string password, string databasePath, string databaseServer, bool UpdateStructureDebugMode)
            : this(username, password, databasePath, databaseServer, 3050,UpdateStructureDebugMode)
        {}

        public FBConnectionPool(string username, string password, string databasePath, string databaseServer, int port, bool UpdateStructureDebugMode)
			: this(username,password,databasePath,databaseServer,port,5,10,600,UpdateStructureDebugMode)
		{
			
		}
		
		protected override Connection CreateConnection()
		{
			return new FBConnection(this,connectionString);
		}
		
	}
}
