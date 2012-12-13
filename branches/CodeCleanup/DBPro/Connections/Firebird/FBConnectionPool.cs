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
using System.IO;
using System.Reflection;

namespace Org.Reddragonit.Dbpro.Connections.Firebird
{
	/// <summary>
	/// Description of FBConnectionPool.
	/// </summary>
	public class FBConnectionPool : ConnectionPool
	{

        private const int DEFAULT_TIMEOUT = 300;

		public FBConnectionPool(string username, string password, string databasePath, string databaseServer, int minPoolSize, int maxPoolSize, long maxKeepAlive,bool UpdateStructureDebugMode,string connectionName) 
			: this(username,password,databasePath,databaseServer,3050,minPoolSize,maxPoolSize,maxKeepAlive,UpdateStructureDebugMode,connectionName,true)
		{ }
		
		public FBConnectionPool(string username, string password, string databasePath, string databaseServer, int minPoolSize, int maxPoolSize, long maxKeepAlive,bool UpdateStructureDebugMode,string connectionName,bool allowTableDeletions) 
			: this(username,password,databasePath,databaseServer,3050,minPoolSize,maxPoolSize,maxKeepAlive,UpdateStructureDebugMode,connectionName,allowTableDeletions)
		{ }

        public FBConnectionPool(string username, string password, string databasePath, string databaseServer, int port, int minPoolSize, int maxPoolSize, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions)
            : this(username,password,databasePath,databaseServer,port,minPoolSize,maxPoolSize,DEFAULT_TIMEOUT,UpdateStructureDebugMode,connectionName,allowTableDeletions)
        { }

		public FBConnectionPool(string username, string password, string databasePath, string databaseServer, int port, int minPoolSize, int maxPoolSize, long maxKeepAlive, bool UpdateStructureDebugMode,string connectionName,bool allowTableDeletions)
			: this(username,password,databasePath,databaseServer,port,null,minPoolSize,maxPoolSize,maxKeepAlive,UpdateStructureDebugMode,connectionName,allowTableDeletions,ConnectionPool.DEFAULT_READ_TIMEOUT,false)
		{ }

		public FBConnectionPool(string username, string password, string databasePath, string databaseServer, bool UpdateStructureDebugMode,string connectionName)
			: this(username, password, databasePath, databaseServer, 3050,UpdateStructureDebugMode,connectionName)
		{}

        public FBConnectionPool(string username, string password, string databasePath, string databaseServer, int port, bool UpdateStructureDebugMode, string connectionName)
			: this(username,password,databasePath,databaseServer,port,5,10,DEFAULT_TIMEOUT,UpdateStructureDebugMode,connectionName,true)
		{
			
		}

        public FBConnectionPool(string username, string password, string databasePath, string databaseServer, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions)
			: this(username, password, databasePath, databaseServer, 3050,UpdateStructureDebugMode,connectionName,allowTableDeletions)
		{}

        public FBConnectionPool(string username, string password, string databasePath, string databaseServer, int port, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions)
			: this(username,password,databasePath,databaseServer,port,5,10,DEFAULT_TIMEOUT,UpdateStructureDebugMode,connectionName,allowTableDeletions)
		{}

        public FBConnectionPool(string username, string password, string databasePath, string databaseServer, int minPoolSize, int maxPoolSize, long maxKeepAlive, bool UpdateStructureDebugMode, string connectionName, int readTimeout)
            : this(username, password, databasePath, databaseServer, 3050,null, minPoolSize, maxPoolSize, maxKeepAlive, UpdateStructureDebugMode, connectionName, true,readTimeout,false)
        { }

        public FBConnectionPool(string username, string password, string databasePath, string databaseServer, int minPoolSize, int maxPoolSize, long maxKeepAlive, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions, int readTimeout)
            : this(username, password, databasePath, databaseServer, 3050,null, minPoolSize, maxPoolSize, maxKeepAlive, UpdateStructureDebugMode, connectionName, allowTableDeletions,readTimeout,false)
        { }

        public FBConnectionPool(string username, string password, string databasePath, string databaseServer, int port, int minPoolSize, int maxPoolSize, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions, int readTimeout)
            : this(username, password, databasePath, databaseServer, port,null, minPoolSize, maxPoolSize, DEFAULT_TIMEOUT, UpdateStructureDebugMode, connectionName, allowTableDeletions,readTimeout,false)
        { }

        public FBConnectionPool(string username, string password, string databasePath, string databaseServer, bool UpdateStructureDebugMode, string connectionName,int readTimeout)
            : this(username, password, databasePath, databaseServer, 3050, UpdateStructureDebugMode, connectionName,readTimeout)
        { }

        public FBConnectionPool(string username, string password, string databasePath, string databaseServer, int port, bool UpdateStructureDebugMode, string connectionName,int readTimeout)
            : this(username, password, databasePath, databaseServer, port,null, 5, 10, DEFAULT_TIMEOUT, UpdateStructureDebugMode, connectionName, true,readTimeout,false)
        {

        }

        public FBConnectionPool(string username, string password, string databasePath, string databaseServer, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions,int readTimeout)
            : this(username, password, databasePath, databaseServer, 3050, UpdateStructureDebugMode, connectionName, allowTableDeletions,readTimeout,false)
        { }

        public FBConnectionPool(string username, string password, string databasePath, string databaseServer, int port, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions,int readTimeout,bool Readonly)
            : this(username, password, databasePath, databaseServer, port,null, 5, 10, DEFAULT_TIMEOUT, UpdateStructureDebugMode, connectionName, allowTableDeletions,readTimeout,Readonly)
        { }

        public FBConnectionPool(string username, string password, string databasePath, string databaseServer, int minPoolSize, int maxPoolSize, long maxKeepAlive, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions, bool Readonly)
            : this(username, password, databasePath, databaseServer, 3050, minPoolSize, maxPoolSize, maxKeepAlive, UpdateStructureDebugMode, connectionName, allowTableDeletions,Readonly)
        { }

        public FBConnectionPool(string username, string password, string databasePath, string databaseServer, int port, int minPoolSize, int maxPoolSize, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions, bool Readonly)
            : this(username, password, databasePath, databaseServer, port, minPoolSize, maxPoolSize, DEFAULT_TIMEOUT, UpdateStructureDebugMode, connectionName, allowTableDeletions,Readonly)
        { }

        public FBConnectionPool(string username, string password, string databasePath, string databaseServer, int port, int minPoolSize, int maxPoolSize, long maxKeepAlive, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions, bool Readonly)
            : this(username, password, databasePath, databaseServer, port,null, minPoolSize, maxPoolSize, maxKeepAlive, UpdateStructureDebugMode, connectionName, allowTableDeletions, ConnectionPool.DEFAULT_READ_TIMEOUT, Readonly)
        { }

        public FBConnectionPool(string username, string password, string databasePath, string databaseServer, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions, bool Readonly)
            : this(username, password, databasePath, databaseServer, 3050, UpdateStructureDebugMode, connectionName, allowTableDeletions,Readonly)
        { }

        public FBConnectionPool(string username, string password, string databasePath, string databaseServer, int port, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions, bool Readonly)
            : this(username, password, databasePath, databaseServer, port, 5, 10, DEFAULT_TIMEOUT, UpdateStructureDebugMode, connectionName, allowTableDeletions,Readonly)
        { }

        public FBConnectionPool(string username, string password, string databasePath, string databaseServer, int minPoolSize, int maxPoolSize, long maxKeepAlive, bool UpdateStructureDebugMode, string connectionName, int readTimeout, bool Readonly)
            : this(username, password, databasePath, databaseServer, 3050,null, minPoolSize, maxPoolSize, maxKeepAlive, UpdateStructureDebugMode, connectionName, true, readTimeout, Readonly)
        { }

        public FBConnectionPool(string username, string password, string databasePath, string databaseServer, int minPoolSize, int maxPoolSize, long maxKeepAlive, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions, int readTimeout, bool Readonly)
            : this(username, password, databasePath, databaseServer, 3050,null, minPoolSize, maxPoolSize, maxKeepAlive, UpdateStructureDebugMode, connectionName, allowTableDeletions, readTimeout, Readonly)
        { }

        public FBConnectionPool(string username, string password, string databasePath, string databaseServer, int port, int minPoolSize, int maxPoolSize, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions, int readTimeout, bool Readonly)
            : this(username, password, databasePath, databaseServer, port,null, minPoolSize, maxPoolSize, DEFAULT_TIMEOUT, UpdateStructureDebugMode, connectionName, allowTableDeletions, readTimeout, Readonly)
        { }

        public FBConnectionPool(string username, string password, string databasePath, string databaseServer, bool UpdateStructureDebugMode, string connectionName, int readTimeout, bool Readonly)
            : this(username, password, databasePath, databaseServer, 3050, UpdateStructureDebugMode, connectionName, readTimeout,Readonly)
        { }

        public FBConnectionPool(string username, string password, string databasePath, string databaseServer, int port, bool UpdateStructureDebugMode, string connectionName, int readTimeout, bool Readonly)
            : this(username, password, databasePath, databaseServer, port,null, 5, 10, DEFAULT_TIMEOUT, UpdateStructureDebugMode, connectionName, true, readTimeout, Readonly)
        {

        }

        public FBConnectionPool(string username, string password, string databasePath, string databaseServer, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions, int readTimeout, bool Readonly)
            : this(username, password, databasePath, databaseServer, 3050, UpdateStructureDebugMode, connectionName, allowTableDeletions, readTimeout,Readonly)
        { }

        public FBConnectionPool(string username, string password, string databasePath, string databaseServer, int port,string charset, int minPoolSize, int maxPoolSize, long maxKeepAlive, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions, int readTimeout,bool Readonly)
            : base("User=" + username + ";" +
                   "Password=" + password + ";" +
                   "Database=" + databasePath + ";" +
                   "DataSource=" + databaseServer + ";" +
                   "Port=" + port.ToString() + ";"+
                    (charset!=null ? "Charset="+charset : ""), minPoolSize, maxPoolSize, maxKeepAlive, UpdateStructureDebugMode, connectionName, allowTableDeletions, readTimeout,Readonly)
        { }
		
		private string[] _words=null;
		protected override string[] _ReservedWords {
			get {
				if (_words==null)
				{
					_words=Utility.MergeStringArrays(base._ReservedWords,new string[]{
				                                 	"ACTIVE","ASCENDING","AUTO","AUTODDL","BASED",
				                                 	"BASENAME","BASE_NAME","BLOBEDIT","BUFFER","CACHE",
				                                 	"CHECK_POINT_LEN","CHECK_POINT_LENGTH","COMMITTED","COMPILETIME","COMPUTED",
				                                 	"CONDITIONAL","CONTAINING","CSTRING","DB_KEY","DEBUG",
				                                 	"DESCENDING","DISPLAY","EDIT","ENTRY_POINT","EVENT",
				                                 	"EXTERN","FILTER","FREE_IT","GDSCODE","GENERATOR",
				                                 	"GEN_ID","GROUP_COMMIT_WAIT","GROUP_COMMIT_WAIT_TIME","HELP","INACTIVE",
				                                 	"INIT","INPUT_TYPE","ISQL","LC_MESSAGES","LC_TYPE",
				                                 	"LENGTH","LEV","LOGFILE","LOG_BUFFER_SIZE","LOG_BUF_SIZE",
				                                 	"MANUAL","MAXIMUM","MAXIMUM_SEGMENT","MAX_SEGMENT","MERGE",
				                                 	"MINIMUM","MODULE_NAME","NOAUTO","NUM_LOG_BUFS","NUM_LOG_BUFFERS",
				                                 	"OUTPUT_TYPE","OVERFLOW","PAGE","PAGELENGTH","PAGES",
				                                 	"PAGE_SIZE","POST_EVENT","RAW_PARTITIONS","RDB$DB_KEY","RECORD_VERSION",
				                                 	"RESERV","RESERVING","RETAIN","RETURNING_VALUES","RUNTIME",
				                                 	"SHADOW","SHARED","SHELL","SHOW","SINGULAR",
				                                 	"SNAPSHOT","SORT","STABILITY","STARTING","STARTS",
				                                 	"SUB_TYPE","SUSPEND","TERMINATOR","UNCOMMITTED","VERSION",
				                                 	"WEEKDAY","YEARDAY"
				                                 });
				}
				return _words;
			}
		}
		
		public override int MaxFieldNameLength {
			get { return 31; }
		}
		
		protected override Connection CreateConnection(bool exclusiveLock)
		{
			return new FBConnection(this,connectionString,_readonly,exclusiveLock);
		}

        protected override void PreInit()
        {
        }

        protected override void _InitClass()
        {
            if (Utility.LocateType(FBConnection._PARAMETER_CLASS_NAME) == null)
                Assembly.Load(FBConnection._ASSEMBLY_NAME);
        }
	}
}