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
			: base("Data Source="+databaseServer+", "+port.ToString()+
			       ";Initial Catalog="+database+";"+
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
		
		protected override string[] _ReservedWords {
			get {
				return Utility.MergeStringArrays(base.ReservedWords,
				                                 new string[]{
				                                 	"ADA","AGGREGATE","ALIAS","ARRAY","BREADTH",
				                                 	"BROWSE","BULK","CLASS","CLOB","CLUSTERED",
				                                 	"COMPLETION","COMPUTE","CONSTRUCTOR","CONTAINSTABLE","CURRENT_PATH",
				                                 	"CURRENT_ROLE","CYCLE","DATA","DBCC","DENY",
				                                 	"DEPTH","DEREF","DESTROY","DESTRUCTOR","DETERMINISTIC",
				                                 	"DICTIONARY","DISK","DISTRIBUTED","DUMMY","DUMP",
				                                 	"EACH","EQUALS","ERRLVL","EVERY","FILLFACTOR",
				                                 	"FORTRAN","FREE","FREETEXT","FREETEXTTABLE","GROUPING",
				                                 	"HOST","IDENTITYCOL","IDENTITY_INSERT","INCLUDE","INITIALIZE",
				                                 	"ITERATE","KILL","LARGE","LESS","LIMIT",
				                                 	"LINENO","LOAD","LOCALTIME","LOCALTIMESTAMP","LOCATOR",
				                                 	"MAP","MODIFIES","NCLOB","NOCHECK","NONCLUSTERED",
				                                 	"NONE","OFFSETS","OLD","OPENDATASOURCE","OPENQUERY",
				                                 	"OPENROWSET","OPENXML","OPERATION","ORDINALITY","PASCAL",
				                                 	"PATH","POSTFIX","PREFIX","PREORDER","READS",
				                                 	"RECONFIGURE","RECURSIVE","REF","REFERENCING","REPLICATION",
				                                 	"RESULT","ROUTINE","ROWCOUNT","ROWGUIDCOL","RULE",
				                                 	"SCOPE","SEARCH","SEQUENCE","SETS","SHUTDOWN",
				                                 	"SPECIFIC","SPECIFICTYPE","SQLCA","SQLEXCEPTION","STATE",
				                                 	"STRUCTURE","TERMINATE","TEXTSIZE","THAN","TREAT",
				                                 	"UNDER","UNNEST","UPDATETEXT","USE","WITHOUT",
				                                 });
			}
		}
		
		protected override int MaxFieldNameLength {
			get { return 128; }
		}
		
		protected override Connection CreateConnection()
		{
			return new MsSqlConnection(this,connectionString);
		}
	}
}
