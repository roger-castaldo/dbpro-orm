using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Org.Reddragonit.Dbpro.Connections.MsSql
{
	class MsSqlConnectionPool : ConnectionPool
	{
		public MsSqlConnectionPool(string username, string password, string database, string databaseServer, int minPoolSize, int maxPoolSize, long maxKeepAlive, bool UpdateStructureDebugMode,string connectionName)
			: this(username, password, database, databaseServer, 1433, minPoolSize, maxPoolSize, maxKeepAlive, UpdateStructureDebugMode,connectionName,true,false)
		{ }
		
		public MsSqlConnectionPool(string username, string password, string database, string databaseServer, int minPoolSize, int maxPoolSize, long maxKeepAlive, bool UpdateStructureDebugMode,string connectionName,bool allowTableDeletions)
			: this(username, password, database, databaseServer, 1433, minPoolSize, maxPoolSize, maxKeepAlive, UpdateStructureDebugMode,connectionName,allowTableDeletions,false)
		{ }

		public MsSqlConnectionPool(string username, string password, string database, string databaseServer, bool UpdateStructureDebugMode,string connectionName)
			: this(username, password, database, databaseServer, 1433, UpdateStructureDebugMode,connectionName)
		{}

		public MsSqlConnectionPool(string username, string password, string database, string databaseServer, int port, bool UpdateStructureDebugMode,string connectionName)
			: this(username, password, database, databaseServer, port, 5, 10, 600, UpdateStructureDebugMode,connectionName,true,false)
		{
			
		}
		
		public MsSqlConnectionPool(string username, string password, string database, string databaseServer, bool UpdateStructureDebugMode,string connectionName,bool allowTableDeletions)
			: this(username, password, database, databaseServer, 1433, UpdateStructureDebugMode,connectionName,allowTableDeletions)
		{}

		public MsSqlConnectionPool(string username, string password, string database, string databaseServer, int port, bool UpdateStructureDebugMode,string connectionName,bool allowTableDeletions)
			: this(username, password, database, databaseServer, port, 5, 10, 600, UpdateStructureDebugMode,connectionName,allowTableDeletions,false)
		{}

        public MsSqlConnectionPool(string username, string password, string database, string databaseServer, int minPoolSize, int maxPoolSize, long maxKeepAlive, bool UpdateStructureDebugMode, string connectionName,int readTimeout)
            : this(username, password, database, databaseServer, 1433, minPoolSize, maxPoolSize, maxKeepAlive, UpdateStructureDebugMode, connectionName, true, readTimeout)
        { }

        public MsSqlConnectionPool(string username, string password, string database, string databaseServer, int minPoolSize, int maxPoolSize, long maxKeepAlive, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions,int readTimeout)
            : this(username, password, database, databaseServer, 1433, minPoolSize, maxPoolSize, maxKeepAlive, UpdateStructureDebugMode, connectionName, allowTableDeletions,readTimeout)
        { }

        public MsSqlConnectionPool(string username, string password, string database, string databaseServer, int port, int minPoolSize, int maxPoolSize, long maxKeepAlive, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions,int readTimeout)
            :this (username,password,database,databaseServer,port,minPoolSize,maxPoolSize,maxKeepAlive,UpdateStructureDebugMode,connectionName,allowTableDeletions,readTimeout,false)
        { }

        public MsSqlConnectionPool(string username, string password, string database, string databaseServer, bool UpdateStructureDebugMode, string connectionName,int readTimeout)
            : this(username, password, database, databaseServer, 1433, UpdateStructureDebugMode, connectionName,readTimeout)
        { }

        public MsSqlConnectionPool(string username, string password, string database, string databaseServer, int port, bool UpdateStructureDebugMode, string connectionName,int readTimeout)
            : this(username, password, database, databaseServer, port, 5, 10, 600, UpdateStructureDebugMode, connectionName, true,readTimeout)
        {

        }

        public MsSqlConnectionPool(string username, string password, string database, string databaseServer, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions,int readTimeout)
            : this(username, password, database, databaseServer, 1433, UpdateStructureDebugMode, connectionName, allowTableDeletions, readTimeout,false)
        { }

        public MsSqlConnectionPool(string username, string password, string database, string databaseServer, int port, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions,int readTimeout,bool Readonly)
            : this(username, password, database, databaseServer, port, 5, 10, 600, UpdateStructureDebugMode, connectionName, allowTableDeletions,readTimeout,Readonly)
        { }

        public MsSqlConnectionPool(string username, string password, string database, string databaseServer, int minPoolSize, int maxPoolSize, long maxKeepAlive, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions, bool Readonly)
            : this(username, password, database, databaseServer, 1433, minPoolSize, maxPoolSize, maxKeepAlive, UpdateStructureDebugMode, connectionName, allowTableDeletions,Readonly)
        { }

        public MsSqlConnectionPool(string username, string password, string database, string databaseServer, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions, bool Readonly)
            : this(username, password, database, databaseServer, 1433, UpdateStructureDebugMode, connectionName, allowTableDeletions, Readonly)
        { }

        public MsSqlConnectionPool(string username, string password, string database, string databaseServer, int port, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions, bool Readonly)
            : this(username, password, database, databaseServer, port, 5, 10, 600, UpdateStructureDebugMode, connectionName, allowTableDeletions, Readonly)
        { }

        public MsSqlConnectionPool(string username, string password, string database, string databaseServer, int minPoolSize, int maxPoolSize, long maxKeepAlive, bool UpdateStructureDebugMode, string connectionName, int readTimeout, bool Readonly)
            : this(username, password, database, databaseServer, 1433, minPoolSize, maxPoolSize, maxKeepAlive, UpdateStructureDebugMode, connectionName, true, readTimeout, Readonly)
        { }

        public MsSqlConnectionPool(string username, string password, string database, string databaseServer, int minPoolSize, int maxPoolSize, long maxKeepAlive, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions, int readTimeout, bool Readonly)
            : this(username, password, database, databaseServer, 1433, minPoolSize, maxPoolSize, maxKeepAlive, UpdateStructureDebugMode, connectionName, allowTableDeletions, readTimeout, Readonly)
        { }

        public MsSqlConnectionPool(string username, string password, string database, string databaseServer, bool UpdateStructureDebugMode, string connectionName, int readTimeout, bool Readonly)
            : this(username, password, database, databaseServer, 1433, UpdateStructureDebugMode, connectionName, readTimeout, Readonly)
        { }

        public MsSqlConnectionPool(string username, string password, string database, string databaseServer, int port, bool UpdateStructureDebugMode, string connectionName, int readTimeout, bool Readonly)
            : this(username, password, database, databaseServer, port, 5, 10, 600, UpdateStructureDebugMode, connectionName, true, readTimeout, Readonly)
        {

        }

        public MsSqlConnectionPool(string username, string password, string database, string databaseServer, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions, int readTimeout, bool Readonly)
            : this(username, password, database, databaseServer, 1433, UpdateStructureDebugMode, connectionName, allowTableDeletions, readTimeout, Readonly)
        { }

        public MsSqlConnectionPool(string username, string password, string database, string databaseServer, int port, int minPoolSize, int maxPoolSize, long maxKeepAlive, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions, bool Readonly)
            : base("Data Source=" + databaseServer + ", " + port.ToString() +
                   ";Initial Catalog=" + database + ";" +
                   "User ID=" + username + ";" +
                   "Password=" + password + ";", minPoolSize, maxPoolSize, maxKeepAlive, UpdateStructureDebugMode, connectionName, allowTableDeletions, Readonly)
        { }

        public MsSqlConnectionPool(string username, string password, string database, string databaseServer, int port, int minPoolSize, int maxPoolSize, long maxKeepAlive, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions, int readTimeout, bool Readonly)
            : base("Data Source=" + databaseServer + ", " + port.ToString() +
                   ";Initial Catalog=" + database + ";" +
                   "User ID=" + username + ";" +
                   "Password=" + password + ";", minPoolSize, maxPoolSize, maxKeepAlive, UpdateStructureDebugMode, connectionName, allowTableDeletions, readTimeout, Readonly)
        { }

		private string[] _words = null;
		protected override string[] _ReservedWords {
			get {
				if (_words==null)
				{
					_words=Utility.MergeStringArrays(base._ReservedWords,
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
					                                 	"MAP","MODIFIES","NAME","NCLOB","NOCHECK","NONCLUSTERED",
					                                 	"NONE","OFFSETS","OLD","OPENDATASOURCE","OPENQUERY",
					                                 	"OPENROWSET","OPENXML","OPERATION","ORDINALITY","PASCAL",
					                                 	"PATH","POSTFIX","PREFIX","PREORDER","READS",
					                                 	"RECONFIGURE","RECURSIVE","REF","REFERENCING","REPLICATION",
					                                 	"RESULT","ROUTINE","ROWCOUNT","ROWGUIDCOL","RULE",
					                                 	"SCOPE","SEARCH","SEQUENCE","SETS","SHUTDOWN",
					                                 	"SPECIFIC","SPECIFICTYPE","SQLCA","SQLEXCEPTION","STATE",
					                                 	"STRUCTURE","TERMINATE","TEXTSIZE","THAN","TREAT",
					                                 	"UNDER","UNNEST","UPDATETEXT","USE","WITHOUT"
					                                 });
				}
				return _words;
			}
		}
		
		protected override int MaxFieldNameLength {
			get { return 128; }
		}
		
		protected override Connection CreateConnection(bool exclusiveLock)
		{
			return new MsSqlConnection(this,connectionString,_readonly,exclusiveLock);
		}
		
		internal override bool AllowChangingBasicAutogenField {
			get { return false; }
		}
		
		protected override void PreInit()
		{
            if (!_readonly)
            {
                Connection c = CreateConnection(false);
                bool exists = false;
                bool create = false;
                string query = new StreamReader(this.GetType().Assembly.GetManifestResourceStream("Org.Reddragonit.Dbpro.Connections.MsSql.IdentitySP.sql")).ReadToEnd();
                string version = query.Substring(query.IndexOf("-- Version: ") + 12, query.IndexOf("\n", query.IndexOf("-- Version: ") + 12) - query.IndexOf("-- Version: ") - 12);
                c.ExecuteQuery("SELECT name FROM sys.procedures where name='Org_Reddragonit_DbPro_Create_Remove_Identity'");
                if (c.Read())
                {
                    if (c[0].ToString() != null)
                        exists = true;
                }
                c.Close();
                if (exists)
                {
                    c.ExecuteQuery("EXEC Org_Reddragonit_DbPro_Create_Remove_Identity null,null,null,1");
                    c.Read();
                    if (double.Parse(c[0].ToString()) != double.Parse(version))
                        create = true;
                    c.Close();
                    if (create)
                        c.ExecuteNonQuery("DROP PROCEDURE Org_Reddragonit_DbPro_Create_Remove_Identity");
                }
                else
                    create = true;
                if (create)
                    c.ExecuteNonQuery(query);
                c.Commit();

                create = false;
                c.ExecuteQuery("SELECT name FROM sys.objects WHERE type IN ('FN', 'IF', 'TF') AND name='Org_Reddragonit_Dbpro_Connections_MsSql_ConvertBigintToCharstring'");
                if (c.Read())
                {
                    if (c[0].ToString() != null)
                        create = true;
                }
                c.Close();
                if (create)
                {
                    query = new StreamReader(this.GetType().Assembly.GetManifestResourceStream("Org.Reddragonit.Dbpro.Connections.MsSql.CONVERT_BIGINT_CHAR.sql")).ReadToEnd();
                    c.ExecuteNonQuery(query);
                    c.Commit();
                }

                create = false;
                c.ExecuteQuery("SELECT name FROM sys.objects WHERE type IN ('FN', 'IF', 'TF') AND name='Org_Reddragonit_Dbpro_Connections_MsSql_GeneateUniqueID'");
                if (c.Read())
                {
                    if (c[0].ToString() != null)
                        create = true;
                }
                c.Close();
                if (create)
                {
                    query = new StreamReader(this.GetType().Assembly.GetManifestResourceStream("Org.Reddragonit.Dbpro.Connections.MsSql.GENERATE_UNIQUE_ID.sql")).ReadToEnd();
                    c.ExecuteNonQuery(query);
                    c.Commit();
                }

                c.CloseConnection();
            }
		}
	}
}
