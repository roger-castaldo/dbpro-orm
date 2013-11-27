/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 23/03/2008
 * Time: 8:58 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */

using System;
using System.Text.RegularExpressions;
using System.Collections.Generic;
using System.Threading;
using ExtractedTableMap = Org.Reddragonit.Dbpro.Connections.ExtractedTableMap;
using ExtractedFieldMap = Org.Reddragonit.Dbpro.Connections.ExtractedFieldMap;
using ForeignRelationMap = Org.Reddragonit.Dbpro.Connections.ForeignRelationMap;
using FieldType = Org.Reddragonit.Dbpro.Structure.Attributes.FieldType;
using VersionTypes = Org.Reddragonit.Dbpro.Structure.Attributes.VersionField.VersionTypes;
using UpdateDeleteAction =  Org.Reddragonit.Dbpro.Structure.Attributes.ForeignField.UpdateDeleteAction;
using Org.Reddragonit.Dbpro.Connections.PoolComponents;
using Org.Reddragonit.Dbpro.Structure.Attributes;
using System.Reflection;
using System.Xml;
using Org.Reddragonit.Dbpro.Virtual;
using System.Data;

namespace Org.Reddragonit.Dbpro.Connections
{
	/// <summary>
	/// Description of ConnectionPool.
	/// </summary>
	public abstract class ConnectionPool
	{

        private const int MaxGetConnectionTrials = 20;
        private const int MaxMutexTimeout = 1000;
        internal const int DEFAULT_READ_TIMEOUT = 60;
		
		private List<Connection> locked=new List<Connection>();
		private Queue<Connection> unlocked=new Queue<Connection>();

        protected abstract string connectionString{get;}
		
		private int minPoolSize=0;
		private int maxPoolSize=0;
		private long maxKeepAlive=0;
        internal int readTimeout=300;
		private bool _debugMode=false;
        public bool DebugMode
        {
            get { return _debugMode; }
        }
		private bool _allowTableDeletions=true;
        protected bool _readonly = false;
        private bool _classless=false;
        public bool Classless
        {
            get { return _classless; }
        }
		
		private bool isClosed=false;
		private bool isReady=false;
		private string _connectionName;

        internal abstract QueryBuilder queryBuilder { get; }
		protected abstract Connection CreateConnection(bool exclusiveLock);
        protected abstract void _InitClass();
        protected abstract bool _IsCoreStoredProcedure(StoredProcedure storedProcedure);
        internal abstract IDbDataParameter CreateParameter(string parameterName, object parameterValue, Org.Reddragonit.Dbpro.Structure.Attributes.FieldType type, int fieldLength);
        protected abstract IDbDataParameter _CreateParameter(string parameterName, object parameterValue);

        private Dictionary<Type, ClassViewAttribute> _classViewAttributes;
        internal ClassViewAttribute this[Type type]
        {
            get
            {
                if (_classViewAttributes == null)
                    _classViewAttributes = new Dictionary<Type, ClassViewAttribute>();
                if (!_classViewAttributes.ContainsKey(type))
                {
                    if (new List<Type>(type.GetInterfaces()).Contains(typeof(IClassView))
                        && type.GetCustomAttributes(typeof(ClassViewAttribute), false).Length > 0)
                        _classViewAttributes.Add(type, (ClassViewAttribute)type.GetCustomAttributes(typeof(ClassViewAttribute), false)[0]);
                }
                if (_classViewAttributes.ContainsKey(type))
                    return _classViewAttributes[type];
                return null;
            }
        }
        
        internal IDbDataParameter CreateParameter(string parameterName, object parameterValue)
        {
            if (parameterValue != null)
            {
                if (Utility.IsEnum(parameterValue.GetType()))
                    parameterValue = Utility.ConvertEnumParameter(parameterValue, this);
            }
            return _CreateParameter(parameterName, parameterValue);
        }

        internal virtual string TrueString
        {
            get{return "true";}
        }

        internal virtual string FalseString
        {
            get { return "false"; }
        }

        private Connection CreateConnection()
        {
            return CreateConnection(false);
        }

		protected virtual void PreInit()
		{
		}
		
		protected virtual string[] _ReservedWords{
			get{
				return new string[]{
					"ABSOLUTE","ACTION","ADD","ADMIN","AFTER",
					"ALL","ALLOCATE","ALTER","AND","ANY",
					"ARE","AS","ASC","ASSERTION","AT",
					"AUTHORIZATION","AVG","BEFORE","BEGIN","BETWEEN",
					"BIT","BIT_LENGTH","BLOB","BOTH","BY",
					"CASCADE","CASCADED","CASE","CAST","CATALOG",
					"CHAR","CHARACTER","CHAR_LENGTH","CHARACTER_LENGTH","CHECK",
					"CLOSE","COALESCE","COLLATE","COLLATION","COLUMN",
					"COMMIT","CONNECT","CONNECTION","CONSTRAINT","CONSTRAINTS",
					"CONTINUE","CONVERT","CORRESPONDING","COUNT","CREATE",
					"CROSS","CURRENT","CURRENT_DATE","CURRENT_TIME","CURRENT_TIMESTAMP",
					"CURRENT_USER","DATABASE","DATE","DAY","DEALLOCATE",
					"DEC","DECIMAL","DECLARE","DEFAULT","DEFERRABLE",
					"DEFERRED","DELETE","DESC","DESCRIBE","DESCRIPTOR",
					"DIAGNOSTICS","DISCONNECT","DISTINCT","DO","DOMAIN",
					"DOUBLE","DROP","ECHO","ELSE","END",
					"END-EXEC","ESCAPE","EXCEPT","EXCEPTION","EXEC",
					"EXECUTE","EXISTS","EXIT","EXTERNAL","EXTRACT",
					"FALSE","FETCH","FILE","FLOAT","FOR",
					"FOREIGN","FOUND","FROM","FULL","FUNCTION",
					"GET","GLOBAL","GO","GOTO","GRANT",
					"GROUP","HAVING","HOUR","IDENTITY","IF",
					"IMMEDIATE","IN","INDEX","INDICATOR","INITIALLY",
					"INNER","INPUT","INSENSITIVE","INSERT","INT",
					"INTEGER","INTERSECT","INTERVAL","INTO","IS",
					"ISOLATION","JOIN","KEY","LANGUAGE","LAST",
					"LEADING","LEFT","LEVEL","LIKE","LOCAL",
					"LONG","LOWER","MATCH","MAX","MESSAGE",
					"MIN","MINUTE","MODULE","MONTH","NAMES",
					"NATIONAL","NATURAL","NCHAR","NEXT","NO",
					"NOT","NULL","NULLIF","NUMERIC","OCTET_LENGTH",
					"OF","ON","ONLY","OPEN","OPTION",
					"OR","ORDER","OUTER","OUTPUT","OVERLAPS",
					"PAD","PARAMETER","PARTIAL","PASSWORD","PLAN",
					"POSITION","PRECISION","PREPARE","PRESERVE","PRIMARY",
					"PRIOR","PRIVILEGES","PROCEDURE","PUBLIC","QUIT",
					"READ","REAL","REFERENCES","RELATIVE","RELEASE",
					"RESTRICT","RETURN","RETURNS","REVOKE","RIGHT",
					"ROLE","ROLLBACK","ROWS","SCHEMA","SCROLL",
					"SECOND","SECTION","SELECT","SESSION","SESSION_USER",
					"SET","SIZE","SMALLINT","SOME","SPACE",
					"SQL","SQLCODE","SQLERROR","SQLSTATE","SQLWARNING",
					"STATEMENT","STATIC","STATISTICS","SUBSTRING","SUM",
					"SYSTEM_USER","TABLE","TEMPORARY","THEN","TIME",
					"TIMESTAMP","TIMEZONE_HOUR","TIMEZONE_MINUTE","TO","TRAILING",
					"TRANSACTION","TRANSLATE","TRANSLATION","TRIGGER","TRIM",
					"TRUE","TYPE","UNION","UNIQUE","UNKNOWN",
					"UPDATE","UPPER","USAGE","USER","USING",
					"VALUE","VALUES","VARCHAR","VARIABLE","VARYING",
					"VIEW","WAIT","WHEN","WHENEVER","WHERE",
					"WHILE","WITH","WORK","WRITE","YEAR",
					"ZONE","BINARY","BOOLEAN","GENERAL","IGNORE",
					"NUMBER","OBJECT","OFF","PARAMETERS","PERCENT",
					"TOP","VARBINARY","COMMENT","IDENTIFIED","LOCK",
					"MODE","MODIFY","RENAME","RESOURCE","ROW",
					"SHARE","START","VALIDATE","BACKUP","BREAK",
					"CALL","CHECKPOINT","CONTAINS","CUBE","CURSOR",
					"DYNAMIC","FIRST","HOLDLOCK","INOUT","LATERAL",
					"NEW","OUT","OVER","PRINT","PROC",
					"RAISERROR","READTEXT","RESTORE","ROLLUP","SAVE",
					"SAVEPOINT","SETUSER","TRAN","TRUNCATE","TSEQUAL",
					"WAITFOR","WRITETEXT"
				};
			}
		}
		
		public virtual int MaxFieldNameLength{
			get{
				return int.MaxValue;
			}
		}
		
		internal virtual bool AllowChangingBasicAutogenField{
			get{return true;}
		}
		
		private string[] _reservedWords=null;
        private EnumsHandler _enums;
        internal EnumsHandler Enums
        {
            get { return _enums; }
        }
		private NameTranslator _translator;
        internal NameTranslator Translator
        {
            get { return _translator; }
        }
        private ClassMapping _mapping;
        internal ClassMapping Mapping
        {
            get { return _mapping; }
        }
        private StructureUpdater _updater;
        internal StructureUpdater Updater
        {
            get { return _updater; }
        }
		
		internal string[] ReservedWords{
			get{
				if (_reservedWords==null)
					_reservedWords=_ReservedWords;
				return _reservedWords;
			}
		}
		
		internal string ConnectionName
		{
			get{
				if (_connectionName==null)
				{
					_connectionName=ConnectionPoolManager.DEFAULT_CONNECTION_NAME;
				}
				return _connectionName;
			}
		}
		
		public override bool Equals(object obj)
		{
			if ((obj==null)||(((ConnectionPool)obj).connectionString==null))
			{
				return false;
			}
			return connectionString==((ConnectionPool)obj).connectionString;
		}
		
		internal object GetEnumValue(Type enumType,int ID)
		{
            return _enums.GetEnumValue(enumType, ID);
		}
		
		internal int GetEnumID(Type enumType,string enumName)
		{
            return _enums.GetEnumID(enumType, enumName);
		}

        protected ConnectionPool(XmlElement elem)
		{
			Logger.LogLine("Establishing Connection with string: "+connectionString);
            //set up default values
            minPoolSize = 5;
            maxPoolSize = 10;
            maxKeepAlive = 600;
            _debugMode = false;
            _connectionName = null;
            _allowTableDeletions = true;
            _readonly = false;
            _classless = false;
            readTimeout = 300;
            foreach (XmlNode node in elem.ChildNodes)
            {
                if (node.Name == "ConnectionParameter")
                {
                    switch(node.Attributes["parameter_name"].Value){
                        case "minPoolSize":
                            if (node.Attributes["parameter_value"].Value != "null")
                                minPoolSize = int.Parse(node.Attributes["parameter_value"].Value);
                            break;
                        case "maxPoolSize":
                            if (node.Attributes["parameter_value"].Value != "null")
                                maxPoolSize = int.Parse(node.Attributes["parameter_value"].Value);
                            break;
                        case "maxKeepAlive":
                            if (node.Attributes["parameter_value"].Value != "null")
                                maxKeepAlive = int.Parse(node.Attributes["parameter_value"].Value);
                            break;
                        case "UpdateStructureDebugMode":
                            if (node.Attributes["parameter_value"].Value != "null")
                                _debugMode = bool.Parse(node.Attributes["parameter_value"].Value);
                            break;
                        case "connectionName":
                            if (node.Attributes["parameter_value"].Value != "null")
                                _connectionName = node.Attributes["parameter_value"].Value;
                            break;
                        case "allowTableDeletions":
                            if (node.Attributes["parameter_value"].Value != "null")
                                _allowTableDeletions = bool.Parse(node.Attributes["parameter_value"].Value);
                            break;
                        case "Readonly":
                            if (node.Attributes["parameter_value"].Value != "null")
                                _readonly = bool.Parse(node.Attributes["parameter_value"].Value);
                            break;
                        case "Classless":
                            if (node.Attributes["parameter_value"].Value != "null")
                                _classless = bool.Parse(node.Attributes["parameter_value"].Value);
                            break;
                        case "readTimeout":
                            if (node.Attributes["parameter_value"].Value != "null")
                                readTimeout= int.Parse(node.Attributes["parameter_value"].Value);
                            break;
                    }
                }
            }
			ConnectionPoolManager.AddConnection(_connectionName,this);
		}
		
		internal void Init(Dictionary<Type,List<EnumTranslationPair>> translations)
		{
            _InitClass();
            Connection conn = CreateConnection();
            if (!_classless)
            {
                _translator = new NameTranslator(this, conn);
                _enums = new EnumsHandler(this);
                _updater = new StructureUpdater(this, translations);
                List<Type> tables = new List<Type>();

                if (!_debugMode)
                {
                    foreach (Type t in Utility.LocateAllTypesWithAttribute(typeof(Table)))
                    {
                        Table tbl = (Table)t.GetCustomAttributes(typeof(Table), false)[0];
                        if (Utility.StringsEqual(tbl.ConnectionName, ConnectionName))
                            tables.Add(t);
                    }
                    _mapping = new ClassMapping(conn, tables);
                    PreInit();
                    _updater.Init(conn);
                }
            }
            conn.CloseConnection();
            for (int x=0;x<minPoolSize;x++){
            	if (unlocked.Count>=minPoolSize)
            		break;
				unlocked.Enqueue(CreateConnection());
            }
			isReady=true;
		}
		
		public Connection GetConnection()
		{
            Utility.WaitOne(this);
			if (!isReady)
				Init(ConnectionPoolManager._translations);
            Utility.Release(this);
			if (isClosed)
				return null;
			Connection ret=null;
            int count = 0;
			while(true)
			{
                try
                {
                    Utility.WaitOne(this, MaxMutexTimeout);
                    Logger.LogLine("Obtaining Connection: " + this.ConnectionName + " from pool with " + unlocked.Count.ToString() + " unlocked and " + locked.Count.ToString() + " locked connections");
                    while (unlocked.Count > 0)
                    {
                        Logger.LogLine("Obtaining connection from unlocked queue.");
                        ret = unlocked.Dequeue();
                        if (ret.isPastKeepAlive(maxKeepAlive))
                        {
                            Logger.LogLine("Closing obtained connection that is past keep alive to clean up unlocked queue.");
                            ret.Disconnect();
                            ret = null;
                        }
                        else
                            break;
                    }
                    if (ret != null)
                        break;
                    if (!checkMin()&&!isClosed)
                    {
                        ret = CreateConnection();
                        break;
                    }
                    if (isClosed)
                        break;
                    if (checkMax())
                    {
                        ret = CreateConnection();
                        break;
                    }
                    Utility.Release(this);
                }
                catch (Exception e)
                {
                }
				try{
					Thread.Sleep(100);
				}catch (Exception e){}
                count++;
                if (count > MaxGetConnectionTrials)
                    throw new Exception("Unable to obtain a connection after " + MaxGetConnectionTrials.ToString() + " tries.  Assuming deadlock.");
			}
			if (ret!=null)
				locked.Add(ret);
            Utility.Release(this);
			if (ret!=null)
				ret.Reset();
			return ret;
		}
		
		public void ClosePool()
		{
            Utility.WaitOne(this);
			while (unlocked.Count>0)
				unlocked.Dequeue().Disconnect();
			foreach (Connection conn in locked)
				conn.Disconnect();
			isClosed=true;
            Utility.WaitOne(this);
		}

        internal void CleanConnection(string id)
        {
            Utility.WaitOne(this);
            if (locked.Count > 0)
            {
                for (int x = 0; x < locked.Count; x++)
                {
                    if (locked[x].ID == id)
                    {
                        locked.RemoveAt(x);
                        break;
                    }
                }
            }
            Utility.Release(this);
        }
		
		internal void returnConnection(Connection conn)
		{
            Utility.WaitOne(this);
			locked.Remove(conn);
            Utility.Release(this);
            Logger.LogLine("Checking max queue size against " + locked.Count.ToString() + "+" + unlocked.Count.ToString() + " < " + maxPoolSize.ToString());
			if (checkMax(1)&&!isClosed&&!conn.isPastKeepAlive(maxKeepAlive))
			{
                Logger.LogLine("Returning connection "+conn.ID+" to queue");
                Utility.WaitOne(this);
				unlocked.Enqueue(conn);
                Utility.Release(this);
			}else
			{
				if (isClosed)
					Logger.LogLine("Closing returned connection since pool is closed.");
				else if (conn.isPastKeepAlive(maxKeepAlive))
				    Logger.LogLine("Closing returned connection since it is passed keep alive");
				else
 	               	Logger.LogLine("Closing returned connection since it exceeds the maximum queue");
				conn.Disconnect();
			}
            while (!checkMin())
            {
                Utility.WaitOne(this);
                unlocked.Enqueue(CreateConnection());
                Utility.Release(this);
            }
		}

        private bool checkMax(int addition)
        {
            if (maxPoolSize <= 0) return true;
            else return maxPoolSize > (locked.Count + unlocked.Count+addition);
        }
		
		private bool checkMax()
		{
			if (maxPoolSize<=0) return true;
			else return maxPoolSize>(locked.Count+unlocked.Count);
		}
		
		private bool checkMin()
		{
			if (minPoolSize<=0) return true;
			else return minPoolSize<(locked.Count+unlocked.Count);
		}

        internal Connection LockDownForBackupRestore()
        {
            Logger.LogLine("Attempting to Lock down connection pool: " + ConnectionName + " for BackupRestore...");
            Utility.WaitOne(this);
            Logger.LogLine("Closing down all connections in pool: " + ConnectionName + " for BackupRestore...");
            while (unlocked.Count > 0)
            {
                Logger.LogLine("Disconnecting and closing connection " + unlocked.Peek().ID + " from unlocked queue in pool " + ConnectionName);
                unlocked.Dequeue().Disconnect();
            }
            foreach (Connection conn in locked)
            {
                Logger.LogLine("Locking connection " + conn.ID + " for backup in pool " + ConnectionName);
                conn.LockForBackup();
            }
            isClosed = true;
            isReady = false;
            Logger.LogLine("Returning new connection from pool: " + ConnectionName + " for BackupRestore process");
            return CreateConnection(true);
        }
        
        internal void ReinstateConnection(Connection conn){
            Logger.LogLine("Attempting to reinstate connection "+conn.ID+" for pool: " + ConnectionName);
            Utility.WaitOne(this);
            Logger.LogLine("Checking to see if the pool("+ConnectionName+") is closed while trying to reinstate connection");
        	if (isClosed)
        		throw new Exception("Unable to restore the connection for pool: "+ConnectionName+" as the pool is closed.  Trying to commit the transaction.");
        	locked.Add(conn);
            Utility.Release(this);
        }

        internal void UnlockPoolPostBackupRestore()
        {
            Logger.LogLine("Reopening connection pool: " + ConnectionName + " to indicate that the BackupRestore has been completed.");
            for (int x = 0; x < minPoolSize+locked.Count; x++)
            {
                if (!checkMin())
                    break;
                unlocked.Enqueue(CreateConnection());
            }
            foreach (Connection conn in locked)
            {
                Logger.LogLine("Unlocking connection " + conn.ID + " in pool: " + ConnectionName + " to release the pool from backup.");
                conn.UnlockForBackup();
            }
            Logger.LogLine("Marking the pool(" + ConnectionName + ") as open and ready and releasing the lock after completing a BackupRestore.");
            isReady = true;
            isClosed = false;
            Utility.Release(this);
        }

        private List<ForeignKey> ExtractExpectedForeignKeys(Connection conn)
        {
            List<ForeignKey> keys = new List<ForeignKey>();
            foreach (Type t in _updater.CreatedTypes)
            {
                sTable tbl = _mapping[t];
                foreach (string prop in tbl.Properties)
                {
                    List<string> ifields = new List<string>();
                    List<string> efields = new List<string>();
                    if (tbl.GetRelationForProperty(prop).HasValue)
                    {
                        sTableRelation rel = tbl.GetRelationForProperty(prop).Value;
                        foreach (sTableField fld in tbl[prop])
                        {
                            ifields.Add(fld.Name);
                            efields.Add(fld.ExternalField);
                        }
                        keys.Add(new ForeignKey(tbl.Name, ifields, rel.ExternalTable, efields, rel.OnUpdate.ToString(), rel.OnDelete.ToString()));
                    }
                    if (_mapping.PropertyHasIntermediateTable(t, prop))
                    {
                        sTable itbl = _mapping[t, prop];
                        ifields.Clear();
                        efields.Clear();
                        foreach (sTableField fld in itbl["PARENT"])
                        {
                            ifields.Add(fld.Name);
                            efields.Add(fld.ExternalField);
                        }
                        keys.Add(new ForeignKey(itbl.Name, ifields, itbl.Relations[0].ExternalTable, efields, ForeignField.UpdateDeleteAction.CASCADE.ToString(), ForeignField.UpdateDeleteAction.CASCADE.ToString()));
                        ifields.Clear();
                        efields.Clear();
                        foreach (sTableField fld in itbl["CHILD"])
                        {
                            ifields.Add(fld.Name);
                            efields.Add(fld.ExternalField);
                        }
                        keys.Add(new ForeignKey(itbl.Name, ifields, itbl.Relations[1].ExternalTable, efields, ForeignField.UpdateDeleteAction.CASCADE.ToString(), ForeignField.UpdateDeleteAction.CASCADE.ToString()));
                    }
                }
            }
            for (int x = 0; x < keys.Count; x++)
            {
                for (int y = x + 1; y < keys.Count; y++)
                {
                    if (keys[x].Equals(keys[y]))
                    {
                        keys.RemoveAt(y);
                        y--;
                    }
                }
            }
            return keys;
        }

        private List<PrimaryKey> ExtractExpectedPrimaryKeys(Connection conn)
        {
            List<PrimaryKey> keys = new List<PrimaryKey>();
            foreach (Type t in _updater.CreatedTypes)
            {
                sTable tbl = _mapping[t];
                if (tbl.PrimaryKeyFields.Length > 0)
                    keys.Add(new PrimaryKey(tbl.Name, new List<string>(tbl.PrimaryKeyFields)));
                string[] props = tbl.Properties;
                foreach (string prop in props)
                {
                    if (_mapping.PropertyHasIntermediateTable(t, prop))
                    {
                        tbl = _mapping[t, prop];
                        if (tbl.PrimaryKeyFields.Length > 0)
                            keys.Add(new PrimaryKey(tbl.Name, new List<string>(tbl.PrimaryKeyFields)));
                    }
                }
            }
            return keys;
        }

        internal void EmptyAllTables(Connection conn)
        {
            List<string> tables = new List<string>();
            conn.ExecuteQuery(conn.queryBuilder.SelectTableNames());
            while (conn.Read())
                tables.Add(conn[0].ToString());
            conn.Close();
            foreach (string str in tables){
                conn.ExecuteNonQuery(conn.queryBuilder.DeleteAll(str));
                conn.Commit();
            }
        }

        private void GetPkFksCollection(Connection conn,out Dictionary<PrimaryKey, List<ForeignKey>> primaryKeys, out Dictionary<ForeignKey, PrimaryKey> foreignKeys)
        {
            primaryKeys = new Dictionary<PrimaryKey, List<ForeignKey>>();
            foreignKeys = new Dictionary<ForeignKey, PrimaryKey>();
            List<PrimaryKey> pks = ExtractExpectedPrimaryKeys(conn);
            List<ForeignKey> fks = ExtractExpectedForeignKeys(conn);
            foreach (PrimaryKey pk in pks)
            {
                primaryKeys.Add(pk, new List<ForeignKey>());
                foreach (ForeignKey fk in fks)
                {
                    if (pk.IsForForeignRelation(fk))
                        primaryKeys[pk].Add(fk);
                }
            }
            foreach (ForeignKey fk in fks)
            {
                foreach (PrimaryKey pk in pks)
                {
                    if (pk.ContainsForeignFields(fk))
                    {
                        foreignKeys.Add(fk, pk);
                        break;
                    }
                }
            }
        }

        internal void RecurDropPK(PrimaryKey pk, Dictionary<PrimaryKey, List<ForeignKey>> pks, Dictionary<ForeignKey, PrimaryKey> fks, ref List<string> queries, Connection conn)
        {
            string query;
            bool add = true;
            query = conn.queryBuilder.DropPrimaryKey(pk);
            if (query.Contains("\n"))
            {
                foreach (string str in query.Split('\n'))
                {
                    if (str.Trim().Length > 0)
                    {
                        if (queries.Contains(str))
                        {
                            add = false;
                            break;
                        }
                    }
                }
            }
            else
                add = !queries.Contains(query);
            if (add)
            {
                foreach (ForeignKey fk in pks[pk])
                {
                    if (!fks.ContainsKey(fk))
                    {
                        query = conn.queryBuilder.DropForeignKey(fk.InternalTable, fk.ExternalTable, fk.ExternalFields[0], fk.InternalFields[0]);
                        if (query.Contains("\n"))
                            queries.AddRange(query.Split('\n'));
                        else
                            queries.Add(query);
                    }
                    else
                    {
                        if (fks[fk].Name != fk.ExternalTable)
                            RecurDropPK(fks[fk], pks, fks, ref queries, conn);
                        query = conn.queryBuilder.DropForeignKey(fk.InternalTable, fk.ExternalTable, fk.ExternalFields[0], fk.InternalFields[0]);
                        if (query.Contains("\n"))
                            queries.AddRange(query.Split('\n'));
                        else
                            queries.Add(query);
                    }
                }
                query = conn.queryBuilder.DropPrimaryKey(pk);
                if (query.Contains("\n"))
                    queries.AddRange(query.Split('\n'));
                else
                    queries.Add(query);
            }
        }

        internal void DisableRelationships(Connection conn)
        {
            List<string> queries = new List<string>();
            string query;
            Dictionary<PrimaryKey, List<ForeignKey>> pks;
            Dictionary<ForeignKey, PrimaryKey> fks;
            GetPkFksCollection(conn, out pks, out fks);
            foreach (PrimaryKey pk in pks.Keys)
            {
                if (pks[pk].Count == 0)
                {
                    query = conn.queryBuilder.DropPrimaryKey(pk);
                    if (query.Contains("\n"))
                        queries.AddRange(query.Split('\n'));
                    else
                        queries.Add(query);
                }
            }
            foreach (PrimaryKey pk in pks.Keys)
            {
                if (pks[pk].Count > 0)
                {
                    RecurDropPK(pk, pks, fks, ref queries, conn);
                }
            }
            Utility.RemoveEmptyStrings(ref queries);
            Utility.RemoveDuplicateStrings(ref queries, null);
            foreach (string str in queries)
            {
                conn.ExecuteNonQuery(str);
                System.Threading.Thread.Sleep(50);
                conn.Commit();
            }
        }

        internal void EnableRelationships(Connection conn)
        {
            List<string> queries = new List<string>();
            foreach (PrimaryKey pk in ExtractExpectedPrimaryKeys(conn))
                queries.Add(conn.queryBuilder.CreatePrimaryKey(pk));
            foreach (ForeignKey fk in ExtractExpectedForeignKeys(conn))
                queries.Add(conn.queryBuilder.CreateForeignKey(fk));
            Utility.RemoveEmptyStrings(ref queries);
            Utility.RemoveDuplicateStrings(ref queries, null);
            foreach (string str in queries)
            {
                conn.ExecuteNonQuery(str);
                System.Threading.Thread.Sleep(50);
            }
        }

        internal bool IsCoreStoredProcedure(StoredProcedure storedProcedure)
        {
            return _IsCoreStoredProcedure(storedProcedure);
        }

        internal virtual string WrapAlias(string alias)
        {
            return "\"" + alias + "\"";
        }
    }
}
