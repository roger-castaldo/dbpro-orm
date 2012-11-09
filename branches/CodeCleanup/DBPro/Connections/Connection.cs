using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using Org.Reddragonit.Dbpro.Connections.Parameters;
using Org.Reddragonit.Dbpro.Structure;
using FieldType = Org.Reddragonit.Dbpro.Structure.Attributes.FieldType;
using VersionTypes = Org.Reddragonit.Dbpro.Structure.Attributes.VersionField.VersionTypes;
using System.Reflection;
using System.Threading;
using Org.Reddragonit.Dbpro.Virtual.Attributes;
using Org.Reddragonit.Dbpro.Connections.PoolComponents;

namespace Org.Reddragonit.Dbpro.Connections
{
	
	
	public abstract class Connection : IDataReader
	{
		private const int MAX_COMM_QUERIES = 5;
        private const int MAX_READCHECK_TRIES = 5;
		
		private ConnectionPool pool;
		protected IDbConnection conn=null;
		protected IDbCommand comm=null;
		protected IDataReader reader=null;
		protected IDbTransaction trans=null;
        private bool _exclusiveLock;
		private bool isConnected=false;
		private DateTime creationTime;
		protected string connectionString;
		private int commCntr;
        private bool firstRead = false;
        private bool firstReadResult;
        private bool lockedForBackup=false;
        private string _uniqueID;
        private bool _readonly;
		
		private QueryBuilder _qb;
		internal virtual QueryBuilder queryBuilder
		{
			get{
				if (_qb==null)
					_qb=new QueryBuilder(pool,this);
				return _qb;
			}
		}

        protected bool Readonly
        {
            get { return _readonly; }
        }
		
		internal virtual bool UsesGenerators{
			get{
				return false;
			}
		}
		
		internal virtual bool UsesIdentities{
			get{
				return false;
			}
		}

        internal virtual string ConcatenationCharacter
        {
            get {
                return "||";
            }
        }

        internal virtual string WrapAlias(string alias)
        {
            return "\"" + alias + "\"";
        }

        internal string ID
        {
            get { return _uniqueID; }
        }
		
		public ConnectionPool Pool{
			get{return pool;}
		}
		
		protected abstract IDbConnection EstablishConnection();
		protected abstract IDbCommand EstablishCommand();
		internal abstract string DefaultTableString{
			get;
		}
		internal abstract IDbDataParameter CreateParameter(string parameterName,object parameterValue,Org.Reddragonit.Dbpro.Structure.Attributes.FieldType type, int fieldLength);
		public abstract IDbDataParameter CreateParameter(string parameterName,object parameterValue);
		internal abstract string TranslateFieldType(Org.Reddragonit.Dbpro.Structure.Attributes.FieldType type,int fieldLength);
		internal abstract void GetDropAutogenStrings(ExtractedTableMap map,ConnectionPool pool,out List<IdentityField> identities,out List<Generator> generators,out List<Trigger> triggers);
		internal abstract void GetAddAutogen(ExtractedTableMap map,ConnectionPool pool, out List<IdentityField> identities, out List<Generator> generators, out List<Trigger> triggers,out List<StoredProcedure> procedures);
		internal abstract List<Trigger> GetVersionTableTriggers(ExtractedTableMap table,VersionTypes versionType,ConnectionPool pool);
        internal abstract List<Trigger> GetDeleteParentTrigger(ExtractedTableMap table, ExtractedTableMap parent, ConnectionPool pool);
        internal abstract void DisableAutogens();
        internal abstract void EnableAndResetAutogens();
        internal abstract IDbTransaction EstablishExclusiveTransaction();
		
		internal virtual List<string> GetDropTableString(string table,bool isVersioned)
		{
			List<string> ret = new List<string>();
			if (isVersioned)
			{
				ret.Add(queryBuilder.DropTrigger(queryBuilder.VersionTableInsertTriggerName(table)));
				ret.Add(queryBuilder.DropTrigger(queryBuilder.VersionTableUpdateTriggerName(table)));
				ret.Add(queryBuilder.DropTable(queryBuilder.VersionTableName(table)));
			}
			ret.Add(queryBuilder.DropTable(table));
			return ret;
		}

        public string CreateParameterName(string parameter)
        {
            return queryBuilder.CreateParameterName(parameter);
        }
		
		public Connection(ConnectionPool pool,string connectionString,bool Readonly,bool exclusiveLock){
			this.connectionString=connectionString;
			this.pool=pool;
            this._uniqueID = System.Convert.ToBase64String(Guid.NewGuid().ToByteArray());
            ResetConnection(false);
		}

        private void ThreadedReaderClose()
        {
            Thread.Sleep(200);
            try { reader.Close(); }
            catch (Exception e) { }
        }

        private void ThreadedConnectionClose()
        {
            Thread.Sleep(200);
            try { conn.Close(); }
            catch (Exception e) { }
        }

        private void ThreadedTransactionCommit()
        {
            Thread.Sleep(200);
            try { this.Commit(); }
            catch (Exception e) { }
        }

        internal void ResetConnection(bool ignoreReader)
        {
            Logger.LogLine("Resetting connection "+_uniqueID+" in pool " + pool.ConnectionName + " using connection string: " + connectionString);
            Thread t;
            if ((reader != null)&&(!ignoreReader))
            {
                Logger.LogLine("Attempting to close the currently open reader for resetting.");
                t = new Thread(new ThreadStart(ThreadedReaderClose));
                t.IsBackground = true;
                t.Start();
                try
                {
                    t.Start();
                    t.Join(new TimeSpan(0, 0, 0, pool.readTimeout));
                }
                catch (Exception e) {
                }
                Logger.LogLine("Currently open reader closed, continuing to reset the connection.");
            }
            bool createTrans = false;
            if (conn != null)
            {
                if (trans != null)
                {
                    createTrans = true;
                    Logger.LogLine("Attempting to close the currently open transaction for resetting.");
                    t = new Thread(new ThreadStart(ThreadedTransactionCommit));
                    t.IsBackground = true;
                    try
                    {
                        t.Start();
                        t.Join(new TimeSpan(0, 0, 0, pool.readTimeout));
                    }
                    catch (Exception e)
                    { }
                    Logger.LogLine("Currently open transaction closed, continuing to reset the connection.");
                }
                Logger.LogLine("Attempting to close the currently open connection for resetting.");
                t = new Thread(new ThreadStart(ThreadedConnectionClose));
                t.IsBackground = true;
                try {
                    t.Start();
                    t.Join(new TimeSpan(0, 0, 0, pool.readTimeout)); }
                catch (Exception e)
                {}
                Logger.LogLine("Currently open connection closed, reopning things to finalize connection reset.");
            }
            creationTime = System.DateTime.Now;
            Logger.LogLine("Establishing new connection in pool " + pool.ConnectionName + " through connection reset");
            conn = EstablishConnection();
            conn.Open();
            if (createTrans)
            {
                Logger.LogLine("Creating new transaction for connection in reset to replace existing");
                StartTransaction();
            }
            if (comm == null)
                comm = EstablishCommand();
            else
            {
                Logger.LogLine("Moving command in connection to newly created connection and transaction in reset");
                comm.Connection = conn;
                comm.Transaction = trans;
            }
            isConnected = true;
            commCntr = 0;
        }
		
		internal string ConnectionName
		{
			get{return pool.ConnectionName;}
		}
		
		internal void Disconnect()
		{
			try{
                if (trans!=null)
				    this.Commit();
			}catch (Exception e){}
            try
            {
                conn.Close();
            }
            catch (Exception e) { }
			isConnected=false;
		}
		
		internal bool isPastKeepAlive(long secondsToLive)
		{
			return !(secondsToLive<0)&&(System.DateTime.Now.Subtract(creationTime).TotalSeconds>secondsToLive);
		}
		
		internal void Reset()
		{
            if (trans != null)
            {
                trans.Rollback();
                trans = null;
                comm.Transaction = null;
            }
		}

        internal void StartTransaction()
        {
            if (_exclusiveLock)
                trans = EstablishExclusiveTransaction();
            else
                trans = conn.BeginTransaction();
            comm.Transaction = trans;
        }
		
		public void Commit()
		{
            if (trans != null)
            {
                Utility.WaitOne(this);
                while (lockedForBackup)
                {
                    Logger.LogLine("Waiting for backup lock on connection " + _uniqueID + " in pool " + pool.ConnectionName + " to release to commit transaction.");
                    Utility.Release(this);
                    Thread.Sleep(1000);
                    Utility.WaitOne(this);
                }
                if (lockedForBackup)
                {
                    Logger.LogLine("Attempting to reinstate connection " + _uniqueID + " for pool " + pool.ConnectionName + " to reopen it and commit transaction.");
                    pool.ReinstateConnection(this);
                }
                trans.Commit();
                trans = null;
                comm.Transaction = null;
                Utility.Release(this);
            }
		}
		
		public void RollBack()
		{
            if (trans != null)
            {
                trans.Rollback();
                trans = null;
                comm.Transaction = null;
            }
		}
		
		public void CloseConnection()
		{
			if (isConnected)
			{
				if ((reader!=null)&&!reader.IsClosed)
					reader.Close();
                if (trans != null)
                {
                    this.Commit();
                    trans = null;
                }
				comm.CommandText="";
				comm.Parameters.Clear();
			}
            pool.returnConnection(this);
		}
		
		~Connection()
		{
			if (isConnected)
			{
				Disconnect();
			}
		}
		
		public void SetCommandType(CommandType type)
		{
			comm.CommandType=type;
		}

        public void Delete(Type tableType, SelectParameter[] pars){
            if (_readonly)
                throw new Exception("Unable to delete to a readonly database.");
            if (!Pool.Mapping.IsMappableType(tableType))
                throw new Exception("Unable to delete type " + tableType.FullName + " no matching Table Map found for the connection pool "+Pool.ConnectionName+".");
            List<IDbDataParameter> parameters = new List<IDbDataParameter>();
            string del = queryBuilder.Delete(tableType, pars, out parameters);
            if (del != null)
            {
                bool abort = false;
                ConnectionPoolManager.RunTriggers(this,tableType, pars, ConnectionPoolManager.TriggerTypes.PRE_DELETE,out abort);
                if (!abort)
                {
                    ExecuteNonQuery(del, parameters);
                    ConnectionPoolManager.RunTriggers(this, tableType, pars, ConnectionPoolManager.TriggerTypes.POST_DELETE, out abort);
                }
            }
            else
                throw new Exception("An error occured attempting to build the delete query.");
        }

        public void Delete(Table table)
        {
            if (_readonly)
                throw new Exception("Unable to delete from a readonly database.");
            if (!table.IsSaved)
                throw new Exception("Unable to delete an object from the database that is not saved.");
            if (table.ConnectionName != ConnectionName)
                throw new Exception("Unable to delete an object from a database connection it is not part of.");
            bool abort = false;
            ConnectionPoolManager.RunTriggers(this, null, table, ConnectionPoolManager.TriggerTypes.PRE_DELETE, out abort);
            if (!abort)
            {
                List<IDbDataParameter> pars;
                string del = queryBuilder.Delete(table, out pars);
                ExecuteNonQuery(del, pars);
                ConnectionPoolManager.RunTriggers(this, null, table, ConnectionPoolManager.TriggerTypes.POST_DELETE, out abort);
            }
        }

        internal void DeleteAll(Type tableType)
        {
            if (_readonly)
                throw new Exception("Unable to delete from a readonly database.");
            bool abort = false;
            ConnectionPoolManager.RunTriggers(this, tableType, ConnectionPoolManager.TriggerTypes.PRE_DELETE_ALL, out abort);
            if (!abort)
            {
                this.ExecuteNonQuery(queryBuilder.DeleteAll(tableType));
                ConnectionPoolManager.RunTriggers(this, tableType, ConnectionPoolManager.TriggerTypes.POST_DELETE_ALL, out abort);
            }
        }

        public void Update(Type tableType, Dictionary<string, object> updateFields, SelectParameter[] parameters)
        {
            if (_readonly)
                throw new Exception("Unable to update to a readonly database.");
            if (!Pool.Mapping.IsMappableType(tableType))
                throw new Exception("Unable to update type " + tableType.FullName + " no matching Table Map found for the connection pool " + Pool.ConnectionName + ".");
            List<IDbDataParameter> pars = new List<IDbDataParameter>();
            string query = queryBuilder.Update(tableType, updateFields, parameters, out pars);
            if (query != null)
            {
                bool abort = false;
                ConnectionPoolManager.RunTriggers(this, tableType, updateFields, parameters, ConnectionPoolManager.TriggerTypes.PRE_UPDATE, out abort);
                if (!abort)
                {
                    ExecuteNonQuery(query, pars.ToArray());
                    ConnectionPoolManager.RunTriggers(this, tableType, updateFields, parameters, ConnectionPoolManager.TriggerTypes.POST_UPDATE, out abort);
                }
            }
        }
		
		private Table Update(Table table)
        {
            if (_readonly)
                throw new Exception("Unable to update to a readonly database.");
			if (table.ConnectionName!=ConnectionName)
				throw new Exception("Cannot update an entry into a table into the database connection that it was not specified for.");
			if ((table.ChangedFields==null)||(table.ChangedFields.Count==0))
				return table;
            sTable map = Pool.Mapping[table.GetType()];
            table._changedFields = table.ChangedFields;
			if (Pool.Mapping.IsMappableType(table.GetType().BaseType)!=null)
			{
				Table ta = Update((Table)table.ToType(table.GetType().BaseType,null));
				table.CopyValuesFrom(ta);
			}
            foreach (string prop in map.ForeignTableProperties)
            {
                PropertyInfo pi = table.GetType().GetProperty(prop, Utility._BINDING_FLAGS);
                if (pi.PropertyType.IsArray)
                {
                    Table[] vals = (Table[])table.GetField(prop);
                    if (vals != null)
                    {
                        foreach (Table t in vals)
                            this.Save(t);
                    }
                }
                else
                {
                    Table ext = (Table)table.GetField(prop);
                    if (ext != null)
                    {
                        ext = Save(ext);
                        table.SetField(prop, ext);
                    }
                }
            }
            string query = "";
            string select = "";
            List<IDbDataParameter> pars = new List<IDbDataParameter>();
            List<IDbDataParameter> selectPars = new List<IDbDataParameter>();
            Org.Reddragonit.Dbpro.Structure.Attributes.Table tbl = (Org.Reddragonit.Dbpro.Structure.Attributes.Table)table.GetType().GetCustomAttributes(typeof(Org.Reddragonit.Dbpro.Structure.Attributes.Table),false)[0];
            if (tbl.AlwaysInsert)
                query = queryBuilder.Insert(table, out pars, out select, out selectPars);
            else
                query = queryBuilder.Update(table, out pars);
            if (query.Length > 0)
            {
                ExecuteNonQuery(query, pars);
                if ((select != null) && (tbl.AlwaysInsert))
                {
                    ExecuteQuery(select, selectPars);
                    Read();
                    table.SetField(map.AutoGenProperty, this[0]);
                    Close();
                    table._isSaved = true;
                    table.LoadStatus = LoadStatus.Complete;
                }
            }
            foreach (string prop in map.Properties)
            {
                PropertyInfo pi = table.GetType().GetProperty(prop, Utility._BINDING_FLAGS);
                if (pi.PropertyType.IsArray)
                {
                    if (Pool.Mapping.IsMappableType(pi.PropertyType.GetElementType()))
                    {
                        Dictionary<string, List<List<IDbDataParameter>>> queries = queryBuilder.UpdateMapArray(table, prop, false);
                        foreach (string str in queries.Keys)
                        {
                            foreach (List<IDbDataParameter> p in queries[str])
                                ExecuteNonQuery(str, p);
                        }
                    }
                    else
                        InsertArrayValue(table, map, (Array)table.GetField(prop), prop, false);
                }
            }
			return table;
		}

        public Table Save(Table table)
        {
            Table ret = table;
            if (table.ConnectionName != ConnectionName)
            {
                throw new Exception("Cannot insert an entry into a table into the database connection that it was not specified for.");
            }
            if (table.LoadStatus == LoadStatus.Partial)
                ret= table;
            if (table.IsSaved)
            {
                if (table.LoadStatus == LoadStatus.Complete)
                {
                    Table orig = table.LoadCopyOfOriginal(this);
                    bool abort = false;
                    ConnectionPoolManager.RunTriggers(this, orig, table, ConnectionPoolManager.TriggerTypes.PRE_UPDATE, out abort);
                    if (!abort)
                    {
                        ret = Update(table);
                        ConnectionPoolManager.RunTriggers(this, orig, ret, ConnectionPoolManager.TriggerTypes.POST_UPDATE, out abort);
                    }
                }
                else
                    ret = table;
            }
            else
            {
                bool abort = false;
                ConnectionPoolManager.RunTriggers(this, null, table, ConnectionPoolManager.TriggerTypes.PRE_INSERT, out abort);
                if (!abort)
                {
                    ret = Insert(table, false);
                    ConnectionPoolManager.RunTriggers(this, null, ret, ConnectionPoolManager.TriggerTypes.POST_INSERT, out abort);
                }
            }
            if (!ret.IsProxied)
                ret = (Table)LazyProxy.Instance(ret);
            return ret;
        }

        internal Table SaveWithAutogen(Table table)
        {
            Table ret = table;
            if (table.ConnectionName != ConnectionName)
            {
                throw new Exception("Cannot insert an entry into a table into the database connection that it was not specified for.");
            }
            if (table.LoadStatus == LoadStatus.Partial)
                ret = table;
            if (!table.IsSaved)
                ret = Insert(table,true);
            return ret;
        }
		
		private Table Insert(Table table,bool ignoreAutogen)
		{
            if (_readonly)
                throw new Exception("Unable to insert into a readonly database.");
            sTable map = pool.Mapping[table.GetType()];
            if (!ignoreAutogen)
            {
                if (pool.Mapping.IsMappableType(table.GetType().BaseType))
                {
                    Table tblPar = (Table)table.ToType(table.GetType().BaseType, null);
                    List<SelectParameter> tmpPars = new List<SelectParameter>();
                    sTable pMap = pool.Mapping[tblPar.GetType()];
                    foreach (string str in pMap.PrimaryKeyProperties)
                        tmpPars.Add(new EqualParameter(str, tblPar.GetField(str)));
                    List<Org.Reddragonit.Dbpro.Structure.Table> tmpTbls = Select(tblPar.GetType(), tmpPars.ToArray());
                    if (tmpTbls.Count > 0)
                    {
                        Table ta = tmpTbls[0];
                        Table orig = ta.LoadCopyOfOriginal(this);
                        List<string> pProps = new List<string>(pMap.PrimaryKeyProperties);
                        foreach (string prop in pMap.Properties)
                        {
                            if (!pProps.Contains(prop))
                            {
                                ta.SetField(prop, tblPar.GetField(prop));
                            }
                        }
                        bool abort = false;
                        ConnectionPoolManager.RunTriggers(this, orig, ta, ConnectionPoolManager.TriggerTypes.PRE_UPDATE, out abort);
                        if (!abort)
                        {
                            tblPar = Update(ta);
                            orig = tblPar.LoadCopyOfOriginal(this);
                            ConnectionPoolManager.RunTriggers(this, orig, tblPar, ConnectionPoolManager.TriggerTypes.PRE_UPDATE, out abort);
                        }
                    }
                    else
                    {
                        bool abort = false;
                        ConnectionPoolManager.RunTriggers(this, null, tblPar, ConnectionPoolManager.TriggerTypes.PRE_INSERT, out abort);
                        if (!abort)
                        {
                            tblPar = Insert(tblPar, ignoreAutogen);
                            ConnectionPoolManager.RunTriggers(this, null, tblPar, ConnectionPoolManager.TriggerTypes.POST_INSERT, out abort);
                        }
                    }
                    table.CopyValuesFrom(tblPar);
                }
                foreach (string prop in map.ForeignTableProperties)
                {
                    PropertyInfo pi = table.GetType().GetProperty(prop, Utility._BINDING_FLAGS);
                    if (pi.PropertyType.IsArray)
                    {
                        Table[] vals = (Table[])table.GetField(prop);
                        if (vals != null)
                        {
                            foreach (Table t in vals)
                                this.Save(t);
                        }
                    }
                    else
                    {
                        Table ext = (Table)table.GetField(prop);
                        if (ext != null)
                        {
                            ext = Save(ext);
                            table.SetField(prop, ext);
                        }
                    }
                }
            }
			string query = "";
			string select="";
			List<IDbDataParameter> pars = new List<IDbDataParameter>();
			List<IDbDataParameter> selectPars = new List<IDbDataParameter>();
            if (ignoreAutogen)
                query = queryBuilder.InsertWithIdentity(table, out pars);
            else
                query = queryBuilder.Insert(table, out pars, out select, out selectPars);
			ExecuteNonQuery(query,pars);
			if ((select!=null)&&(!ignoreAutogen))
			{
				ExecuteQuery(select,selectPars);
				Read();
                table.SetField(map.AutoGenProperty,this[0]);
				Close();
			}
			table._isSaved=true;
			table.LoadStatus= LoadStatus.Complete;
            List<string> fProps = new List<string>(map.ForeignTableProperties);
            foreach (string prop in map.Properties)
            {
                PropertyInfo pi = table.GetType().GetProperty(prop, Utility._BINDING_FLAGS);
                if (pi.PropertyType.IsArray)
                {
                    if (fProps.Contains(prop))
                    {
                        Dictionary<string, List<List<IDbDataParameter>>> queries = queryBuilder.UpdateMapArray(table, prop, ignoreAutogen);
                        if (queries != null)
                        {
                            foreach (string str in queries.Keys)
                            {
                                foreach (List<IDbDataParameter> p in queries[str])
                                    ExecuteNonQuery(str, p);
                            }
                        }
                    }
                    else
                    {
                        InsertArrayValue(table, map, (Array)table.GetField(prop), prop, ignoreAutogen);
                    }
                }
            }
			return table;
		}
		
		private void InsertArrayValue(Table table,sTable map,Array values,string prop,bool ignoreAutogen)
		{
			List<IDbDataParameter> pars = new List<IDbDataParameter>();
			string query="";
			string fields="";
			string paramString="";
			string conditions = "";
			pars = new List<IDbDataParameter>();
            sTable arMap = pool.Mapping[table.GetType(), prop];
            foreach (string pk in map.PrimaryKeyProperties)
            {
                foreach (sTableField fld in map[pk])
                {
                    foreach (sTableField f in arMap.Fields)
                    {
                        if (f.ExternalField == fld.Name)
                        {
                            fields += f.Name + ", ";
                            paramString += queryBuilder.CreateParameterName(f.Name) + ", ";
                            conditions += f.Name + " = " + queryBuilder.CreateParameterName(f.Name) + " AND ";
                            pars.Add(CreateParameter(queryBuilder.CreateParameterName(f.Name), QueryBuilder.LocateFieldValue(table, fld, pool)));
                            break;
                        }
                    }
                }
            }
			query = queryBuilder.Delete(arMap.Name,conditions.Substring(0,conditions.Length-4));
			ExecuteNonQuery(query,pars);
			if (values!=null)
			{
				pars.Add(CreateParameter(queryBuilder.CreateParameterName("VALUE"),null));
                if (ignoreAutogen)
                    pars.Add(CreateParameter(CreateParameterName("index_id"), null));
                fields += Pool.CorrectName("VALUE") + (ignoreAutogen ? ", " + Pool.CorrectName("VALUE_INDEX") : "");
				paramString+=CreateParameterName("VALUE")+(ignoreAutogen ? ", "+CreateParameterName("index_id") : "");
				query = queryBuilder.Insert(arMap.Name,fields,paramString);
                long index = 0;
				foreach (object obj in values)
				{
					pars.RemoveAt(pars.Count-1);
                    if(ignoreAutogen)
                        pars.RemoveAt(pars.Count - 1);
					pars.Add(CreateParameter(queryBuilder.CreateParameterName("VALUE"),obj));
                    if (ignoreAutogen)
                        pars.Add(CreateParameter(CreateParameterName("index_id"), index, FieldType.LONG, 8));
					ExecuteNonQuery(query,pars);
                    index++;
				}
			}
		}

		private List<Table> AddArrayedTablesToSelect(List<Table> ret, System.Type type)
		{
            while (pool.Mapping.IsMappableType(type))
            {
                sTable map = pool.Mapping[type];
                string query = "";
                foreach (string prop in map.Properties)
                {
                    PropertyInfo pi = type.GetProperty(prop, Utility._BINDING_FLAGS);
                    if (pi.PropertyType.IsArray)
                    {
                        if (pool.Mapping.IsMappableType(pi.PropertyType.GetElementType()))
                        {
                            foreach (Table t in ret)
                            {
                                List<IDbDataParameter> pars = new List<IDbDataParameter>();
                                sTable external = pool.Mapping[pi.PropertyType.GetElementType()];
                                sTable arMap = pool.Mapping[type, pi.Name];
                                string fields = "";
                                string conditions = "";
                                foreach (sTableField fld in arMap.Fields)
                                {
                                    if (fld.ClassProperty == null)
                                    {
                                        foreach (sTableField f in external.Fields)
                                        {
                                            if (fld.ExternalField == f.Name)
                                            {
                                                fields += fld.Name + " AS " + f.Name + ", ";
                                                break;
                                            }
                                        }
                                    }
                                    else
                                    {
                                        foreach (sTableField f in map.Fields)
                                        {
                                            if (fld.ExternalField == f.Name)
                                            {
                                                conditions += fld.Name + " = " + queryBuilder.CreateParameterName(fld.Name) + " AND ";
                                                pars.Add(CreateParameter(queryBuilder.CreateParameterName(fld.Name), QueryBuilder.LocateFieldValue(t, f, pool)));
                                                break;
                                            }
                                        }
                                    }
                                }
                                fields = fields.Substring(0, fields.Length - 2);
                                conditions = conditions.Substring(0, conditions.Length - 4);
                                query = String.Format(queryBuilder.OrderBy, string.Format(queryBuilder.SelectWithConditions, fields, arMap.Name, conditions), pool.CorrectName("VALUE_INDEX"));
                                ArrayList values = new ArrayList();
                                ExecuteQuery(query, pars);
                                while (Read())
                                {
                                    Table ta = (Table)LazyProxy.Instance(pi.PropertyType.GetElementType().GetConstructor(System.Type.EmptyTypes).Invoke(new object[0]));
                                    ta.SetValues(this);
                                    ta.LoadStatus = LoadStatus.Partial;
                                    values.Add(ta);
                                }
                                Close();
                                Array obj;
                                obj = Array.CreateInstance(pi.PropertyType.GetElementType(), values.Count);
                                for (int x = 0; x < values.Count; x++)
                                    ((Array)obj).SetValue(values[x], x);
                                t.SetField(pi.Name, obj);
                            }
                        }
                        else
                        {
                            sTable arMap = Pool.Mapping[type, pi.Name];
                            string conditions = "";
                            foreach (sTableField fld in arMap.Fields)
                            {
                                foreach (sTableField f in map.Fields)
                                {
                                    if (Utility.StringsEqual(fld.ExternalField, f.Name))
                                    {
                                        conditions += fld.Name + " = " + queryBuilder.CreateParameterName(f.Name)+" AND ";
                                        break;
                                    }
                                }
                            }
                            query = "SELECT " + Pool.CorrectName("VALUE") + " FROM " + arMap.Name + " WHERE " + conditions.Substring(0, conditions.Length - 4) + " ORDER BY " + Pool.CorrectName("VALUE_INDEX") + " ASC";
                            foreach (Table t in ret)
                            {
                                List<IDbDataParameter> pars = new List<IDbDataParameter>();
                                foreach (string str in map.PrimaryKeyProperties)
                                {
                                    foreach (sTableField fld in map[str])
                                        pars.Add(CreateParameter(queryBuilder.CreateParameterName(fld.Name), QueryBuilder.LocateFieldValue(t,fld,pool)));
                                }
                                ArrayList values = new ArrayList();
                                ExecuteQuery(query, pars);
                                while (Read())
                                {
                                    if (pi.PropertyType.GetElementType().IsEnum)
                                        values.Add(this.pool.GetEnumValue(pi.PropertyType.GetElementType(), this.GetInt32(0)));
                                    else
                                        values.Add(this[0]);
                                }
                                Close();
                                Array obj = Array.CreateInstance(pi.PropertyType.GetElementType(), values.Count);
                                for (int x = 0; x < values.Count; x++)
                                    ((Array)obj).SetValue(values[x], x);
                                t.SetField(prop, obj);
                            }
                        }
                    }
                }
                type = type.BaseType;
            }
			return ret;
		}

        public List<Object> SelectVirtualTable(System.Type type)
        {
            if (type.GetCustomAttributes(typeof(VirtualTableAttribute), true).Length == 0)
                throw new Exception("Unable to execute a Virtual Table Query from a class that does not have a VirtualTableAttribute attached to it.");
            List<Object> ret = new List<object>();
            sTable tbl = pool.Mapping.GetVirtualTable(type);
            this.ExecuteQuery("SELECT * FROM " + tbl.Name);
            while (Read())
            {
                object obj = type.GetConstructor(Type.EmptyTypes).Invoke(new object[0]);
                foreach (sTableField fld in tbl.Fields)
                {
                    if (!this.IsDBNull(this.GetOrdinal(fld.Name)))
                        type.GetProperty(fld.ClassProperty, Utility._BINDING_FLAGS).SetValue(obj, this[fld.Name], new object[0]);
                }
                ret.Add(obj);
            }
            Close();
            return ret;
        }
		
		public List<Table> SelectAll(System.Type type)
		{
            return SelectAll(type, new string[0]);
		}

        public List<Table> SelectAll(System.Type type, List<string> OrderByFields)
        {
            return SelectAll(type, OrderByFields.ToArray());
        }

        public List<Table> SelectAll(System.Type type, string[] OrderByFields)
        {
            if (!type.IsSubclassOf(typeof(Table)))
            {
                throw new Exception("Unable to perform select on Type object without object inheriting Org.Reddragonit.DbPro.Structure.Table");
            }
            if (((Table)type.GetConstructor(System.Type.EmptyTypes).Invoke(new object[0])).ConnectionName != ConnectionName)
            {
                throw new Exception("Cannot select from a table from the database connection that it was not specified for.");
            }
            List<Table> ret = new List<Table>();
            ExecuteQuery(queryBuilder.SelectAll(type,OrderByFields));
            while (Read())
            {
                Table t = (Table)LazyProxy.Instance(type.GetConstructor(System.Type.EmptyTypes).Invoke(new object[0]));
                t.SetValues(this);
                t.LoadStatus = LoadStatus.Complete;
                ret.Add(t);
            }
            Close();
            ret = AddArrayedTablesToSelect(ret, type);
            return ret;
        }
		
		public List<Table> Select(System.Type type,List<SelectParameter> parameters)
		{
			if (parameters==null)
				return Select(type,new SelectParameter[0]);
			else
				return Select(type,parameters.ToArray());
		}

        public object SelectMax(string fieldName, System.Type type, List<SelectParameter> parameters)
        {
            if (parameters == null)
                return SelectMax(fieldName,type, new SelectParameter[0]);
            else
                return SelectMax(fieldName, type, parameters.ToArray());
        }

        public object SelectMax(string fieldName, System.Type type, SelectParameter[] parameters)
        {
            object ret = null;
            List<IDbDataParameter> pars = new List<IDbDataParameter>();
            string query = queryBuilder.SelectMax(type, fieldName, parameters, out pars);
            ExecuteQuery(query, pars);
            if (this.Read())
                ret = this[0];
            Close();
            if (ret is DBNull)
            	ret=null;
            return ret;
        }

        public object SelectMin(string fieldName, System.Type type, List<SelectParameter> parameters)
        {
            if (parameters == null)
                return SelectMin(fieldName, type, new SelectParameter[0]);
            else
                return SelectMin(fieldName, type, parameters.ToArray());
        }

        public object SelectMin(string fieldName, System.Type type, SelectParameter[] parameters)
        {
            object ret = null;
            List<IDbDataParameter> pars = new List<IDbDataParameter>();
            string query = queryBuilder.SelectMin(type, fieldName, parameters, out pars);
            ExecuteQuery(query, pars);
            if (this.Read())
                ret = this[0];
            Close();
            if (ret is DBNull)
                ret = null;
            return ret;
        }

        public List<Table> Select(System.Type type, SelectParameter[] parameters)
        {
            return Select(type, parameters, null);
        }
		
		public List<Table> Select(System.Type type,SelectParameter[] parameters,string[] OrderByFields)
		{
			if (!type.IsSubclassOf(typeof(Table)))
			{
				throw new Exception("Unable to perform select on Type object without object inheriting Org.Reddragonit.DbPro.Structure.Table");
			}
			if (((Table)type.GetConstructor(System.Type.EmptyTypes).Invoke(new object[0])).ConnectionName!=ConnectionName)
			{
				throw new Exception("Cannot select from a table from the database connection that it was not specified for.");
			}
			List<Table> ret = new List<Table>();
			List<IDbDataParameter> pars = new List<IDbDataParameter>();
			string query = queryBuilder.Select(type,parameters,out pars,OrderByFields);
			ExecuteQuery(query,pars);
            Logger.LogLine("Query executed, beginning to read results");
			while (Read())
			{
                Logger.LogLine("Creating a lazy proxy instance for the table type " + type.FullName);
				Table t = (Table)LazyProxy.Instance(type.GetConstructor(System.Type.EmptyTypes).Invoke(new object[0]));
                Logger.LogLine("Reading result and loading table object " + type.FullName + " from query");
				t.SetValues(this);
                Logger.LogLine("Setting load status for " + type.FullName + " as completed and adding to results");
				t.LoadStatus=LoadStatus.Complete;
				ret.Add(t);
			}
			Close();
			ret = AddArrayedTablesToSelect(ret, type);
			return ret;
		}
		
		public long SelectCount(System.Type type,List<SelectParameter> parameters)
		{
			if (parameters==null)
				return SelectCount(type,new SelectParameter[0]);
			else
				return SelectCount(type,parameters.ToArray());
		}
		
		public long SelectCount(System.Type type,SelectParameter[] parameters)
		{
			if (!type.IsSubclassOf(typeof(Table)))
			{
				throw new Exception("Unable to perform select on Type object without object inheriting Org.Reddragonit.DbPro.Structure.Table");
			}
			if (((Table)type.GetConstructor(System.Type.EmptyTypes).Invoke(new object[0])).ConnectionName!=ConnectionName)
			{
				throw new Exception("Cannot select from a table from the database connection that it was not specified for.");
			}
			long ret=0;
			List<IDbDataParameter> pars = new List<IDbDataParameter>();
			string query = queryBuilder.SelectCount(type,parameters,out pars);
			ExecuteQuery(query,pars);
			if (Read())
				ret=long.Parse(this[0].ToString());
			Close();
			return ret;
		}
		
		public List<Table> SelectPaged(System.Type type,List<SelectParameter> parameters,ulong? StartIndex,ulong? RowCount)
		{
			if (parameters==null)
				return SelectPaged(type,new SelectParameter[0],StartIndex,RowCount);
			else
				return SelectPaged(type,parameters.ToArray(),StartIndex,RowCount);
		}
		
		public List<Table> SelectPaged(System.Type type,SelectParameter[] parameters,ulong? StartIndex,ulong? RowCount)
		{
			if (!type.IsSubclassOf(typeof(Table)))
			{
				throw new Exception("Unable to perform select on Type object without object inheriting Org.Reddragonit.DbPro.Structure.Table");
			}
			if (((Table)type.GetConstructor(System.Type.EmptyTypes).Invoke(new object[0])).ConnectionName!=ConnectionName)
			{
				throw new Exception("Cannot select from a table from the database connection that it was not specified for.");
			}
			List<Table> ret = new List<Table>();
			List<IDbDataParameter> pars = new List<IDbDataParameter>();
			string query = queryBuilder.SelectPaged(type,parameters,out pars,StartIndex,RowCount,null);
			ExecuteQuery(query,pars);
			while (Read())
			{
				Table t = (Table)LazyProxy.Instance(type.GetConstructor(System.Type.EmptyTypes).Invoke(new object[0]));
				t.SetValues(this);
				t.LoadStatus=LoadStatus.Complete;
				ret.Add(t);
			}
			Close();
			ret = AddArrayedTablesToSelect(ret, type);
			return ret;
		}
		
		private string FormatParameters(string queryString,ref IDbDataParameter[] parameters)
		{
			if (parameters==null)
				return queryString;
			else
			{
				string ret = queryString;
				List<IDbDataParameter> pars = new List<IDbDataParameter>();
				foreach (IDbDataParameter par in parameters)
				{
					if (Utility.IsParameterNull(par))
					{
						ret = Utility.StripNullParameter(ret,par.ParameterName);
					}else
						pars.Add(par);
				}
				parameters=pars.ToArray();
				return ret;
			}
		}

        internal void LockForBackup()
        {
            Utility.WaitOne(this);
            lockedForBackup = true;
            try
            {
                reader.Close();
            }catch(Exception e){
            }
            try
            {
                conn.Close();
            }
            catch (Exception e)
            {
            }
        }

        internal void UnlockForBackup()
        {
            lockedForBackup = false;
            ResetConnection(false);
            Utility.Release(this);
        }
		
		public int ExecuteNonQuery(string queryString)
		{
            return ExecuteNonQuery(queryString, new IDbDataParameter[0]);
		}

		public int ExecuteNonQuery(string queryString, List<IDbDataParameter> parameters)
		{
			return ExecuteNonQuery(queryString, parameters.ToArray());
		}

		public int ExecuteNonQuery(string queryString, IDbDataParameter[] parameters)
		{
            if (_readonly)
            {
                if (queryString.ToUpper().StartsWith("INSERT"))
                    throw new Exception("Unable to insert into a readonly database.");
                else if (queryString.ToUpper().StartsWith("DELETE"))
                    throw new Exception("Unable to delete from a readonly database.");
                else if (queryString.ToUpper().StartsWith("UPDATE"))
                    throw new Exception("Unable to update into a readonly database.");
            }
			commCntr++;
			if (commCntr>=MAX_COMM_QUERIES){
				commCntr=0;
				comm = EstablishCommand();
				if (trans!=null)
					comm.Transaction=trans;
			}
			if ((trans == null)&&!queryString.ToUpper().StartsWith("SELECT"))
            {
                Logger.LogLine("Opening transaction for query since it is not performing a select");
                StartTransaction();
            }
            else if (trans != null)
                Logger.LogLine("Connection already has an open transaction");
			comm.CommandText = FormatParameters(queryString + " ", ref parameters);
			comm.Parameters.Clear();
            comm.CommandType = CommandType.Text;
			if (parameters != null)
			{
				foreach (IDbDataParameter param in parameters)
				{
					comm.Parameters.Add(param);
				}
			}
			Logger.LogLine(comm.CommandText);
            if (parameters != null)
            {
                foreach (IDbDataParameter param in parameters)
                {
                    if (param.Value!=null)
                        Logger.LogLine(param.ParameterName+": "+param.Value.ToString());
                    else
                        Logger.LogLine(param.ParameterName +": NULL");
                }
            }
            try
            {
                int ret = comm.ExecuteNonQuery();
                Logger.LogLine("Successfully executed: "+comm.CommandText);
                return ret;
            }
            catch (Exception e)
            {
                string pars = "";
                if (parameters != null)
                {
                    foreach (IDbDataParameter param in parameters)
                    {
                        if (param.Value != null)
                            pars+=param.ParameterName + ": " + param.Value.ToString()+"\n";
                        else
                            pars+=param.ParameterName + ": NULL"+"\n";
                    }
                }
                throw new Exception("An error occured in executing the query: "+queryString+"\nwith the parameters: "+pars,e);
            }
		}

        public int ExecuteStoredProcedureNoReturn(string procedureName)
        {
            return ExecuteStoredProcedureNoReturn(procedureName, new IDbDataParameter[0]);
        }

        public int ExecuteStoredProcedureNoReturn(string procedureName,List<IDbDataParameter> parameters)
        {
            return ExecuteStoredProcedureNoReturn(procedureName, parameters.ToArray());
        }

        public int ExecuteStoredProcedureNoReturn(string procedureName,IDbDataParameter[] parameters)
        {
        	commCntr++;
			if (commCntr>=MAX_COMM_QUERIES){
				commCntr=0;
				comm = EstablishCommand();
				if (trans!=null)
					comm.Transaction=trans;
			}
        	if (trans == null)
            {
                Logger.LogLine("Opening transaction for query since it is not performing a select");
                StartTransaction();
            }
            else if (trans != null)
                Logger.LogLine("Connection already has an open transaction");
            comm.CommandText = procedureName;
            comm.Parameters.Clear();
            comm.CommandType = CommandType.StoredProcedure;
            if (parameters != null)
            {
                foreach (IDbDataParameter param in parameters)
                {
                    comm.Parameters.Add(param);
                }
            }
            Logger.LogLine(comm.CommandText);
            if (parameters != null)
            {
                foreach (IDbDataParameter param in parameters)
                {
                    if (param.Value != null)
                        Logger.LogLine(param.ParameterName + ": " + param.Value.ToString());
                    else
                        Logger.LogLine(param.ParameterName + ": NULL");
                }
            }
            try
            {
                int ret =  comm.ExecuteNonQuery();
                Logger.LogLine("Successfully executed: "+comm.CommandText);
                return ret;
            }
            catch (Exception e)
            {
                string pars = "";
                if (parameters != null)
                {
                    foreach (IDbDataParameter param in parameters)
                    {
                        if (param.Value != null)
                            pars += param.ParameterName + ": " + param.Value.ToString() + "\n";
                        else
                            pars += param.ParameterName + ": NULL" + "\n";
                    }
                }
                throw new Exception("An error occured in executing the procedure: " + procedureName + "\nwith the parameters: " + pars, e);
            }
        }

		public void ExecuteQuery(string queryString)
		{
			ExecuteQuery(queryString,new IDbDataParameter[0]);
		}
		
		public void ExecuteQuery(string queryString,List<IDbDataParameter> parameters)
		{
			ExecuteQuery(queryString, parameters.ToArray());
		}

		public void ExecuteQuery(string queryString, IDbDataParameter[] parameters)
		{
            if (_readonly)
            {
                if (queryString.ToUpper().StartsWith("INSERT"))
                    throw new Exception("Unable to insert into a readonly database.");
                else if (queryString.ToUpper().StartsWith("DELETE"))
                    throw new Exception("Unable to delete from a readonly database.");
                else if (queryString.ToUpper().StartsWith("UPDATE"))
                    throw new Exception("Unable to update into a readonly database.");
            }
			commCntr++;
			if (commCntr>=MAX_COMM_QUERIES){
				commCntr=0;
				comm = EstablishCommand();
				if (trans!=null)
					comm.Transaction=trans;
			}
            if ((trans == null)&&!queryString.ToUpper().StartsWith("SELECT"))
            {
                Logger.LogLine("Opening transaction for query since it is not performing a select");
                StartTransaction();
            }
            else if (trans!=null)
                Logger.LogLine("Connection already has an open transaction");
			if ((queryString!=null)&&(queryString.Length>0)){
				reader = null;
				comm.CommandText = FormatParameters(queryString + " ", ref parameters);
				comm.Parameters.Clear();
                comm.CommandType = CommandType.Text;
				if (parameters != null)
				{
					foreach (IDbDataParameter param in parameters)
					{
						comm.Parameters.Add(param);
					}
				}
				Logger.LogLine(comm.CommandText);
                if (parameters != null)
                {
                    foreach (IDbDataParameter param in parameters)
                    {
                        if (param.Value != null)
                            Logger.LogLine(param.ParameterName + ": " + param.Value.ToString());
                        else
                            Logger.LogLine(param.ParameterName + ": NULL");
                    }
                }
                if ((reader != null) && (!reader.IsClosed))
                {
                    try
                    {
                        reader.Close();
                    }
                    catch (Exception e) { }
                }
                try
                {
                    reader = comm.ExecuteReader();
                    Logger.LogLine("Successfully executed: "+comm.CommandText);
                    CheckReadLock();
                }
                catch (Exception e)
                {
                    string pars = "";
                    if (parameters != null)
                    {
                        foreach (IDbDataParameter param in parameters)
                        {
                            if (param.Value != null)
                                pars += param.ParameterName + ": " + param.Value.ToString() + "\n";
                            else
                                pars += param.ParameterName + ": NULL" + "\n";
                        }
                    }
                    throw new Exception("An error occured in executing the query: " + queryString + "\nwith the parameters: " + pars, e);
                }
			}
		}

        public void ExecuteStoredProcedureReturn(string procedureName)
        {
            ExecuteStoredProcedureReturn(procedureName, new IDbDataParameter[0]);
        }

        public void ExecuteStoredProcedureReturn(string procedureName, List<IDbDataParameter> parameters)
        {
            ExecuteStoredProcedureReturn(procedureName, parameters.ToArray());
        }

        public void ExecuteStoredProcedureReturn(string procedureName, IDbDataParameter[] parameters)
        {
        	commCntr++;
			if (commCntr>=MAX_COMM_QUERIES){
				commCntr=0;
				comm = EstablishCommand();
				if (trans!=null)
					comm.Transaction=trans;
			}
            if (trans == null)
            {
                Logger.LogLine("Opening transaction for query since it is not performing a select");
                StartTransaction();
            }
            else if (trans != null)
                Logger.LogLine("Connection already has an open transaction");
            reader = null;
            comm.CommandText = procedureName;
            comm.Parameters.Clear();
            comm.CommandType = CommandType.StoredProcedure;
            if (parameters != null)
            {
                foreach (IDbDataParameter param in parameters)
                {
                    comm.Parameters.Add(param);
                }
            }
            Logger.LogLine(comm.CommandText);
            if (parameters != null)
            {
                foreach (IDbDataParameter param in parameters)
                {
                    if (param.Value != null)
                        Logger.LogLine(param.ParameterName + ": " + param.Value.ToString());
                    else
                        Logger.LogLine(param.ParameterName + ": NULL");
                }
            }
            if ((reader!=null)&&(!reader.IsClosed))
            {
                try
                {
                    reader.Close();
                }
                catch (Exception e) { }
            }
            try
            {
                reader = comm.ExecuteReader();
                Logger.LogLine("Successfully executed: "+comm.CommandText);
                CheckReadLock();
            }
            catch (Exception e)
            {
                string pars = "";
                if (parameters != null)
                {
                    foreach (IDbDataParameter param in parameters)
                    {
                        if (param.Value != null)
                            pars += param.ParameterName + ": " + param.Value.ToString() + "\n";
                        else
                            pars += param.ParameterName + ": NULL" + "\n";
                    }
                }
                throw new Exception("An error occured in executing the procedure: " + procedureName+ "\nwith the parameters: " + pars, e);
            }
        }

        #region CheckReadLock
        private void CheckReadLock()
        {
            Thread runner = new Thread(new ThreadStart(RunReadCheck));
            runner.IsBackground = true;
            for (int x = 0; x < MAX_READCHECK_TRIES; x++)
            {
                try
                {
                    Logger.LogLine("Checking data reader lock attempt "+x.ToString());
                    runner.Start();
                    runner.Join(new TimeSpan(0, 0, 0, pool.readTimeout));
                    if (firstRead)
                        break;
                }
                catch (Exception e)
                {
                }
                if (!firstRead)
                {
                    Logger.LogLine("Reading of first result failed with timeout of " + pool.readTimeout.ToString() + " seconds, resetting connection and reattempting query.");
                    ResetConnection(true);
                    reader = comm.ExecuteReader();
                }
            }
            if (!firstRead)
                throw new Exception("Unable to get around locked database connection after 5 attempted resets, query["+comm.CommandText+"] aborted.");
        }

        private void RunReadCheck()
        {
            firstReadResult = reader.Read();
            firstRead = true;
        }
        #endregion

        #region Reader
        public int Depth {
			get {
				return reader.Depth;
			}
		}
		
		public bool IsClosed {
			get {
				return reader.IsClosed;
			}
		}
		
		public int RecordsAffected {
			get {
				return reader.RecordsAffected;
			}
		}
		
		public int FieldCount {
			get {
				return reader.FieldCount;
			}
		}
		
		public void Close()
		{
			reader.Close();
		}
		
		public DataTable GetSchemaTable()
		{
			return reader.GetSchemaTable();
		}
		
		public bool NextResult()
		{
			return reader.NextResult();
		}
		
		public bool Read()
		{
            if ((reader == null) || reader.IsClosed)
                return false;
            else
            {
                if (!firstRead)
                    return reader.Read();
                else
                {
                    firstRead = false;
                    return firstReadResult;
                }
            }
		}
		
		public void Dispose()
		{
			reader.Dispose();
		}
		
		public string GetName(int i)
		{
			return reader.GetName(i);
		}
		
		public string GetDataTypeName(int i)
		{
			return reader.GetDataTypeName(i);
		}

        public int GetOrdinal(string name)
        {
            return reader.GetOrdinal(name);
        }

        public bool ContainsField(string name)
        {
            try
            {
                reader.GetOrdinal(name);
                return true;
            }
            catch (Exception e)
            {
                return false;
            }
        }
		
		public byte GetByte(int i)
		{
			return reader.GetByte(i);
		}
		
		public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
		{
			return reader.GetBytes(i,fieldOffset,buffer,bufferoffset,length);
		}
		
		public char GetChar(int i)
		{
			return reader.GetChar(i);
		}
		
		public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
		{
			return reader.GetChars(i,fieldoffset,buffer,bufferoffset,length);
		}
		
		public Guid GetGuid(int i)
		{
			return reader.GetGuid(i);
		}
		
		public short GetInt16(int i)
		{
			return reader.GetInt16(i);
		}
		
		public long GetInt64(int i)
		{
			return reader.GetInt64(i);
		}
		
		public float GetFloat(int i)
		{
			return reader.GetFloat(i);
		}
		
		public double GetDouble(int i)
		{
			return reader.GetDouble(i);
		}
		
		public decimal GetDecimal(int i)
		{
			return reader.GetDecimal(i);
		}
		
		public DateTime GetDateTime(int i)
		{
			return reader.GetDateTime(i);
		}
		
		public IDataReader GetData(int i)
		{
			return reader.GetData(i);
		}
		
		public bool IsDBNull(int i)
		{
			return reader.IsDBNull(i);
		}
		
		public virtual bool GetBoolean(int i)
		{
			return reader.GetBoolean(i);
		}
		
		public int GetInt32(int i)
		{
			return reader.GetInt32(i);
		}
		
		public string GetString(int i)
		{
			return reader.GetString(i);
		}

        public object this[int i]
        {
            get
            {
                return this.GetValue(i);
            }
        }

        public object this[string name]
        {
            get
            {
                return this.GetValue(this.GetOrdinal(name));
            }
        }

        public virtual Type GetFieldType(int i)
        {
            return reader.GetFieldType(i);
        }

        public virtual object GetValue(int i)
        {
            return reader.GetValue(i);
        }

        public virtual int GetValues(object[] values)
        {
            object[] ret = new object[reader.FieldCount];
            for (int x = 0; x < reader.FieldCount; x++)
            {
                if (reader.IsDBNull(x))
                    ret[x] = null;
                else
                    ret[x] = this.GetValue(x);
            }
            return ret.Length;
        }
		
		public List<string> FieldNames{
			get{
				List<string> ret = new List<string>();
				for (int x=0;x<reader.FieldCount;x++)
				{
					ret.Add(reader.GetName(x));
				}
				return ret;
			}
		}
		#endregion
		
	}
}