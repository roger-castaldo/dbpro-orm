using System;
using System.Data;
using System.Collections.Generic;
using System.Collections;
using Org.Reddragonit.Dbpro.Structure;
using Org.Reddragonit.Dbpro.Structure.Mapping;
using FieldNamePair = Org.Reddragonit.Dbpro.Structure.Mapping.TableMap.FieldNamePair;
using FieldType = Org.Reddragonit.Dbpro.Structure.Attributes.Field.FieldType;
using VersionTypes = Org.Reddragonit.Dbpro.Structure.Attributes.VersionField.VersionTypes;

namespace Org.Reddragonit.Dbpro.Connections
{
	public abstract class Connection : IDataReader
	{
		
		internal struct ForiegnRelationMap
		{
			private string _internalField;
			private string _externalField;
			private string _onUpdate;
			private string _onDelete;
			
			public string InternalField{
				get{return _internalField;}
				set{_internalField=value;}
			}
			
			public string ExternalField{
				get{return _externalField;}
				set{_externalField=value;}
			}
			
			public string OnUpdate{
				get{return _onUpdate;}
				set{_onUpdate=value;}
			}
			
			public string OnDelete{
				get{return _onDelete;}
				set{_onDelete=value;}
			}
		}

        internal struct ExtractedTableMap
        {
            private string _tableName;
            private VersionTypes? _versionType;
            private List<ExtractedFieldMap> _fields;

            public ExtractedTableMap(string tableName)
            {
                _tableName=tableName;
                _fields=new List<ExtractedFieldMap>();
                _versionType=null;
            }

            public string TableName{get{return _tableName;}}
            public List<ExtractedFieldMap> Fields{get{return _fields;}set{_fields=value;}}
            public VersionTypes? VersionType{get{return _versionType;}set{_versionType=value;}}
            public List<ExtractedFieldMap> PrimaryKeys{
            	get{
            		List<ExtractedFieldMap> ret = new List<ExtractedFieldMap>();
            		foreach (ExtractedFieldMap efm in Fields)
            		{
            			if (efm.PrimaryKey)
            				ret.Add(efm);
            		}
            		return ret;
            	}
            }
        }

        internal struct ExtractedFieldMap
        {
            private string _fieldName;
            private string _type;
            private long _size;
            private bool _primaryKey;
            private bool _nullable;
            private bool _autogen;
            private string _updateAction;
            private string _deleteAction;
            private string _externalTable;
            private string _externalField;
            private bool _versioned;
            
            public ExtractedFieldMap(string fieldName, string type, long size, bool primary, bool nullable,bool autogen,bool versioned) 
            {
            	_fieldName = fieldName;
                _type = type;
                _size = size;
                _primaryKey = primary;
                _nullable = nullable;
                _externalField = null;
                _updateAction = null;
                _deleteAction = null;
                _externalTable = null;
                _autogen=autogen;
                _versioned=versioned;
            }

            public ExtractedFieldMap(string fieldName, string type, long size, bool primary, bool nullable,bool versioned) : this(fieldName,type,size,primary,nullable,versioned,false)
            {
            }
            
            public ExtractedFieldMap(string fieldName, string type, long size, bool primary, bool nullable) : this(fieldName,type,size,primary,nullable,false,false)
            {
            }

            public string FieldName { get { return _fieldName; } }
            public string Type { get { return _type; } }
            public long Size { get { return _size; } }
            public bool PrimaryKey { get { return _primaryKey; } }
            public bool Nullable { get { return _nullable; } }
            public bool AutoGen {get {return _autogen;} set{_autogen=value;}}
            public string UpdateAction { get { return _updateAction; } set { _updateAction = value; } }
            public string DeleteAction { get { return _deleteAction; } set { _deleteAction = value; } }
            public string ExternalTable { get { return _externalTable; } set { _externalTable= value; } }
            public string ExternalField { get { return _externalField; } set { _externalField = value; } }
            public bool Versioned{get{return _versioned;}set{_versioned=value;}}
        }

		private ConnectionPool pool;	
		protected IDbConnection conn;
		protected IDbCommand comm;
		protected IDataReader reader;
		protected IDbTransaction trans;
		private bool isConnected=false;
		private DateTime creationTime;
		protected string connectionString;
		
		private QueryBuilder _qb;
		internal virtual QueryBuilder queryBuilder
		{
			get{
				if (_qb==null)
					_qb=new QueryBuilder();
				return _qb;
			}
		}
		
		protected abstract IDbConnection EstablishConnection();
		protected abstract IDbCommand EstablishCommand();
		protected abstract List<string> ConstructCreateStrings(Table table);
		internal abstract IDbDataParameter CreateParameter(string parameterName,object parameterValue);
		internal abstract string TranslateFieldType(Org.Reddragonit.Dbpro.Structure.Attributes.Field.FieldType type,int fieldLength);
        internal abstract List<ExtractedTableMap> GetTableList();
        internal abstract List<string> GetDropConstraintsScript();
        internal abstract List<string> GetDropTableString(string table,bool isVersioned);
        internal abstract List<string> GetDropAutogenStrings(string table, string field,string type);
        internal abstract List<string> GetAddAutogenString(string table, string field, string type);
        internal abstract List<string> GetCreateTableStringsForAlterations(ExtractedTableMap table);
        internal abstract List<string> GetVersionTableTriggers(string tableName,string versionTableName,string versionFieldName,VersionTypes versionType,List<ExtractedFieldMap> fields);
		
		public Connection(ConnectionPool pool,string connectionString){
			creationTime=System.DateTime.Now;
			this.connectionString=connectionString;
			conn = EstablishConnection();
			conn.Open();
			trans=conn.BeginTransaction();
			comm = EstablishCommand();
            comm.Transaction = trans;
		}
        
        internal string ConnectionName
        {
        	get{return pool.ConnectionName;}
        }
		
		internal void Disconnect()
		{
			try{
				trans.Commit();
			}catch (Exception e){}
			conn.Close();
			isConnected=false;
		}
		
		internal bool isPastKeepAlive(long secondsToLive)
		{
			return !(secondsToLive<0)||((System.DateTime.Now.Ticks-creationTime.Ticks)>secondsToLive);
		}
		
		public void Commit()
		{
			trans.Commit();
			trans=conn.BeginTransaction();
			comm.Transaction=trans;
		}
		
		public void RollBack()
		{
			trans.Rollback();
			trans=conn.BeginTransaction();
		}
		
		public void CloseConnection()
		{
			if (isConnected)
			{
				if ((reader!=null)&&!reader.IsClosed)
					reader.Close();
				trans.Commit();
				trans=null;
				trans=conn.BeginTransaction();
				comm.CommandText="";
				comm.Parameters.Clear();
				pool.returnConnection(this);
			}
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
		
		public void CreateTable(Table table,bool debug)
		{
			if (table.ConnectionName!=ConnectionName)
			{
				throw new Exception("Cannot create a table into the database connection that it was not specified for.");
			}
			if (debug) 
			{
				foreach (string str in ConstructCreateStrings(table))
				{
					System.Diagnostics.Debug.WriteLine(str);
				}
			}else
			{
				foreach (string str in ConstructCreateStrings(table))
				{
					ExecuteNonQuery(str);
				}
				this.Commit();
			}
		}
		
		private Table Update(Table table)
		{
			if (table.ConnectionName!=ConnectionName)
			{
				throw new Exception("Cannot update an entry into a table into the database connection that it was not specified for.");
			}
			TableMap map = ClassMapper.GetTableMap(table.GetType());
            foreach (Type t in map.ForiegnTables)
            {
                Table ext = (Table)table.GetType().GetProperty(map.GetClassPropertyName(map.GetFieldInfoForForiegnTable(t))).GetValue(table, new object[0]);
                if (ext != null)
                {
                    Save(ext);
                    table.GetType().GetProperty(map.GetClassPropertyName(map.GetFieldInfoForForiegnTable(t))).SetValue(table, ext, new object[0]);
                }
            }
            foreach (ExternalFieldMap efm in map.ExternalFieldMapArrays)
            {
                Table[] values = (Table[])table.GetType().GetProperty(map.GetClassFieldName(efm)).GetValue(table, new object[0]);
                foreach (Table t in values)
                {
                    this.Save(t);
                }
            }
            List<IDbDataParameter> pars = new List<IDbDataParameter>();
            string query = queryBuilder.Update(table,out pars,this);
            this.ExecuteNonQuery(query, pars);
            foreach (ExternalFieldMap efm in map.ExternalFieldMapArrays)
            {
            	Dictionary<string, List<List<IDbDataParameter>>> queries = queryBuilder.UpdateMapArray(table,efm,this);
            	foreach (string str in queries.Keys)
            	{
            		foreach (List<IDbDataParameter> p in queries[str])
            		{
            			ExecuteNonQuery(str,p);
            		}
            	}
            }
			return table;
		}

        public Table Save(Table table)
        {
        	if (table.ConnectionName!=ConnectionName)
			{
				throw new Exception("Cannot insert an entry into a table into the database connection that it was not specified for.");
			}
            if (table.IsSaved)
            {
                return Update(table);
            }
            else
            {
                return Insert(table);
            }
        }
		
		private Table Insert(Table table)
		{
            TableMap map = ClassMapper.GetTableMap(table.GetType());
            foreach (Type t in map.ForiegnTables)
            {
                Table ext = (Table)table.GetType().GetProperty(map.GetClassPropertyName(map.GetFieldInfoForForiegnTable(t))).GetValue(table, new object[0]);
                if (ext != null)
                {
                    Save(ext);
                    table.GetType().GetProperty(map.GetClassPropertyName(map.GetFieldInfoForForiegnTable(t))).SetValue(table, ext, new object[0]);
                }
            }
            foreach (ExternalFieldMap efm in map.ExternalFieldMapArrays)
            {
                Table[] vals = (Table[])table.GetType().GetProperty(map.GetClassFieldName(efm)).GetValue(table, new object[0]);
                foreach (Table t in vals)
                {
                    this.Save(t);
                }
            }
            if (map.ParentType!=null)
            {
            	Table ta = Insert((Table)Convert.ChangeType(table,map.ParentType));
            	table.CopyValuesFrom(ta);
            }
			string query = "";
			string select="";
			List<IDbDataParameter> pars = new List<IDbDataParameter>();
			List<IDbDataParameter> selectPars = new List<IDbDataParameter>();
			query=queryBuilder.Insert(table,out pars,out select,out selectPars,this);
			ExecuteNonQuery(query,pars);
			ExecuteQuery(select,selectPars);
			Read();
			table.SetValues(this);
            Close();
            foreach (ExternalFieldMap efm in map.ExternalFieldMapArrays)
            {
                Dictionary<string, List<List<IDbDataParameter>>> queries = queryBuilder.UpdateMapArray(table,efm,this);
            	foreach (string str in queries.Keys)
            	{
            		foreach (List<IDbDataParameter> p in queries[str])
            		{
            			ExecuteNonQuery(str,p);
            		}
            	}
            }
			return table;
		}

        private List<Table> AddArrayedTablesToSelect(List<Table> ret, System.Type type)
        {
            string query = "";
            TableMap map = ClassMapper.GetTableMap(type);
            foreach (ExternalFieldMap f in map.ExternalFieldMapArrays)
            {
                foreach (Table t in ret)
                {
                    List<IDbDataParameter> pars = new List<IDbDataParameter>();
                    TableMap external = ClassMapper.GetTableMap(f.Type);
                    query = queryBuilder.SelectAll(f.Type);
                    if (query.Contains(" WHERE "))
                    {
                        query = query.Replace(" WHERE ", ", " + map.Name + "_" + external.Name + " WHERE ");
                    }
                    else
                    {
                        query += ", " + map.Name + "_" + external.Name + " WHERE ";
                    }
                    foreach (InternalFieldMap ifm in map.PrimaryKeys)
                    {
                        query += map.Name + "_" + external.Name + "." + ifm.FieldName + " = @" + ifm.FieldName + " AND ";
                        pars.Add(CreateParameter("@" + ifm.FieldName, t.GetType().GetProperty(map.GetClassFieldName(ifm)).GetValue(t, new object[0])));
                    }
                    foreach (InternalFieldMap ifm in external.PrimaryKeys)
                    {
                        query += map.Name + "_" + external.Name + "." + ifm.FieldName + " = " + external.Name + "." + ifm.FieldName + " AND ";
                    }
                    ArrayList values = new ArrayList();
                    ExecuteQuery(query.Substring(0, query.Length - 4), pars);
                    while (Read())
                    {
                        Table ta = (Table)f.Type.GetConstructor(System.Type.EmptyTypes).Invoke(new object[0]);
                        ta.SetValues(this);
                        values.Add(ta);
                    }
                    Close();
                    Array obj = Array.CreateInstance(f.Type , values.Count);
                    values.CopyTo(obj);
                    t.GetType().GetProperty(map.GetClassFieldName(f)).SetValue(t, obj, new object[0]);
                }
            }
            return ret;
        }
		
		public List<Table> SelectAll(System.Type type)
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
			ExecuteQuery(queryBuilder.SelectAll(type));
			while (Read())
			{
				Table t = (Table)type.GetConstructor(System.Type.EmptyTypes).Invoke(new object[0]);
				t.SetValues(this);
				ret.Add(t);
			}
			Close();
            ret = AddArrayedTablesToSelect(ret, type);
			return ret;
		}
		
		public List<Table> Select(System.Type type,List<SelectParameter> parameters)
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
			string query = queryBuilder.Select(type,parameters,out pars,this);
			ExecuteQuery(query,pars);
			while (Read())
			{
				Table t = (Table)type.GetConstructor(System.Type.EmptyTypes).Invoke(new object[0]);
				t.SetValues(this);
				ret.Add(t);
			}
			Close();
            ret = AddArrayedTablesToSelect(ret, type);
			return ret;
		}
		
		private string FormatParameters(string queryString,List<IDbDataParameter> parameters)
		{
			if (parameters==null)
				return queryString;
			else
			{
				string ret = queryString;
				foreach (IDbDataParameter par in parameters)
				{
					if (par.Value==null)
					{
						ret=ret.Replace("= "+par.ParameterName+" ","is "+par.ParameterName+" ");
					}
				}
				return ret;
			}
		}
		
		public int ExecuteNonQuery(string queryString)
		{
			return ExecuteNonQuery(queryString,null);
		}
		
		public int ExecuteNonQuery(string queryString,List<IDbDataParameter> parameters)
		{
			comm.CommandText=FormatParameters(queryString+" ",parameters);
			comm.Parameters.Clear();
			if (parameters!=null)
			{
				foreach (IDbDataParameter param in parameters)
				{
					comm.Parameters.Add(param);
				}
			}
			System.Diagnostics.Debug.WriteLine(comm.CommandText);
			return comm.ExecuteNonQuery();
		}
		
		public void ExecuteQuery(string queryString)
		{
			ExecuteQuery(queryString,null);
		}
		
		public void ExecuteQuery(string queryString,List<IDbDataParameter> parameters)
		{
			comm.CommandText=FormatParameters(queryString+" ",parameters);
			comm.Parameters.Clear();
			if (parameters!=null)
			{
				foreach (IDbDataParameter param in parameters)
				{
					comm.Parameters.Add(param);
				}
			}
			reader=comm.ExecuteReader();
		}
		
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
			return reader.Read();
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
		
		public Type GetFieldType(int i)
		{
			return reader.GetFieldType(i);
		}
		
		public object GetValue(int i)
		{
			return reader.GetValue(i);
		}
		
		public int GetValues(object[] values)
		{
			return GetValues(values);
		}
		
		public int GetOrdinal(string name)
		{
			return reader.GetOrdinal(name);
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
			return reader.GetInt32(i);
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
		public object this[int i]
		{
			get {
				return reader[i];
			}
		}
		
		public object this[string name]
		{
			get {
				return reader[name];
			}
		}
		
		public bool GetBoolean(int i)
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
