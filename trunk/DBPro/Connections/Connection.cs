using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;

using Org.Reddragonit.Dbpro.Connections.Parameters;
using Org.Reddragonit.Dbpro.Structure;
using Org.Reddragonit.Dbpro.Structure.Mapping;
using FieldNamePair = Org.Reddragonit.Dbpro.Structure.Mapping.TableMap.FieldNamePair;
using FieldType = Org.Reddragonit.Dbpro.Structure.Attributes.FieldType;
using VersionTypes = Org.Reddragonit.Dbpro.Structure.Attributes.VersionField.VersionTypes;
using System.Reflection;

namespace Org.Reddragonit.Dbpro.Connections
{
	
	
	public abstract class Connection : IDataReader
	{
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
					_qb=new QueryBuilder(pool,this);
				return _qb;
			}
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
		internal abstract void GetAddAutogen(ExtractedTableMap map,ConnectionPool pool, out List<IdentityField> identities, out List<Generator> generators, out List<Trigger> triggers);
		internal abstract List<Trigger> GetVersionTableTriggers(ExtractedTableMap table,VersionTypes versionType,ConnectionPool pool);
		
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
		
		public Connection(ConnectionPool pool,string connectionString){
			creationTime=System.DateTime.Now;
			this.connectionString=connectionString;
			conn = EstablishConnection();
			conn.Open();
			trans=conn.BeginTransaction();
			comm = EstablishCommand();
			comm.Transaction = trans;
			this.pool=pool;
			isConnected=true;
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
            try
            {
                conn.Close();
            }
            catch (Exception e) { }
			isConnected=false;
		}
		
		internal bool isPastKeepAlive(long secondsToLive)
		{
			return !(secondsToLive<0)||((System.DateTime.Now.Ticks-creationTime.Ticks)>secondsToLive);
		}
		
		internal void Reset()
		{
			trans.Rollback();
			trans = conn.BeginTransaction();
			comm.Transaction=trans;
		}
		
		public void Commit()
		{
            System.Diagnostics.Debug.WriteLine("COMMIT");
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
				comm.Transaction=trans;
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

        public void Delete(Table table)
        {
            if (!table.IsSaved)
                throw new Exception("Unable to delete an object from the database that is not saved.");
            if (table.ConnectionName != ConnectionName)
                throw new Exception("Unable to delete an object from a database connection it is not part of.");
            List<IDbDataParameter> pars;
            string del = queryBuilder.Delete(table, out pars);
            ExecuteNonQuery(del, pars);
        }
		
		private Table Update(Table table)
		{
			if (table.ConnectionName!=ConnectionName)
			{
				throw new Exception("Cannot update an entry into a table into the database connection that it was not specified for.");
			}
			if ((table.ChangedFields==null)||(table.ChangedFields.Count==0))
				return table;
			TableMap map = ClassMapper.GetTableMap(table.GetType());
			if (map.ParentType!=null)
			{
				Table ta = Update((Table)table.ToType(map.ParentType,null));
				table.CopyValuesFrom(ta);
			}
			foreach (Type t in map.ForeignTables)
			{
				foreach (ExternalFieldMap efm in map.GetFieldInfoForForeignTable(t))
				{
					Table ext = (Table)table.GetField(map.GetClassPropertyName(efm));
					if (ext != null)
					{
						ext=Save(ext);
						table.SetField(map.GetClassPropertyName(efm),ext);
					}
				}
			}
			foreach (ExternalFieldMap efm in map.ExternalFieldMapArrays)
			{
				Table[] values = (Table[])table.GetField(map.GetClassFieldName(efm));
				if (values!=null)
				{
					foreach (Table t in values)
					{
						this.Save(t);
					}
				}
			}
			List<IDbDataParameter> pars = new List<IDbDataParameter>();
			string query = queryBuilder.Update(table,out pars);
            if (query.Length>0)
			    this.ExecuteNonQuery(query, pars);
			foreach (ExternalFieldMap efm in map.ExternalFieldMapArrays)
			{
				Dictionary<string, List<List<IDbDataParameter>>> queries = queryBuilder.UpdateMapArray(table,efm);
				foreach (string str in queries.Keys)
				{
					foreach (List<IDbDataParameter> p in queries[str])
					{
						ExecuteNonQuery(str,p);
					}
				}
			}
			foreach (InternalFieldMap ifm in map.Fields)
			{
				if (ifm.IsArray)
					InsertArrayValue(table,map,(Array)table.GetField(map.GetClassFieldName(ifm.FieldName)),ifm);
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
                    ret=Update(table);
                else
                    ret=table;
            }
            else
            {
                ret=Insert(table);
            }
            if (!ret.IsProxied)
                ret = (Table)LazyProxy.Instance(ret);
            return ret;
        }
		
		private Table Insert(Table table)
		{
			TableMap map = ClassMapper.GetTableMap(table.GetType());
			if (map.ParentType!=null)
			{
				Table ta = Insert((Table)table.ToType(map.ParentType,null));
				table.CopyValuesFrom(ta);
			}
			foreach (Type t in map.ForeignTables)
			{
				foreach(ExternalFieldMap efm in map.GetFieldInfoForForeignTable(t))
				{
					Table ext = (Table)table.GetField(map.GetClassPropertyName(efm));
					if (ext != null)
					{
						ext=Save(ext);
						table.SetField(map.GetClassPropertyName(efm),ext);
					}
				}
			}
			foreach (ExternalFieldMap efm in map.ExternalFieldMapArrays)
			{
				Table[] vals = (Table[])table.GetField(map.GetClassFieldName(efm));
				if (vals!=null)
				{
					foreach (Table t in vals)
					{
						this.Save(t);
					}
				}
			}
			string query = "";
			string select="";
			List<IDbDataParameter> pars = new List<IDbDataParameter>();
			List<IDbDataParameter> selectPars = new List<IDbDataParameter>();
			query=queryBuilder.Insert(table,out pars,out select,out selectPars);
			ExecuteNonQuery(query,pars);
			if (select!=null)
			{
				ExecuteQuery(select,selectPars);
				Read();
				foreach (InternalFieldMap ifm in map.InternalPrimaryKeys)
				{
					if (ifm.AutoGen)
					{
						table.SetField(map.GetClassFieldName(ifm.FieldName),this[0]);
						break;
					}
				}
				Close();
			}
			table._isSaved=true;
			table.LoadStatus= LoadStatus.Complete;
			foreach (ExternalFieldMap efm in map.ExternalFieldMapArrays)
			{
				Dictionary<string, List<List<IDbDataParameter>>> queries = queryBuilder.UpdateMapArray(table,efm);
				if (queries!=null)
				{
					foreach (string str in queries.Keys)
					{
						foreach (List<IDbDataParameter> p in queries[str])
						{
							ExecuteNonQuery(str,p);
						}
					}
				}
			}
			foreach (InternalFieldMap ifm in map.Fields)
			{
				if (ifm.IsArray)
				{
					InsertArrayValue(table,map,(Array)table.GetField(map.GetClassFieldName(ifm.FieldName)),ifm);
				}
			}
			return table;
		}
		
		private void InsertArrayValue(Table table,TableMap map,Array values,InternalFieldMap field)
		{
			List<IDbDataParameter> pars = new List<IDbDataParameter>();
			string query="";
			string fields="";
			string paramString="";
			string conditions = "";
			pars = new List<IDbDataParameter>();
			foreach (InternalFieldMap primary in map.PrimaryKeys)
			{
				fields+=Pool.CorrectName(map.Name+"_"+primary.FieldName)+", ";
				paramString+=Pool.CorrectName(queryBuilder.CreateParameterName(map.Name+"_"+primary.FieldName))+", ";
				conditions+=Pool.CorrectName(map.Name+"_"+primary.FieldName)+" = "+Pool.CorrectName(queryBuilder.CreateParameterName(map.Name+"_"+primary.FieldName))+" AND ";
				pars.Add(CreateParameter(Pool.CorrectName(queryBuilder.CreateParameterName(map.Name+"_"+primary.FieldName)),QueryBuilder.LocateFieldValue(table,map,primary.FieldName,pool)));
			}
			query = queryBuilder.Delete(Pool.CorrectName(map.Name+"_"+field.FieldName),conditions.Substring(0,conditions.Length-4));
			ExecuteNonQuery(query,pars);
			if (values!=null)
			{
				pars.Add(CreateParameter(Pool.CorrectName(queryBuilder.CreateParameterName(field.FieldName+"_VALUE")),null));
				fields+=Pool.CorrectName(field.FieldName+"_VALUE");
				paramString+=Pool.CorrectName(queryBuilder.CreateParameterName(field.FieldName+"_VALUE"));
				query = queryBuilder.Insert(Pool.CorrectName(map.Name+"_"+field.FieldName),fields,paramString);
				foreach (object obj in values)
				{
					pars.RemoveAt(pars.Count-1);
					pars.Add(CreateParameter(Pool.CorrectName(queryBuilder.CreateParameterName(field.FieldName+"_VALUE")),obj));
					ExecuteNonQuery(query,pars);
				}
			}
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
					string fields = "";
					foreach (InternalFieldMap ifm in external.PrimaryKeys)
					{
						fields+=pool.CorrectName("child_"+ifm.FieldName)+" AS "+ifm.FieldName+", ";
					}
					fields = fields.Substring(0,fields.Length-2);
					string conditions= "";
                    foreach (InternalFieldMap ifm in map.PrimaryKeys)
                    {
                        conditions += map.Name + "_" + external.Name + "." + Pool.CorrectName("parent_" + ifm.FieldName) + " = " + queryBuilder.CreateParameterName("parent_" + ifm.FieldName) + " AND ";
                        pars.Add(CreateParameter(queryBuilder.CreateParameterName("parent_" + ifm.FieldName), QueryBuilder.LocateFieldValue(t, map, ifm.FieldName, pool)));
                    }
					conditions=conditions.Substring(0,conditions.Length-4);
					query = string.Format(queryBuilder.SelectWithConditions,fields,pool.CorrectName(map.Name+"_"+external.Name),conditions);
					ArrayList values = new ArrayList();
					ExecuteQuery(query, pars);
					while (Read())
					{
						Table ta = (Table)LazyProxy.Instance(f.Type.GetConstructor(System.Type.EmptyTypes).Invoke(new object[0]));
						ta.SetValues(this);
						ta.LoadStatus=LoadStatus.Partial;
						values.Add(ta);
					}
					Close();
					Array obj = Array.CreateInstance(f.Type , values.Count);
                    for (int x = 0; x < values.Count; x++)
                        ((object[])obj)[x] = values[x];
					t.SetField(map.GetClassFieldName(f),obj);
				}
			}
			foreach (InternalFieldMap ifm in map.Fields)
			{
				if (ifm.IsArray)
				{
					query = "SELECT "+Pool.CorrectName(ifm.FieldName+"_VALUE")+" FROM "+
						Pool.CorrectName(map.Name+"_"+ifm.FieldName)+" WHERE ";
					foreach (InternalFieldMap primary in map.PrimaryKeys)
						query+=" "+Pool.CorrectName(map.Name+"_"+primary.FieldName)+" = "+Pool.CorrectName(queryBuilder.CreateParameterName(map.Name+"_"+primary.FieldName))+" AND ";
					query = query.Substring(0,query.Length-4);
					query+=" ORDER BY "+Pool.CorrectName(map.Name+"_"+ifm.FieldName+"_ID")+" ASC";
					foreach (Table t in ret)
					{
						List<IDbDataParameter> pars = new List<IDbDataParameter>();
						foreach (InternalFieldMap primary in map.PrimaryKeys)
							pars.Add(CreateParameter(Pool.CorrectName(queryBuilder.CreateParameterName(map.Name+"_"+primary.FieldName)),QueryBuilder.LocateFieldValue(t,map,primary.FieldName,pool)));
						ArrayList values = new ArrayList();
						ExecuteQuery(query,pars);
						while (Read())
						{
							values.Add(this[0]);
						}
						Close();
						Array obj = Array.CreateInstance(ifm.ObjectType,values.Count);
                        for (int x = 0; x < values.Count; x++)
                            ((object[])obj)[x] = values[x];
						t.SetField(map.GetClassFieldName(ifm),obj);
					}
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
				Table t = (Table)LazyProxy.Instance(type.GetConstructor(System.Type.EmptyTypes).Invoke(new object[0]));
				t.SetValues(this);
				t.LoadStatus=LoadStatus.Complete;
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
		
		public List<Table> Select(System.Type type,SelectParameter[] parameters)
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
			string query = queryBuilder.Select(type,parameters,out pars);
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
			string query = queryBuilder.SelectPaged(type,parameters,out pars,StartIndex,RowCount);
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
					if (par.Value==null||
					    ((par is Npgsql.NpgsqlParameter)&&(par.Value.ToString().Length==0))
					   )
					{
						ret=ret.Replace("= "+par.ParameterName+" ","IS NULL ");
						ret=ret.Replace(par.ParameterName+",","NULL,");
						ret=ret.Replace(par.ParameterName+")","NULL)");
					}else
						pars.Add(par);
				}
				parameters=pars.ToArray();
				return ret;
			}
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
			System.Diagnostics.Debug.WriteLine(comm.CommandText);
            if (parameters != null)
            {
                foreach (IDbDataParameter param in parameters)
                {
                    if (param.Value!=null)
                        System.Diagnostics.Debug.WriteLine(param.ParameterName+": "+param.Value.ToString());
                    else
                        System.Diagnostics.Debug.WriteLine(param.ParameterName +": NULL");
                }
            }
            try
            {
                return comm.ExecuteNonQuery();
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
            System.Diagnostics.Debug.WriteLine(comm.CommandText);
            if (parameters != null)
            {
                foreach (IDbDataParameter param in parameters)
                {
                    if (param.Value != null)
                        System.Diagnostics.Debug.WriteLine(param.ParameterName + ": " + param.Value.ToString());
                    else
                        System.Diagnostics.Debug.WriteLine(param.ParameterName + ": NULL");
                }
            }
            try
            {
                return comm.ExecuteNonQuery();
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
				System.Diagnostics.Debug.WriteLine(comm.CommandText);
                if (parameters != null)
                {
                    foreach (IDbDataParameter param in parameters)
                    {
                        if (param.Value != null)
                            System.Diagnostics.Debug.WriteLine(param.ParameterName + ": " + param.Value.ToString());
                        else
                            System.Diagnostics.Debug.WriteLine(param.ParameterName + ": NULL");
                    }
                }
                try
                {
                    reader = comm.ExecuteReader();
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
            System.Diagnostics.Debug.WriteLine(comm.CommandText);
            if (parameters != null)
            {
                foreach (IDbDataParameter param in parameters)
                {
                    if (param.Value != null)
                        System.Diagnostics.Debug.WriteLine(param.ParameterName + ": " + param.Value.ToString());
                    else
                        System.Diagnostics.Debug.WriteLine(param.ParameterName + ": NULL");
                }
            }
            try
            {
                reader = comm.ExecuteReader();
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
			if ((reader==null)||reader.IsClosed)
				return false;
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
		
		public bool ContainsField(string name)
		{
			try{
				reader.GetOrdinal(name);
				return true;
			}catch (Exception e)
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
