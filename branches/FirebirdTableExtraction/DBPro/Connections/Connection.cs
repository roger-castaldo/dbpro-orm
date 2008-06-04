using System;
using System.Data;
using System.Collections.Generic;
using System.Collections;
using Org.Reddragonit.Dbpro.Structure;
using Org.Reddragonit.Dbpro.Structure.Mapping;
using FieldNamePair = Org.Reddragonit.Dbpro.Structure.Mapping.TableMap.FieldNamePair;

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
		
		protected abstract IDbConnection EstablishConnection();
		protected abstract IDbCommand EstablishCommand();
		protected abstract IDbDataParameter CreateParameter(string parameterName,object parameterValue);
		protected abstract List<string> ConstructCreateStrings(Table table);
		protected abstract string TranslateFieldType(Org.Reddragonit.Dbpro.Structure.Attributes.Field.FieldType type,int fieldLength);
		
		public Connection(ConnectionPool pool,string connectionString){
			creationTime=System.DateTime.Now;
			this.connectionString=connectionString;
			conn = EstablishConnection();
			conn.Open();
			trans=conn.BeginTransaction();
			comm = EstablishCommand();
            comm.Transaction = trans;
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
            string query = "UPDATE " + map.Name + " SET ";
            string fields = "";
            string conditions = "";
            List<IDbDataParameter> pars = new List<IDbDataParameter>();
            foreach (FieldNamePair fnp in map.FieldNamePairs)
            {
                if (map[fnp] is ExternalFieldMap)
                {
                    if (!((ExternalFieldMap)map[fnp]).IsArray)
                    {
                        TableMap relatedTableMap = ClassMapper.GetTableMap(table.GetType().GetProperty(fnp.ClassFieldName).PropertyType);
                        if (table.GetType().GetProperty(fnp.ClassFieldName).GetValue(table, new object[0]) == null)
                        {
                            foreach (FieldMap fm in relatedTableMap.PrimaryKeys)
                            {
                                fields += relatedTableMap.GetTableFieldName(fm) + " = @" + relatedTableMap.GetTableFieldName(fm) + ", ";
                                pars.Add(CreateParameter("@" + relatedTableMap.GetTableFieldName(fm), null));
                            }
                        }
                        else
                        {
                            Table relatedTable = (Table)table.GetType().GetProperty(fnp.ClassFieldName).GetValue(table, new object[0]);
                            foreach (FieldMap fm in relatedTableMap.PrimaryKeys)
                            {
                                fields += relatedTableMap.GetTableFieldName(fm) + " = @" + relatedTableMap.GetTableFieldName(fm) + ", ";
                                pars.Add(CreateParameter("@" + relatedTableMap.GetTableFieldName(fm), relatedTable.GetType().GetProperty(relatedTableMap.GetClassFieldName(fm)).GetValue(relatedTable, new Object[0])));
                            }
                        }
                        if (map[fnp].PrimaryKey || !map.HasPrimaryKeys)
                        {
                            Table relatedTable = (Table)table.GetInitialPrimaryValue(fnp);
                            if (relatedTable == null)
                            {
                                foreach (FieldMap fm in relatedTableMap.PrimaryKeys)
                                {
                                    conditions += relatedTableMap.GetTableFieldName(fm) + " is null AND ";
                                }
                            }
                            else
                            {
                                foreach (FieldMap fm in relatedTableMap.PrimaryKeys)
                                {
                                    conditions += relatedTableMap.GetTableFieldName(fm) + " = @init_" + relatedTableMap.GetTableFieldName(fm) + " AND ";
                                    pars.Add(CreateParameter("@init_" + relatedTableMap.GetTableFieldName(fm), relatedTable.GetType().GetProperty(relatedTableMap.GetClassFieldName(fm)).GetValue(relatedTable, new Object[0])));
                                }
                            }
                        }
                    }
                }
                else
                {
                    fields += fnp.TableFieldName+" = @"+fnp.TableFieldName +", ";
                    if (table.GetType().GetProperty(fnp.ClassFieldName).GetValue(table, new object[0]) == null)
                    {
                        pars.Add(CreateParameter("@" + fnp.TableFieldName, null));
                    }
                    else
                    {
                        pars.Add(CreateParameter("@" + fnp.TableFieldName, table.GetType().GetProperty(fnp.ClassFieldName).GetValue(table, new object[0])));
                    }
                    if (map[fnp].PrimaryKey || !map.HasPrimaryKeys)
                    {
                        if (table.GetInitialPrimaryValue(fnp) == null)
                        {
                            conditions += fnp.TableFieldName + " IS NULL AND ";
                        }
                        else
                        {
                            conditions += fnp.TableFieldName + " = @init_"+fnp.TableFieldName +" AND ";
                            pars.Add(CreateParameter("@init_" + fnp.TableFieldName, table.GetInitialPrimaryValue(fnp)));
                        }
                    }
                }
            }
            query += fields.Substring(0, fields.Length - 2);
            query += " WHERE " + conditions.Substring(0, conditions.Length - 4);
            this.ExecuteNonQuery(query, pars);
            foreach (ExternalFieldMap efm in map.ExternalFieldMapArrays)
            {
                TableMap relatedMap = ClassMapper.GetTableMap(efm.Type);
                string delString = "DELETE FROM " + map.Name + "_" + relatedMap.Name + " WHERE ";
                pars.Clear();
                foreach (InternalFieldMap ifm in map.PrimaryKeys)
                {
                    delString += ifm.FieldName + " = @" + ifm.FieldName + " AND ";
                    pars.Add(CreateParameter("@" + ifm.FieldName, table.GetType().GetProperty(map.GetClassFieldName(ifm)).GetValue(table, new object[0])));
                }
                ExecuteNonQuery(delString.Substring(0, delString.Length - 4), pars);
                Table[] values = (Table[])table.GetType().GetProperty(map.GetClassFieldName(efm)).GetValue(table, new object[0]);
                delString = "INSERT INTO " + map.Name + "_" + relatedMap.Name + "(";
                string valueString = "VALUES(";
                foreach (InternalFieldMap ifm in map.PrimaryKeys)
                {
                    delString += ifm.FieldName + ",";
                    valueString += "@" + ifm.FieldName + ",";
                }
                foreach (InternalFieldMap ifm in relatedMap.PrimaryKeys)
                {
                    delString += ifm.FieldName + ",";
                    valueString += "@" + ifm.FieldName + ",";
                }
                delString = delString.Substring(0, delString.Length - 1) + ") " + valueString.Substring(0, valueString.Length - 1) + ")";
                foreach (Table t in values)
                {
                    foreach (InternalFieldMap ifm in relatedMap.PrimaryKeys)
                    {
                        for (int x = 0; x < pars.Count; x++)
                        {
                            if (pars[x].ParameterName == "@" + ifm.FieldName)
                            {
                                pars.RemoveAt(x);
                                break;
                            }
                        }
                        pars.Add(CreateParameter("@" + ifm.FieldName, t.GetType().GetProperty(relatedMap.GetClassFieldName(ifm)).GetValue(t, new object[0])));
                    }
                    ExecuteNonQuery(delString, pars);
                }
            }
			return table;
		}

        public Table Save(Table table)
        {
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
			string query = "INSERT INTO "+ClassMapper.GetTableMap(table.GetType()).Name+"(";
			string values="";
            string fields = "";
            string tableName = ClassMapper.GetTableMap(table.GetType()).Name;
			string select="SELECT * FROM "+tableName;
			List<IDbDataParameter> pars = new List<IDbDataParameter>();
			foreach (FieldNamePair fnp in ClassMapper.GetTableMap(table.GetType()).FieldNamePairs)
			{
                if (map[fnp] is ExternalFieldMap)
                {
                    if (!((ExternalFieldMap)map[fnp]).IsArray)
                    {
                        TableMap relatedTableMap = ClassMapper.GetTableMap(table.GetType().GetProperty(fnp.ClassFieldName).PropertyType);
                        if (table.GetType().GetProperty(fnp.ClassFieldName).GetValue(table, new object[0]) == null)
                        {
                            foreach (FieldMap fm in relatedTableMap.PrimaryKeys)
                            {
                                values += relatedTableMap.GetTableFieldName(fm) + ",";
                                pars.Add(CreateParameter("@" + relatedTableMap.GetTableFieldName(fm), null));
                            }
                        }
                        else
                        {
                            if (!select.Contains("WHERE"))
                            {
                                select += " WHERE ";
                            }
                            select = select.Replace(" WHERE ", ", " + relatedTableMap.Name + " WHERE ");
                            Table relatedTable = (Table)table.GetType().GetProperty(fnp.ClassFieldName).GetValue(table, new object[0]);
                            foreach (FieldMap fm in relatedTableMap.PrimaryKeys)
                            {
                                values += relatedTableMap.GetTableFieldName(fm) + ",";
                                pars.Add(CreateParameter("@" + relatedTableMap.GetTableFieldName(fm), relatedTable.GetType().GetProperty(relatedTableMap.GetClassFieldName(fm)).GetValue(relatedTable, new Object[0])));
                                select += ClassMapper.GetTableMap(table.GetType()).Name + "." + relatedTableMap.GetTableFieldName(fm) + " = " + relatedTableMap.Name + "." + relatedTableMap.GetTableFieldName(fm) + " AND ";
                                select += relatedTableMap.Name + "." + relatedTableMap.GetTableFieldName(fm) + " = @" + relatedTableMap.GetTableFieldName(fm) + " AND ";
                            }
                            foreach (FieldMap fm in relatedTableMap.Fields)
                            {
                                fields += "," + relatedTableMap.Name + "." + relatedTableMap.GetTableFieldName(fm);
                            }
                        }
                    }
                }
                else
                {
                    fields += "," + tableName + "." + fnp.TableFieldName;
                    values += fnp.TableFieldName + ",";
                    if (table.IsFieldNull(fnp.ClassFieldName) == null)
                    {
                        pars.Add(CreateParameter("@" + fnp.TableFieldName,null));
                    }
                    else
                    {
                        pars.Add(CreateParameter("@" + fnp.TableFieldName, table.GetType().GetProperty(fnp.ClassFieldName).GetValue(table, new object[0])));
                    }
                }
			}
			foreach (InternalFieldMap f in ClassMapper.GetTableMap(table.GetType()).Fields)
			{
                if (!f.Nullable)
                {
                    if (!select.Contains(" WHERE "))
                    {
                        select += " WHERE ";
                    }
                    if (!f.AutoGen)
                    {
                        select += tableName + "." + f.FieldName + " = @" + f.FieldName + " AND ";
                    }
                }
                else
                {
                    if (!select.Contains(" WHERE "))
                    {
                        select += " WHERE ";
                    }
                    if (!f.AutoGen)
                    {
                        if (table.IsFieldNull(map.GetClassFieldName(f)))
                        {
                            select += tableName + "." + f.FieldName + " is @" + f.FieldName + " AND ";
                        }
                        else
                        {
                            select += tableName + "." + f.FieldName + " = @" + f.FieldName + " AND ";
                        }
                    }
                }
			}
            foreach (InternalFieldMap f in map.InternalPrimaryKeys)
            {
                if (f.AutoGen)
                {
                    select = "SELECT * FROM " + map.Name + " WHERE " + map.GetTableFieldName(f) + " IN (SELECT MAX(" + map.GetTableFieldName(f) + ") " + select.Replace("SELECT * ", "") ;
                    select = select.Substring(0, select.Length - 4) + ") AND";
                }
            }
            select = select.Replace("*", fields.Substring(1));
			values=values.Substring(0,values.Length-1);
			query+=values+") VALUES(@"+values.Replace(",",",@")+")";
			select = select.Substring(0,select.Length-4);
			ExecuteNonQuery(query,pars);
			for (int x=0;x<pars.Count;x++)
			{
				if (!select.Contains(pars[x].ParameterName ))
				{
					pars.RemoveAt(x);
					x--;
				}
			}
			ExecuteQuery(select,pars);
			Read();
			table.SetValues(this);
            Close();
            foreach (ExternalFieldMap efm in map.ExternalFieldMapArrays)
            {
                TableMap relatedMap = ClassMapper.GetTableMap(efm.Type);
                string delString = "DELETE FROM " + map.Name + "_" + relatedMap.Name + " WHERE ";
                pars.Clear();
                foreach (InternalFieldMap ifm in map.PrimaryKeys)
                {
                    delString += ifm.FieldName + " = @" + ifm.FieldName + " AND ";
                    pars.Add(CreateParameter("@" + ifm.FieldName, table.GetType().GetProperty(map.GetClassFieldName(ifm)).GetValue(table, new object[0])));
                }
                ExecuteNonQuery(delString.Substring(0, delString.Length - 4), pars);
                Table[] vals = (Table[])table.GetType().GetProperty(map.GetClassFieldName(efm)).GetValue(table, new object[0]);
                delString = "INSERT INTO " + map.Name + "_" + relatedMap.Name + "(";
                string valueString = "VALUES(";
                foreach (InternalFieldMap ifm in map.PrimaryKeys)
                {
                    delString += ifm.FieldName+",";
                    valueString+="@"+ifm.FieldName+",";
                }
                foreach (InternalFieldMap ifm in relatedMap.PrimaryKeys)
                {
                    delString += ifm.FieldName + ",";
                    valueString += "@" + ifm.FieldName + ",";
                }
                delString = delString.Substring(0, delString.Length - 1) + ") " + valueString.Substring(0, valueString.Length - 1) + ")";
                foreach (Table t in vals)
                {
                    foreach (InternalFieldMap ifm in relatedMap.PrimaryKeys)
                    {
                        for (int x = 0; x < pars.Count; x++)
                        {
                            if (pars[x].ParameterName == "@" + ifm.FieldName)
                            {
                                pars.RemoveAt(x);
                                break;
                            }
                        }
                        pars.Add(CreateParameter("@" + ifm.FieldName, t.GetType().GetProperty(relatedMap.GetClassFieldName(ifm)).GetValue(t, new object[0])));
                    }
                    ExecuteNonQuery(delString, pars);
                }
            }
			return table;
		}

        private string ConstructSelectString(System.Type type)
        {
            TableMap map = ClassMapper.GetTableMap(type);
            string fields = "";
            string joins = "";
            string tables = "";
            string where = "";
            string query = "SELECT * FROM " + map.Name;
            foreach (FieldNamePair fnp in map.FieldNamePairs)
            {
                if (map[fnp] is ExternalFieldMap)
                {
                    if (!((ExternalFieldMap)map[fnp]).IsArray)
                    {
                        TableMap relatedTableMap = ClassMapper.GetTableMap(type.GetProperty(fnp.ClassFieldName).PropertyType);
                        if (map.GetFieldInfoForForiegnTable(type.GetProperty(fnp.ClassFieldName).PropertyType).Nullable)
                        {
                            joins += " LEFT JOIN " + relatedTableMap.Name + " ON ";
                            foreach (FieldMap fm in relatedTableMap.PrimaryKeys)
                            {
                                joins += map.Name + "." + relatedTableMap.GetTableFieldName(fm) + " = " + relatedTableMap.Name + "." + relatedTableMap.GetTableFieldName(fm) + " AND ";
                            }
                            joins = joins.Substring(0, joins.Length - 4);
                        }
                        else
                        {
                            tables += "," + relatedTableMap.Name;
                            foreach (FieldMap fm in relatedTableMap.PrimaryKeys)
                            {
                                where += map.Name + "." + relatedTableMap.GetTableFieldName(fm) + " = " + relatedTableMap.Name + "." + relatedTableMap.GetTableFieldName(fm) + " AND ";
                            }
                        }
                        foreach (FieldMap fm in relatedTableMap.Fields)
                        {
                            fields += "," + relatedTableMap.Name + "." + relatedTableMap.GetTableFieldName(fm);
                        }
                    }
                }
                else
                {
                    fields += "," + map.Name + "." + fnp.TableFieldName;
                }
            }
            query += joins;
            query += tables;
            query.Replace("*", fields.Substring(1));
            if (where.Length > 0)
            {
                query += " WHERE " + where.Substring(0, where.Length - 4);
            }
            return query;
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
                    query = this.ConstructSelectString(f.Type);
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
			List<Table> ret = new List<Table>();
            string query = this.ConstructSelectString(type);
			ExecuteQuery(query);
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
			List<Table> ret = new List<Table>();
            string query = ConstructSelectString(type);
			List<IDbDataParameter> pars = new List<IDbDataParameter>();
            TableMap map = ClassMapper.GetTableMap(type);
			if (parameters!=null)
			{
                if (!query.Contains(" WHERE "))
                {
                    query += " WHERE 1=1 ";
                }
				foreach (SelectParameter s in parameters)
				{
					foreach (FieldNamePair f in map.FieldNamePairs)
					{
						if (s.FieldName==f.ClassFieldName)
						{
							query+=" AND "+f.TableFieldName+" = @"+f.TableFieldName;
							pars.Add(CreateParameter("@"+f.TableFieldName,s.FieldValue));
						}
					}
				}
			}
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
		
		public int ExecuteNonQuery(string queryString)
		{
			return ExecuteNonQuery(queryString,null);
		}
		
		public int ExecuteNonQuery(string queryString,List<IDbDataParameter> parameters)
		{
			comm.CommandText=queryString;
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
			comm.CommandText=queryString;
			comm.Parameters.Clear();
			if (parameters!=null)
			{
				foreach (IDbDataParameter param in parameters)
				{
					comm.Parameters.Add(param);
				}
			}
            System.Diagnostics.Debug.WriteLine(comm.CommandText);
			reader=comm.ExecuteReader();
		}
		
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
		
	}
}
