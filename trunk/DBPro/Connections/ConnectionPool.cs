/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 23/03/2008
 * Time: 8:58 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using Org.Reddragonit.Dbpro.Structure.Mapping;
using ExtractedTableMap = Org.Reddragonit.Dbpro.Connections.Connection.ExtractedTableMap;
using ExtractedFieldMap = Org.Reddragonit.Dbpro.Connections.Connection.ExtractedFieldMap;
using ForiegnRelationMap = Org.Reddragonit.Dbpro.Connections.Connection.ForiegnRelationMap;
using FieldType = Org.Reddragonit.Dbpro.Structure.Attributes.Field.FieldType;
using VersionTypes = Org.Reddragonit.Dbpro.Structure.Attributes.VersionField.VersionTypes;
using UpdateDeleteAction =  Org.Reddragonit.Dbpro.Structure.Attributes.ForiegnField.UpdateDeleteAction;

namespace Org.Reddragonit.Dbpro.Connections
{
	/// <summary>
	/// Description of ConnectionPool.
	/// </summary>
	public abstract class ConnectionPool
	{
		private List<Connection> locked=new List<Connection>();
		private Queue<Connection> unlocked=new Queue<Connection>();
		private Mutex mut = new Mutex(false);
		protected string connectionString;
		
		private int minPoolSize=0;
		private int maxPoolSize=0;
		private long maxKeepAlive=0;
		
		private bool isClosed=false;
		private bool isReady=false;
		private string _connectionName;
		
		protected abstract Connection CreateConnection();
		
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
		
		public ConnectionPool(string connectionString,int minPoolSize,int maxPoolSize,long maxKeepAlive,bool UpdateStructureDebugMode,string connectionName)
		{
			mut.WaitOne();
			this.connectionString=connectionString;
			this.minPoolSize=minPoolSize;
			this.maxPoolSize=maxPoolSize;
			this.maxKeepAlive=maxKeepAlive;
			UpdateStructure(UpdateStructureDebugMode);
			for (int x=0;x<minPoolSize;x++)
				unlocked.Enqueue(CreateConnection());
			isReady=true;
			mut.ReleaseMutex();
			ConnectionPoolManager.AddConnection(connectionName,this);
			_connectionName=connectionName;
		}
		
		private ExtractedTableMap GetVersionedTableMap(ExtractedTableMap map,Connection conn)
		{
			ExtractedTableMap tmpEtm = new ExtractedTableMap(map.TableName+"_VERSION");
			string type=conn.TranslateFieldType(FieldType.DATETIME,0);
			if (map.VersionType.Value==VersionTypes.NUMBER)
				type=conn.TranslateFieldType(FieldType.LONG,0);
			ExtractedFieldMap tmpEfm = new ExtractedFieldMap(map.TableName+"_VERSION_ID",type,0,true,false,false,true);
			tmpEtm.Fields.Add(tmpEfm);
			foreach (ExtractedFieldMap efm in map.Fields)
			{
				if (efm.PrimaryKey||efm.Versioned)
				{
					tmpEfm = new ExtractedFieldMap(efm.FieldName,efm.Type,efm.Size,efm.PrimaryKey,!efm.PrimaryKey,false,true);
					tmpEtm.Fields.Add(tmpEfm);
				}
			}
			return tmpEtm;
		}

		private void UpdateStructure(bool Debug)
		{
			Connection conn = CreateConnection();
			List<ExtractedTableMap> curStructure = conn.GetTableList();
			for(int x=0;x<curStructure.Count;x++)
			{
				for(int y=0;y<curStructure.Count;y++)
				{
					if(curStructure[x].TableName.ToUpper()+"_VERSION"==curStructure[y].TableName.ToUpper())
					{
						ExtractedTableMap etm = curStructure[x];
						foreach (Connection.ExtractedFieldMap efm in curStructure[y].Fields)
						{
							if (efm.FieldName==curStructure[x].TableName.ToUpper()+"_VERSION_ID")
							{
								if (efm.Type.ToUpper()==conn.TranslateFieldType(FieldType.DATETIME,0))
									etm.VersionType=VersionTypes.DATESTAMP;
								else
									etm.VersionType=VersionTypes.NUMBER;
							}else
							{
								for (int z=0;z<etm.Fields.Count;z++)
								{
									Connection.ExtractedFieldMap ef = etm.Fields[z];
									if (efm.FieldName==ef.FieldName)
									{
										ef.Versioned=true;
										etm.Fields.RemoveAt(z);
										etm.Fields.Insert(z,ef);
										break;
									}
								}
							}
						}
						curStructure.RemoveAt(y);
						curStructure.RemoveAt(x);
						curStructure.Insert(x,etm);
						break;
					}
				}
			}
			List<ExtractedTableMap> tables = new List<ExtractedTableMap>();
			foreach (System.Type type in ClassMapper.TableTypes)
			{
				TableMap tm = ClassMapper.GetTableMap(type);
				ExtractedTableMap etm = new ExtractedTableMap(tm.Name);
				etm.VersionType=tm.VersionType;
				foreach (InternalFieldMap ifm in tm.Fields)
				{
					etm.Fields.Add(new ExtractedFieldMap(ifm.FieldName, conn.TranslateFieldType(ifm.FieldType, ifm.FieldLength), ifm.FieldLength, ifm.PrimaryKey, ifm.Nullable, ifm.AutoGen,ifm.Versionable));
				}
				foreach (Type t in tm.ForiegnTables)
				{
					TableMap rt = ClassMapper.GetTableMap(t);
					foreach (InternalFieldMap ifm in rt.PrimaryKeys)
					{
						ExtractedFieldMap efm;
						for (int x = 0; x < etm.Fields.Count; x++)
						{
							efm = etm.Fields[x];
							if (efm.FieldName == rt.GetTableFieldName(ifm))
							{
								efm.ExternalTable = rt.Name;
								efm.ExternalField = efm.FieldName;
								efm.DeleteAction = tm.GetFieldInfoForForiegnTable(t).OnDelete.ToString();
								efm.UpdateAction = tm.GetFieldInfoForForiegnTable(t).OnUpdate.ToString();
								etm.Fields.RemoveAt(x);
								etm.Fields.Insert(x,efm);
							}
						}
					}
				}
				tables.Add(etm);
				foreach (ExternalFieldMap e in tm.ExternalFieldMapArrays)
				{
					TableMap t = ClassMapper.GetTableMap(e.Type);
					etm = new ExtractedTableMap(tm.Name + "_" + t.Name);
					ExtractedFieldMap efm;
					foreach (InternalFieldMap ifm in tm.PrimaryKeys)
					{
						efm = new ExtractedFieldMap(ifm.FieldName, conn.TranslateFieldType(ifm.FieldType, ifm.FieldLength), ifm.FieldLength, true, ifm.Nullable,ifm.Versionable);
						efm.ExternalTable = tm.Name;
						efm.ExternalField = ifm.FieldName;
						efm.DeleteAction = e.OnDelete.ToString();
						efm.UpdateAction = e.OnUpdate.ToString();
						etm.Fields.Add(efm);
					}
					foreach (InternalFieldMap ifm in t.PrimaryKeys)
					{
						efm = new ExtractedFieldMap(ifm.FieldName, conn.TranslateFieldType(ifm.FieldType, ifm.FieldLength), ifm.FieldLength, true, ifm.Nullable,ifm.Versionable);
						efm.ExternalTable = t.Name;
						efm.ExternalField = ifm.FieldName;
						efm.DeleteAction = e.OnDelete.ToString();
						efm.UpdateAction = e.OnUpdate.ToString();
						etm.Fields.Add(efm);
					}
					tables.Add(etm);
				}
			}
			List<string> alterations = new List<string>();
			foreach (ExtractedTableMap etm in tables)
			{
				bool tableExists = false;
				foreach (ExtractedTableMap e in curStructure)
				{
					if (e.TableName == etm.TableName)
					{
						tableExists = true;
						if (e.VersionType.HasValue&&!etm.VersionType.HasValue)
						{
							alterations.AddRange(conn.GetDropTableString(etm.TableName+"_VERSION",false));
						}else if (!e.VersionType.HasValue&&etm.VersionType.HasValue)
						{
							alterations.AddRange(conn.GetCreateTableStringsForAlterations(this.GetVersionedTableMap(etm,conn)));
						}
						foreach (ExtractedFieldMap efm in etm.Fields)
						{
							bool fieldExists = false;
							foreach (ExtractedFieldMap f in e.Fields)
							{
								if (efm.FieldName == f.FieldName)
								{
									fieldExists = true;
									if ((efm.Type != f.Type)||
									    (efm.Type.ToUpper().Contains("CHAR") && (efm.Size != f.Size))
									   )
									{
										if (f.AutoGen)
										{
											alterations.AddRange(conn.GetDropAutogenStrings(e.TableName, f.FieldName, f.Type));
										}
										alterations.Add(conn.queryBuilder.AlterFieldType(etm.TableName, efm.FieldName, efm.Type, efm.Size));
										if (f.Versioned&&e.VersionType.HasValue)
											alterations.Add(conn.queryBuilder.AlterFieldType(etm.TableName+"_VERSION",efm.FieldName,efm.Type,efm.Size));
										if (efm.AutoGen && efm.PrimaryKey)
										{
											alterations.AddRange(conn.GetAddAutogenString(etm.TableName, efm.FieldName, efm.Type));
										}
									}else if (efm.AutoGen != f.AutoGen)
									{
										if ((efm.AutoGen) && (!f.AutoGen))
											alterations.AddRange(conn.GetDropAutogenStrings(e.TableName, f.FieldName, f.Type));
										else
											alterations.AddRange(conn.GetAddAutogenString(etm.TableName, efm.FieldName, efm.Type));
									}
									if (efm.Versioned!=f.Versioned&&e.VersionType.HasValue)
									{
										if (efm.Versioned)
											alterations.Add(conn.queryBuilder.CreateColumn(etm.TableName+"_VERSION",efm.FieldName,efm.Type,efm.Size));
										else
											alterations.Add(conn.queryBuilder.DropColumn(etm.TableName+"_VERSION",efm.FieldName));
									}
									break;
								}
							}
							if (!fieldExists)
							{
								alterations.Add(conn.queryBuilder.CreateColumn(etm.TableName, efm.FieldName, efm.Type, efm.Size));
								if (efm.Versioned)
								{
									alterations.Add(conn.queryBuilder.CreateColumn(etm.TableName+"_VERSION",efm.FieldName,efm.Type,efm.Size));
								}
							}
						}
						break;
					}
				}
				if (!tableExists)
				{
					alterations.AddRange(conn.GetCreateTableStringsForAlterations(etm));
					if (etm.VersionType.HasValue)
					{
						alterations.AddRange(conn.GetCreateTableStringsForAlterations(this.GetVersionedTableMap(etm,conn)));
					}
				}
			}
			foreach (ExtractedTableMap etm in curStructure)
			{
				bool tableExists = false;
				foreach (ExtractedTableMap e in tables)
				{
					if (e.TableName == etm.TableName)
					{
						tableExists = true;
						foreach (ExtractedFieldMap efm in etm.Fields)
						{
							bool fieldExists = false;
							foreach (ExtractedFieldMap f in e.Fields)
							{
								if (f.FieldName == efm.FieldName)
								{
									fieldExists = true;
									break;
								}
							}
							if (!fieldExists)
							{
								alterations.Add(conn.queryBuilder.DropColumn(etm.TableName, efm.FieldName));
								if (efm.Versioned)
									alterations.Add(conn.queryBuilder.DropColumn(etm.TableName+"_VERSION",efm.FieldName));
							}
						}
						break;
					}
				}
				if (!tableExists)
				{
					alterations.AddRange(conn.GetDropTableString(etm.TableName,etm.VersionType.HasValue));
				}
			}
			if (alterations.Count > 0)
			{
				List<string> tmp = new List<string>();
				List<string> tmp1 = new List<string>();
				Dictionary<string, List<ForiegnRelationMap>> rels = new Dictionary<string, List<ForiegnRelationMap>>();
				foreach (ExtractedTableMap etm in tables)
				{
					tmp.Clear();
					rels.Clear();
					foreach (ExtractedFieldMap efm in etm.Fields)
					{
						if (efm.PrimaryKey)
						{
							if (!efm.Nullable)
							{
								alterations.Add(conn.queryBuilder.CreateNullConstraint(etm.TableName,efm.FieldName));
								if (etm.VersionType.HasValue)
									alterations.Add(conn.queryBuilder.CreateNullConstraint(etm.TableName+"_VERSION",efm.FieldName));
							}
							tmp.Add(efm.FieldName);
						}
						if (efm.ExternalTable != null)
						{
							if (!rels.ContainsKey(efm.ExternalTable))
							{
								rels.Add(efm.ExternalTable, new List<ForiegnRelationMap>());
							}
							List<ForiegnRelationMap> t = rels[efm.ExternalTable];
							ForiegnRelationMap f = new ForiegnRelationMap();
							f.ExternalField = efm.ExternalField;
							f.InternalField = efm.FieldName;
							f.OnDelete = efm.DeleteAction;
							f.OnUpdate = efm.UpdateAction;
							t.Add(f);
							rels.Remove(efm.ExternalTable);
							rels.Add(efm.ExternalTable, t);
						}
					}
					if (tmp.Count > 0)
					{
						alterations.Add(conn.queryBuilder.CreatePrimaryKey(etm.TableName, tmp));
						if (etm.VersionType.HasValue)
							alterations.Add(conn.queryBuilder.CreatePrimaryKey(etm.TableName+"_VERSION",tmp));
					}
					if (rels.Count > 0)
					{
						foreach (string str in rels.Keys)
						{
							tmp.Clear();
							tmp1.Clear();
							string updateAction="";
							string deleteAction="";
							foreach (ForiegnRelationMap f in rels[str])
							{
								tmp.Add(f.InternalField);
								tmp1.Add(f.ExternalField);
								updateAction=f.OnUpdate;
								deleteAction=f.OnDelete;
							}
							alterations.Add(conn.queryBuilder.CreateForiegnKey(etm.TableName, tmp, str, tmp1,updateAction,deleteAction));
						}
					}
					if (etm.VersionType.HasValue)
					{
						tmp.Clear();
						List<ExtractedFieldMap> tmpFields = new List<ExtractedFieldMap>();
						foreach (ExtractedFieldMap efm in etm.Fields)
						{
							if (efm.PrimaryKey)
							{
								tmp.Add(efm.FieldName);
							}
							if (efm.Versioned)
							{
								tmpFields.Add(efm);
							}
						}
						alterations.Add(conn.queryBuilder.CreateForiegnKey(etm.TableName+"_VERSION",tmp,etm.TableName,tmp,UpdateDeleteAction.CASCADE.ToString(),UpdateDeleteAction.CASCADE.ToString()));
						alterations.AddRange(conn.GetVersionTableTriggers(etm.TableName,etm.TableName+"_VERSION",etm.TableName+"_VERSION_ID",etm.VersionType.Value,tmpFields));
					}
				}
				try{
					if (Debug)
					{
						foreach (string str in conn.GetDropConstraintsScript())
						{
							if (!str.EndsWith(";"))
								System.Diagnostics.Debug.WriteLine(str + ";");
							else
								System.Diagnostics.Debug.WriteLine(str);
							
						}
						foreach (string str in alterations)
						{
							if (!str.EndsWith(";"))
								System.Diagnostics.Debug.WriteLine(str + ";");
							else
								System.Diagnostics.Debug.WriteLine(str);
						}
					}
					else
					{
						foreach (string str in conn.GetDropConstraintsScript())
						{
							conn.ExecuteNonQuery(str);
						}
						foreach (string str in alterations)
						{
							if (str.EndsWith(" COMMIT;"))
							{
								conn.ExecuteNonQuery(str.Substring(0,str.Length-8));
								conn.Commit();
							}else
								conn.ExecuteNonQuery(str);
						}
					}
				}catch (Exception e)
				{
					System.Diagnostics.Debug.WriteLine(e.Message);
					System.Diagnostics.Debug.WriteLine(e.StackTrace);
					throw e;
				}
			}
			conn.Commit();
		}
		
		public Connection getConnection()
		{
			if (isClosed)
				return null;
			Connection ret=null;
			while(true)
			{
				mut.WaitOne();
				if (unlocked.Count>0)
				{
					ret=unlocked.Dequeue();
					if (ret.isPastKeepAlive(maxKeepAlive))
					{
						ret.Disconnect();
						ret=null;
					}
					else
						break;
				}
				if (isClosed)
					break;
				if (!checkMin())
				{
					ret=CreateConnection();
					break;
				}
				mut.ReleaseMutex();
				try{
					Thread.Sleep(100);
				}catch (Exception e){}
			}
			if (ret!=null)
				locked.Add(ret);
			mut.ReleaseMutex();
			return ret;
		}
		
		public void ClosePool()
		{
			mut.WaitOne();
			while (unlocked.Count>0)
				unlocked.Dequeue().Disconnect();
			foreach (Connection conn in locked)
				conn.Disconnect();
			isClosed=true;
			mut.ReleaseMutex();
		}
		
		internal void returnConnection(Connection conn)
		{
			mut.WaitOne();
			locked.Remove(conn);
			mut.ReleaseMutex();
			if (!checkMax()&&!isClosed&&!conn.isPastKeepAlive(maxKeepAlive))
			{
				mut.WaitOne();
				unlocked.Enqueue(conn);
				mut.ReleaseMutex();
			}else
			{
				conn.Disconnect();
			}
		}
		
		private bool checkMax()
		{
			if (maxPoolSize<=0) return false;
			else return maxPoolSize>(locked.Count+unlocked.Count);
		}
		
		private bool checkMin()
		{
			if (minPoolSize<=0) return true;
			else return minPoolSize<(locked.Count+unlocked.Count);
		}
	}
}
