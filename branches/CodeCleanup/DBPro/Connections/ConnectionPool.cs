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
using Org.Reddragonit.Dbpro.Connections.Interfaces;
using Org.Reddragonit.Dbpro.Virtual.Attributes;
using Org.Reddragonit.Dbpro.Virtual;
using Org.Reddragonit.Dbpro.Connections.PoolComponents;
using Org.Reddragonit.Dbpro.Structure.Attributes;
using System.Reflection;

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
		protected string connectionString;
		
		private int minPoolSize=0;
		private int maxPoolSize=0;
		private long maxKeepAlive=0;
        internal int readTimeout;
		private bool _debugMode=false;
		private bool _allowTableDeletions=true;
        protected bool _readonly = false;
		
		private bool isClosed=false;
		private bool isReady=false;
		private string _connectionName;
		
		protected abstract Connection CreateConnection(bool exclusiveLock);
        protected abstract void _InitClass();

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
        private ClassMapping _mapping;
        internal ClassMapping Mapping
        {
            get { return _mapping; }
        }
		
		internal string[] ReservedWords{
			get{
				if (_reservedWords==null)
					_reservedWords=_ReservedWords;
				return _reservedWords;
			}
		}
		
		internal string CorrectName(string currentName)
		{
            return _translator.CorrectName(currentName);
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

        protected ConnectionPool(string connectionString, int minPoolSize, int maxPoolSize, long maxKeepAlive, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions)
            :this(connectionString,minPoolSize,maxPoolSize,maxKeepAlive,UpdateStructureDebugMode,connectionName,allowTableDeletions,DEFAULT_READ_TIMEOUT,false)
        { }

        protected ConnectionPool(string connectionString, int minPoolSize, int maxPoolSize, long maxKeepAlive, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions,bool Readonly)
            : this(connectionString, minPoolSize, maxPoolSize, maxKeepAlive, UpdateStructureDebugMode, connectionName, allowTableDeletions, DEFAULT_READ_TIMEOUT, Readonly)
        { }
		
		protected ConnectionPool(string connectionString,int minPoolSize,int maxPoolSize,long maxKeepAlive,bool UpdateStructureDebugMode,string connectionName,bool allowTableDeletions,int readTimeout,bool Readonly)
		{
			Logger.LogLine("Establishing Connection with string: "+connectionString);
			this.connectionString=connectionString;
			this.minPoolSize=minPoolSize;
			this.maxPoolSize=maxPoolSize;
			this.maxKeepAlive=maxKeepAlive;
			_debugMode=UpdateStructureDebugMode;
			_connectionName=connectionName;
			_allowTableDeletions=allowTableDeletions;
            _readonly = Readonly;
            this.readTimeout = readTimeout;
			ConnectionPoolManager.AddConnection(connectionName,this);
		}
		
		internal void Init(Dictionary<Type,List<EnumTranslationPair>> translations)
		{
            _translator = new NameTranslator(this);
            _enums = new EnumsHandler();
            List<Type> tables = new List<Type>();
            List<Type> virtualTables = new List<Type>();
            _InitClass();
            if (!_debugMode)
            {
                foreach (Type t in Utility.LocateAllTypesWithAttribute(typeof(Table)))
                {
                    Table tbl = (Table)t.GetCustomAttributes(typeof(Table), false)[0];
                    if (Utility.StringsEqual(tbl.ConnectionName, ConnectionName))
                        tables.Add(t);
                }
                foreach (Type t in Utility.LocateAllTypesWithAttribute(typeof(VirtualTableAttribute)))
                {
                    if (tables.Contains(VirtualTableAttribute.GetMainTableTypeForVirtualTable(t)))
                        virtualTables.Add(t);
                }
                _mapping = new ClassMapping(this, tables, virtualTables);
                PreInit();
            }
            if (!(_debugMode&&(tables.Count==0)&&(virtualTables.Count==0)))
			    UpdateStructure(_debugMode,_allowTableDeletions,translations);
            for (int x=0;x<minPoolSize;x++){
            	if (unlocked.Count>=minPoolSize)
            		break;
				unlocked.Enqueue(CreateConnection());
            }
			isReady=true;
		}

		private void ExtractCurrentStructure(out List<ExtractedTableMap> tables,out List<Trigger> triggers,out List<Generator> generators,out List<IdentityField> identities,out List<View> views,out List<StoredProcedure> procedures,Connection conn)
		{
			tables = new List<ExtractedTableMap>();
			triggers = new List<Trigger>();
			generators=new List<Generator>();
			identities=new List<IdentityField>();
            views = new List<View>();
            procedures = new List<StoredProcedure>();
			conn.ExecuteQuery(conn.queryBuilder.SelectTriggers());
			while (conn.Read())
			{
				triggers.Add(new Trigger((string)conn[0],(string)conn[1],(string)conn[2]));
			}
			conn.Close();
            conn.ExecuteQuery(conn.queryBuilder.SelectProcedures());
            while (conn.Read())
            {
                procedures.Add(new StoredProcedure(conn[0].ToString(), conn[1].ToString(), conn[2].ToString(), conn[3].ToString(), conn[4].ToString()));
            }
            conn.Close();
            conn.ExecuteQuery(conn.queryBuilder.SelectViews());
            while (conn.Read())
                views.Add(new View((string)conn[0], (string)conn[1]));
            conn.Close();
			conn.ExecuteQuery(conn.queryBuilder.SelectTableNames());
			while (conn.Read())
			{
				tables.Add(new ExtractedTableMap((string)conn[0]));
			}
			conn.Close();
			for(int x=0;x<tables.Count;x++)
			{
				ExtractedTableMap etm = tables[x];
                etm.Indices = conn.queryBuilder.ExtractTableIndexes(etm.TableName, conn);
				conn.ExecuteQuery(conn.queryBuilder.SelectTableFields(etm.TableName));
				while (conn.Read())
				{
					etm.Fields.Add(new ExtractedFieldMap(conn[0].ToString(),conn[1].ToString(),
					                                     long.Parse(conn[2].ToString()),bool.Parse(conn[3].ToString()),bool.Parse(conn[4].ToString()),
					                                     bool.Parse(conn[5].ToString())));
				}
				conn.Close();
				conn.ExecuteQuery(conn.queryBuilder.SelectForeignKeys(etm.TableName));
				while (conn.Read())
				{
					etm.ForeignFields.Add(new ForeignRelationMap(conn[5].ToString(),conn[0].ToString(),conn[1].ToString(),
					                                             conn[2].ToString(),conn[3].ToString(),conn[4].ToString()));
				}
				conn.Close();
				tables.RemoveAt(x);
				tables.Insert(x,etm);
			}
			if (conn.UsesGenerators)
			{
				conn.ExecuteQuery(conn.queryBuilder.SelectGenerators());
				while (conn.Read())
				{
					generators.Add(new Generator((string)conn[0]));
				}
				conn.Close();
				for(int x=0;x<generators.Count;x++)
				{
					Generator gen = generators[x];
					conn.ExecuteQuery(conn.queryBuilder.GetGeneratorValue(gen.Name));
					conn.Read();
					gen.Value=long.Parse(conn[0].ToString());
					conn.Close();
					generators.RemoveAt(x);
					generators.Insert(x,gen);
				}
			}
			if (conn.UsesIdentities)
			{
				conn.ExecuteQuery(conn.queryBuilder.SelectIdentities());
				while (conn.Read())
				{
					identities.Add(new IdentityField((string)conn[0],(string)conn[1],(string)conn[2],(string)conn[3]));
				}
				conn.Close();
			}
		}

        private View _CreateViewForVirtualTable(Type virtualTable,Connection conn)
        {
            return new View(_mapping.GetVirtualTable(virtualTable).Name, VirtualTableQueryBuilder.ConstructQuery(virtualTable, conn));
        }
		
		private void ExtractExpectedStructure(out List<ExtractedTableMap> tables,out List<Trigger> triggers,out List<Generator> generators,out List<IdentityField> identities,out List<View> views,out List<StoredProcedure> procedures,Connection conn)
		{
			tables = new List<ExtractedTableMap>();
			triggers = new List<Trigger>();
			generators=new List<Generator>();
			identities = new List<IdentityField>();
            views = new List<View>();
            procedures = new List<StoredProcedure>();
			List<Trigger> tmpTriggers = new List<Trigger>();
			List<Generator> tmpGenerators = new List<Generator>();
			List<IdentityField> tmpIdentities = new List<IdentityField>();
            List<StoredProcedure> tmpProcedures = new List<StoredProcedure>();
            
            foreach (Type t in Utility.LocateTypeInstances(typeof(ICustomViewDefiner)))
            {
                List<View> tmpViews = ((ICustomViewDefiner)t.GetConstructor(Type.EmptyTypes).Invoke(new object[] { })).GetViewsForConnectionPool(this);
                if (tmpViews != null)
                    views.AddRange(tmpViews);
            }

            foreach (Type t in Utility.LocateTypeInstances(typeof(ICustomStoredProcedureDefiner)))
            {
                tmpProcedures = ((ICustomStoredProcedureDefiner)t.GetConstructor(Type.EmptyTypes).Invoke(new object[] { })).GetStoredProceduresForConnectionPool(this);
                if (tmpProcedures != null)
                    procedures.AddRange(tmpProcedures);
            }

            foreach (Type t in _mapping.VirtualTypes)
                views.Add(_CreateViewForVirtualTable(t,conn));

            Dictionary<string, string> AutoDeleteParentTables = new Dictionary<string, string>();

			foreach (System.Type type in _mapping.Types)
			{
                sTable tm = _mapping[type];
				ExtractedTableMap etm = new ExtractedTableMap(tm.Name);
                if (type.IsEnum)
                {
                    foreach (sTableField f in tm.Fields)
                        etm.Fields.Add(new ExtractedFieldMap(f.Name, conn.TranslateFieldType(f.Type, f.Length), f.Length, tm.AutoGenField == f.Name, false, tm.AutoGenField == f.Name));
                }
                else
                {
                    Table tbl = (Table)type.GetCustomAttributes(typeof(Table), false)[0];
                    foreach (TableIndex ti in type.GetCustomAttributes(typeof(TableIndex), false))
                    {
                        List<string> tfields = new List<string>();
                        foreach (string str in ti.Fields)
                        {
                            sTableField[] flds = tm[str];
                            if (flds.Length == 0)
                                tfields.Add(str);
                            else
                            {
                                foreach (sTableField f in flds)
                                    tfields.Add(f.Name);
                            }
                        }
                        etm.Indices.Add(new Index(CorrectName(ti.Name), tfields.ToArray(), ti.Unique, ti.Ascending));
                    }
                    List<string> pProps = new List<string>(tm.PrimaryKeyProperties);
                    foreach (string prop in tm.Properties)
                    {
                        PropertyInfo pi = type.GetProperty(prop, Utility._BINDING_FLAGS_WITH_INHERITANCE);
                        if (!pi.PropertyType.IsArray)
                        {
                            sTableRelation? rel = tm.GetRelationForProperty(prop);
                            foreach (sTableField f in tm[prop])
                            {
                                etm.Fields.Add(new ExtractedFieldMap(f.Name, conn.TranslateFieldType(f.Type, f.Length), f.Length, pProps.Contains(prop), (rel.HasValue ? rel.Value.Nullable : f.Nullable), (tm.AutoGenField != null ? f.Name == tm.AutoGenField : false)));
                                if (rel.HasValue)
                                    etm.ForeignFields.Add(new ForeignRelationMap(type.Name + "_" + prop, f.Name, rel.Value.ExternalTable, f.ExternalField, rel.Value.OnDelete.ToString(), rel.Value.OnUpdate.ToString()));
                            }
                        }
                        else
                        {
                            sTable iMap = _mapping[type, prop];
                            ExtractedTableMap ietm = new ExtractedTableMap(iMap.Name);
                            List<string> ipKeys = new List<string>(iMap.PrimaryKeyFields);
                            string extTable = (_mapping.IsMappableType(pi.PropertyType.GetElementType()) ? _mapping[pi.PropertyType.GetElementType()].Name : null);
                            foreach (sTableField f in iMap.Fields)
                            {
                                ietm.Fields.Add(new ExtractedFieldMap(f.Name, conn.TranslateFieldType(f.Type, f.Length), f.Length, ipKeys.Contains(f.Name), false, (iMap.AutoGenField != null ? iMap.AutoGenField == f.Name : false)));
                                if (f.ExternalField != null && ipKeys.Contains(f.Name))
                                    ietm.ForeignFields.Add(new ForeignRelationMap(type.Name + "_" + prop + (_mapping.IsMappableType(pi.PropertyType.GetElementType()) ? "_intermediate" : ""), f.Name, etm.TableName, f.ExternalField, ForeignField.UpdateDeleteAction.CASCADE.ToString(), ForeignField.UpdateDeleteAction.CASCADE.ToString()));
                                if (f.ExternalField != null && f.ClassProperty == null && extTable != null)
                                    ietm.ForeignFields.Add(new ForeignRelationMap(type.Name + "_" + prop, f.Name, extTable, f.ExternalField, ForeignField.UpdateDeleteAction.CASCADE.ToString(), ForeignField.UpdateDeleteAction.CASCADE.ToString()));
                            }
                            tables.Add(ietm);
                        }
                    }
                    if (_mapping.IsMappableType(type.BaseType))
                    {
                        sTable pMap = _mapping[type.BaseType];
                        foreach (string str in pMap.PrimaryKeyFields)
                            etm.ForeignFields.Add(new ForeignRelationMap(type.Name + "_parent", str, pMap.Name, str, ForeignField.UpdateDeleteAction.CASCADE.ToString(), ForeignField.UpdateDeleteAction.CASCADE.ToString()));
                        if (tbl.AutoDeleteParent)
                            AutoDeleteParentTables.Add(tm.Name, pMap.Name);
                    }
                    if (_mapping.HasVersionTable(type))
                    {
                        VersionTypes vt;
                        sTable vtm = _mapping.GetVersionTable(type, out vt);
                        ExtractedTableMap vetm = new ExtractedTableMap(vtm.Name);
                        List<string> vpkeys = new List<string>(vtm.PrimaryKeyFields);
                        foreach (sTableField f in vtm.Fields)
                        {
                            vetm.Fields.Add(new ExtractedFieldMap(f.Name, conn.TranslateFieldType(f.Type, f.Length), f.Length, vpkeys.Contains(f.Name), !vpkeys.Contains(f.Name), vtm.AutoGenField == f.Name));
                            if (vpkeys.Contains(f.Name))
                                vetm.ForeignFields.Add(new ForeignRelationMap(type.Name + "_version", f.Name, tm.Name, f.Name, ForeignField.UpdateDeleteAction.CASCADE.ToString(), ForeignField.UpdateDeleteAction.CASCADE.ToString()));
                        }
                        triggers.AddRange(conn.GetVersionTableTriggers(vetm, vt, this));
                        tables.Add(vetm);
                    }
                }
                tables.Add(etm);
			}
			foreach(ExtractedTableMap etm in tables)
			{
				Logger.LogLine(etm.TableName+":");
				foreach (ExtractedFieldMap efm in etm.Fields)
					Logger.LogLine("\t"+efm.FieldName+" - "+efm.PrimaryKey.ToString());
				foreach (ExtractedFieldMap efm in etm.PrimaryKeys)
				{
					if (efm.AutoGen)
					{
						conn.GetAddAutogen(etm,this,out tmpIdentities,out tmpGenerators,out tmpTriggers,out tmpProcedures);
						if (tmpGenerators!=null)
							generators.AddRange(tmpGenerators);
						if (tmpTriggers!=null)
							triggers.AddRange(tmpTriggers);
						if (tmpIdentities!=null)
							identities.AddRange(tmpIdentities);
                        if (tmpProcedures != null)
                            procedures.AddRange(tmpProcedures);
					}
				}
                if (AutoDeleteParentTables.ContainsKey(etm.TableName))
                {
                    ExtractedTableMap ptm=new ExtractedTableMap();
                    foreach (ExtractedTableMap m in tables)
                    {
                        if (AutoDeleteParentTables[etm.TableName] == m.TableName){
                            ptm = m;
                            break;
                        }
                    }
                    triggers.AddRange(conn.GetDeleteParentTrigger(etm, ptm, this));
                }
			}
		}
		
		private void CompareTriggers(List<Trigger> curTriggers,List<Trigger> expectedTriggers,out List<Trigger> dropTriggers,out List<Trigger> createTriggers)
		{
			createTriggers=new List<Trigger>();
			dropTriggers=new List<Trigger>();
			//remove triggers that exist but are not needed
			foreach (Trigger trig in curTriggers)
			{
				bool found=false;
				foreach (Trigger t in expectedTriggers)
				{
					if (Utility.StringsEqualIgnoreCaseWhitespace(t.Name,trig.Name))
					{
						found=true;
						break;
					}
				}
				if (!found)
					dropTriggers.Add(trig);
			}
			
			//add triggers that are needed but do not exist
			foreach (Trigger trig in expectedTriggers)
			{
				bool found=false;
				foreach (Trigger t in curTriggers)
				{
					if (Utility.StringsEqualIgnoreCaseWhitespace(t.Name,trig.Name))
					{
						found=true;
						break;
					}
				}
				if (!found)
					createTriggers.Add(trig);
			}
			
			//compare triggers that exist in both to make sure they are the same
			foreach (Trigger trig in expectedTriggers)
			{
				foreach (Trigger t in curTriggers)
				{
					if (Utility.StringsEqualIgnoreCaseWhitespace(t.Name,trig.Name))
					{
						if (!Utility.StringsEqualIgnoreCaseWhitespace(trig.Conditions,t.Conditions)||
						    !Utility.StringsEqualIgnoreCaseWhitespace(trig.Code,t.Code))
						{
							dropTriggers.Add(t);
							createTriggers.Add(trig);
						}
						break;
					}
				}
			}
		}
		
		private void CompareGenerators(List<Generator> curGenerators,List<Generator> expectedGenerators,out List<Generator> dropGenerators,out List<Generator> createGenerators)
		{
			createGenerators = new List<Generator>();
			dropGenerators=new List<Generator>();
			//remove generators that exist but are not needed
			foreach (Generator gen in curGenerators)
			{
				bool found=false;
				foreach (Generator g in expectedGenerators)
				{
					if (gen.Name==g.Name)
					{
						found=true;
						break;
					}
				}
				if (!found)
					dropGenerators.Add(gen);
			}
			
			//add generators that are needed but do not exist
			foreach (Generator gen in expectedGenerators)
			{
				bool found=false;
				foreach (Generator g in curGenerators)
				{
					if (gen.Name==g.Name)
					{
						found=true;
						break;
					}
				}
				if (!found)
					createGenerators.Add(gen);
			}
		}
		
		private void CompareIdentities(List<IdentityField> curIdentities,List<IdentityField> expectedIdentities,out List<IdentityField> dropIdentities,out List<IdentityField> createIdentities, out List<IdentityField> setIdentities)
		{
			createIdentities=new List<IdentityField>();
			dropIdentities=new List<IdentityField>();
			setIdentities=new List<IdentityField>();
			
			//remove identities that exist but are not needed
			foreach (IdentityField idf in curIdentities)
			{
				bool found=false;
				foreach (IdentityField i in expectedIdentities)
				{
					if ((idf.TableName==i.TableName)&&(idf.FieldName==i.FieldName)&&(idf.FieldType==i.FieldType))
					{
						found=true;
						if (idf.CurValue!=i.CurValue)
							setIdentities.Add(idf);
						break;
					}
				}
				if (!found)
					dropIdentities.Add(idf);
			}
			
			//add generators that are needed but do not exist
			foreach (IdentityField idf in expectedIdentities)
			{
				bool found=false;
				foreach (IdentityField i in curIdentities)
				{
					if ((idf.TableName==i.TableName)&&(idf.FieldName==i.FieldName)&&(idf.FieldType==i.FieldType))
					{
						found=true;
						break;
					}
				}
				if (!found)
					createIdentities.Add(idf);
			}
		}
		
		private void ExtractConstraintDropsCreates(List<ExtractedTableMap> curStructure,List<ExtractedTableMap> expectedStructure,Connection conn,out List<string> constraintDrops, out List<string> constraintCreates)
		{
			constraintDrops=new List<string>();
			constraintCreates=new List<string>();
			foreach (ExtractedTableMap etm in expectedStructure)
			{
				bool found=false;
				foreach (ExtractedTableMap e in curStructure)
				{
					if (etm.TableName==e.TableName)
					{
						found=true;
						foreach (ExtractedFieldMap efm in etm.Fields)
						{
							bool foundField=false;
							foreach (ExtractedFieldMap ee in e.Fields)
							{
								if (efm.FieldName==ee.FieldName)
								{
									foundField=true;
									if (efm.Nullable&&!ee.Nullable)
										constraintDrops.Add(conn.queryBuilder.DropNullConstraint(etm.TableName,efm));
									else if (!efm.Nullable&&ee.Nullable)
										constraintCreates.Add(conn.queryBuilder.CreateNullConstraint(etm.TableName,efm));
									break;
								}
							}
							if (!foundField&&!efm.Nullable)
							{
								constraintCreates.Add(conn.queryBuilder.CreateNullConstraint(etm.TableName,efm));
							}
						}
						break;
					}
				}
				if (!found)
				{
					foreach (ExtractedFieldMap efm in etm.Fields)
					{
						if (!efm.Nullable)
							constraintCreates.Add(conn.queryBuilder.CreateNullConstraint(etm.TableName,efm));
					}
				}
			}
			
			foreach (ExtractedTableMap etm in curStructure)
			{
				bool found=false;
				foreach (ExtractedTableMap e in expectedStructure)
				{
					if (etm.TableName==e.TableName)
					{
						found=true;
						foreach (ExtractedFieldMap efm in etm.Fields)
						{
							bool foundField=false;
							foreach (ExtractedFieldMap ee in e.Fields)
							{
								if (ee.FieldName==efm.FieldName)
								{
									foundField=true;
									break;
								}
							}
							if (!foundField)
								constraintDrops.Add(conn.queryBuilder.DropNullConstraint(etm.TableName,efm));
						}
						break;
					}
				}
				if (!found)
				{
					foreach (ExtractedFieldMap efm in etm.Fields)
					{
						if (!efm.Nullable)
							constraintDrops.Add(conn.queryBuilder.DropNullConstraint(etm.TableName,efm));
					}
				}
			}
		}
		
		private void ExtractPrimaryKeyCreationsDrops(List<ExtractedTableMap> curStructure,List<ExtractedTableMap> expectedStructure,out List<PrimaryKey> primaryKeyDrops,out List<PrimaryKey> primaryKeyCreations)
		{
			primaryKeyDrops=new List<PrimaryKey>();
			primaryKeyCreations=new List<PrimaryKey>();
			foreach (ExtractedTableMap etm in expectedStructure)
			{
				bool found=false;
				foreach (ExtractedTableMap e in curStructure)
				{
					if (etm.TableName==e.TableName)
					{
						found=true;
						bool keyDifferent=false;
						foreach (ExtractedFieldMap efm in etm.PrimaryKeys)
						{
							bool foundField=false;
							foreach (ExtractedFieldMap ee in e.PrimaryKeys)
							{
								if (ee.FieldName==efm.FieldName)
								{
									foundField=true;
									if ((ee.PrimaryKey!=efm.PrimaryKey)||(ee.PrimaryKey&&efm.PrimaryKey&&((ee.Type!=efm.Type)||(ee.Size!=efm.Size))))
										keyDifferent=true;
									break;
								}
							}
							if (!foundField)
								keyDifferent=true;
							if (keyDifferent)
								break;
						}
						if (keyDifferent)
						{
							primaryKeyDrops.Add(new PrimaryKey(e));
							primaryKeyCreations.Add(new PrimaryKey(etm));
						}
					}
				}
				if (!found)
				{
					if (etm.PrimaryKeys.Count>0)
						primaryKeyCreations.Add(new PrimaryKey(etm));
				}
			}
			
			foreach (ExtractedTableMap etm in curStructure)
			{
				bool found=false;
				foreach (ExtractedTableMap e in expectedStructure)
				{
					if (e.TableName==etm.TableName)
					{
						found=true;
						bool primaryDiff=false;
						foreach (ExtractedFieldMap efm in etm.PrimaryKeys)
						{
							bool foundField=false;
							foreach (ExtractedFieldMap ee in e.PrimaryKeys)
							{
								if (ee.FieldName==efm.FieldName)
								{
									foundField=true;
									break;
								}
							}
							if (!foundField)
								primaryDiff=true;
							if (primaryDiff)
								break;
						}
						if (primaryDiff)
						{
							primaryKeyDrops.Add(new PrimaryKey(etm));
							primaryKeyCreations.Add(new PrimaryKey(e));
						}
						break;
					}
				}
				if (!found)
				{
					if (etm.PrimaryKeys.Count>0)
						primaryKeyDrops.Add(new PrimaryKey(etm));
				}
			}
		}

        private void ExtractIndexCreationsDrops(List<ExtractedTableMap> curStructure, List<ExtractedTableMap> expectedStructure, out Dictionary<string, List<Index>> dropIndexes, out Dictionary<string, List<Index>> createIndexes)
        {
            dropIndexes = new Dictionary<string, List<Index>>();
            createIndexes = new Dictionary<string, List<Index>>();
            List<Index> indAdd = new List<Index>();
            List<Index> indDel = new List<Index>();
            foreach (ExtractedTableMap etm in expectedStructure)
            {
                bool found = false;
                foreach (ExtractedTableMap e in curStructure)
                {
                    if (e.TableName == etm.TableName)
                    {
                        found = true;
                        indAdd = new List<Index>();
                        indDel = new List<Index>();
                        foreach (Index ind in etm.Indices)
                        {
                            bool foundindex = false;
                            foreach (Index i in e.Indices)
                            {
                                if (i.Name == ind.Name)
                                {
                                    foundindex = true;
                                    if (!i.Equals(ind))
                                    {
                                        indDel.Add(i);
                                        indAdd.Add(ind);
                                    }
                                    break;
                                }
                            }
                            if (!foundindex)
                                indAdd.Add(ind);
                        }
                        if (indAdd.Count>0)
                            createIndexes.Add(etm.TableName, indAdd);
                        if (indDel.Count>0)
                            dropIndexes.Add(etm.TableName, indDel);
                    }
                }
                if (!found)
                    createIndexes.Add(etm.TableName,etm.Indices);
            }

            foreach (ExtractedTableMap etm in curStructure)
            {
                bool found = false;
                indDel = new List<Index>();
                if (dropIndexes.ContainsKey(etm.TableName))
                {
                    indDel = dropIndexes[etm.TableName];
                    dropIndexes.Remove(etm.TableName);
                }
                foreach (ExtractedTableMap e in expectedStructure)
                {
                    if (e.TableName == etm.TableName)
                    {
                        found = true;
                        foreach (Index ind in etm.Indices)
                        {
                            bool foundindex = false;
                            foreach (Index i in e.Indices)
                            {
                                if (i.Name == ind.Name)
                                {
                                    foundindex = true;
                                    break;
                                }
                            }
                            if (!foundindex)
                                indDel.Add(ind);
                        }
                        if (indDel.Count>0)
                            dropIndexes.Add(etm.TableName, indDel);
                    }
                }
                if (!found)
                    dropIndexes.Add(etm.TableName,etm.Indices);
            }
        }
		
		private void ExtractForeignKeyCreatesDrops(List<ExtractedTableMap> curStructure,List<ExtractedTableMap> expectedStructure,out List<ForeignKey> foreignKeyDrops,out List<ForeignKey> foreignKeyCreations)
		{
			foreignKeyDrops=new List<ForeignKey>();
			foreignKeyCreations=new List<ForeignKey>();
			foreach (ExtractedTableMap etm in expectedStructure)
			{
				bool found=false;
				foreach (ExtractedTableMap e in curStructure)
				{
                    if (etm.TableName == e.TableName)
					{
						found=true;
						foreach (string tableName in etm.RelatedTables)
						{
                            foreach (List<ForeignRelationMap> exfrms in etm.RelatedFieldsForTable(tableName))
                            {
                                bool foundRelation = false;
                                foreach (List<ForeignRelationMap> curfrms in e.RelatedFieldsForTable(tableName))
                                {
                                    if (exfrms.Count == curfrms.Count)
                                    {
                                        bool foundField = true;
                                        foreach (ForeignRelationMap exfrm in exfrms)
                                        {
                                            foundField = false;
                                            foreach (ForeignRelationMap curfrm in curfrms)
                                            {
                                                if ((CorrectName(exfrm.InternalField) == CorrectName(curfrm.InternalField)) && (CorrectName(exfrm.ExternalField) == CorrectName(curfrm.ExternalField)))
                                                {
                                                    foundField = ((etm.GetField(exfrm.InternalField).Type == e.GetField(curfrm.InternalField).Type) && (etm.GetField(exfrm.InternalField).Size == e.GetField(curfrm.InternalField).Size) && (etm.GetField(exfrm.InternalField).Nullable == e.GetField(curfrm.InternalField).Nullable) && (etm.GetField(exfrm.InternalField).PrimaryKey == e.GetField(curfrm.InternalField).PrimaryKey));
                                                    break;
                                                }
                                            }
                                            if (!foundField)
                                                break;
                                        }
                                        if (foundField)
                                        {
                                            foundRelation = true;
                                            break;
                                        }
                                    }
                                }
                                if (!foundRelation)
                                    foreignKeyCreations.Add(new ForeignKey(etm, tableName, exfrms[0].ID));
                            }
						}
						break;
					}
				}
				if (!found)
				{
					foreach (string tableName in etm.RelatedTables)
					{
                        foreach (List<ForeignRelationMap> frms in etm.RelatedFieldsForTable(tableName))
                        {
                            foreignKeyCreations.Add(new ForeignKey(etm, tableName,frms[0].ID));
                        }
					}
				}
			}

            foreach (ExtractedTableMap etm in curStructure)
            {
                bool found = false;
                foreach (ExtractedTableMap e in expectedStructure)
                {
                    if (etm.TableName == e.TableName)
                    {
                        found = true;
                        foreach (string tableName in etm.RelatedTables)
                        {
                            foreach (List<ForeignRelationMap> exfrms in etm.RelatedFieldsForTable(tableName))
                            {
                                bool foundRelation = false;
                                foreach (List<ForeignRelationMap> curfrms in e.RelatedFieldsForTable(tableName))
                                {
                                    if (exfrms.Count == curfrms.Count)
                                    {
                                        bool foundField = true;
                                        foreach (ForeignRelationMap exfrm in exfrms)
                                        {
                                            foundField = false;
                                            foreach (ForeignRelationMap curfrm in curfrms)
                                            {
                                                if ((CorrectName(exfrm.InternalField) == CorrectName(curfrm.InternalField)) && (CorrectName(exfrm.ExternalField) == CorrectName(curfrm.ExternalField)))
                                                {
                                                    foundField = ((etm.GetField(exfrm.InternalField).Type == e.GetField(curfrm.InternalField).Type) && (etm.GetField(exfrm.InternalField).Size == e.GetField(curfrm.InternalField).Size) && (etm.GetField(exfrm.InternalField).Nullable == e.GetField(curfrm.InternalField).Nullable) && (etm.GetField(exfrm.InternalField).PrimaryKey == e.GetField(curfrm.InternalField).PrimaryKey));
                                                    break;
                                                }
                                            }
                                            if (!foundField)
                                                break;
                                        }
                                        if (foundField)
                                        {
                                            foundRelation = true;
                                            break;
                                        }
                                    }
                                }
                                if (!foundRelation)
                                    foreignKeyDrops.Add(new ForeignKey(etm, tableName, exfrms[0].ID));
                            }
                        }
                        break;
                    }
                }
                if (!found)
                {
                    foreach (string tableName in etm.RelatedTables)
                    {
                        foreach (List<ForeignRelationMap> frms in etm.RelatedFieldsForTable(tableName))
                        {
                            foreignKeyDrops.Add(new ForeignKey(etm, tableName, frms[0].ID));
                        }
                    }
                }
            }
		}


        private void CompareViews(List<View> curViews, List<View> expectedViews, out List<View> createViews, out List<View> dropViews)
        {
            createViews = new List<View>();
            dropViews = new List<View>();

            for (int x = 0; x < expectedViews.Count; x++)
            {
                bool add = true;
                for (int y = 0; y < curViews.Count; y++)
                {
                    if (expectedViews[x].Name == curViews[y].Name)
                    {
                        add = false;
                        if (expectedViews[x].Query != curViews[y].Query)
                        {
                            dropViews.Add(expectedViews[x]);
                            createViews.Add(expectedViews[x]);
                        }
                        break;
                    }
                }
                if (add)
                    createViews.Add(expectedViews[x]);
            }

            for (int x = 0; x < curViews.Count; x++)
            {
                bool remove = true;
                for (int y = 0; y < expectedViews.Count; y++)
                {
                    if (curViews[x].Name == expectedViews[y].Name)
                    {
                        remove = false;
                        break;
                    }
                }
                if (remove)
                    dropViews.Add(curViews[x]);
            }
        }

        private void CompareStoredProcedures(List<StoredProcedure> curProcedures, List<StoredProcedure> expectedProcedures, out List<StoredProcedure> createProcedures, out List<StoredProcedure> updateProcedures, out List<StoredProcedure> dropStoredProcedures)
        {
            createProcedures = new List<StoredProcedure>();
            dropStoredProcedures = new List<StoredProcedure>();
            updateProcedures = new List<StoredProcedure>();

            for (int x = 0; x < expectedProcedures.Count; x++)
            {
                bool add = true;
                for (int y = 0; y < curProcedures.Count; y++)
                {
                    if (expectedProcedures[x].ProcedureName == curProcedures[y].ProcedureName)
                    {
                        add = false;
                        if (!Utility.StringsEqual(expectedProcedures[x].DeclareLines, curProcedures[y].DeclareLines)
                            || !Utility.StringsEqual(expectedProcedures[x].ReturnLine, curProcedures[y].ReturnLine)
                            || !Utility.StringsEqual(expectedProcedures[x].ParameterLines, curProcedures[y].ParameterLines)
                            || !Utility.StringsEqual(expectedProcedures[x].Code, curProcedures[y].Code))
                            updateProcedures.Add(expectedProcedures[x]);
                    }
                }
                if (add)
                    createProcedures.Add(expectedProcedures[x]);
            }

            for (int x = 0; x < curProcedures.Count; x++)
            {
                bool delete = true;
                for (int y = 0; y < expectedProcedures.Count; y++)
                {
                    if (curProcedures[x].ProcedureName == expectedProcedures[y].ProcedureName)
                    {
                        delete=false;
                        break;
                    }
                }
                if (delete)
                    dropStoredProcedures.Add(curProcedures[x]);
            }
        }

        private void CleanUpForeignKeys(ref List<ForeignKey> foreignKeys)
		{
			for (int x=0;x<foreignKeys.Count;x++)
			{
				ForeignKey key = foreignKeys[x];
				if (key.ExternalFields.Count>1){
					if (key.ExternalFields.IndexOf(key.ExternalFields[0],1)>=1)
					{
						List<string> externalFields = new List<string>();
						List<string> internalFields = new List<string>();
						for (int y=0;y<key.ExternalFields.Count;y++)
						{
							string str = key.ExternalFields[y];
							if (externalFields.Contains(str))
							{
								foreignKeys.Add(new ForeignKey(key.InternalTable,internalFields,
								                               key.ExternalTable,externalFields,
								                               key.OnUpdate,key.OnDelete));
								internalFields=new List<string>();
								externalFields=new List<string>();
							}
							externalFields.Add(str);
							internalFields.Add(key.InternalFields[y]);
						}
						foreignKeys.Add(new ForeignKey(key.InternalTable,internalFields,
						                               key.ExternalTable,externalFields,
						                               key.OnUpdate,key.OnDelete));
						foreignKeys.RemoveAt(x);
						x--;
					}
				}
			}
		}

        private void UpdateStructure(bool Debug, bool AllowTableDeletions, Dictionary<Type, List<EnumTranslationPair>> translations)
		{
			Connection conn = CreateConnection();
			List<ExtractedTableMap> curStructure =new List<ExtractedTableMap>();
			List<Trigger> curTriggers = new List<Trigger>();
			List<Generator> curGenerators = new List<Generator>();
			List<IdentityField> curIdentities = new List<IdentityField>();
            List<View> curViews = new List<View>();
            List<StoredProcedure> curProcedures = new List<StoredProcedure>();
			
			ExtractCurrentStructure(out curStructure,out curTriggers,out curGenerators,out curIdentities,out curViews,out curProcedures,conn);
			
			List<ExtractedTableMap> expectedStructure = new List<ExtractedTableMap>();
			List<Trigger> expectedTriggers = new List<Trigger>();
			List<Generator> expectedGenerators = new List<Generator>();
			List<IdentityField> expectedIdentities = new List<IdentityField>();
            List<View> expectedViews = new List<View>();
            List<StoredProcedure> expectedProcedures = new List<StoredProcedure>();
			
			ExtractExpectedStructure(out expectedStructure,out expectedTriggers,out expectedGenerators,out expectedIdentities,out expectedViews,out expectedProcedures,conn);

			List<Trigger> dropTriggers = new List<Trigger>();
			List<Trigger> createTriggers = new List<Trigger>();
            List<Trigger> recreateTriggers = new List<Trigger>();
			
			CompareTriggers(curTriggers,expectedTriggers,out dropTriggers,out createTriggers);

            List<StoredProcedure> dropProcedures = new List<StoredProcedure>();
            List<StoredProcedure> createProcedures = new List<StoredProcedure>();
            List<StoredProcedure> updateProcedures = new List<StoredProcedure>();

            CompareStoredProcedures(curProcedures, expectedProcedures, out createProcedures, out updateProcedures, out dropProcedures);
			
			List<Generator> dropGenerators = new List<Generator>();
			List<Generator> createGenerators = new List<Generator>();
			
			CompareGenerators(curGenerators,expectedGenerators,out dropGenerators,out createGenerators);
			
			List<string> constraintDrops = new List<string>();
			List<string> constraintCreations = new List<string>();
			
			ExtractConstraintDropsCreates(curStructure,expectedStructure,conn,out constraintDrops,out constraintCreations);
			
			List<PrimaryKey> primaryKeyDrops = new List<PrimaryKey>();
			List<PrimaryKey> primaryKeyCreations = new List<PrimaryKey>();
			
			ExtractPrimaryKeyCreationsDrops(curStructure,expectedStructure,out primaryKeyDrops,out primaryKeyCreations);
			
			List<ForeignKey> foreignKeyDrops = new List<ForeignKey>();
			List<ForeignKey> foreignKeyCreations = new List<ForeignKey>();

            ExtractForeignKeyCreatesDrops(curStructure, expectedStructure, out foreignKeyDrops, out foreignKeyCreations);
			
			List<IdentityField> dropIdentities=new List<IdentityField>();
			List<IdentityField> createIdentities=new List<IdentityField>();
			List<IdentityField> setIdentities=new List<IdentityField>();

            CompareIdentities(curIdentities, expectedIdentities, out dropIdentities, out createIdentities, out setIdentities);

            Dictionary<string, List<Index>> dropIndexes = new Dictionary<string, List<Index>>();
            Dictionary<string, List<Index>> createIndexes = new Dictionary<string, List<Index>>();

            ExtractIndexCreationsDrops(curStructure, expectedStructure, out dropIndexes, out createIndexes);

            List<View> createViews = new List<View>();
            List<View> dropViews = new List<View>();

            CompareViews(curViews, expectedViews, out createViews, out dropViews);

			List<string> tableCreations = new List<string>();
			List<string> tableAlterations = new List<string>();
			
			//locate tables and fields that need to be drop
			for (int x=0;x<curStructure.Count;x++)
			{
				ExtractedTableMap etm = curStructure[x];
				bool found=false;
				foreach (ExtractedTableMap e in expectedStructure)
				{
					if (etm.TableName==e.TableName)
					{
						found=true;
						foreach (ExtractedFieldMap efm in etm.Fields)
						{
							bool foundField=false;
							foreach(ExtractedFieldMap ee in e.Fields)
							{
								if (efm.FieldName==ee.FieldName)
								{
									foundField=true;
									break;
								}
							}
							if (!foundField)
							{
								tableAlterations.Add(conn.queryBuilder.DropColumn(etm.TableName,efm.FieldName));
							}
						}
						break;
					}
				}
				if (!found && AllowTableDeletions)
				{
                    foreach (Trigger t in curTriggers)
                    {
                        if (t.Conditions.Contains("FOR " + etm.TableName + " "))
                        {
                            dropTriggers.Add(t);
                        }
                    }
					tableAlterations.Add(conn.queryBuilder.DropTable(etm.TableName));
				}
			}
			
			//locate tables and Columns that need to be created or columns that need to be alter for type in the expected structure
			foreach (ExtractedTableMap etm in expectedStructure)
			{
				bool found=false;
				foreach (ExtractedTableMap e in curStructure)
				{
					if (etm.TableName==e.TableName)
					{
						found=true;
						foreach (ExtractedFieldMap efm in etm.Fields)
						{
							bool foundField=false;
							foreach (ExtractedFieldMap ee in e.Fields)
							{
								if (efm.FieldName==ee.FieldName)
								{
									foundField=true;
									if (((efm.Type!=ee.Type)||(efm.Size!=ee.Size))&&
                                        !((efm.Type=="BLOB")&&(ee.Type=="BLOB")))
									{
										if (efm.PrimaryKey&&ee.PrimaryKey)
										{
											primaryKeyDrops.Add(new PrimaryKey(etm));
											primaryKeyCreations.Add(new PrimaryKey(etm));
											for(int x=0;x<expectedStructure.Count;x++)
											{
												if (expectedStructure[x].RelatesToField(etm.TableName,efm.FieldName))
												{
                                                    foreach (List<ForeignRelationMap> frms in expectedStructure[x].RelatedFieldsForTable(etm.TableName))
                                                    {
                                                        foreignKeyDrops.Add(new ForeignKey(expectedStructure[x], etm.TableName,frms[0].ID));
                                                        foreignKeyCreations.Add(new ForeignKey(expectedStructure[x], etm.TableName,frms[0].ID));
                                                    }
												}
											}
										}
										foreach (string tbl in etm.ExternalTablesForField(efm.FieldName))
										{
                                            foreach (List<ForeignRelationMap> frms in etm.RelatedFieldsForTable(tbl))
                                            {
                                                foreignKeyDrops.Add(new ForeignKey(etm, tbl,frms[0].ID));
                                                foreignKeyCreations.Add(new ForeignKey(etm, tbl,frms[0].ID));
                                            }
										}
										tableAlterations.Add(conn.queryBuilder.AlterFieldType(etm.TableName,efm,ee));
									}
                                    if (efm.Nullable!=ee.Nullable){
                                        if (efm.PrimaryKey && ee.PrimaryKey)
                                        {
                                            primaryKeyDrops.Add(new PrimaryKey(etm));
                                            primaryKeyCreations.Add(new PrimaryKey(etm));
                                            for (int x = 0; x < expectedStructure.Count; x++)
                                            {
                                                if (expectedStructure[x].RelatesToField(etm.TableName, efm.FieldName))
                                                {
                                                    foreach (List<ForeignRelationMap> frms in expectedStructure[x].RelatedFieldsForTable(etm.TableName))
                                                    {
                                                        foreignKeyDrops.Add(new ForeignKey(expectedStructure[x], etm.TableName, frms[0].ID));
                                                        foreignKeyCreations.Add(new ForeignKey(expectedStructure[x], etm.TableName, frms[0].ID));
                                                    }
                                                }
                                            }
                                        }
                                        foreach (string tbl in etm.ExternalTablesForField(efm.FieldName))
                                        {
                                            foreach (List<ForeignRelationMap> frms in etm.RelatedFieldsForTable(tbl))
                                            {
                                                foreignKeyDrops.Add(new ForeignKey(etm, tbl, frms[0].ID));
                                                foreignKeyCreations.Add(new ForeignKey(etm, tbl, frms[0].ID));
                                            }
                                        }
                                        if (!efm.Nullable)
                                            constraintCreations.Add(conn.queryBuilder.CreateNullConstraint(etm.TableName, efm));
                                        else
                                            constraintDrops.Add(conn.queryBuilder.DropNullConstraint(etm.TableName, efm));
                                    }
									break;
								}
							}
							if (!foundField)
								tableAlterations.Add(conn.queryBuilder.CreateColumn(etm.TableName,efm));
						}
						break;
					}
				}
				if (!found)
				{
					tableCreations.Add(conn.queryBuilder.CreateTable(etm));
				}
			}

            foreach (PrimaryKey pk in primaryKeyDrops)
            {
                foreach (Trigger t in curTriggers)
                {
                    if (t.Conditions.Contains("FOR " + pk.Name + " "))
                        dropTriggers.Add(t);
                }
                foreach (Trigger t in expectedTriggers)
                {
                    if (t.Conditions.Contains("FOR " + pk.Name + " "))
                        createTriggers.Add(t);
                }
            }
			
			CleanUpForeignKeys(ref foreignKeyDrops);
			CleanUpForeignKeys(ref foreignKeyCreations);
			
			List<string> alterations = new List<string>();
			//add drops to alterations
			alterations.AddRange(constraintDrops);
			alterations.Add(" COMMIT;");

            foreach (View vw in dropViews)
                alterations.Add(conn.queryBuilder.DropView(vw.Name));
            alterations.Add(" COMMIT;");
			
			foreach (Trigger trig in dropTriggers)
				alterations.Add(conn.queryBuilder.DropTrigger(trig.Name));
			alterations.Add(" COMMIT;");

            foreach (string str in dropIndexes.Keys)
            {
                foreach (Index ind in dropIndexes[str])
                    alterations.Add(conn.queryBuilder.DropTableIndex(str, ind.Name));
            }
            alterations.Add(" COMMIT;");

			foreach (Generator gen in dropGenerators)
				alterations.Add(conn.queryBuilder.DropGenerator(gen.Name));
			alterations.Add(" COMMIT;");

            foreach (StoredProcedure proc in dropProcedures)
                alterations.Add(conn.queryBuilder.DropProcedure(proc.ProcedureName));
            alterations.Add(" COMMIT;");
			
			foreach (ForeignKey fk in foreignKeyDrops)
				alterations.Add(conn.queryBuilder.DropForeignKey(fk.InternalTable,fk.ExternalTable,fk.ExternalFields[0],fk.InternalFields[0]));
			alterations.Add(" COMMIT;");
			
			foreach (PrimaryKey pk in primaryKeyDrops)
			{
				foreach (string field in pk.Fields)
					alterations.Add(conn.queryBuilder.DropPrimaryKey(pk));
			}
			alterations.Add(" COMMIT;");
			
			foreach (IdentityField idf in dropIdentities)
				alterations.Add(conn.queryBuilder.DropIdentityField(idf));
			alterations.Add(" COMMIT;");
			
			alterations.AddRange(tableAlterations);
			alterations.Add(" COMMIT;");
			
			alterations.AddRange(tableCreations);
			alterations.Add(" COMMIT;");

            foreach (View vw in createViews)
                alterations.Add(conn.queryBuilder.CreateView(vw));
            alterations.Add(" COMMIT;");
			
			//add creations to alterations
			alterations.AddRange(constraintCreations);
			alterations.Add(" COMMIT;");
			
			foreach (PrimaryKey pk in primaryKeyCreations)
				alterations.Add(conn.queryBuilder.CreatePrimaryKey(pk));
			alterations.Add(" COMMIT;");
			
			foreach (ForeignKey fk in foreignKeyCreations)
				alterations.Add(conn.queryBuilder.CreateForeignKey(fk));
			alterations.Add(" COMMIT;");

            foreach (string str in createIndexes.Keys)
            {
                foreach (Index ind in createIndexes[str])
                    alterations.Add(conn.queryBuilder.CreateTableIndex(str, ind.Fields, ind.Name, ind.Unique, ind.Ascending));
            }
            alterations.Add(" COMMIT;");

            foreach (StoredProcedure proc in updateProcedures)
                alterations.Add(conn.queryBuilder.UpdateProcedure(proc));
            alterations.Add(" COMMIT;");

            foreach (StoredProcedure proc in createProcedures)
                alterations.Add(conn.queryBuilder.CreateProcedure(proc));
            alterations.Add(" COMMIT;");
			
			foreach (Generator gen in createGenerators)
			{
				alterations.Add(conn.queryBuilder.CreateGenerator(gen.Name));
				alterations.Add(conn.queryBuilder.SetGeneratorValue(gen.Name,gen.Value));
			}
			alterations.Add(" COMMIT;");
			
			foreach (Trigger trig in createTriggers)
			{
				alterations.Add(conn.queryBuilder.CreateTrigger(trig));
			}
			alterations.Add(" COMMIT;");
			
			foreach(IdentityField idf in createIdentities)
				alterations.Add(conn.queryBuilder.CreateIdentityField(idf));
			alterations.Add(" COMMIT;");
			
			foreach (IdentityField idf in setIdentities)
				alterations.Add(conn.queryBuilder.SetIdentityFieldValue(idf));
			alterations.Add(" COMMIT;");
			
            //foreach (Type t in _enumTableMaps.Keys)
            //{
            //    foreach (string str in Enum.GetNames(t))
            //    {
            //        alterations.Add(String.Format(
            //            "INSERT INTO {2}({0}) SELECT VAL FROM "+
            //            " ( "+
            //            " SELECT VAL, SUM(CNT) AS CNT "+
            //            " FROM ( "+
            //            " SELECT '{1}' AS VAL, "+
            //            " SUM(CASE RES_VALUE WHEN '{1}' THEN 1 ELSE 0 END) AS CNT   "+
            //            " FROM {2} "+
            //            " UNION    "+
            //            " SELECT '{1}' AS VAL, 0 AS CNT FROM {3} "+
            //            " ) tbl GROUP BY VAL "+
            //            " ) sums "+
            //            " WHERE CNT=0;",CorrectName("VALUE"),
            //            str,_enumTableMaps[t],conn.DefaultTableString
            //        ));
            //    }
            //    alterations.Add(" COMMIT;");
            //}
			
            for (int x = 0; x < alterations.Count; x++)
            {
                if (alterations[x].Contains(";ALTER"))
                {
                    string tmp = alterations[x];
                    alterations.RemoveAt(x);
                    alterations.Insert(x, tmp.Substring(0, tmp.IndexOf(";ALTER") + 1));
                    alterations.Insert(x + 1, tmp.Substring(tmp.IndexOf(";ALTER") + 1));
                }
                else if (alterations[x].Contains(";\nALTER"))
                {
                    string tmp = alterations[x];
                    alterations.RemoveAt(x);
                    alterations.Insert(x, tmp.Substring(0, tmp.IndexOf(";\nALTER") + 1));
                    alterations.Insert(x + 1, tmp.Substring(tmp.IndexOf(";\nALTER") + 1));
                }
                if (alterations[x].StartsWith("ALTER") && !alterations[x].TrimEnd(new char[]{'\n',' ','\t'}).EndsWith(";"))
                {
                    alterations[x] = alterations[x] + ";";
                }
            }

            Utility.RemoveDuplicateStrings(ref alterations, new string[] { " COMMIT;" });
			
			if (alterations.Count>12)
			{
				try{
					if (Debug)
					{
						foreach (string str in alterations)
						{
							if (!str.EndsWith(";"))
								Logger.LogLine(str + ";");
							else
								Logger.LogLine(str);
						}
					}
					else
					{
						foreach (string str in alterations)
						{
							if (str.Length>0)
							{
								if (str==" COMMIT;")
									conn.Commit();
								else if (str.EndsWith(" COMMIT;"))
								{
									conn.ExecuteNonQuery(str.Substring(0,str.Length-8));
									conn.Commit();
								}
								else
									conn.ExecuteNonQuery(str);
							}
						}
					}
				}catch (Exception e)
				{
					Logger.LogLine(e.Message);
					Logger.LogLine(e.StackTrace);
					throw e;
				}
			}
			conn.Commit();
			
			if (!Debug&&(_enums.Count>0))
			{
				foreach (Type t in _enums.Keys)
				{
                    if (translations.ContainsKey(t))
                    {
                        foreach (EnumTranslationPair etp in translations[t])
                        {
                            conn.ExecuteNonQuery(String.Format(
                                "UPDATE {0} SET {1} = '{3}' WHERE {1} = '{2}'",
                                new object[]{
                                _enums[t],
                                CorrectName("VALUE"),
                                etp.OriginalName,
                                etp.NewName}
                            ));
                            conn.Close();
                        }
                    }
                    Dictionary<string, int> enumValuesMap = new Dictionary<string, int>();
					Dictionary< int,string> enumReverseValuesMap = new Dictionary<int, string>();
                    List<string> enumNames = new List<string>(Enum.GetNames(t));
                    List<int> deletes = new List<int>();
                    conn.ExecuteQuery(String.Format("SELECT ID,{1} FROM {0}",
                        _enums[t],
                        CorrectName("VALUE")));
                    while (conn.Read()) {
                        if (enumNames.Contains(conn[1].ToString()))
                        {
                            enumValuesMap.Add(conn[1].ToString(), (int)conn[0]);
                            enumReverseValuesMap.Add((int)conn[0], conn[1].ToString());
                            enumNames.Remove(conn[1].ToString());
                        }
                        else
                            deletes.Add((int)conn[0]);
                    }
                    conn.Close();
                    if (deletes.Count > 0)
                    {
                        foreach (int i in deletes)
                        {
                            conn.ExecuteNonQuery(String.Format("DELETE FROM {0} WHERE ID = {1}",
                                _enums[t],
                                i));
                            conn.Close();
                        }
                    }
                    if (enumNames.Count > 0)
                    {
                        foreach (string str in enumNames)
                        {
                            conn.ExecuteNonQuery(String.Format("INSERT INTO {0}({1}) VALUES('{2}')",
                                _enums[t],
                                CorrectName("VALUE"),
                                str));
                            conn.Close();
                            conn.ExecuteQuery(String.Format("SELECT ID FROM {0} WHERE {1}='{2}'",
                                _enums[t],
                                CorrectName("VALUE"),
                                str));
                            conn.Read();
                            enumValuesMap.Add(str, (int)conn[0]);
                            enumReverseValuesMap.Add((int)conn[0], str);
                            conn.Close();
                        }
                    }
                    conn.Commit();
                    _enums.AssignMapValues(t, enumValuesMap, enumReverseValuesMap);
				}
			}
			
			conn.CloseConnection();
		}
		
		public Connection getConnection()
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
            List<ExtractedTableMap> maps;
            List<Trigger> triggers;
            List<Generator> gens;
            List<IdentityField> identities;
            List<View> views;
            List<ForeignKey> keys = new List<ForeignKey>();
            List<StoredProcedure> procedures;
            ExtractExpectedStructure(out maps, out triggers, out gens, out identities,out views,out procedures, conn);
            foreach (ExtractedTableMap map in maps)
            {
                List<string> extTables = new List<string>();
                foreach (ForeignRelationMap frm in map.ForeignFields)
                {
                    if (!extTables.Contains(frm.ExternalTable))
                        extTables.Add(frm.ExternalTable);
                }
                foreach (string str in extTables)
                {
                    foreach (List<ForeignRelationMap> frms in map.RelatedFieldsForTable(str))
                        keys.Add(new ForeignKey(map, str,frms[0].ID));
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
            List<ExtractedTableMap> maps;
            List<Trigger> triggers;
            List<Generator> gens;
            List<IdentityField> identities;
            List<PrimaryKey> keys = new List<PrimaryKey>();
            List<View> views;
            List<StoredProcedure> procedures;
            ExtractExpectedStructure(out maps, out triggers, out gens, out identities,out views,out procedures, conn);
            foreach (ExtractedTableMap map in maps)
            {
                if ((map.PrimaryKeys != null) && (map.PrimaryKeys.Count > 0))
                {
                    keys.Add(new PrimaryKey(map));
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
    }
}