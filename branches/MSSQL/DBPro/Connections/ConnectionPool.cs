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
using Org.Reddragonit.Dbpro.Structure.Mapping;
using ExtractedTableMap = Org.Reddragonit.Dbpro.Connections.ExtractedTableMap;
using ExtractedFieldMap = Org.Reddragonit.Dbpro.Connections.ExtractedFieldMap;
using ForeignRelationMap = Org.Reddragonit.Dbpro.Connections.ForeignRelationMap;
using FieldType = Org.Reddragonit.Dbpro.Structure.Attributes.FieldType;
using VersionTypes = Org.Reddragonit.Dbpro.Structure.Attributes.VersionField.VersionTypes;
using UpdateDeleteAction =  Org.Reddragonit.Dbpro.Structure.Attributes.ForeignField.UpdateDeleteAction;

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
		private bool _debugMode=false;
		
		private bool isClosed=false;
		private bool isReady=false;
		private string _connectionName;
		
		protected abstract Connection CreateConnection();
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
		
		protected virtual int MaxFieldNameLength{
			get{
				return int.MaxValue;
			}
		}
		
		private string[] _reservedWords=null;
		private Dictionary<string, string> _nameTranslations = new Dictionary<string, string>();
		
		internal string[] ReservedWords{
			get{
				if (_reservedWords==null)
					_reservedWords=_ReservedWords;
				return _reservedWords;
			}
		}
		
		internal string CorrectName(string currentName)
		{
			if (_nameTranslations.ContainsValue(currentName))
			{
				foreach (string str in _nameTranslations.Keys)
				{
					if (_nameTranslations[str]==currentName)
						return str;
				}
				return null;
			}
			else{
				string ret = currentName;
				bool reserved=false;
				foreach (string str in ReservedWords)
				{
					if (Utility.StringsEqualIgnoreCaseWhitespace(str,currentName))
					{
						reserved=true;
						break;
					}
				}
				if (reserved)
					ret="RES_"+ret;
				if (ret.Length>MaxFieldNameLength)
				{
					int _nameCounter=0;
					while (_nameTranslations.ContainsKey(ret.Substring(0,MaxFieldNameLength-1-(_nameCounter.ToString().Length))+"_"+_nameCounter.ToString()))
					{
						_nameCounter++;
					}
					ret=ret.Substring(0,MaxFieldNameLength-1-(_nameCounter.ToString().Length));
					ret+="_"+_nameCounter.ToString();
				}
				if (!_nameTranslations.ContainsKey(ret))
					_nameTranslations.Add(ret,currentName);
				return ret;
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
		
		protected ConnectionPool(string connectionString,int minPoolSize,int maxPoolSize,long maxKeepAlive,bool UpdateStructureDebugMode,string connectionName)
		{
			System.Diagnostics.Debug.WriteLine("Establishing Connection with string: "+connectionString);
			this.connectionString=connectionString;
			this.minPoolSize=minPoolSize;
			this.maxPoolSize=maxPoolSize;
			this.maxKeepAlive=maxKeepAlive;
			_debugMode=UpdateStructureDebugMode;
			_connectionName=connectionName;
			ConnectionPoolManager.AddConnection(connectionName,this);
		}
		
		internal void Init()
		{
			PreInit();
			ClassMapper.CorrectNamesForConnection(this);
			UpdateStructure(_debugMode);
			for (int x=0;x<minPoolSize;x++)
				unlocked.Enqueue(CreateConnection());
			isReady=true;
		}

		private void ExtractCurrentStructure(out List<ExtractedTableMap> tables,out List<Trigger> triggers,out List<Generator> generators,out List<IdentityField> identities,Connection conn)
		{
			tables = new List<ExtractedTableMap>();
			triggers = new List<Trigger>();
			generators=new List<Generator>();
			identities=new List<IdentityField>();
			conn.ExecuteQuery(conn.queryBuilder.SelectTriggers());
			while (conn.Read())
			{
				triggers.Add(new Trigger((string)conn[0],(string)conn[1],(string)conn[2]));
			}
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
					etm.ForeignFields.Add(new ForeignRelationMap(conn[0].ToString(),conn[1].ToString(),
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
		
		private void ExtractExpectedStructure(out List<ExtractedTableMap> tables,out List<Trigger> triggers,out List<Generator> generators,out List<IdentityField> identities,Connection conn)
		{
			tables = new List<ExtractedTableMap>();
			triggers = new List<Trigger>();
			generators=new List<Generator>();
			identities = new List<IdentityField>();
			List<Trigger> tmpTriggers = new List<Trigger>();
			List<Generator> tmpGenerators = new List<Generator>();
			List<IdentityField> tmpIdentities = new List<IdentityField>();
			foreach (System.Type type in ClassMapper.TableTypesForConnection(ConnectionName))
			{
				TableMap tm = ClassMapper.GetTableMap(type);
				ExtractedTableMap etm = new ExtractedTableMap(tm.Name);
				foreach (InternalFieldMap ifm in tm.Fields)
				{
					if (!ifm.IsArray)
						etm.Fields.Add(new ExtractedFieldMap(ifm.FieldName,conn.TranslateFieldType(ifm.FieldType,ifm.FieldLength),
					                                     ifm.FieldLength,ifm.PrimaryKey,ifm.Nullable,ifm.AutoGen));
					else
					{
						ExtractedTableMap etmField = new ExtractedTableMap(CorrectName(tm.Name+"_"+ifm.FieldName));
						foreach (InternalFieldMap ifmField in tm.PrimaryKeys)
						{
							etmField.Fields.Add(new ExtractedFieldMap(CorrectName(tm.Name+"_"+ifmField.FieldName),conn.TranslateFieldType(ifmField.FieldType,ifmField.FieldLength),
							                                          ifmField.FieldLength,true,false,false));
							etmField.ForeignFields.Add(new ForeignRelationMap(CorrectName(tm.Name+"_"+ifmField.FieldName),tm.Name,
							                                                  ifmField.FieldName,UpdateDeleteAction.CASCADE.ToString(),UpdateDeleteAction.CASCADE.ToString()));
						}
						etmField.Fields.Add(new ExtractedFieldMap(CorrectName(tm.Name+"_"+ifm.FieldName+"_ID"),conn.TranslateFieldType(FieldType.LONG,0),8,
						                                          true,false,true));
						etmField.Fields.Add(new ExtractedFieldMap(CorrectName(ifm.FieldName+"_VALUE"),conn.TranslateFieldType(ifm.FieldType,ifm.FieldLength),ifm.FieldLength,
						                                          false,ifm.Nullable,false));
						tables.Add(etmField);
						conn.GetAddAutogen(etmField,this,out tmpIdentities,out tmpGenerators,out tmpTriggers);
						if (tmpGenerators!=null)
							generators.AddRange(tmpGenerators);
						if (tmpTriggers!=null)
							triggers.AddRange(tmpTriggers);
						if (tmpIdentities!=null)
							identities.AddRange(tmpIdentities);
					}
				}
				foreach (Type t in tm.ForeignTables)
				{
					TableMap ftm = ClassMapper.GetTableMap(t);
					ExternalFieldMap efm = tm.GetFieldInfoForForeignTable(t);
					foreach (InternalFieldMap ifm in ftm.PrimaryKeys)
					{
						etm.ForeignFields.Add(new ForeignRelationMap(CorrectName(efm.AddOnName+"_"+ifm.FieldName),ftm.Name,
						                                             ifm.FieldName,efm.OnUpdate.ToString(),efm.OnDelete.ToString()));
					}
				}
				tables.Add(etm);
				foreach (ExternalFieldMap efm in tm.ExternalFieldMapArrays)
				{
					TableMap ftm = ClassMapper.GetTableMap(efm.Type);
					ExtractedTableMap aetm = new ExtractedTableMap(CorrectName(tm.Name+"_"+ftm.Name));
					foreach (InternalFieldMap ifm in tm.PrimaryKeys)
					{
						aetm.Fields.Add(new ExtractedFieldMap(CorrectName(tm.Name+"_"+ifm.FieldName),conn.TranslateFieldType(ifm.FieldType,ifm.FieldLength),
						                                      ifm.FieldLength,true,false,false));
						aetm.ForeignFields.Add(new ForeignRelationMap(tm.Name+"_"+ifm.FieldName,tm.Name,ifm.FieldName,efm.OnUpdate.ToString(),efm.OnDelete.ToString()));
					}
					foreach (InternalFieldMap ifm in ftm.PrimaryKeys)
					{
						aetm.Fields.Add(new ExtractedFieldMap(CorrectName(ftm.Name+"_"+ifm.FieldName),conn.TranslateFieldType(ifm.FieldType,ifm.FieldLength),
						                                      ifm.FieldLength,true,false,false));
						aetm.ForeignFields.Add(new ForeignRelationMap(CorrectName(ftm.Name+"_"+ifm.FieldName),ftm.Name,ifm.FieldName,efm.OnUpdate.ToString(),efm.OnDelete.ToString()));
					}
					tables.Add(aetm);
				}
				if (tm.VersionType!=null)
				{
					ExtractedTableMap vetm = new ExtractedTableMap(CorrectName(conn.queryBuilder.VersionTableName(tm.Name)));
					if (tm.VersionType.Value==VersionTypes.DATESTAMP)
						vetm.Fields.Add(new ExtractedFieldMap(CorrectName(conn.queryBuilder.VersionFieldName(tm.Name)),conn.TranslateFieldType(FieldType.DATETIME,0),8,
						                                      true,false,false));
					else
						vetm.Fields.Add(new ExtractedFieldMap(CorrectName(conn.queryBuilder.VersionFieldName(tm.Name)),conn.TranslateFieldType(FieldType.LONG,0),8,
						                                      true,false,false));
					foreach (InternalFieldMap ifm in tm.Fields)
					{
						if (ifm.Versionable||ifm.PrimaryKey)
						{
							vetm.Fields.Add(new ExtractedFieldMap(ifm.FieldName,conn.TranslateFieldType(ifm.FieldType,ifm.FieldLength),
							                                      ifm.FieldLength,ifm.PrimaryKey,ifm.Nullable,ifm.AutoGen));
							if (ifm.PrimaryKey)
								vetm.ForeignFields.Add(new ForeignRelationMap(ifm.FieldName,etm.TableName,ifm.FieldName,"CASCADE","CASCADE"));
						}
					}
					triggers.AddRange(conn.GetVersionTableTriggers(vetm,tm.VersionType.Value,this));
					tables.Add(vetm);
				}
			}
			foreach(ExtractedTableMap etm in tables)
			{
				System.Diagnostics.Debug.WriteLine(etm.TableName+":");
				foreach (ExtractedFieldMap efm in etm.Fields)
					System.Diagnostics.Debug.WriteLine("\t"+efm.FieldName+" - "+efm.PrimaryKey.ToString());
				foreach (ExtractedFieldMap efm in etm.PrimaryKeys)
				{
					if (efm.AutoGen)
					{
						conn.GetAddAutogen(etm,this,out tmpIdentities,out tmpGenerators,out tmpTriggers);
						if (tmpGenerators!=null)
							generators.AddRange(tmpGenerators);
						if (tmpTriggers!=null)
							triggers.AddRange(tmpTriggers);
						if (tmpIdentities!=null)
							identities.AddRange(tmpIdentities);
					}
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
					if ((idf.TableName==i.TableName)&&(idf.FieldName==i.FieldType)&&(idf.FieldType==i.FieldType))
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
					if ((idf.TableName==i.TableName)&&(idf.FieldName==i.FieldType)&&(idf.FieldType==i.FieldType))
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
										constraintDrops.Add(conn.queryBuilder.DropNullConstraint(etm.TableName,efm,conn));
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
								constraintDrops.Add(conn.queryBuilder.DropNullConstraint(etm.TableName,efm,conn));
						}
						break;
					}
				}
				if (!found)
				{
					foreach (ExtractedFieldMap efm in etm.Fields)
					{
						if (!efm.Nullable)
							constraintDrops.Add(conn.queryBuilder.DropNullConstraint(etm.TableName,efm,conn));
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
		
		private void ExtractForeignKeyCreatesDrops(List<ExtractedTableMap> curStructure,List<ExtractedTableMap> expectedStructure,out List<ForeignKey> foreignKeyDrops,out List<ForeignKey> foreignKeyCreations)
		{
			foreignKeyDrops=new List<ForeignKey>();
			foreignKeyCreations=new List<ForeignKey>();
			foreach (ExtractedTableMap etm in expectedStructure)
			{
				bool found=false;
				foreach (ExtractedTableMap e in curStructure)
				{
					if (etm.TableName==e.TableName)
					{
						found=true;
						foreach (string tableName in etm.RelatedTables)
						{
							bool foundtable=false;
							foreach (string etableName in e.RelatedTables)
							{
								if (etableName==tableName)
								{
									foundtable=true;
									bool diffRelation=false;
									foreach (ForeignRelationMap frm in etm.RelatedFieldsForTable(tableName))
									{
										bool foundRelation=false;
										foreach (ForeignRelationMap efrm in e.RelatedFieldsForTable(etableName))
										{
											if ((frm.InternalField==efrm.InternalField)&&(frm.ExternalField==efrm.ExternalField)&&(frm.ExternalTable==efrm.ExternalTable)&&(frm.OnDelete==efrm.OnDelete)&&(frm.OnUpdate==efrm.OnUpdate))
											{
												foundRelation=true;
												foreach (ExtractedFieldMap efm in etm.Fields)
												{
													if (efm.FieldName==frm.InternalField)
													{
														foreach (ExtractedFieldMap ee in e.Fields)
														{
															if (ee.FieldName==frm.InternalField)
															{
																if ((ee.Type!=efm.Type)||(ee.Size!=efm.Size)||(ee.Nullable!=efm.Nullable)||(ee.PrimaryKey!=efm.PrimaryKey))
																{
																	diffRelation=true;
																}
																break;
															}
														}
														break;
													}
												}
												break;
											}
										}
										if (!foundRelation)
											foundRelation=true;
										if (diffRelation)
											break;
									}
									if (diffRelation)
									{
										foreignKeyDrops.Add(new ForeignKey(e,etableName));
										foreignKeyCreations.Add(new ForeignKey(etm,tableName));
									}
									break;
								}
							}
							if (!foundtable)
							{
								foreignKeyCreations.Add(new ForeignKey(etm,tableName));
							}
						}
						break;
					}
				}
				if (!found)
				{
					foreach (string tableName in etm.RelatedTables)
					{
						foreignKeyCreations.Add(new ForeignKey(etm,tableName));
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
						foreach (string tableName in etm.RelatedTables)
						{
							bool foundTable=false;
							foreach (string etableName in e.RelatedTables)
							{
								if (tableName==etableName)
								{
									foundTable=true;
									break;
								}
							}
							if (!foundTable)
								foreignKeyDrops.Add(new ForeignKey(etm,tableName));
						}
						break;
					}
				}
				if (!found)
				{
					foreach (string tableName in etm.RelatedTables)
						foreignKeyDrops.Add(new ForeignKey(etm,tableName));
				}
			}
		}

		private void UpdateStructure(bool Debug)
		{
			Connection conn = CreateConnection();
			List<ExtractedTableMap> curStructure =new List<ExtractedTableMap>();
			List<Trigger> curTriggers = new List<Trigger>();
			List<Generator> curGenerators = new List<Generator>();
			List<IdentityField> curIdentities = new List<IdentityField>();
			
			ExtractCurrentStructure(out curStructure,out curTriggers,out curGenerators,out curIdentities,conn);
			
			List<ExtractedTableMap> expectedStructure = new List<ExtractedTableMap>();
			List<Trigger> expectedTriggers = new List<Trigger>();
			List<Generator> expectedGenerators = new List<Generator>();
			List<IdentityField> expectedIdentities = new List<IdentityField>();
			
			ExtractExpectedStructure(out expectedStructure,out expectedTriggers,out expectedGenerators,out expectedIdentities,conn);
			
			List<Trigger> dropTriggers = new List<Trigger>();
			List<Trigger> createTriggers = new List<Trigger>();
			
			CompareTriggers(curTriggers,expectedTriggers,out dropTriggers,out createTriggers);
			
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
			
			ExtractForeignKeyCreatesDrops(curStructure,expectedStructure,out foreignKeyDrops,out foreignKeyCreations);
			
			List<IdentityField> dropIdentities=new List<IdentityField>();
			List<IdentityField> createIdentities=new List<IdentityField>();
			List<IdentityField> setIdentities=new List<IdentityField>();
			
			CompareIdentities(curIdentities,expectedIdentities,out dropIdentities,out createIdentities,out setIdentities);
			
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
				if (!found)
				{
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
									if ((efm.Type!=ee.Type)||(efm.Size!=ee.Size))
									{
										if (efm.PrimaryKey&&ee.PrimaryKey)
										{
											primaryKeyDrops.Add(new PrimaryKey(etm));
											primaryKeyCreations.Add(new PrimaryKey(etm));
											for(int x=0;x<expectedStructure.Count;x++)
											{
												if (expectedStructure[x].RelatesToField(etm.TableName,efm.FieldName))
												{
													foreignKeyDrops.Add(new ForeignKey(expectedStructure[x],etm.TableName));
													foreignKeyCreations.Add(new ForeignKey(expectedStructure[x],etm.TableName));
												}
											}
										}
										foreach (string tbl in etm.ExternalTablesForField(efm.FieldName))
										{
											foreignKeyDrops.Add(new ForeignKey(etm,tbl));
											foreignKeyCreations.Add(new ForeignKey(etm,tbl));
										}
										tableAlterations.Add(conn.queryBuilder.AlterFieldType(etm.TableName,efm));
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
			
			List<string> alterations = new List<string>();
			//add drops to alterations
			alterations.AddRange(constraintDrops);
			alterations.Add(" COMMIT;");
			
			foreach (Trigger trig in dropTriggers)
				alterations.Add(conn.queryBuilder.DropTrigger(trig.Name));
			alterations.Add(" COMMIT;");
			
			foreach (Generator gen in dropGenerators)
				alterations.Add(conn.queryBuilder.DropGenerator(gen.Name));
			alterations.Add(" COMMIT;");
			
			foreach (ForeignKey fk in foreignKeyDrops)
				alterations.Add(conn.queryBuilder.DropForeignKey(fk.InternalTable,fk.ExternalTable,conn));
			alterations.Add(" COMMIT;");
			
			foreach (PrimaryKey pk in primaryKeyDrops)
			{
				foreach (string field in pk.Fields)
					alterations.Add(conn.queryBuilder.DropPrimaryKey(pk,conn));
			}
			alterations.Add(" COMMIT;");
			
			foreach (IdentityField idf in dropIdentities)
				alterations.Add(conn.queryBuilder.DropIdentityField(idf));
			alterations.Add(" COMMIT;");
			
			alterations.AddRange(tableAlterations);
			alterations.Add(" COMMIT;");
			
			alterations.AddRange(tableCreations);
			alterations.Add(" COMMIT;");
			
			//add creations to alterations
			alterations.AddRange(constraintCreations);
			alterations.Add(" COMMIT;");
			
			foreach (PrimaryKey pk in primaryKeyCreations)
				alterations.Add(conn.queryBuilder.CreatePrimaryKey(pk.Name,pk.Fields));
			alterations.Add(" COMMIT;");
			
			foreach (ForeignKey fk in foreignKeyCreations)
				alterations.Add(conn.queryBuilder.CreateForeignKey(fk.InternalTable,fk.InternalFields,fk.ExternalTable,fk.ExternalFields,fk.OnUpdate,fk.OnDelete));
			alterations.Add(" COMMIT;");
			
			foreach (Generator gen in createGenerators)
			{
				alterations.Add(conn.queryBuilder.CreateGenerator(gen.Name));
				alterations.Add(conn.queryBuilder.SetGeneratorValue(gen.Name,gen.Value));
			}
			alterations.Add(" COMMIT;");
			
			foreach (Trigger trig in createTriggers)
			{
				alterations.Add(conn.queryBuilder.CreateTrigger(trig.Name,trig.Conditions,trig.Code));
			}
			alterations.Add(" COMMIT;");
			
			foreach(IdentityField idf in createIdentities)
				alterations.Add(conn.queryBuilder.CreateIdentityField(idf));
			alterations.Add(" COMMIT;");
			
			foreach (IdentityField idf in setIdentities)
				alterations.Add(conn.queryBuilder.SetIdentityFieldValue(idf));
			alterations.Add(" COMMIT;");
			
			Utility.RemoveDuplicateStrings(ref alterations,new string[]{" COMMIT;"});
			
			if (alterations.Count>12)
			{
				try{
					if (Debug)
					{
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
					System.Diagnostics.Debug.WriteLine(e.Message);
					System.Diagnostics.Debug.WriteLine(e.StackTrace);
					throw e;
				}
			}
			conn.Commit();
			conn.CloseConnection();
		}
		
		public Connection getConnection()
		{
			mut.WaitOne();
			if (!isReady)
				Init();
			mut.ReleaseMutex();
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
			if (ret!=null)
				ret.Reset();
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
