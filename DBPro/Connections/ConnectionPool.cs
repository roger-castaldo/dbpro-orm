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
		
		internal virtual bool AllowChangingBasicAutogenField{
			get{return true;}
		}
		
		private string[] _reservedWords=null;
		private Dictionary<string, string> _nameTranslations = new Dictionary<string, string>();
		internal Dictionary<Type, string> _enumTableMaps = new Dictionary<Type, string>();
		internal Dictionary<Type,Dictionary<string, int>> _enumValuesMap = new Dictionary<Type, Dictionary<string, int>>();
		internal Dictionary<Type, Dictionary<int,string>> _enumReverseValuesMap = new Dictionary<Type, Dictionary<int, string>>();
		
		internal string[] ReservedWords{
			get{
				if (_reservedWords==null)
					_reservedWords=_ReservedWords;
				return _reservedWords;
			}
		}
		
		internal string CorrectName(string currentName)
		{
            if (_nameTranslations.ContainsValue(currentName.ToUpper()))
            {
                foreach (string str in _nameTranslations.Keys)
                {
                    if (_nameTranslations[str] == currentName.ToUpper())
                        return str.ToUpper();
                }
                return null;
            }
            else if (_nameTranslations.ContainsKey(currentName))
                return currentName;
            else
            {
                string ret = currentName;
                bool reserved = false;
                foreach (string str in ReservedWords)
                {
                    if (Utility.StringsEqualIgnoreCaseWhitespace(str, currentName))
                    {
                        reserved = true;
                        break;
                    }
                }
                if (reserved)
                    ret = "RES_" + ret;
                ret = ShortenName(ret);
                if (_nameTranslations.ContainsKey(ret))
                {
                    int _nameCounter = 0;
                    while (_nameTranslations.ContainsKey(ret.Substring(0, MaxFieldNameLength - 1 - (_nameCounter.ToString().Length)) + "_" + _nameCounter.ToString()))
                    {
                        _nameCounter++;
                    }
                    ret = ret.Substring(0, MaxFieldNameLength - 1 - (_nameCounter.ToString().Length));
                    ret += "_" + _nameCounter.ToString();
                }
                if (!_nameTranslations.ContainsKey(ret))
                    _nameTranslations.Add(ret, currentName.ToUpper());
                return ret.ToUpper();
            }
		}

        private string ShortenName(string name)
        {
            if (name.Length <= MaxFieldNameLength)
                return name;
            string ret = "";
            if (name.Contains("_"))
            {
                string[] tmp = name.Split('_');
                int len = (int)Math.Floor((double)MaxFieldNameLength / (double)tmp.Length);
                foreach (string str in tmp)
                {
                    if (str.Length != 0)
                    {
                        if (str.Length > len - 1)
                            ret += str.Substring(0, len - 1) + "_";
                        else
                            ret += str + "_";
                    }
                }
                ret = ret.Substring(0, ret.Length - 1);
            }
            else
            {
                int diff = name.Length - MaxFieldNameLength - 1;
                int len = (int)Math.Floor((double)(name.Length - diff) / (double)2);
                ret = name.Substring(0, len) + "_" + name.Substring(name.Length - len);
            }
            return ret;
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
			return Enum.Parse(enumType,_enumReverseValuesMap[enumType][ID]);
		}
		
		internal int GetEnumID(Type enumType,string enumName)
		{
			return _enumValuesMap[enumType][enumName];
		}

        protected ConnectionPool(string connectionString, int minPoolSize, int maxPoolSize, long maxKeepAlive, bool UpdateStructureDebugMode, string connectionName, bool allowTableDeletions)
            :this(connectionName,minPoolSize,maxPoolSize,maxKeepAlive,UpdateStructureDebugMode,connectionName,allowTableDeletions,DEFAULT_READ_TIMEOUT)
        { }
		
		protected ConnectionPool(string connectionString,int minPoolSize,int maxPoolSize,long maxKeepAlive,bool UpdateStructureDebugMode,string connectionName,bool allowTableDeletions,int readTimeout)
		{
			Logger.LogLine("Establishing Connection with string: "+connectionString);
			this.connectionString=connectionString;
			this.minPoolSize=minPoolSize;
			this.maxPoolSize=maxPoolSize;
			this.maxKeepAlive=maxKeepAlive;
			_debugMode=UpdateStructureDebugMode;
			_connectionName=connectionName;
			_allowTableDeletions=allowTableDeletions;
            this.readTimeout = readTimeout;
			ConnectionPoolManager.AddConnection(connectionName,this);
		}
		
		internal void Init()
		{
            if (!_debugMode)
			    PreInit();
			ClassMapper.CorrectNamesForConnection(this);
            if (!(_debugMode&&(ClassMapper.TableTypesForConnection(this.ConnectionName).Count==0)))
			    UpdateStructure(_debugMode,_allowTableDeletions);
            for (int x=0;x<minPoolSize;x++){
            	if (unlocked.Count>=minPoolSize)
            		break;
				unlocked.Enqueue(CreateConnection());
            }
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
					{
						if (ifm.FieldType==FieldType.ENUM)
						{
							if (!_enumTableMaps.ContainsKey(ifm.ObjectType))
							{
								string[] split = ifm.ObjectType.FullName.Replace("+",".").Split(".".ToCharArray());
								string name="";
								if (split.Length>1)
									name+=split[split.Length-2]+"_"+split[split.Length-1];
								else
									name+=split[0];
                                string newName = "";
                                foreach (char c in name.ToCharArray())
                                {
                                    if (c.ToString().ToUpper() == c.ToString())
                                    {
                                        newName += "_" + c.ToString().ToLower();
                                    }
                                    else
                                    {
                                        newName += c;
                                    }
                                }
                                if (newName[0] == '_')
                                {
                                    newName = newName[1].ToString().ToUpper() + newName.Substring(2);
                                }
                                newName = "ENUM_" + newName;
                                newName = newName.Replace("__", "_").Replace("__","_");
								name=CorrectName(newName.ToUpper());
								ExtractedTableMap enumMap = new ExtractedTableMap(name);
								enumMap.Fields.Add(new ExtractedFieldMap("ID",conn.TranslateFieldType(FieldType.INTEGER,4),
								                                         4,true,false,true));
								enumMap.Fields.Add(new ExtractedFieldMap(CorrectName("VALUE"),conn.TranslateFieldType(FieldType.STRING,500),
								                                         500,false,false,false));
								tables.Add(enumMap);
								conn.GetAddAutogen(enumMap,this,out tmpIdentities,out tmpGenerators,out tmpTriggers);
								if (tmpGenerators!=null)
								{
									generators.AddRange(tmpGenerators);
									tmpGenerators.Clear();
								}
								if (tmpTriggers!=null)
								{
									triggers.AddRange(tmpTriggers);
									tmpTriggers.Clear();
								}
								if (tmpIdentities!=null)
								{
									identities.AddRange(tmpIdentities);
									tmpIdentities.Clear();
								}
								_enumTableMaps.Add(ifm.ObjectType,name);
							}
							etm.Fields.Add(new ExtractedFieldMap(ifm.FieldName,conn.TranslateFieldType(FieldType.INTEGER,4),4,ifm.PrimaryKey,ifm.Nullable,false));
							etm.ForeignFields.Add(new ForeignRelationMap(ifm.FieldName+"_ENUM",ifm.FieldName,CorrectName(_enumTableMaps[ifm.ObjectType]),
							                                             "ID",UpdateDeleteAction.SET_NULL.ToString(),UpdateDeleteAction.SET_NULL.ToString()));
						}
						else
							etm.Fields.Add(new ExtractedFieldMap(ifm.FieldName,conn.TranslateFieldType(ifm.FieldType,ifm.FieldLength),
							                                     ifm.FieldLength,ifm.PrimaryKey,ifm.Nullable,ifm.AutoGen));
					}
					else
					{
						ExtractedTableMap etmField = new ExtractedTableMap(CorrectName(tm.Name+"_"+ifm.FieldName));
						foreach (InternalFieldMap ifmField in tm.PrimaryKeys)
						{
							etmField.Fields.Add(new ExtractedFieldMap(CorrectName(tm.Name+"_"+ifmField.FieldName),conn.TranslateFieldType(ifmField.FieldType,ifmField.FieldLength),
							                                          ifmField.FieldLength,true,false,false));
							etmField.ForeignFields.Add(new ForeignRelationMap(ifm.FieldName,CorrectName(tm.Name+"_"+ifmField.FieldName),tm.Name,
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
					foreach (ExternalFieldMap efm in tm.GetFieldInfoForForeignTable(t)){
						foreach (InternalFieldMap ifm in ftm.PrimaryKeys)
						{
							etm.ForeignFields.Add(new ForeignRelationMap(efm.AddOnName,CorrectName(efm.AddOnName+"_"+ifm.FieldName),ftm.Name,
							                                             ifm.FieldName,efm.OnUpdate.ToString(),efm.OnDelete.ToString()));
						}
					}
				}
                if (tm.ParentType != null)
                {
                    TableMap parentMap = ClassMapper.GetTableMap(tm.ParentType);
                    foreach (InternalFieldMap ifm in parentMap.PrimaryKeys)
                    {
                        etm.ForeignFields.Add(new ForeignRelationMap(parentMap.Name,CorrectName(ifm.FieldName), CorrectName(parentMap.Name), CorrectName(ifm.FieldName), UpdateDeleteAction.CASCADE.ToString(), UpdateDeleteAction.CASCADE.ToString()));
                    }
                }
				tables.Add(etm);
				foreach (ExternalFieldMap efm in tm.ExternalFieldMapArrays)
				{
					TableMap ftm = ClassMapper.GetTableMap(efm.Type);
					ExtractedTableMap aetm = new ExtractedTableMap(CorrectName(tm.Name+"_"+ftm.Name));
					foreach (InternalFieldMap ifm in tm.PrimaryKeys)
					{
						aetm.Fields.Add(new ExtractedFieldMap(CorrectName("parent_"+ifm.FieldName),conn.TranslateFieldType(ifm.FieldType,ifm.FieldLength),
						                                      ifm.FieldLength,true,false,false));
						aetm.ForeignFields.Add(new ForeignRelationMap(efm.AddOnName+"_parent","parent_"+ifm.FieldName,tm.Name,ifm.FieldName,efm.OnUpdate.ToString(),efm.OnDelete.ToString()));
					}
					foreach (InternalFieldMap ifm in ftm.PrimaryKeys)
					{
						aetm.Fields.Add(new ExtractedFieldMap(CorrectName("child_"+ifm.FieldName),conn.TranslateFieldType(ifm.FieldType,ifm.FieldLength),
						                                      ifm.FieldLength,true,false,false));
						aetm.ForeignFields.Add(new ForeignRelationMap(efm.AddOnName+"_child",CorrectName("child_"+ifm.FieldName),ftm.Name,ifm.FieldName,efm.OnUpdate.ToString(),efm.OnDelete.ToString()));
					}
					tables.Add(aetm);
				}
				if (tm.VersionType!=null)
				{
					ExtractedTableMap vetm = new ExtractedTableMap(CorrectName(conn.queryBuilder.VersionTableName(tm.Name)));
					if (tm.VersionType.Value==VersionTypes.DATESTAMP)
						vetm.Fields.Add(new ExtractedFieldMap(CorrectName(conn.queryBuilder.VersionFieldName(tm.Name)),conn.TranslateFieldType(FieldType.DATETIME,0),8,
						                                      true,false,true));
					else
						vetm.Fields.Add(new ExtractedFieldMap(CorrectName(conn.queryBuilder.VersionFieldName(tm.Name)),conn.TranslateFieldType(FieldType.LONG,0),8,
						                                      true,false,true));
					foreach (InternalFieldMap ifm in tm.Fields)
					{
						if (ifm.Versionable||ifm.PrimaryKey)
						{
							vetm.Fields.Add(new ExtractedFieldMap(ifm.FieldName,conn.TranslateFieldType(ifm.FieldType,ifm.FieldLength),
							                                      ifm.FieldLength,ifm.PrimaryKey,ifm.Nullable,false));
							if (ifm.PrimaryKey)
								vetm.ForeignFields.Add(new ForeignRelationMap(vetm.TableName,ifm.FieldName,etm.TableName,ifm.FieldName,"CASCADE","CASCADE"));
						}
					}
					triggers.AddRange(conn.GetVersionTableTriggers(vetm,tm.VersionType.Value,this));
					tables.Add(vetm);
				}
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

		private void UpdateStructure(bool Debug,bool AllowTableDeletions)
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
            List<Trigger> recreateTriggers = new List<Trigger>();
			
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

            ExtractForeignKeyCreatesDrops(curStructure, expectedStructure, out foreignKeyDrops, out foreignKeyCreations);
			
			List<IdentityField> dropIdentities=new List<IdentityField>();
			List<IdentityField> createIdentities=new List<IdentityField>();
			List<IdentityField> setIdentities=new List<IdentityField>();

            CompareIdentities(curIdentities, expectedIdentities, out dropIdentities, out createIdentities, out setIdentities);
			
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
			
			foreach (Trigger trig in dropTriggers)
				alterations.Add(conn.queryBuilder.DropTrigger(trig.Name));
			alterations.Add(" COMMIT;");
			
			foreach (Generator gen in dropGenerators)
				alterations.Add(conn.queryBuilder.DropGenerator(gen.Name));
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
			
			//add creations to alterations
			alterations.AddRange(constraintCreations);
			alterations.Add(" COMMIT;");
			
			foreach (PrimaryKey pk in primaryKeyCreations)
				alterations.Add(conn.queryBuilder.CreatePrimaryKey(pk));
			alterations.Add(" COMMIT;");
			
			foreach (ForeignKey fk in foreignKeyCreations)
				alterations.Add(conn.queryBuilder.CreateForeignKey(fk));
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
			
			foreach (Type t in _enumTableMaps.Keys)
			{
				foreach (string str in Enum.GetNames(t))
				{
					alterations.Add(String.Format(
						"INSERT INTO {2}({0}) SELECT VAL FROM "+
						" ( "+
						" SELECT VAL, SUM(CNT) AS CNT "+
						" FROM ( "+
						" SELECT '{1}' AS VAL, "+
						" SUM(CASE RES_VALUE WHEN '{1}' THEN 1 ELSE 0 END) AS CNT   "+
						" FROM {2} "+
						" UNION    "+
						" SELECT '{1}' AS VAL, 0 AS CNT FROM {3} "+
						" ) tbl GROUP BY VAL "+
						" ) sums "+
						" WHERE CNT=0;",CorrectName("VALUE"),
						str,_enumTableMaps[t],conn.DefaultTableString
					));
				}
				alterations.Add(" COMMIT;");
			}
			
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
			
			if (!Debug&&(_enumTableMaps.Count>0))
			{
				foreach (Type t in _enumTableMaps.Keys)
				{
					_enumValuesMap.Add(t,new Dictionary<string, int>());
					_enumReverseValuesMap.Add(t,new Dictionary<int, string>());
					foreach (string str in Enum.GetNames(t))
					{
						conn.ExecuteQuery(String.Format(
							"SELECT ID FROM {0} WHERE {1} = '{2}'",
							_enumTableMaps[t],
							CorrectName("VALUE"),
							str
						));
						conn.Read();
						_enumValuesMap[t].Add(str,(int)conn[0]);
						_enumReverseValuesMap[t].Add((int)conn[0],str);
						conn.Close();
					}
				}
			}
			
			conn.CloseConnection();
		}
		
		public Connection getConnection()
		{
            Utility.WaitOne(this);
			if (!isReady)
				Init();
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
            return CreateConnection();
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
            List<ForeignKey> keys = new List<ForeignKey>();
            ExtractExpectedStructure(out maps, out triggers, out gens, out identities, conn);
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

        internal void DisableRelationships(Connection conn)
        {
            foreach (ForeignKey fk in ExtractExpectedForeignKeys(conn))
            {
                conn.ExecuteNonQuery(conn.queryBuilder.DropForeignKey(this.CorrectName(fk.InternalTable), this.CorrectName(fk.ExternalTable),this.CorrectName(fk.ExternalFields[0]),this.CorrectName(fk.InternalFields[0])));
            }
        }

        internal void EnableRelationships(Connection conn)
        {
            foreach (ForeignKey fk in ExtractExpectedForeignKeys(conn))
            {
                conn.ExecuteNonQuery(conn.queryBuilder.CreateForeignKey(fk));
            }
        }
	}
}
