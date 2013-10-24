using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Reflection;
using System.Xml;
using System.Data;
using Org.Reddragonit.Dbpro.Structure.Attributes;

namespace Org.Reddragonit.Dbpro.Connections.MsSql
{
	class MsSqlConnectionPool : ConnectionPool
	{
        private const string _PARAMETER_TYPE_NAME = "System.Data.SqlClient.SqlParameter";

        private string _connectionString;
        protected override string connectionString
        {
            get { return _connectionString; }
        }

        public MsSqlConnectionPool(XmlElement elem)
            : base(elem)
        {
            int port=1433;
            string databaseServer = null;
            string database = null;
            string username = null;
            string password = null;
            foreach (XmlNode node in elem.ChildNodes)
            {
                if (node.Name == "ConnectionParameter")
                {
                    switch(node.Attributes["parameter_name"].Value){
                        case "databaseServer":
                            databaseServer=node.Attributes["parameter_value"].Value;
                            break;
                        case "port":
                            port = int.Parse(node.Attributes["parameter_value"].Value);
                            break;
                        case "username":
                            username = node.Attributes["parameter_value"].Value;
                            break;
                        case "password":
                            password = node.Attributes["parameter_value"].Value;
                            break;
                        case "database":
                            database = node.Attributes["parameter_value"].Value;
                            break;
                    }
                }
            }
            _connectionString = "Data Source=" + databaseServer + ", " + port.ToString() +
                   ";Initial Catalog=" + database + ";" +
                   "User ID=" + username + ";" +
                   "Password=" + password + ";";
            _catalog = database;
        }

        private string _catalog;
        public string Catalog
        {
            get { return _catalog; }
        }

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
		
		public override int MaxFieldNameLength {
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

                c.CloseConnection();
            }
		}

        protected override void _InitClass()
        {
            if (Utility.LocateType(MsSqlConnection._CONNECTION_TYPE_NAME) == null)
                Assembly.Load(MsSqlConnection._ASSEMBLY_NAME);
        }

        protected override bool _IsCoreStoredProcedure(StoredProcedure storedProcedure)
        {
            bool ret = false;
            XmlDocument doc = new XmlDocument();
            doc.LoadXml(new StreamReader(Assembly.GetAssembly(typeof(MsSqlConnection)).GetManifestResourceStream("Org.Reddragonit.Dbpro.Connections.MsSql.StringIDProcedures.xml")).ReadToEnd());
            foreach (XmlElement proc in doc.GetElementsByTagName("Procedure"))
            {
                if (proc.ChildNodes[0].InnerText == storedProcedure.ProcedureName)
                {
                    ret = true;
                    break;
                }
            }
            return ret;
        }

        private QueryBuilder _builder;
        internal override QueryBuilder queryBuilder
        {
            get
            {
                if (_builder == null)
                    _builder = new MSSQLQueryBuilder(this);
                return _builder;
            }
        }

        internal override System.Data.IDbDataParameter CreateParameter(string parameterName, object parameterValue)
        {
            if (parameterValue != null)
            {
                if (Utility.IsEnum(parameterValue.GetType()))
                {
                    if (parameterValue != null)
                        parameterValue = GetEnumID(parameterValue.GetType(), parameterValue.ToString());
                    else
                        parameterValue = (int?)null;
                }
            }
            if ((parameterValue is uint) || (parameterValue is UInt32))
            {
                parameterValue = System.Text.ASCIIEncoding.ASCII.GetString(System.BitConverter.GetBytes(uint.Parse(parameterValue.ToString()))).ToCharArray();
            }
            else if ((parameterValue is UInt16) || (parameterValue is ushort))
            {
                parameterValue = System.Text.ASCIIEncoding.ASCII.GetString(System.BitConverter.GetBytes(ushort.Parse(parameterValue.ToString()))).ToCharArray();
            }
            else if ((parameterValue is ulong) || (parameterValue is UInt64))
            {
                parameterValue = System.Text.ASCIIEncoding.ASCII.GetString(System.BitConverter.GetBytes(ulong.Parse(parameterValue.ToString()))).ToCharArray();
            }
            return (IDbDataParameter)Utility.LocateType(_PARAMETER_TYPE_NAME).GetConstructor(new Type[] { typeof(string), typeof(object) }).Invoke(new object[] { parameterName, parameterValue });
        }

        internal override IDbDataParameter CreateParameter(string parameterName, object parameterValue, FieldType type, int fieldLength)
        {
            if (parameterValue != null)
            {
                if (Utility.IsEnum(parameterValue.GetType()))
                {
                    if (parameterValue != null)
                        parameterValue = GetEnumID(parameterValue.GetType(), parameterValue.ToString());
                    else
                        parameterValue = (int?)null;
                }
            }
            IDbDataParameter ret = CreateParameter(parameterName, parameterValue);
            return ret;
        }

        internal override string TrueString
        {
            get
            {
                return "1";
            }
        }

        internal override string FalseString
        {
            get
            {
                return "0";
            }
        }


        internal List<IDbDataParameter> DuplicateParameters(List<IDbDataParameter> parameters)
        {
            List<IDbDataParameter> ret = new List<IDbDataParameter>();
            foreach (IDbDataParameter par in parameters)
                ret.Add(CreateParameter(par.ParameterName, par.Value));
            return ret;
        }
    }
}
