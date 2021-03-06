/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 23/03/2008
 * Time: 9:30 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */

using System;
using Org.Reddragonit.Dbpro.Connections;
using System.IO;
using System.Reflection;
using System.Xml;
using Org.Reddragonit.Dbpro.Structure.Attributes;
using System.Data;

namespace Org.Reddragonit.Dbpro.Connections.Firebird
{
	/// <summary>
	/// Description of FBConnectionPool.
	/// </summary>
	public class FBConnectionPool : ConnectionPool
	{
        internal const string _PARAMETER_CLASS_NAME = "FirebirdSql.Data.FirebirdClient.FbParameter";

        private string _connectionString;
        protected override string connectionString
        {
            get { return _connectionString; }
        }

        public FBConnectionPool(XmlElement elem)
            : base(elem)
        {
            int port = 3050;
            string databaseServer = null;
            string databasePath = null;
            string username = null;
            string password = null;
            string charset= null;
            foreach (XmlNode node in elem.ChildNodes)
            {
                if (node.Name == "ConnectionParameter")
                {
                    switch (node.Attributes["parameter_name"].Value)
                    {
                        case "databaseServer":
                            databaseServer = node.Attributes["parameter_value"].Value;
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
                        case "databasePath":
                            databasePath = node.Attributes["parameter_value"].Value;
                            break;
                        case "charset":
                            charset = (node.Attributes["parameter_value"].Value == "null" ? null : node.Attributes["parameter_value"].Value);
                            break;
                    }
                }
            }
            _connectionString = "User=" + username + ";" +
                   "Password=" + password + ";" +
                   "Database=" + databasePath + ";" +
                   "DataSource=" + databaseServer + ";" +
                   "Port=" + port.ToString() + ";" +
                    (charset != null ? "Charset=" + charset : "");
        }
		
		private string[] _words=null;
		protected override string[] _ReservedWords {
			get {
				if (_words==null)
				{
					_words=Utility.MergeStringArrays(base._ReservedWords,new string[]{
				                                 	"ACTIVE","ASCENDING","AUTO","AUTODDL","BASED",
				                                 	"BASENAME","BASE_NAME","BLOBEDIT","BUFFER","CACHE",
				                                 	"CHECK_POINT_LEN","CHECK_POINT_LENGTH","COMMITTED","COMPILETIME","COMPUTED",
				                                 	"CONDITIONAL","CONTAINING","CSTRING","DB_KEY","DEBUG",
				                                 	"DESCENDING","DISPLAY","EDIT","ENTRY_POINT","EVENT",
				                                 	"EXTERN","FILTER","FREE_IT","GDSCODE","GENERATOR",
				                                 	"GEN_ID","GROUP_COMMIT_WAIT","GROUP_COMMIT_WAIT_TIME","HELP","INACTIVE",
				                                 	"INIT","INPUT_TYPE","ISQL","LC_MESSAGES","LC_TYPE",
				                                 	"LENGTH","LEV","LOGFILE","LOG_BUFFER_SIZE","LOG_BUF_SIZE",
				                                 	"MANUAL","MAXIMUM","MAXIMUM_SEGMENT","MAX_SEGMENT","MERGE",
				                                 	"MINIMUM","MODULE_NAME","NOAUTO","NUM_LOG_BUFS","NUM_LOG_BUFFERS",
				                                 	"OUTPUT_TYPE","OVERFLOW","PAGE","PAGELENGTH","PAGES",
				                                 	"PAGE_SIZE","POST_EVENT","RAW_PARTITIONS","RDB$DB_KEY","RECORD_VERSION",
				                                 	"RESERV","RESERVING","RETAIN","RETURNING_VALUES","RUNTIME",
				                                 	"SHADOW","SHARED","SHELL","SHOW","SINGULAR",
				                                 	"SNAPSHOT","SORT","STABILITY","STARTING","STARTS",
				                                 	"SUB_TYPE","SUSPEND","TERMINATOR","UNCOMMITTED","VERSION",
				                                 	"WEEKDAY","YEARDAY","BIN_AND","BIN_OR"
				                                 });
				}
				return _words;
			}
		}
		
		public override int MaxFieldNameLength {
			get { return 31; }
		}

        private bool _supportsBoolean = false;
        internal bool SupportsBoolean { get { return _supportsBoolean; } }
        private bool _booleanChecked = false;

        protected override Connection CreateConnection(bool exclusiveLock)
		{
			Connection ret = new FBConnection(this,connectionString,_readonly,exclusiveLock);
            if (!_booleanChecked)
            {
                _booleanChecked = true;
                try
                {
                    ret.ExecuteQuery("SELECT CAST(SUBSTRING(rdb$get_context('SYSTEM', 'ENGINE_VERSION') FROM 1 FOR POSITION('.' IN rdb$get_context('SYSTEM', 'ENGINE_VERSION'))) AS INTEGER) from rdb$database");
                    ret.Read();
                    _supportsBoolean = (int)ret[0] >= 3;
                    ret.Close();
                }
                catch (Exception e) { }
            }
            return ret;
		}

        protected override void PreInit()
        {
        }

        protected override void _InitClass()
        {
            if (Utility.LocateType(_PARAMETER_CLASS_NAME) == null)
                Assembly.Load(FBConnection._ASSEMBLY_NAME);
        }

        protected override bool _IsCoreStoredProcedure(StoredProcedure storedProcedure)
        {
            return false;
        }

        private QueryBuilder _builder;
        internal override QueryBuilder queryBuilder
        {
            get
            {
                if (_builder == null)
                    _builder = new FBQueryBuilder(this);
                return _builder;
            }
        }

        internal override IDbDataParameter CreateParameter(string parameterName, object parameterValue, FieldType type, int fieldLength)
        {
            object val = parameterValue;
            if (type == FieldType.BYTE)
            {
                if (!((fieldLength == -1) || (fieldLength > 32767)))
                {
                    if (parameterValue != null)
                    {
                        if (fieldLength == 1)
                            val = (char)(byte)val;
                        else
                        {
                            char[] tmp = new char[((byte[])parameterValue).Length];
                            for (int x = 0; x < tmp.Length; x++)
                            {
                                tmp[x] = (char)((byte[])parameterValue)[x];
                            }
                            val = tmp;
                        }
                    }
                }
            }
            return CreateParameter(parameterName, val);
        }

        protected override IDbDataParameter _CreateParameter(string parameterName, object parameterValue)
        {
            if (parameterValue is bool && !SupportsBoolean)
            {
                if ((bool)parameterValue)
                    parameterValue = 'T';
                else
                    parameterValue = 'F';
            }
            else if ((parameterValue is uint) || (parameterValue is UInt32))
            {
                parameterValue = BitConverter.ToInt32(BitConverter.GetBytes(uint.Parse(parameterValue.ToString())), 0);
            }
            else if ((parameterValue is UInt16) || (parameterValue is ushort))
            {
                parameterValue = BitConverter.ToInt16(BitConverter.GetBytes(ushort.Parse(parameterValue.ToString())), 0);
            }
            else if ((parameterValue is ulong) || (parameterValue is UInt64))
            {
                parameterValue = BitConverter.ToInt64(BitConverter.GetBytes(ulong.Parse(parameterValue.ToString())), 0);
            }
            return (System.Data.IDbDataParameter)Utility.LocateType(_PARAMETER_CLASS_NAME).GetConstructor(new Type[] { typeof(string), typeof(object) }).Invoke(new object[] { parameterName, parameterValue });
        }

        internal override string TrueString
        {
            get
            {
                return (SupportsBoolean ? "true" : "'T'");
            }
        }

        internal override string FalseString
        {
            get
            {
                return (SupportsBoolean ? "false" : "'F'");
            }
        }
        
	}
}
