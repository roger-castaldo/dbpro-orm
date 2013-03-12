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

namespace Org.Reddragonit.Dbpro.Connections.Firebird
{
	/// <summary>
	/// Description of FBConnectionPool.
	/// </summary>
	public class FBConnectionPool : ConnectionPool
	{
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
				                                 	"WEEKDAY","YEARDAY"
				                                 });
				}
				return _words;
			}
		}
		
		public override int MaxFieldNameLength {
			get { return 31; }
		}
		
		protected override Connection CreateConnection(bool exclusiveLock)
		{
			return new FBConnection(this,connectionString,_readonly,exclusiveLock);
		}

        protected override void PreInit()
        {
        }

        protected override void _InitClass()
        {
            if (Utility.LocateType(FBConnection._PARAMETER_CLASS_NAME) == null)
                Assembly.Load(FBConnection._ASSEMBLY_NAME);
        }

        protected override bool _IsCoreStoredProcedure(StoredProcedure storedProcedure)
        {
            bool ret = false;
            XmlDocument doc = new XmlDocument();
            doc.LoadXml(new StreamReader(Assembly.GetAssembly(typeof(FBConnection)).GetManifestResourceStream("Org.Reddragonit.Dbpro.Connections.Firebird.StringIDProcedures.xml")).ReadToEnd());
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
	}
}
