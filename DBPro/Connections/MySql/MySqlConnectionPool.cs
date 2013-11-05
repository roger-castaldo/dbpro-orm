/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 16/03/2009
 * Time: 8:47 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using System;
using System.Reflection;
using System.Xml;
using System.Data;
using Org.Reddragonit.Dbpro.Structure.Attributes;

namespace Org.Reddragonit.Dbpro.Connections.MySql
{
	/// <summary>
	/// Description of MySqlConnectionPool.
	/// </summary>
	public class MySqlConnectionPool : ConnectionPool 
	{
        internal const string _PARAMETER_NAME = "MySql.Data.MySqlClient.MySqlParameter";
        private const string _SQL_DB_TYPE_ENUM = "MySql.Data.MySqlClient.MySqlDbType";

        private string _connectionString;
        protected override string connectionString
        {
            get { return _connectionString; }
        }
				
		private string _dbName=null;
		internal string DbName{
			get{return _dbName;}
		}
		
        public MySqlConnectionPool(XmlElement elem)
            : base(elem)
        {
            int port=3306;
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
            _connectionString = "Uid=" + username + ";" +
               "Pwd=" + password + ";" +
               "Database=" + database + ";" +
               "Server=" + databaseServer + ";" +
               "Port=" + port.ToString() + ";";
            _dbName = database;
        }

        protected override Connection CreateConnection(bool exclusiveLock)
		{
			return new MySqlConnection(this,connectionString,_readonly,exclusiveLock);
		}

        private static Version _version;
        internal static Version AssemblyVersion
        {
            get { return _version; }
        }

        protected override void _InitClass()
        {
            if (Utility.LocateType(_PARAMETER_NAME) == null)
                Assembly.Load(MySqlConnection._ASSEMBLY_NAME);
            _version = Assembly.Load(MySqlConnection._ASSEMBLY_NAME).GetName().Version;
        }

        protected override bool _IsCoreStoredProcedure(StoredProcedure storedProcedure)
        {
            return false;
        }

        private QueryBuilder _queryBuilder = null;
        internal override QueryBuilder queryBuilder
        {
            get
            {
                if (_queryBuilder == null)
                    _queryBuilder = new MySqlQueryBuilder(this);
                return _queryBuilder;
            }
        }

        protected override IDbDataParameter _CreateParameter(string parameterName, object parameterValue)
        {
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
            return (IDbDataParameter)Utility.LocateType(_PARAMETER_NAME).GetConstructor(new Type[] { typeof(string), typeof(object) }).Invoke(new object[] { parameterName, parameterValue });
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
            if (((type == FieldType.CHAR) || (type == FieldType.STRING))
                && ((fieldLength == -1) || (fieldLength > 65350)))
            {
                Type t = Utility.LocateType(_PARAMETER_NAME);
                PropertyInfo pi = t.GetProperty("MySqlDbType", Utility._BINDING_FLAGS);
                pi.SetValue(ret, Enum.Parse(Utility.LocateType(_SQL_DB_TYPE_ENUM), "Text"), new object[] { });
            }
            return ret;
        }
		
		
	}
}
