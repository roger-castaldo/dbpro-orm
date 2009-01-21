using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using Org.Reddragonit.Dbpro.Connections;
using Org.Reddragonit.Dbpro.Structure.Mapping;
using FieldType = Org.Reddragonit.Dbpro.Structure.Attributes.FieldType; 
using VersionTypes = Org.Reddragonit.Dbpro.Structure.Attributes.VersionField.VersionTypes;

namespace Org.Reddragonit.Dbpro.Connections.MsSql
{
    class MsSqlConnection : Connection 
    {
        public MsSqlConnection(ConnectionPool pool, string ConnectionString)
            : base(pool, ConnectionString)
        { }

        internal override System.Data.IDbDataParameter CreateParameter(string parameterName, object parameterValue)
        {
            return new SqlParameter(parameterName, parameterValue);
        }

		internal override void GetAddAutogen(string tableName, ExtractedFieldMap field,ConnectionPool pool, out List<string> queryStrings, out List<Generator> generators, out List<Trigger> triggers)
		{
			throw new NotImplementedException();
		}
		
		internal override void GetDropAutogenStrings(string tableName, ExtractedFieldMap field,ConnectionPool pool, out List<string> queryStrings, out List<Generator> generators, out List<Trigger> triggers)
		{
			throw new NotImplementedException();
		}
		
		internal override List<string> GetDropTableString(string table,bool isVersioned)
		{
			throw new NotImplementedException();
		}

        protected override System.Data.IDbCommand EstablishCommand()
        {
            return new SqlCommand("", (SqlConnection)conn);
        }

        protected override System.Data.IDbConnection EstablishConnection()
        {
            return new SqlConnection(connectionString);
        }
		
		internal override List<Trigger> GetVersionTableTriggers(ExtractedTableMap table,VersionTypes versionType,ConnectionPool pool)
		{
			throw new NotImplementedException();
		}

        internal override string TranslateFieldType(FieldType type, int fieldLength)
        {
            string ret = null;
            switch (type)
            {
                case FieldType.BOOLEAN:
                    ret = "BIT";
                    break;
                case FieldType.BYTE:
                    if (fieldLength == -1)
                        ret = "VARBINARY(MAX)";
                    else
                        ret = "VARBINARY(" + fieldLength.ToString() + ")";
                    break;
                case FieldType.CHAR:
                    if (fieldLength == -1)
                        ret = "NVARCHAR(MAX)";
                    else
                        ret = "NVARCHAR(" + fieldLength.ToString() + ")";
                    break;
                case FieldType.DATE:
                case FieldType.DATETIME:
                case FieldType.TIME:
                    ret = "DATETIME";
                    break;
                case FieldType.DECIMAL:
                case FieldType.DOUBLE:
                    ret = "DECIMAL";
                    break;
                case FieldType.FLOAT:
                    ret = "FLOAT";
                    break;
                case FieldType.IMAGE:
                    ret = "IMAGE";
                    break;
                case FieldType.INTEGER:
                    ret = "INT";
                    break;
                case FieldType.LONG:
                    ret = "BIGINT";
                    break;
                case FieldType.MONEY:
                    ret = "MONEY";
                    break;
                case FieldType.SHORT:
                    ret = "SMALLINT";
                    break;
                case FieldType.STRING:
                    if (fieldLength == -1)
                        ret = "TEXT";
                    else
                        ret = "NVARCHAR(" + fieldLength.ToString() + ")";
                    break;
            }
            return ret;
        }
    }
}
