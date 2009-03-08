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
        private QueryBuilder _builder;
        internal override QueryBuilder queryBuilder
        {
            get
            {
                if (_builder == null)
                    _builder = new MSSQLQueryBuilder(Pool);
                return _builder;
            }
        }

        public MsSqlConnection(ConnectionPool pool, string ConnectionString)
            : base(pool, ConnectionString)
        { }

        internal override System.Data.IDbDataParameter CreateParameter(string parameterName, object parameterValue)
        {
            return new SqlParameter(parameterName, parameterValue);
        }

		internal override void GetAddAutogen(string tableName,List<ExtractedFieldMap> primaryFields,ConnectionPool pool, out List<string> queryStrings, out List<Generator> generators, out List<Trigger> triggers)
		{
			queryStrings=new List<string>();
			generators=new List<Generator>();
			triggers = new List<Trigger>();
			if (primaryFields.Count==1)
			{
				queryStrings.Add("ALTER TABLE "+tableName+" ALTER COLUMN "+primaryFields[0].FieldName+" "+primaryFields[0].FullFieldType+" IDENTITY(1,1)");
			}else{
				throw new Exception("Unable to create complex primary keys with autogen on this database as it does not support before insert triggers.");
			}
		}
		
		internal override void GetDropAutogenStrings(string tableName, ExtractedFieldMap field,ConnectionPool pool, out List<string> queryStrings, out List<Generator> generators, out List<Trigger> triggers)
		{
			queryStrings=new List<string>();
			generators=new List<Generator>();
			triggers = new List<Trigger>();
			queryStrings.Add("ALTER TABLE "+tableName+" ALTER COLUMN "+field.FieldName+" "+field.FullFieldType);
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
                    if ((fieldLength == -1)||(fieldLength>8000))
                        ret = "IMAGE";
                    else 
                        ret = "VARBINARY(" + fieldLength.ToString() + ")";
                    break;
                case FieldType.CHAR:
                    if ((fieldLength == -1)||(fieldLength>8000))
                        ret = "TEXT";
                    else
                        ret = "VARCHAR(" + fieldLength.ToString() + ")";
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
                    if ((fieldLength == -1)||(fieldLength>8000))
                        ret = "TEXT";
                    else
                        ret = "VARCHAR(" + fieldLength.ToString() + ")";
                    break;
            }
            return ret;
        }
    }
}
