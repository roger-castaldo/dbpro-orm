using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using Org.Reddragonit.Dbpro.Connections;
using Org.Reddragonit.Dbpro.Structure.Mapping;
using FieldType = Org.Reddragonit.Dbpro.Structure.Attributes.Field.FieldType; 

namespace Org.Reddragonit.Dbpro.Connections.MsSql
{
    class MsSqlConnection : Connection 
    {
        public MsSqlConnection(ConnectionPool pool, string ConnectionString)
            : base(pool, ConnectionString)
        { }

        protected override System.Data.IDbDataParameter CreateParameter(string parameterName, object parameterValue)
        {
            return new SqlParameter(parameterName, parameterValue);
        }

        internal override List<string> GetAddAutogenString(string table, string field, string type)
        {
            throw new Exception("The method or operation is not implemented.");
        }

        internal override List<string> GetDropAutogenStrings(string table, string field,string type)
        {
            throw new Exception("The method or operation is not implemented.");
        }

		internal override List<string> GetCreateTableStringsForAlterations(ExtractedTableMap table)
		{
			throw new NotImplementedException();
		}
		
		internal override string GetAlterFieldTypeString(string table, string field, string type,long size)
		{
			throw new NotImplementedException();
		}
		
		internal override string GetDropTableString(string table)
		{
			throw new NotImplementedException();
		}
		
		internal override string GetDropColumnString(string table, string field)
		{
			throw new NotImplementedException();
		}
		
		internal override string GetCreateColumnString(string table, string field, string type,long size)
		{
			throw new NotImplementedException();
		}
		
		internal override string GetForiegnKeyCreateString(string table, List<string> fields, string foriegnTable, List<string> foriegnFields)
		{
			throw new NotImplementedException();
		}
		
		internal override string GetNullConstraintCreateString(string table, string field)
		{
			throw new NotImplementedException();
		}
		
		internal override string GetPrimaryKeyCreateString(string table, List<string> fields)
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

        internal override List<Connection.ExtractedTableMap> GetTableList()
        {
            throw new Exception("The method or operation is not implemented.");
        }
        
		internal override List<string> GetDropConstraintsScript()
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

        protected override List<string> ConstructCreateStrings(Org.Reddragonit.Dbpro.Structure.Table table)
        {
            List<string> ret = new List<string>();
            TableMap t = ClassMapper.GetTableMap(table.GetType());
            string tmp = "CREATE TABLE " + t.Name + "(\n";
            foreach (FieldMap f in t.Fields)
            {
                InternalFieldMap ifm = (InternalFieldMap)f;
                tmp += "\t" + ifm.FieldName + " " + TranslateFieldType(ifm.FieldType, ifm.FieldLength);
                if (ifm.AutoGen)
                {
                    tmp += " IDENTITY(1,1)";
                }
                if (!ifm.Nullable)
                {
                    tmp += " NOT NULL";
                }
                tmp += ",\n";
            }
            if (t.PrimaryKeys.Count > 0)
            {
                tmp += "\tPRIMARY KEY(";
                foreach (FieldMap f in t.PrimaryKeys)
                {
                    if (f is InternalFieldMap)
                    {
                        InternalFieldMap ifm = (InternalFieldMap)f;
                        tmp += ifm.FieldName + ",";
                    }
                }
                tmp = tmp.Substring(0, tmp.Length - 1);
                tmp += "),\n";
            }
            if (t.ForiegnTablesCreate.Count > 0)
            {
                foreach (Type type in t.ForiegnTablesCreate)
                {
                    if (t.GetFieldInfoForForiegnTable(type).IsArray)
                    {
                        TableMap ext = ClassMapper.GetTableMap(type);
                        string externalTable = "CREATE TABLE " + t.Name + "_" + ext.Name + "(";
                        string pkeys = "\nPRIMARY KEY(";
                        string fkeys = "\nFOREIGN KEY(";
                        string fields = "";
                        foreach (InternalFieldMap f in t.PrimaryKeys)
                        {
                            externalTable += "\n\t" + f.FieldName + " " + TranslateFieldType(f.FieldType, f.FieldLength) + ",";
                            pkeys += f.FieldName + ",";
                            fkeys += f.FieldName + ",";
                        }
                        fkeys = fkeys.Substring(0, fkeys.Length - 1);
                        fkeys += ")\nREFERENCES " + t.Name + "(" + fkeys.Replace("\nFOREIGN KEY(", "").Replace(")", "") + ")\n\t\tON UPDATE CASCADE ON DELETE CASCADE,";
                        fkeys += "\nFOREIGN KEY(";
                        foreach (InternalFieldMap f in ext.PrimaryKeys)
                        {
                            externalTable += "\n\t" + f.FieldName + " " + TranslateFieldType(f.FieldType, f.FieldLength) + ",";
                            pkeys += f.FieldName + ",";
                            fkeys += f.FieldName + ",";
                            fields += f.FieldName + ",";
                        }
                        fkeys = fkeys.Substring(0, fkeys.Length - 1);
                        fkeys += ")\nREFERENCES " + ext.Name + "(" + fields.Substring(0, fields.Length - 1) + ")\nON UPDATE CASCADE ON DELETE CASCADE\n";
                        pkeys = pkeys.Substring(0, pkeys.Length - 1) + "),";
                        externalTable = externalTable + pkeys + fkeys + ");";
                        ret.Add(externalTable);
                    }
                    else
                    {
                        tmp += "\tFOREIGN KEY(";
                        foreach (InternalFieldMap ifm in ClassMapper.GetTableMap(type).PrimaryKeys)
                        {
                            tmp += ifm.FieldName + ",";
                        }
                        tmp = tmp.Substring(0, tmp.Length - 1);
                        tmp += ")\n\t\tREFERENCES " + ClassMapper.GetTableMap(type).Name + "(";
                        foreach (InternalFieldMap ifm in ClassMapper.GetTableMap(type).PrimaryKeys)
                        {
                            tmp += ifm.FieldName + ",";
                        }
                        tmp = tmp.Substring(0, tmp.Length - 1);
                        tmp += ")\n\t\tON UPDATE " + t.GetFieldInfoForForiegnTable(type).OnUpdate.ToString() + "\n";
                        tmp += "\t\tON DELETE " + t.GetFieldInfoForForiegnTable(type).OnDelete.ToString() + ",\n";
                    }
                }
            }
            tmp = tmp.Substring(0, tmp.Length - 2) + "\n";
            tmp += ");\n\n";
            ret.Insert(0, tmp);
            return ret;
        }
    }
}
