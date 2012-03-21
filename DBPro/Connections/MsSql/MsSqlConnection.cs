using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using Org.Reddragonit.Dbpro.Connections;
using Org.Reddragonit.Dbpro.Structure.Mapping;
using FieldType = Org.Reddragonit.Dbpro.Structure.Attributes.FieldType;
using VersionTypes = Org.Reddragonit.Dbpro.Structure.Attributes.VersionField.VersionTypes;
using System.Reflection;

namespace Org.Reddragonit.Dbpro.Connections.MsSql
{
	class MsSqlConnection : Connection
	{
        internal const string _ASSEMBLY_NAME = "System.Data";
        private const string _PARAMETER_TYPE_NAME = "System.Data.SqlClient.SqlParameter";
        internal const string _CONNECTION_TYPE_NAME = "System.Data.SqlClient.SqlConnection";
        private const string _COMMAND_TYPE_NAME = "System.Data.SqlClient.SqlCommand";

        internal override string ConcatenationCharacter
        {
            get
            {
                return "+";
            }
        }

		private QueryBuilder _builder;
		internal override QueryBuilder queryBuilder
		{
			get
			{
				if (_builder == null)
					_builder = new MSSQLQueryBuilder(Pool,this);
				return _builder;
			}
		}
		
		internal override string DefaultTableString {
			get {
				return "sysobjects";
			}
		}
		
		internal override bool UsesIdentities {
			get { return true; }
		}

		public MsSqlConnection(ConnectionPool pool, string ConnectionString,bool Readonly,bool exclusiveLock)
			: base(pool, ConnectionString,Readonly,exclusiveLock)
		{ }

		internal override IDbDataParameter CreateParameter(string parameterName, object parameterValue, FieldType type, int fieldLength)
		{
            if (parameterValue != null)
            {
                if (parameterValue.GetType().IsEnum)
                {
                    if (parameterValue != null)
                        parameterValue = Pool.GetEnumID(parameterValue.GetType(), parameterValue.ToString());
                    else
                        parameterValue = (int?)null;
                }
            }
			IDbDataParameter ret = CreateParameter(parameterName,parameterValue);
			if (((type==FieldType.CHAR)||(type==FieldType.STRING))
			    &&((fieldLength == -1)||(fieldLength>8000)))
			{
                Type t = Utility.LocateType(_PARAMETER_TYPE_NAME);
                PropertyInfo pi = t.GetProperty("SqlDbType");
                pi.SetValue(ret, SqlDbType.Text, new object[] { });
			}
			return ret;
		}

        internal override IDbTransaction EstablishExclusiveTransaction()
        {
            return conn.BeginTransaction(IsolationLevel.Serializable);
        }
		
		public override System.Data.IDbDataParameter CreateParameter(string parameterName, object parameterValue)
		{
            if (parameterValue != null)
            {
                if (parameterValue.GetType().IsEnum)
                {
                    if (parameterValue != null)
                        parameterValue = Pool.GetEnumID(parameterValue.GetType(), parameterValue.ToString());
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

		internal override void GetAddAutogen(ExtractedTableMap map,ConnectionPool pool, out List<IdentityField> identities, out List<Generator> generators, out List<Trigger> triggers)
		{
			identities=new List<IdentityField>();
			generators=null;
			triggers = new List<Trigger>();
			ExtractedFieldMap field = map.PrimaryKeys[0];
			if ((map.PrimaryKeys.Count>1)&&(!field.AutoGen))
			{
				foreach (ExtractedFieldMap efm in map.PrimaryKeys)
				{
					if (efm.AutoGen)
					{
						field=efm;
						break;
					}
				}
			}
			string fields = "";
			string primarys="";
			foreach (ExtractedFieldMap efm in map.Fields)
			{
				if (!efm.AutoGen)
					fields+=","+efm.FieldName;
				if (efm.PrimaryKey)
					primarys+="tbl."+efm.FieldName+" = ins."+efm.FieldName+" AND ";
			}
			primarys = primarys.Substring(0,primarys.Length-4);
			if (field.Type.ToUpper().Contains("DATE")||field.Type.ToUpper().Contains("TIME"))
			{
				triggers.Add(new Trigger(pool.CorrectName("TRIG_INSERT_"+map.TableName),
				                         "ON "+map.TableName+" INSTEAD OF INSERT\n",
				                         "AS "+
				                         "BEGIN SET NOCOUNT ON;\n"+
                                         "INSERT INTO " + map.TableName + "(" + pool.CorrectName(field.FieldName) + fields + ") SELECT GETDATE()" + fields.Replace(",", ",tbl.") + " from INSERTED ins;\n" +
				                         "END"));
			}else if (field.Type.ToUpper().Contains("INT")){
				if (map.PrimaryKeys.Count==1)
					identities.Add(new IdentityField(map.TableName,field.FieldName,field.FullFieldType,"1"));
				else
				{
					string code = "AS \n";
					string declares="";
					string sets="";
					string queryFields="";
					foreach (ExtractedFieldMap efm in map.PrimaryKeys)
					{
						declares+="DECLARE "+pool.CorrectName(queryBuilder.CreateParameterName(efm.FieldName))+" "+efm.Type+";\n";
						if (!efm.AutoGen)
						{
							sets+=" SET "+pool.CorrectName(queryBuilder.CreateParameterName(efm.FieldName))+" = (SELECT "+efm.FieldName+" FROM INSERTED);\n";
							queryFields+=" AND "+efm.FieldName+" = "+pool.CorrectName(queryBuilder.CreateParameterName(efm.FieldName));
						}
					}
					code+=declares;
					code+="BEGIN \n";
					code+=sets;
					code+="SET "+pool.CorrectName(queryBuilder.CreateParameterName(field.FieldName))+" = (SELECT MAX("+field.FieldName+") FROM "+map.TableName+" WHERE ";
					code+=queryFields.Substring(4)+");\n";
					code+="IF ("+pool.CorrectName(queryBuilder.CreateParameterName(field.FieldName))+" is NULL)\n";
					code+="\tSET "+pool.CorrectName(queryBuilder.CreateParameterName(field.FieldName))+" = -1;\n";
					code+="INSERT INTO "+map.TableName+"("+pool.CorrectName(field.FieldName)+fields+") SELECT "+pool.CorrectName(queryBuilder.CreateParameterName(field.FieldName))+"+1"+fields.Replace(",",",tbl.")+" from INSERTED ins,"+map.TableName+" tbl WHERE "+primarys+";\n";
					code+="END";
					triggers.Add(new Trigger(pool.CorrectName("TRIG_INSERT_"+map.TableName),"ON "+map.TableName+" INSTEAD OF INSERT\n",code));
				}
            }
            else if (field.Type.ToUpper().Contains("VARCHAR")){
                string code = "AS \n";
                code += "DECLARE @IDVAL VARCHAR(38),\n" +
                "@cnt BIGINT;\n" +
                "SET @cnt = 1;\n" +
                "WHILE (@cnt>0)\n" +
                "BEGIN\n" +
                "	SET @IDVAL = (SELECT [dbo].[Org_Reddragonit_Dbpro_Connections_MsSql_GeneateUniqueID] (\n" +
                "		   CEILING(RAND( (DATEPART(mm, GETDATE()) * 100000 )+ (DATEPART(ss, GETDATE()) * 1000 )+ DATEPART(ms, GETDATE()) )*((DATEPART(mm, GETDATE()) * 100000 )+ (DATEPART(ss, GETDATE()) * 1000 )+ DATEPART(ms, GETDATE()))*(Year(GetDate())*Month(GetDate())*Day(GetDate())*RAND()))\n" +
                "		  ,CEILING(RAND( (DATEPART(mm, GETDATE()) * 100000 )+ (DATEPART(ss, GETDATE()) * 1000 )+ DATEPART(ms, GETDATE()) )*((DATEPART(mm, GETDATE()) * 100000 )+ (DATEPART(ss, GETDATE()) * 1000 )+ DATEPART(ms, GETDATE()))*(Year(GetDate())*Month(GetDate())*Day(GetDate())*RAND()))\n" +
                "		  ,CEILING(RAND( (DATEPART(mm, GETDATE()) * 100000 )+ (DATEPART(ss, GETDATE()) * 1000 )+ DATEPART(ms, GETDATE()) )*((DATEPART(mm, GETDATE()) * 100000 )+ (DATEPART(ss, GETDATE()) * 1000 )+ DATEPART(ms, GETDATE()))*(Year(GetDate())*Month(GetDate())*Day(GetDate())*RAND()))\n" +
                "		  ,CEILING(RAND( (DATEPART(mm, GETDATE()) * 100000 )+ (DATEPART(ss, GETDATE()) * 1000 )+ DATEPART(ms, GETDATE()) )*((DATEPART(mm, GETDATE()) * 100000 )+ (DATEPART(ss, GETDATE()) * 1000 )+ DATEPART(ms, GETDATE()))*(Year(GetDate())*Month(GetDate())*Day(GetDate())*RAND()))\n" +
                "		  ,CEILING(RAND( (DATEPART(mm, GETDATE()) * 100000 )+ (DATEPART(ss, GETDATE()) * 1000 )+ DATEPART(ms, GETDATE()) )*((DATEPART(mm, GETDATE()) * 100000 )+ (DATEPART(ss, GETDATE()) * 1000 )+ DATEPART(ms, GETDATE()))*(Year(GetDate())*Month(GetDate())*Day(GetDate())*RAND()))));\n" +
                "	SET @cnt = (SELECT COUNT(*) FROM " + map.TableName + " WHERE " + pool.CorrectName(field.FieldName) + " = @IDVAL);\n" +
                "END\n" +
                "INSERT INTO " + map.TableName + "(" + pool.CorrectName(field.FieldName) + fields + ") SELECT @IDVAL" + fields.Replace(",", ",tbl.") + " from INSERTED ins;\n" +
                "END";
                triggers.Add(new Trigger(pool.CorrectName("TRIG_INSERT_" + map.TableName),
                                         "ON " + map.TableName + " INSTEAD OF INSERT\n",
                                         code));
            }else
                throw new Exception("Unable to create autogenerator for non date or digit type.");
		}
		
		internal override void GetDropAutogenStrings(ExtractedTableMap map,ConnectionPool pool, out List<IdentityField> identities, out List<Generator> generators, out List<Trigger> triggers)
		{
			identities=new List<IdentityField>();
			generators=null;
			triggers = new List<Trigger>();
			ExtractedFieldMap field=map.PrimaryKeys[0];
			if ((map.PrimaryKeys.Count>1)&&(!field.AutoGen))
			{
				foreach (ExtractedFieldMap efm in map.PrimaryKeys)
				{
					if (efm.AutoGen)
					{
						field=efm;
						break;
					}
				}
			}
            if (field.Type.ToUpper().Contains("DATE") || field.Type.ToUpper().Contains("TIME") || (map.PrimaryKeys.Count > 0) || (field.Type.ToUpper().Contains("VARCHAR")))
			{
				triggers.Add(new Trigger(pool.CorrectName("TRIG_INSERT_"+map.TableName),"",""));
			}
			else if (field.Type.ToUpper().Contains("INT"))
			{
				identities.Add(new IdentityField(map.TableName,field.FieldName,field.FullFieldType,""));
			}
			else
			{
				throw new Exception("Unable to create autogenerator for non date or digit type.");
			}
		}

        protected override IDbCommand EstablishCommand()
        {
            return (IDbCommand)Utility.LocateType(_COMMAND_TYPE_NAME).GetConstructor(new Type[] { typeof(string), Utility.LocateType(_CONNECTION_TYPE_NAME) }).Invoke(new object[] { "", conn });
        }

        protected override IDbConnection EstablishConnection()
        {
            return (IDbConnection)Utility.LocateType(_CONNECTION_TYPE_NAME).GetConstructor(new Type[] { typeof(String) }).Invoke(new object[] { connectionString });
        }
		
		internal override List<Trigger> GetVersionTableTriggers(ExtractedTableMap table,VersionTypes versionType,ConnectionPool pool)
		{
			List<Trigger> ret = new List<Trigger>();
			string tmp = "AS \n BEGIN\n";
			tmp+="\tINSERT INTO "+pool.CorrectName(table.TableName)+" SELECT null";
			for(int x=1;x<table.Fields.Count;x++)
			{
				ExtractedFieldMap efm = table.Fields[x];
				tmp+=",tbl."+efm.FieldName;
			}
			tmp+=" FROM INSERTED ins, "+queryBuilder.RemoveVersionName(table.TableName)+" tbl WHERE ";
			for (int x=1;x<table.Fields.Count;x++)
			{
				ExtractedFieldMap efm = table.Fields[x];
				if (efm.PrimaryKey)
				{
					tmp+="tbl."+efm.FieldName+" = ins."+efm.FieldName+" AND ";					
				}
			}
			tmp=tmp.Substring(0,tmp.Length-4)+";";
			tmp+="\nEND\n\n";
			ret.Add(new Trigger(pool.CorrectName(queryBuilder.VersionTableInsertTriggerName(queryBuilder.RemoveVersionName(table.TableName))),
			                    "ON "+queryBuilder.RemoveVersionName(table.TableName)+" AFTER INSERT,UPDATE ",
			                                     tmp));
			return ret;
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
				case FieldType.ENUM:
					ret = "INT";
					break;
                case FieldType.UNSIGNED_INTEGER:
                    ret = "CHAR(4)";
                    break;
				case FieldType.LONG:
					ret = "BIGINT";
					break;
                case FieldType.UNSIGNED_LONG:
                    ret = "CHAR(8)";
                    break;
				case FieldType.MONEY:
					ret = "MONEY";
					break;
				case FieldType.SHORT:
					ret = "SMALLINT";
					break;
                case FieldType.UNSIGNED_SHORT:
                    ret = "CHAR(2)";
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

        internal override void DisableAutogens()
        {
            string query = "";
            this.ExecuteQuery(queryBuilder.SelectIdentities());
            while (this.Read())
            {
                query += "SET IDENTITY_INSERT "+this[0].ToString()+" ON;\n";
            }
            this.Close();
            this.ExecuteQuery(queryBuilder.SelectTableNames());
            while (this.Read())
            {
                query += "ALTER TABLE " + this[0].ToString() + " DISABLE TRIGGER all;\n";
            }
            this.Close();
            Logger.LogLine("Disabling of autogens for MSSQL resulting query: " + query);
            foreach (string str in query.Split('\n'))
            {
                if (str.Trim().Length > 0)
                    this.ExecuteNonQuery(str);
            }
        }

        internal override void EnableAndResetAutogens()
        {
            string query = "";
            this.ExecuteQuery(queryBuilder.SelectIdentities());
            while (this.Read())
            {
                query += "SET IDENTITY_INSERT " + this[0].ToString() + " OFF;\n";
            }
            this.ExecuteQuery(queryBuilder.SelectTableNames());
            while (this.Read())
            {
                query += "ALTER TABLE " + this[0].ToString() + " ENABLE TRIGGER all;\n";
            }
            this.Close();
            List<IdentityField> identities = new List<IdentityField>();
            this.ExecuteQuery(this.queryBuilder.SelectIdentities());
            while (this.Read())
            {
                identities.Add(new IdentityField((string)this[0], (string)this[1], (string)this[2], (string)this[3]));
            }
            this.Close();
            foreach (IdentityField id in identities)
            {
                this.ExecuteQuery("SELECT ISNULL(MAX(" + id.FieldName + "),0)+1 FROM " + id.TableName);
                this.Read();
                query += queryBuilder.SetIdentityFieldValue(new IdentityField(id.TableName,id.FieldName,id.FieldType,this[0].ToString()))+"\n";
                this.Close();
            }
            Logger.LogLine("Enabling and resetting of autogens for MSSQL resulting query: " + query);
            foreach (string str in query.Split('\n'))
            {
                if (str.Trim().Length > 0)
                    this.ExecuteNonQuery(str);
            }
        }

	}
}
