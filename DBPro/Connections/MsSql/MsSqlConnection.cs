using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using Org.Reddragonit.Dbpro.Connections;
using FieldType = Org.Reddragonit.Dbpro.Structure.Attributes.FieldType;
using VersionTypes = Org.Reddragonit.Dbpro.Structure.Attributes.VersionField.VersionTypes;
using System.Reflection;
using System.Xml;
using System.IO;

namespace Org.Reddragonit.Dbpro.Connections.MsSql
{
	class MsSqlConnection : Connection
	{
        internal const string _ASSEMBLY_NAME = "System.Data";
        internal const string _CONNECTION_TYPE_NAME = "System.Data.SqlClient.SqlConnection";
        private const string _COMMAND_TYPE_NAME = "System.Data.SqlClient.SqlCommand";

        internal override string ConcatenationCharacter
        {
            get
            {
                return "+";
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

		internal override IDbTransaction EstablishExclusiveTransaction()
        {
            return conn.BeginTransaction(IsolationLevel.Serializable);
        }

		internal override void GetAddAutogen(ExtractedTableMap map, out List<IdentityField> identities, out List<Generator> generators, out List<Trigger> triggers,out List<StoredProcedure> procedures)
		{
            Type t = Pool.Mapping[map.TableName];
			identities=new List<IdentityField>();
			generators=null;
			triggers = new List<Trigger>();
            procedures = new List<StoredProcedure>();
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
				triggers.Add(new Trigger(Pool.Translator.GetInsertTriggerName(t,this),
				                         "ON "+map.TableName+" INSTEAD OF INSERT\n",
				                         "AS "+
				                         "BEGIN SET NOCOUNT ON;\n"+
                                         "INSERT INTO " + map.TableName + "(" + field.FieldName + fields + ") SELECT GETDATE()" + fields.Replace(",", ",tbl.") + " from INSERTED ins;\n" +
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
						declares+="DECLARE "+queryBuilder.CreateParameterName(efm.FieldName)+" "+efm.Type+";\n";
						if (!efm.AutoGen)
						{
							sets+=" SET "+queryBuilder.CreateParameterName(efm.FieldName)+" = (SELECT "+efm.FieldName+" FROM INSERTED);\n";
							queryFields+=" AND "+efm.FieldName+" = "+queryBuilder.CreateParameterName(efm.FieldName);
						}
					}
					code+=declares;
					code+="BEGIN \n";
					code+=sets;
					code+="SET "+queryBuilder.CreateParameterName(field.FieldName)+" = (SELECT MAX("+field.FieldName+") FROM "+map.TableName+" WHERE ";
					code+=queryFields.Substring(4)+");\n";
					code+="IF ("+queryBuilder.CreateParameterName(field.FieldName)+" is NULL)\n";
					code+="\tSET "+queryBuilder.CreateParameterName(field.FieldName)+" = -1;\n";
					code+="INSERT INTO "+map.TableName+"("+field.FieldName+fields+") SELECT "+queryBuilder.CreateParameterName(field.FieldName)+"+1"+fields.Replace(",",",tbl.")+" from INSERTED ins,"+map.TableName+" tbl WHERE "+primarys+";\n";
					code+="END";
					triggers.Add(new Trigger(Pool.Translator.GetInsertTriggerName(t,this),"ON "+map.TableName+" INSTEAD OF INSERT\n",code));
				}
            }
            else if (field.Type.ToUpper().Contains("VARCHAR")){
                XmlDocument doc = new XmlDocument();
                doc.LoadXml(new StreamReader(Assembly.GetAssembly(typeof(MsSqlConnection)).GetManifestResourceStream("Org.Reddragonit.Dbpro.Connections.MsSql.StringIDProcedures.xml")).ReadToEnd());
                foreach (XmlElement proc in doc.GetElementsByTagName("Procedure"))
                    procedures.Add(new StoredProcedure(proc.ChildNodes[0].InnerText,
                        proc.ChildNodes[1].InnerText,
                        proc.ChildNodes[2].InnerText,
                        proc.ChildNodes[3].InnerText,
                        proc.ChildNodes[4].InnerText));
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
                "	SET @cnt = (SELECT COUNT(*) FROM " + map.TableName + " WHERE " + field.FieldName + " = @IDVAL);\n" +
                "END\n" +
                "INSERT INTO " + map.TableName + "(" + field.FieldName + fields + ") SELECT @IDVAL" + fields.Replace(",", ",tbl.") + " from INSERTED ins;\n" +
                "END";
                triggers.Add(new Trigger(Pool.Translator.GetInsertTriggerName(t,this),
                                         "ON " + map.TableName + " INSTEAD OF INSERT\n",
                                         code));
            }else
                throw new Exception("Unable to create autogenerator for non date or digit type.");
		}
		
		internal override void GetDropAutogenStrings(ExtractedTableMap map, out List<IdentityField> identities, out List<Generator> generators, out List<Trigger> triggers)
		{
            Type t = Pool.Mapping[map.TableName];
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
				triggers.Add(new Trigger(Pool.Translator.GetInsertTriggerName(t,this),"",""));
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
		
		internal override List<Trigger> GetVersionTableTriggers(ExtractedTableMap table,VersionTypes versionType)
		{
            Type t = Pool.Mapping.GetTypeForVersionTable(table.TableName);
			List<Trigger> ret = new List<Trigger>();
			string tmp = "AS \n BEGIN\n";
			tmp+="\tINSERT INTO "+table.TableName+" SELECT null";
			for(int x=1;x<table.Fields.Count;x++)
			{
				ExtractedFieldMap efm = table.Fields[x];
				tmp+=",tbl."+efm.FieldName;
			}
			tmp+=" FROM INSERTED ins, "+Pool.Mapping[t].Name+" tbl WHERE ";
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
			ret.Add(new Trigger(Pool.Translator.GetVersionInsertTriggerName(t,this),
			                    "ON "+Pool.Mapping[t].Name+" AFTER INSERT,UPDATE ",
			                                     tmp));
			return ret;
		}

        internal override List<Trigger> GetDeleteParentTrigger(ExtractedTableMap table, ExtractedTableMap parent)
        {
            List<Trigger> ret = new List<Trigger>();
            string tmp = "AS \n BEGIN \n DELETE FROM " + parent.TableName + " WHERE ";
            string fields = "CONCAT(";
            foreach (ExtractedFieldMap efm in parent.PrimaryKeys)
                fields += efm.FieldName + ",";
            fields = fields.Substring(0, fields.Length - 1) + ")";
            tmp += fields + " IN (SELECT " + fields + " FROM DELETED);\nEND\n\n";
            ret.Add(new Trigger(Pool.Translator.GetDeleteParentTriggerName(Pool.Mapping[table.TableName],this),
                "ON " + table.TableName + " AFTER DELETE",
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
