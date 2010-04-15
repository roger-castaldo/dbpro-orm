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

		public MsSqlConnection(ConnectionPool pool, string ConnectionString)
			: base(pool, ConnectionString)
		{ }

		internal override IDbDataParameter CreateParameter(string parameterName, object parameterValue, FieldType type, int fieldLength)
		{
			IDbDataParameter ret = CreateParameter(parameterName,parameterValue);
			if (((type==FieldType.CHAR)||(type==FieldType.STRING))
			    &&((fieldLength == -1)||(fieldLength>8000)))
			{
				((SqlParameter)ret).SqlDbType=SqlDbType.Text;
			}
			return ret;
		}
		
		public override System.Data.IDbDataParameter CreateParameter(string parameterName, object parameterValue)
		{
            if ((parameterValue is uint) || (parameterValue is UInt32))
            {
                parameterValue = System.Text.ASCIIEncoding.ASCII.GetString(System.BitConverter.GetBytes(uint.Parse(parameterValue.ToString()))).ToCharArray();
            }
            else if ((parameterValue is UInt16) || (parameterValue is ushort))
            {
                parameterValue = System.Text.ASCIIEncoding.ASCII.GetString(System.BitConverter.GetBytes(ushort.Parse(parameterValue.ToString()))).ToCharArray();
            }
            else if ((parameterValue is ulong) || (parameterValue is Int64))
            {
                parameterValue = System.Text.ASCIIEncoding.ASCII.GetString(System.BitConverter.GetBytes(ulong.Parse(parameterValue.ToString()))).ToCharArray();
            }
			return new SqlParameter(parameterName, parameterValue);
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
				                         "INSERT INTO "+map.TableName+" SELECT GETDATE()"+fields.Replace(",",",tbl.")+" from INSERTED ins,"+map.TableName+" tbl WHERE "+primarys+";\n"+
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
					code+="INSERT INTO "+map.TableName+" SELECT "+pool.CorrectName(queryBuilder.CreateParameterName(field.FieldName))+"+1"+fields.Replace(",",",tbl.")+" from INSERTED ins,"+map.TableName+" tbl WHERE "+primarys+";\n";
					code+="END";
					triggers.Add(new Trigger(pool.CorrectName("TRIG_INSERT_"+map.TableName),"ON "+map.TableName+" INSTEAD OF INSERT\n",code));
				}
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
			if (field.Type.ToUpper().Contains("DATE") || field.Type.ToUpper().Contains("TIME")||(map.PrimaryKeys.Count>0))
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
