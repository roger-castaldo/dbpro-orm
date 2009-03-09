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
		
		internal override bool UsesIdentities {
			get { return true; }
		}

		public MsSqlConnection(ConnectionPool pool, string ConnectionString)
			: base(pool, ConnectionString)
		{ }

		internal override System.Data.IDbDataParameter CreateParameter(string parameterName, object parameterValue)
		{
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
			foreach (ExtractedFieldMap efm in map.Fields)
			{
				if (!efm.PrimaryKey&&!efm.AutoGen)
					fields+=","+efm.FieldName;
			}
			if (field.Type.ToUpper().Contains("DATE")||field.Type.ToUpper().Contains("TIME"))
			{
				triggers.Add(new Trigger(pool.CorrectName("TRIG_INSERT_"+map.TableName),
				                         "ON "+map.TableName+" INSTEAD OF INSERT\n",
				                         "AS DECLARE "+pool.CorrectName("@"+field.FieldName)+" DATETIME;\n"+
				                         "BEGIN SET NOCOUNT ON;\n"+
				                         "SET "+pool.CorrectName("@"+field.FieldName)+" = (SELECT \n"+
				                         "(CASE WHEN "+field.FieldName+" IS NULL THEN GETDATE() \n"+
				                         "ELSE "+field.FieldName+" END) FROM INSERTED);\n"+
				                         "INSERT INTO TESTING SELECT "+pool.CorrectName("@"+field.FieldName)+fields+" from INSERTED;\n"+
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
						declares+="DECLARE "+pool.CorrectName("@"+efm.FieldName)+" "+efm.Type+";\n";
						if (!efm.AutoGen)
						{
							sets+=" SET "+pool.CorrectName("@"+efm.FieldName)+" = (SELECT "+efm.FieldName+" FROM INSERTED);\n";
							queryFields+=" AND "+efm.FieldName+" = "+pool.CorrectName("@"+efm.FieldName);
						}
					}
					code+=declares;
					code+="BEGIN \n";
					code+=sets;
					code+="SET "+pool.CorrectName("@"+field.FieldName)+" = (SELECT MAX("+field.FieldName+") FROM "+map.TableName+" WHERE ";
					code+=queryFields.Substring(4)+");\n";
					code+="IF ("+pool.CorrectName("@"+field.FieldName)+" is NULL)\n";
					code+="\tTHEN SET "+pool.CorrectName("@"+field.FieldName)+" = -1;\n";
					code+="INSERT INTO "+map.TableName+" SELECT "+pool.CorrectName("@"+field.FieldName)+"+1"+fields+" FROM INSERTED;\n";
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
