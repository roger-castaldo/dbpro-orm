/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 16/03/2009
 * Time: 8:46 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using System;
using System.Data;
using System.Collections.Generic;
using MySql.Data.MySqlClient;
using MyConn = MySql.Data.MySqlClient.MySqlConnection;
using Org.Reddragonit.Dbpro.Structure.Attributes;

namespace Org.Reddragonit.Dbpro.Connections.MySql
{
	/// <summary>
	/// Description of MySqlConnection.
	/// </summary>
	public class MySqlConnection : Connection
	{
		
		private QueryBuilder _queryBuilder = null;
		internal override QueryBuilder queryBuilder {
			get { 
				if (_queryBuilder==null)
					_queryBuilder=new MySqlQueryBuilder(Pool,this);
				return _queryBuilder;
			}
		}
		
		internal override string DefaultTableString {
			get {
				return "information_schema.COLUMNS";
			}
		}
		
		internal override bool UsesIdentities {
			get { return true; }
		}
		
		public MySqlConnection(ConnectionPool pool,string connectionString) :base(pool,connectionString)
		{
		}
		
		public override IDbDataParameter CreateParameter(string parameterName, object parameterValue)
		{
			return (IDbDataParameter)new MySqlParameter(parameterName,parameterValue);
		}
		
		internal override IDbDataParameter CreateParameter(string parameterName, object parameterValue, FieldType type, int fieldLength)
		{
			IDbDataParameter ret= CreateParameter(parameterName,parameterValue);
			if (((type==FieldType.CHAR)||(type==FieldType.STRING))
			    &&((fieldLength == -1)||(fieldLength>65350)))
			{
				((MySqlParameter)ret).MySqlDbType= MySqlDbType.Text;
			}
			return ret;
		}
		
		protected override IDbCommand EstablishCommand()
		{
			return new MySqlCommand("",(MyConn)conn);
		}
		
		protected override IDbConnection EstablishConnection()
		{
			return new MyConn(connectionString);
		}
		
		internal override void GetAddAutogen(ExtractedTableMap map, ConnectionPool pool, out System.Collections.Generic.List<IdentityField> identities, out System.Collections.Generic.List<Generator> generators, out System.Collections.Generic.List<Trigger> triggers)
		{
			identities=new List<IdentityField>();
			generators=null;
			triggers=new List<Trigger>();
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
			if (field.Type.ToUpper().Contains("DATE")||field.Type.ToUpper().Contains("TIME"))
			{
				triggers.Add(new Trigger(pool.CorrectName(map.TableName+"_"+field.FieldName+"_GEN"),"BEFORE INSERT ON "+map.TableName,
				                         "FOR EACH ROW BEGIN\n SET NEW."+field.FieldName+" = CURRENT_DATE();\n END"));
			}else if (field.Type.ToUpper().Contains("INT"))
			{
				if (map.PrimaryKeys.Count==1)
				{
					identities.Add(new IdentityField(map.TableName,field.FieldName,field.FullFieldType,"0"));
				}else{
                    string code = "FOR EACH ROW\nBEGIN\n";
					string queryFields="";
					foreach (ExtractedFieldMap efm in map.PrimaryKeys)
					{
						if (!efm.AutoGen)
							queryFields+=" AND "+efm.FieldName+" = NEW."+efm.FieldName;
					}
					code+="SET NEW."+field.FieldName+" = (SELECT (CASE WHEN MAX("+field.FieldName+") IS NULL THEN 0 ELSE MAX("+field.FieldName+")+1 END) FROM "+map.TableName+" WHERE ";
					code+=queryFields.Substring(4)+");\n";
					code+="END";
                    triggers.Add(new Trigger(pool.CorrectName(map.TableName + "_" + field.FieldName + "_GEN"), "BEFORE INSERT ON " + map.TableName, code));
				}
			}else
				throw new Exception("Unable to create autogenerator for non date or digit type.");
		}
		
		internal override void GetDropAutogenStrings(ExtractedTableMap map, ConnectionPool pool, out System.Collections.Generic.List<IdentityField> identities, out System.Collections.Generic.List<Generator> generators, out System.Collections.Generic.List<Trigger> triggers)
		{
			identities=new List<IdentityField>();
            triggers = new List<Trigger>();
            generators = null;
            ExtractedFieldMap field = map.PrimaryKeys[0];
            if ((map.PrimaryKeys.Count > 1) && (!field.AutoGen))
            {
                foreach (ExtractedFieldMap efm in map.PrimaryKeys)
                {
                    if (efm.AutoGen)
                    {
                        field = efm;
                        break;
                    }
                }
            }
            if (field.Type.ToUpper().Contains("DATE") || field.Type.ToUpper().Contains("TIME"))
            {
                triggers.Add(new Trigger(pool.CorrectName(map.TableName + "_" + field.FieldName + "_GEN"), "", ""));
            }
            else if (field.Type.ToUpper().Contains("INT"))
            {
                if (map.PrimaryKeys.Count>1)
                    triggers.Add(new Trigger(pool.CorrectName(map.TableName + "_" + field.FieldName + "_GEN"), "", ""));
                else
                    identities.Add(new IdentityField(map.TableName, field.FieldName, field.FullFieldType,""));
            }
            else
            {
                throw new Exception("Unable to create autogenerator for non date or digit type.");
            }
		}
		
		internal override List<string> GetDropTableString(string table, bool isVersioned)
		{
			List<string> ret = new List<string>();
			ret.Add(queryBuilder.DropTable(table));
			if (isVersioned)
			{
				ret.Add(queryBuilder.DropTrigger(queryBuilder.VersionTableInsertTriggerName(table)));
				ret.Add(queryBuilder.DropTrigger(queryBuilder.VersionTableUpdateTriggerName(table)));
				ret.Add(queryBuilder.DropTable(table));
			}
			return ret;
		}
		
		internal override List<Trigger> GetVersionTableTriggers(ExtractedTableMap table, VersionField.VersionTypes versionType, ConnectionPool pool)
		{
            List<Trigger> ret = new List<Trigger>();
            string tmp = "FOR EACH ROW\nBEGIN\n";
            tmp += "\tINSERT INTO " + table.TableName + "(" + table.Fields[0].FieldName;
            for (int x = 1; x < table.Fields.Count; x++)
            {
                ExtractedFieldMap efm = table.Fields[x];
                tmp += "," + efm.FieldName;
            }
            tmp += ") VALUES(";
            if (table.Fields[0].Type=="DATETIME")
            	tmp+="CURRENT_DATE()";
            else
            	tmp+="0";
            for (int x = 1; x < table.Fields.Count; x++)
            {
                ExtractedFieldMap efm = table.Fields[x];
                tmp += ",NEW." + efm.FieldName;
            }
            tmp += ");\nEND\n\n";
            ret.Add(new Trigger(pool.CorrectName(queryBuilder.VersionTableInsertTriggerName(queryBuilder.RemoveVersionName(table.TableName))),
                                "AFTER INSERT ON " + queryBuilder.RemoveVersionName(table.TableName),
                                tmp));
            ret.Add(new Trigger(pool.CorrectName(queryBuilder.VersionTableUpdateTriggerName(table.TableName)),
                                "AFTER UPDATE ON " + table.TableName,
                                tmp));
            return ret;
		}
		
		internal override string TranslateFieldType(Org.Reddragonit.Dbpro.Structure.Attributes.FieldType type, int fieldLength)
		{
			string ret=null;
			switch(type)
			{
				case FieldType.BOOLEAN:
					ret="BIT";
					break;
				case FieldType.BYTE:
					if (fieldLength==1)
						ret="TINYINT";
					else if ((fieldLength==-1)||(fieldLength>6535))
						ret="BLOB";
					else
						ret="VARCHAR("+fieldLength.ToString()+") CHARACTER SET BINARY";
					break;
				case FieldType.CHAR:
					if ((fieldLength==-1)||(fieldLength>6535))
						ret="TEXT";
					else
						ret="CHAR("+fieldLength.ToString()+")";
					break;
				case FieldType.DATE:
					ret="DATE";
					break;
				case FieldType.DATETIME:
					ret="DATETIME";
					break;
				case FieldType.TIME:
					ret="TIME";
					break;
				case FieldType.DECIMAL:
					ret="DECIMAL(18,9)";
					break;
				case FieldType.DOUBLE:
					ret="DOUBLE";
					break;
				case FieldType.FLOAT:
					ret="FLOAT";
					break;
				case FieldType.IMAGE:
					ret="BLOB";
					break;
				case FieldType.INTEGER:
					ret="INTEGER";
					break;
				case FieldType.LONG:
					ret="BIGINT";
					break;
				case FieldType.MONEY:
					ret="DECIMAL(18,4)";
					break;
				case FieldType.SHORT:
					ret = "SMALLINT";
					break;
				case FieldType.STRING:
					if ((fieldLength==-1)||(fieldLength>65535))
						ret="TEXT";
					else
						ret="VARCHAR("+fieldLength.ToString()+")";
					break;
			}
			return ret;
		}
	}
}
