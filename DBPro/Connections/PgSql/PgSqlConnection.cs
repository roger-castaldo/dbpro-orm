/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 03/04/2009
 * Time: 10:35 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using System;
using System.Data;
using Npgsql;
using System.Collections.Generic;
using Org.Reddragonit.Dbpro.Structure.Attributes;

namespace Org.Reddragonit.Dbpro.Connections.PgSql
{
	/// <summary>
	/// Description of PgSqlConnection.
	/// </summary>
	public class PgSqlConnection : Connection
	{
		public PgSqlConnection(ConnectionPool pool,string connectionString) : base(pool,connectionString)
		{
		}
		
		internal override string DefaultTableString {
			get {
				return "information_schema.tables";
			}
		}
		
		public override IDbDataParameter CreateParameter(string parameterName, object parameterValue)
		{
			return new NpgsqlParameter(parameterName,parameterValue);
		}
		
		internal override IDbDataParameter CreateParameter(string parameterName, object parameterValue, Org.Reddragonit.Dbpro.Structure.Attributes.FieldType type, int fieldLength)
		{
			return CreateParameter(parameterName,parameterValue);
		}
		
		protected override IDbCommand EstablishCommand()
		{
			return new NpgsqlCommand("",(NpgsqlConnection)conn);
		}
		
		protected override IDbConnection EstablishConnection()
		{
			return new NpgsqlConnection(connectionString);
		}
		
		internal override void GetAddAutogen(ExtractedTableMap map, ConnectionPool pool, out List<IdentityField> identities, out List<Generator> generators, out List<Trigger> triggers)
		{
			throw new NotImplementedException();
		}
		
		internal override void GetDropAutogenStrings(ExtractedTableMap map, ConnectionPool pool, out List<IdentityField> identities, out List<Generator> generators, out List<Trigger> triggers)
		{
			throw new NotImplementedException();
		}
		
		internal override List<string> GetDropTableString(string table, bool isVersioned)
		{
			return base.GetDropTableString(table, isVersioned);
		}
		
		internal override List<Trigger> GetVersionTableTriggers(ExtractedTableMap table, VersionField.VersionTypes versionType, ConnectionPool pool)
		{
			throw new NotImplementedException();
		}
		
		internal override string TranslateFieldType(Org.Reddragonit.Dbpro.Structure.Attributes.FieldType type, int fieldLength)
		{
			string ret=null;
			switch(type)
			{
				case FieldType.BOOLEAN:
					ret="BOOLEAN";
					break;
				case FieldType.BYTE:
					if (fieldLength==1)
						ret="TINYINT";
					else
						ret="OID";
					break;
				case FieldType.CHAR:
					ret="CHARACTER("+fieldLength.ToString()+")";
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
					ret="DOUBLE PRECISION";
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
					ret="CHARACTER VARYING("+fieldLength.ToString()+")";
					break;
			}
			return ret;
		}
		
		internal override bool UsesGenerators {
			get { return true; }
		}
		
		internal override bool UsesIdentities {
			get { return false; }
		}
		
		private PgSqlQueryBuilder _builder=null;
		internal override QueryBuilder queryBuilder {
			get {
				if (_builder==null)
					_builder=new PgSqlQueryBuilder(Pool,this);
				return _builder;
			}
		}
	}
}
