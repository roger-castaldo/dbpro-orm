using System;
using System.Reflection;

namespace Org.Reddragonit.Dbpro.Structure.Attributes
{
	[AttributeUsage(AttributeTargets.Class)]
	public class Table : Attribute
	{

		private string _tableName=null;
		private string _connectionName=null;

		public Table(string TableName,string ConnectionName)
		{
			if (TableName==null)
				throw new Exception("Cannot set Table attribute with null Table Name");
			if (ConnectionName==null)
				throw new Exception("Cannot set Table attribute with null Connection Name");
			_tableName=TableName;
			_connectionName=ConnectionName;
			Assembly asm = Assembly.GetEntryAssembly();
		}

		public string TableName
		{
			get
			{
				return _tableName;
			}
		}
	}
}
