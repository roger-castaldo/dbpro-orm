using System;

namespace Org.Reddragonit.Dbpro.Connections.Attributes
{
	[AttributeUsage(AttributeTargets.Class)]
	public class Connection : Attribute
	{

		private string _connectionName=null;

		public Connection(string ConnectionName)
		{
			if (ConnectionName==null)
				throw new Exception("Cannot create Connection Attribute with null Connection Name");
			_connectionName=ConnectionName;
		}

		public string ConnectionName
		{
			get
			{
				return _connectionName;
			}
		}

	}
}
