using System;
using System.Reflection;

namespace Org.Reddragonit.Dbpro.Structure.Attributes
{
	[AttributeUsage(AttributeTargets.Class)]
	public class Table : Attribute
	{

        public enum TableSettings
        {
            None = 0x00,
            AlwaysInsert = 0x01,
            AutoDeleteParent = 0x02
        }

		private string _tableName=null;
		private string _connectionName=null;
        private bool _alwaysInsert;
        private bool _autoDeleteParent;

		public Table() : this(null,null,TableSettings.None)
		{ }

        public Table(TableSettings settings)
            : this(null, null, settings) { }

        public Table(bool alwaysInsert)
            : this(null, null, (alwaysInsert ? TableSettings.AlwaysInsert : TableSettings.None))
        {
        }

		public Table(string TableName) : this(TableName,null,TableSettings.None)
		{
		}

        public Table(string TableName, TableSettings settings)
            : this(TableName, null, settings)
        { }

        public Table(string TableName,bool alwaysInsert)
            : this(TableName, null, (alwaysInsert ? TableSettings.AlwaysInsert : TableSettings.None))
        {
        }

        public Table(string TableName,string ConnectionName)
            : this(TableName, ConnectionName, TableSettings.None)
        {
        }
		
		public Table(string TableName,string ConnectionName,bool alwaysInsert)
            : this(TableName,ConnectionName,(alwaysInsert ? TableSettings.AlwaysInsert : TableSettings.None))
        {}

        public Table(string TableName,string ConnectionName,TableSettings settings)
		{
			_tableName=TableName;
			_connectionName=ConnectionName;
            _alwaysInsert = ((int)settings&(int)TableSettings.AlwaysInsert) == (int)TableSettings.AlwaysInsert;
            _autoDeleteParent = ((int)settings & (int)TableSettings.AutoDeleteParent) == (int)TableSettings.AutoDeleteParent;
		}
		
		public string ConnectionName
		{
			get{
				if(_connectionName==null)
					_connectionName=Connections.ConnectionPoolManager.DEFAULT_CONNECTION_NAME;
				return _connectionName;
			}
		}

        public bool AlwaysInsert
        {
            get
            {
                return _alwaysInsert;
            }
        }

        public bool AutoDeleteParent
        {
            get { return _autoDeleteParent; }
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
