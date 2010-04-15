using Org.Reddragonit.Dbpro.Structure.Mapping;
using System;
using System.Reflection;

namespace Org.Reddragonit.Dbpro.Structure.Attributes
{
	[AttributeUsage(AttributeTargets.Class)]
	public class Table : Attribute
	{

		private string _tableName=null;
		private string _connectionName=null;

		public Table() : this(null,null)
		{ }

		public Table(string TableName) : this(TableName,null)
		{
		}
		
		public Table(string TableName,string ConnectionName)
		{
			_tableName=TableName;
			_connectionName=ConnectionName;
		}
		
		public string ConnectionName
		{
			get{
				if(_connectionName==null)
					_connectionName=Connections.ConnectionPoolManager.DEFAULT_CONNECTION_NAME;
				return _connectionName;
			}
		}

		public string TableName
		{
			get
			{
				if (_tableName == null)
				{
					Type t = ClassMapper.ClassedTypes[ClassMapper.ClassedTypes.Count-1];
					foreach (object obj in t.GetCustomAttributes(this.GetType(), true))
					{
						if (obj.Equals(this))
						{
							_tableName = "";
							foreach (char c in t.Name.ToCharArray())
							{
								if (c.ToString().ToUpper() == c.ToString())
								{
									_tableName += "_" + c.ToString().ToLower();
								}
								else
								{
									_tableName += c;
								}
							}
							if (_tableName[0] == '_')
							{
								_tableName = _tableName[1].ToString().ToUpper() + _tableName.Substring(2);
							}
							_tableName = _tableName.ToUpper();
                            _tableName = _tableName.Replace("__", "_");
						}
					}
				}
				return _tableName;
			}
		}
	}
}
