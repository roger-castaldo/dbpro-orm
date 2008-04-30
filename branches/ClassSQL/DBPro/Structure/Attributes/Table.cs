using System;
using System.Reflection;

namespace Org.Reddragonit.Dbpro.Structure.Attributes
{
	[AttributeUsage(AttributeTargets.Class)]
	public class Table : Attribute
	{

		private string _tableName=null;

        public Table()
        { }

		public Table(string TableName)
		{
			_tableName=TableName;
		}

		public string TableName
		{
			get
			{
                if (_tableName == null)
                {
                    Assembly asm = Assembly.GetEntryAssembly();
                    foreach (Type t in asm.GetTypes())
                    {
                        if (t.IsSubclassOf(typeof(Org.Reddragonit.Dbpro.Structure.Table)))
                        {
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
                                }
                            }
                        }
                    }
                }
				return _tableName;
			}
		}
	}
}
