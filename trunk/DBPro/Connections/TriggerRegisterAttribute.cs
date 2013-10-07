using System;
using System.Collections.Generic;
using System.Text;

namespace Org.Reddragonit.Dbpro.Connections
{
    [AttributeUsage(AttributeTargets.Class,AllowMultiple=true)]
    public class TriggerRegisterAttribute : Attribute
    {
        private Type _table;
        public Type Table
        {
            get { return _table; }
        }

        public TriggerRegisterAttribute(Type table)
        {
            _table = table;
        }
    }
}
