using System;
using System.Collections.Generic;
using System.Text;

namespace Org.Reddragonit.Dbpro.Virtual.Attributes
{
    [AttributeUsage(AttributeTargets.Property)]
    public class VirtualField : Attribute 
    {
        private Type _referencingTable;
        public Type ReferencingTable
        {
            get { return _referencingTable; }
            set { _referencingTable = value; }
        }

        private string _fieldName;
        public string FieldName
        {
            get { return _fieldName; }
            set { _fieldName = value; }
        }

        public VirtualField(Type ReferencingTable, string FieldName)
        {
            _referencingTable = ReferencingTable;
            _fieldName = FieldName;
        }
    }
}
