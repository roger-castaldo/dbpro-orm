using System;
using System.Collections.Generic;
using System.Text;

namespace Org.Reddragonit.Dbpro.Structure.Attributes
{
    [AttributeUsage(AttributeTargets.Class,AllowMultiple=true,Inherited=false)]
    public class TableIndex : Attribute
    {
        private string _name;
        public string Name
        {
            get { return _name; }
        }

        private string[] _fields;
        public string[] Fields
        {
            get { return _fields; }
        }

        private bool _unique;
        public bool Unique
        {
            get { return _unique; }
        }

        private bool _ascending;
        public bool Ascending
        {
            get { return _ascending; }
        }

        public TableIndex(string name, string[] fields, bool unique,bool ascending)
        {
            _name = name;
            _fields = fields;
            _unique = unique;
            _ascending = ascending;
        }
    }
}
