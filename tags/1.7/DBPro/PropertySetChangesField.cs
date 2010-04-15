using System;
using System.Collections.Generic;
using System.Text;

namespace Org.Reddragonit.Dbpro
{
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Method)]
    public class PropertySetChangesField : Attribute
    {

        private string _fieldAffected;
        public string FieldAffected
        {
            get { return _fieldAffected; }
        }

        public PropertySetChangesField(string fieldAffected)
        {
            _fieldAffected = fieldAffected;
        }

    }
}
