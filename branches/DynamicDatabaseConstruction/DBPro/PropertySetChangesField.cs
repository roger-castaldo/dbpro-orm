using System;
using System.Collections.Generic;
using System.Text;

namespace Org.Reddragonit.Dbpro
{
    [AttributeUsage(AttributeTargets.Property)]
    public class PropertySetChangesField : Attribute
    {

        private string[] _fieldAffected;
        public string[] FieldAffected
        {
            get { return _fieldAffected; }
        }

        public PropertySetChangesField(string[] fieldsAffected)
        {
            _fieldAffected = fieldsAffected;
        }

        public PropertySetChangesField(string fieldAffected)
        {
            _fieldAffected = new string[]{fieldAffected};
        }
    }
}
