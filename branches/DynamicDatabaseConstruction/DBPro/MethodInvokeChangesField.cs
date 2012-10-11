using System;
using System.Collections.Generic;
using System.Text;

namespace Org.Reddragonit.Dbpro
{
    [AttributeUsage(AttributeTargets.Method)]
    public class MethodInvokeChangesField :Attribute 
    {
        private string[] _fieldAffected;
        public string[] FieldAffected
        {
            get { return _fieldAffected; }
        }

        public MethodInvokeChangesField(string[] fieldsAffected)
        {
            _fieldAffected = fieldsAffected;
        }

        public MethodInvokeChangesField(string fieldAffected)
        {
            _fieldAffected = new string[]{fieldAffected};
        }
    }
}
