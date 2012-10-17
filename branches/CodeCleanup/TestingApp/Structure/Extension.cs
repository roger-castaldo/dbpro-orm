using System;
using System.Collections.Generic;
using System.Text;
using Org.Reddragonit.Dbpro.Structure.Attributes;

namespace TestingApp.Structure
{
    [Table()]
    public class Extension : ExtensionNumber
    {

        private string _password;
        [Field(150)]
        public string Password
        {
            get { return _password; }
            set { _password = value; }
        }
    }
}
