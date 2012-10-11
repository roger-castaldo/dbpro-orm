using System;
using System.Collections.Generic;
using System.Text;
using Org.Reddragonit.Dbpro.Structure.Attributes;

namespace TestingApp.Structure
{
    [Table()]
    public class ExtensionNumber : Org.Reddragonit.Dbpro.Structure.Table
    {
        private string _number;
        [PrimaryKeyField(false,50)]
        public string Number
        {
            get { return _number; }
            set { _number = value; }
        }
    }
}
