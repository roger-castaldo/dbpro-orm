using System;
using System.Collections.Generic;
using System.Text;

namespace Org.Reddragonit.Dbpro.Validation
{
    public class NotNullValidation : ValidationAttribute 
    {
        public NotNullValidation(string ErrorMessage, bool ThrowOnError, bool WriteToConsole, bool WriteToDebug)
            : base(ErrorMessage, ThrowOnError, WriteToConsole, WriteToDebug)
        { }

        public sealed override bool IsValidValue(object value){
            return (value != null) && (value.ToString().Length > 0);
        }

        public override string ValidationNotes
        {
            get {return "Not Null"; }
        }
    }
}
