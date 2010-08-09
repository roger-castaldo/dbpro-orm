﻿using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;

namespace Org.Reddragonit.Dbpro.Validation
{
    public class RegexValidation : ValidationAttribute
    {

        private string _regularExpression;
        public string RegularExpression{
            get{return _regularExpression;}
        }

        public RegexValidation(string RegexPattern,string ErrorMessage,bool ThrowOnError,bool WriteToConsole,bool WriteToDebug) 
            : base(ErrorMessage,ThrowOnError,WriteToConsole,WriteToDebug)
        {
            _regularExpression = RegexPattern;
        }

        public sealed override bool IsValidValue(object value)
        {
            Regex r = new Regex(RegularExpression);
            return r.IsMatch(value.ToString());
        }

    }
}