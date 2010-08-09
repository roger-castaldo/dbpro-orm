﻿using System;
using System.Collections.Generic;
using System.Text;

namespace Org.Reddragonit.Dbpro.Validation
{
    [AttributeUsage(AttributeTargets.Property)]
    public abstract class ValidationAttribute : Attribute 
    {

        private bool _throwOnError;
        public bool ThrowExceptionOnError
        {
            get { return _throwOnError; }
        }

        private bool _writeToConsole;
        public bool WriteToConsole
        {
            get { return _writeToConsole; }
        }

        private bool _writeToDebug;
        public bool WriteToDebug
        {
            get { return _writeToDebug; }
        }

        private string _errorMessage;
        public string ErrorMessage
        {
            get { return _errorMessage; }
        }

        public ValidationAttribute(string errorMessage, bool ThrowOnError, bool WriteToConsole, bool WriteToDebug)
        {
            _throwOnError = ThrowOnError;
            _writeToConsole = WriteToConsole;
            _writeToDebug = WriteToDebug;
            _errorMessage = errorMessage;
        }

        public abstract bool IsValidValue(object value);
    }
}