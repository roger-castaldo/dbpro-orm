using System;
using System.Collections.Generic;
using System.Text;
using Org.Reddragonit.Dbpro.Exceptions;

namespace Org.Reddragonit.Dbpro.Validation
{
    /*
     * This is an abstract class used to do some simple backended validation
     * for properties that are being set through the lazy proxy.  Each settings defines 
     * what happens on failure.  It will output the error message, if set, or generate a generic 
     * message to the console, debug and throw an error depending on which are flagged for the
     * operation.  Validation Notes are used to generate the generic message to handle
     * when no error message is set.
     */
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

        //called by the lazy proxy when the validation fails
        //in order to output the appropraite errors according to its settings.
        public void FailValidation(string clazz,string field)
        {
            string msg=ErrorMessage;
            if (msg == null)
                msg = "Validation of type "+this.GetType().ToString()+" failed validation("+ValidationNotes+") for Class: "+clazz+" on Field: "+field;
            if (WriteToConsole)
                Console.WriteLine(msg);
            if (WriteToDebug)
                Logger.LogLine(msg);
            if (ThrowExceptionOnError)
            {
                if (ErrorMessage == null)
                    throw new ValidationException(clazz, field, ValidationNotes);
                else
                    throw new Exception(msg);
            }
        }

        //called to validate that the value attempting to be set is a valid value.
        public abstract bool IsValidValue(object value);
        //called to obtain the generic validation message for the current instance.
        public abstract string ValidationNotes { get; }
    }
}
