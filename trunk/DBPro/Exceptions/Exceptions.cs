/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 19/01/2009
 * Time: 2:18 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using System;

namespace Org.Reddragonit.Dbpro.Exceptions
{
	public class AlterPrimaryKeyException : Exception 
	{
		public AlterPrimaryKeyException(string clazz, string field) : base("Unable to change the primary key value of a saved object.  Class: "+clazz+", Field: "+field)
		{
		}
	}

    public class ValidationException : Exception
    {
        public ValidationException(string clazz,string field,string validationNotes) :
            base("Unable to set the Field: "+field+" in Class: "+clazz+" because it failed validation ("+validationNotes+")"){}
    }
}
