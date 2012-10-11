using System;
using System.Collections.Generic;
using System.Text;

namespace Org.Reddragonit.Dbpro.Connections.ClassSQL
{
    public enum TokenType{
        //keyword token:
		KEYWORD = 1,
        //function token
        FUNCTION = 2,
        //string token
		STRING = 3,
        //number token
		NUMBER = 4,
		// just token, for example comma:
        TOKEN = 5,
		// escaped string - using ``:
		ESCAPED_STRING = 6,
		// any key:
		KEY = 7,
		// variable name - @example:
		VARIABLE  = 8,
		// single or multi line comment:
		COMMENT = 9,
		// dot, required in the future for code completion:
		DOT = 10,
		// any operator:
		OPERATOR = 11
    }
}
