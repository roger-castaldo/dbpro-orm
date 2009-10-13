using System;
using System.Collections.Generic;
using System.Text;

namespace Org.Reddragonit.Dbpro.Connections.ClassSQL
{
    public class QueryToken
    {
        private string _value;
		private TokenType _type;
		private int _start;
		private int _length;
		
		public QueryToken(string value, TokenType type, int start, int length) {
			this._length = length;
			this._start = start;
			this._type = type;
			this._value = value;
		}
		
		public int Length{ get{ return _length;} }

        public int Start { get { return _start; } }

        public TokenType Type { get { return _type; } }

        public string Value { get { return _value; } }
    }
}
