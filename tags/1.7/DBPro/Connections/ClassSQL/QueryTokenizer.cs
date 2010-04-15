/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 04/06/2009
 * Time: 11:12 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using System;
using System.Collections.Generic;

namespace Org.Reddragonit.Dbpro.Connections.ClassSQL
{
	/// <summary>
	/// Description of QueryTokenizer.
	/// </summary>
	public class QueryTokenizer
	{
        /*
				& - 38
				~ - 126
				| - 124
				^ - 94
				/ - 47
				= - 61
				> - 62
				< - 60
				% - 37
				! - 33
				+ - 43
				* - 42
                - - 45
			*/
        private readonly int[] _operatorChars = new int[] { 38, 126, 124, 94, 47, 61, 62, 60, 37, 33, 43, 42, 45 };
        private readonly int[] _whiteSpaces = new int[] { 9, 10, 13, 32 };


        // holds an input data:
		private string _input;
		// current position in the data:
		private int _position = 0;
		// current character:
		private string ch;
		// current character code:
		private int chCode = 0;

        private List<QueryToken> _tokens;

        private bool _exit = false;

        public List<QueryToken> Tokens
        {
            get { return _tokens; }
        }
		
		public QueryTokenizer(string input) {
			// set the data and move to the first token:
			_input = input;
            _tokens=new List<QueryToken>();
			next();
		}
		
		public void parse() {
			// while not end of data found:
			while ( !_exit ) {
				// create empty token:
				QueryToken token;
				// skip all whitespace:
				skipIgnored();
				// if current character code is 0 end of data is reached:
				if ( currentCode() == 0 ) {
					break;
				}
				// remeber start position:
				int start = _position-1;
				// and check what is the character code:
				switch ( currentCode() ) {
					case 47: // /
						if ( preview() == 42 ) { // 42 is *
							// matched a multi line comment:
							next(); // skip *
							token = new QueryToken(null, TokenType.COMMENT, start, skipComment());
						} else {
							// matched single / operator:
							token = new QueryToken(null, TokenType.OPERATOR, start, skipOperator());
						}
						break;
					case 38:
					case 126:
					case 124:
					case 94:
					case 61:
					case 62:
					case 60:
					case 37:
					case 33:
					case 43:
					case 42:
                    case 45:
						// matched an operator:
                        int len = skipOperator();
						token = new QueryToken(_input.Substring(start,len), TokenType.OPERATOR, start, len);
						break;
					case 40: // (
					case 41: // )
					case 44: // ,
                        token = new QueryToken(ch, TokenType.TOKEN, start, 1);
						next(); // skip
						break;
					case 35: // # single line comment:
						token = new QueryToken(null, TokenType.COMMENT, start, skipUntilNl());
						break;
					case 46: // . for code hints
						next(); // skip .
						token = new QueryToken(ch, TokenType.DOT, start, 1);
						break;
					case 64: // @
						token = readVariable(start);
						break;
					case 39: // '
						// string:
						token = readString(start);
						break;
					case 96: // `
						// escaped key:
						token = readEscapedKey(start);
						break;
					default:
						if ( isDigit( currentCode() ) || currentCode() == 45 ) { // 45 is -
							token = readNumber(start);
						} else {
							token = readKey(start);
						}
						break;
				}
				// emit the token:
				emit(token);
			}
		}
		
		private void emit(QueryToken token){
			_tokens.Add(token);
		}
		
		// reads a quoted string:
		private QueryToken readString(int start){
			string sep= "'";
			string input= "";
			// until closing ' found append character to the data:
			while ( !_exit ) {
				input += ch;
				next();
				// if character is ' or is end of data, break
				if ( currentCode() == 39 || currentCode() == 0 ) { // 39 is '
					input += ch;
					next(); // skip `
					break;
				}
			}
			return new QueryToken(input, TokenType.ESCAPED_STRING, start, input.Length);
		}
		
		// reads any key, it may be a function name or keyword name, so-called literal:
		private QueryToken readKey(int start){
			string name = "";
			// until a non-literal character is found append character to the data:
			while ( !_exit ) {
				if (
					isWhiteSpace( currentCode() )
					|| isOperatorChar( currentCode() )
					|| currentCode() == 44 // ,
					|| currentCode() == 40 // (
					|| currentCode() == 41 // )
					|| currentCode() == 0 ) {
					break;
				}
				name += ch;
				next();
			}
			// check if it is a keyword:
			if ( QueryKeywords.IsKeyWord(name.ToUpper()) ) {
				return new QueryToken(name, TokenType.KEYWORD, start, name.Length);
			}
			// or function:
			if ( QueryFunctions.IsFunction(name.ToUpper()) ) {
				return new QueryToken(name, TokenType.FUNCTION, start, name.Length);
			}
			// otherwise it is just a key:
			return new QueryToken(name, TokenType.KEY, start, name.Length);
		}
		
		// reads a variable name:
		private QueryToken readVariable(int start){
			string name = "";
			// until a non-literal character is found append character to the data:
			while ( !_exit ) {
				if (
					isWhiteSpace( currentCode() )
					|| isOperatorChar( currentCode() )
					|| currentCode() == 44 // ,
					|| currentCode() == 40 // (
					|| currentCode() == 41 // )
					|| currentCode() == 0 ) {
					break;
				}
				name += ch;
				next();
			}
			return new QueryToken(name, TokenType.VARIABLE, start, name.Length);
		}
		
		// reads a MySQL escaped string:
		private QueryToken readEscapedKey(int start){
			string sep= "`";
			string input= "";
			// until matching ` is found append character to the data:
			while ( !_exit ) {
				input += ch;
				next();
				// if matching ` or end of data is found, break:
				if ( currentCode() == 96 || currentCode() == 0 ) { // 96 is `
					input += ch;
					next(); // skip `
					break;
				}
			}
			return new QueryToken(input, TokenType.ESCAPED_STRING, start, input.Length);
		}
		
		// reads a number:
		// this bit comes from coreutils JSON class,
		// modified to fit its current usage
		private QueryToken readNumber(int start){
			// the string to accumulate the number characters
			// into that we'll convert to a number at the end
			string input= "";
			// check for a negative number
			if ( currentCode() == 45 ) { // 45 is -
				input += '-';
				next();
			}
			// the number must start with a digit
			if ( !isDigit( currentCode() ) && currentCode() != 46 ) { // 46 is .
				// second - found, it is single line comment with -- notation:
				if ( currentCode() == 45 ) { // 45 is -
					next(); // skip operator
					return new QueryToken(null, TokenType.COMMENT, start, skipUntilNl()+2);
				}
				return new QueryToken("-", TokenType.TOKEN, start, 1);
			}
			
			// read numbers while we can
			while ( isDigit( currentCode() ) && currentCode() != 0 ) {
				input += ch;
				next();
			}
			
			// check for a decimal value
			if ( currentCode() == 46 ) { // 46 is .
				input += ".";
				next();
				while ( isDigit( currentCode() ) ) {
					input += ch;
					next();
				}
			}
			
			// check for scientific notation
			if ( currentCode() == 101 || currentCode() == 69 ) { // 101 is e, 69 is E
                input += "e";
				next();
				// check for sign
				if ( currentCode() == 43 || currentCode() == 45 ) { // 43 is +, 45 is -
					input += ch;
					next();
				}
				// read in the exponent
				while ( isDigit( currentCode() ) ) {
					input += ch;
					next();
				}
			}
			
			return new QueryToken( input, TokenType.NUMBER, start, input.Length );
		}
		
		private void skipIgnored(){
			skipWhite();
		}
		
		// skips all operator characters and reports
		// how many skipped:
		private int skipOperator(){
			int skipped= 0;
			while ( !_exit ) {
				if ( isOperatorChar(currentCode()) ) {
					next();
					skipped++;
				} else {
					break;
				}
			}
			return skipped;
		}
		
		// skips any characters until new line and reports how many
		// characters it skipped:
		private int skipUntilNl(){
			int skipped = 0;
			while ( currentCode() != 10 && currentCode() != 13 && currentCode() != 0 ) {
				next();
				skipped++;
			}
			return skipped;
		}
		
		// skips multi line comment and reports how many
		// characters it skipped
		private int skipComment() {
			int skipped = 0;
			while ( !_exit ) {
				next();
				skipped++;
				// si current character * and next / ? or is it end of data?
				if ( (currentCode() == 42 && preview() == 47) || currentCode() == 0 ) { // 42 is *, 47 is /
					next(); next();
					skipped+=3;
					break;
				}
			}
			return skipped;
		}
		
		private void skipWhite(){
			// As long as there are white spaces in the input 
			// stream, advance
			while ( isWhiteSpace( currentCode() ) ) {
				next();
			}
		}
		
		// returns current character code:
		public int currentCode(){
			return chCode;
		}
		
		// moves to the next character, if no next character 0 set is
		// 0 is used to mark end of data:
		public string next(){
            if (_position == _input.Length)
            {
                _exit = true;
                return null;
            }
			ch = _input[_position].ToString();
			chCode = (ch.Length==1) ? ch[0] : 0;
			_position++;
			return ch;
		}
		
		// returns next character without progressing to it
		// used to make a decision based on following character
		public int preview() {
			return (int)_input[_position];
		}
		
		// moves back one character:
		public string prev() {
			_position--;
			ch = _input[_position].ToString();
			return ch;
		}
		
		// is it a tab, \r, \n or space?
		private bool isWhiteSpace(int chr){
            return ArrayContainsObject(chr, _whiteSpaces);
		}
		
		// is is a digit?
		private bool isDigit(int chr){
			return ( chr >= 48 && chr <= 57 );
		}
		
		// is it a operator character?
		private bool isOperatorChar(int chr){
            return ArrayContainsObject(chr,_operatorChars);
		}

        private bool ArrayContainsObject(object obj, Array objects)
        {
            bool ret = false;
            foreach (object o in objects)
            {
                if (o.Equals(obj))
                {
                    ret=true;
                    break;
                }
            }
            return ret;
        }
	}
}
