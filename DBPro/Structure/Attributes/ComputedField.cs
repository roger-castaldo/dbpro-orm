using System;
using System.Collections.Generic;
using System.Text;

namespace Org.Reddragonit.Dbpro.Structure.Attributes
{
    [AttributeUsage(AttributeTargets.Property)]
    public class ComputedField : Field
    {
        private string _code;
        public string Code
        {
            get { return _code; }
        }

        public ComputedField(string code):base()
		{
            _code=code;
		}

		public ComputedField(string code,int fieldLength) : base(fieldLength,true)
		{
            _code = code;
		}

		public ComputedField(string code,bool nullable) : base(int.MinValue,nullable)
		{
            _code = code;
		}

		public ComputedField(string code,int fieldLength, bool Nullable):base(fieldLength,Nullable)
		{
            _code = code;
		}
		
		public ComputedField(string code,FieldType type):base(type)
		{
            _code = code;
		}
		
		public ComputedField(string code,string FieldName,FieldType type):base(FieldName,type,true,0)
		{
            _code = code;
		}

		public ComputedField(string code,string FieldName,FieldType type,bool Nullable):base(FieldName,type,Nullable,0)
		{
            _code = code;
		}
		
		public ComputedField(string code,string FieldName,FieldType type,int fieldLength) :base(FieldName,type,true,fieldLength)
		{
            _code = code;
		}

        public ComputedField(string code,string FieldName, FieldType type, bool Nullable, int fieldLength):
            base(FieldName,type,Nullable,fieldLength)
        {
            _code = code;
        }
    }
}
