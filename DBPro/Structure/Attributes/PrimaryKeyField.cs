using System;

namespace Org.Reddragonit.Dbpro.Structure.Attributes
{
	/// <summary>
	/// Summary description for PrimaryKeyField.
	/// </summary>
	public class PrimaryKeyField : Field,IPrimaryKeyField
	{

		private bool _autogen=false;

        public PrimaryKeyField(): this(false)
        { }

        public PrimaryKeyField(bool autogen): base(false)
        {
            _autogen = autogen;
        }

        public PrimaryKeyField(int fieldLength)
            : this(fieldLength, false)
        {
        }

        public PrimaryKeyField(int fieldLength, bool Nullable) : base(fieldLength,Nullable)
        {
        }

        public PrimaryKeyField(bool autogen,int fieldLength)
            : base(fieldLength,false)
        {
            _autogen = autogen;
        }
		
		public PrimaryKeyField(string FieldName,FieldType type):this(FieldName,type,0,false)
		{}
		
		public PrimaryKeyField(string FieldName,FieldType type,int FieldLength) : this (FieldName,type,FieldLength,false)
		{}

		public PrimaryKeyField(string FieldName,FieldType type,int FieldLength,bool AutoGen) : base(FieldName,type,false,FieldLength)
		{
			_autogen=AutoGen;
		}

		public bool AutoGen
		{
			get
			{
				return _autogen;
			}
		}
	}
}
