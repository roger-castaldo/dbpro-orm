using System;

namespace Org.Reddragonit.Dbpro.Structure.Attributes
{
	/// <summary>
	/// Summary description for PrimaryKeyField.
	/// </summary>
	public class PrimaryKeyField : Field,IPrimaryKeyField
	{

		private bool _autogen=false;

        public PrimaryKeyField(): base()
        { }

        public PrimaryKeyField(bool autogen): base()
        {
            _autogen = autogen;
        }

        public PrimaryKeyField(int fieldLength)
            : this(fieldLength, true)
        {
        }

        public PrimaryKeyField(int fieldLength, bool Nullable) : base(fieldLength,Nullable)
        {
        }

        public PrimaryKeyField(bool autogen,int fieldLength)
            : this(autogen,fieldLength, true)
        {
        }

        public PrimaryKeyField(bool autogen, bool nullable)
            : this(autogen,int.MinValue, nullable)
        {
        }

        public PrimaryKeyField(bool autogen, int fieldLength, bool Nullable) : base(fieldLength,Nullable)
        {
            _autogen = autogen;
        }
		
		public PrimaryKeyField(string FieldName,FieldType type):this(FieldName,type,false,0,false)
		{}

		public PrimaryKeyField(string FieldName,FieldType type,bool Nullable) : this(FieldName,type,Nullable,0,false)
		{
			_autogen=AutoGen;
		}
		
		public PrimaryKeyField(string FieldName,FieldType type,bool Nullable,int FieldLength) : this (FieldName,type,Nullable,FieldLength,false)
		{}
		
		public PrimaryKeyField(string FieldName,FieldType type,bool Nullable,bool AutoGen) : this(FieldName,type,Nullable,0,AutoGen)
		{}

		public PrimaryKeyField(string FieldName,FieldType type,bool Nullable,int FieldLength,bool AutoGen) : base(FieldName,type,Nullable,FieldLength)
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
