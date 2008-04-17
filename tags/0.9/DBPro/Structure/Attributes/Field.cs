using System;
using System.Reflection;

namespace Org.Reddragonit.Dbpro.Structure.Attributes
{
	[AttributeUsage(AttributeTargets.Property)]
	public class Field : Attribute,IField
	{

		public enum FieldType
		{
			INTEGER,
			SHORT,
			LONG,
			STRING,
			CHAR,
			BYTE,
			DATE,
			TIME,
			DATETIME,
			BOOLEAN,
			MONEY,
			DECIMAL, 
			IMAGE,
			FLOAT,
			DOUBLE
		};

		private string _fieldName;
		private FieldType _fieldType;
		private int _fieldLength;
		private bool _nullable=true;
		
		public Field(string FieldName,FieldType type):this(FieldName,type,true,0)
		{
		}

		public Field(string FieldName,FieldType type,bool Nullable):this(FieldName,type,Nullable,0)
		{
		}
		
		public Field(string FieldName,FieldType type,int fieldLength) :this(FieldName,type,true,fieldLength)
		{
		}

		public Field(string FieldName,FieldType type,bool Nullable,int fieldLength)
		{
			if (FieldName == null)
				throw new Exception("Cannot set Field with null name.");
			_fieldName=FieldName;
			_fieldType=type;
			_nullable=Nullable;
			_fieldLength=fieldLength;
			if (_fieldType.Equals(FieldType.STRING) && (fieldLength==0))
			{
				throw new Exception("Cannot set field type of string without setting the field length.  Set -1 for very large strings.");
			}
		}

		public int Length
		{
			get
			{
				return _fieldLength;
			}
		}

		public FieldType Type
		{
			get
			{
				return _fieldType;
			}
		}

		public string Name
		{
			get
			{
				return _fieldName;
			}
		}
		
		public bool Nullable
		{
			get
			{
				return _nullable;
			}
		}
	}
}
