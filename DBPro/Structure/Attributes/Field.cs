using Org.Reddragonit.Dbpro.Structure.Mapping;
using System;
using System.Reflection;

namespace Org.Reddragonit.Dbpro.Structure.Attributes
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
		DOUBLE,
		ENUM,
        UNSIGNED_INTEGER,
        UNSIGNED_SHORT,
        UNSIGNED_LONG
	};
	
	[AttributeUsage(AttributeTargets.Property)]
	public class Field : Attribute,IField
	{

		private string _fieldName=null ;
		private FieldType? _fieldType;
		private int _fieldLength=int.MinValue ;
		private bool _nullable=true;

		public Field()
		{
		}

		public Field(int fieldLength) : this(fieldLength,true)
		{
		}

		public Field(bool nullable) : this(int.MinValue,nullable)
		{
		}

		public Field(int fieldLength, bool Nullable)
		{
			_fieldLength = fieldLength;
			_nullable=Nullable;
		}
		
		public Field(FieldType type)
		{
			_fieldType=type;
		}
		
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
			_fieldName=FieldName.ToUpper();
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
				string t = Name;
				return _fieldLength;
			}
		}

		public FieldType Type
		{
			get
			{
				string t = Name;
				return _fieldType.Value;
			}
		}

		public string Name
		{
			get
			{
				return _fieldName;
			}
			set{
				_fieldName=value;
			}
		}
		
		internal void InitFieldName(PropertyInfo p)
		{
			if (_fieldName == null)
			{
				_fieldName = "";
				if (p.Name.ToUpper() != p.Name)
				{
					foreach (char c in p.Name.ToCharArray())
					{
						if (c.ToString().ToUpper() == c.ToString())
						{
							_fieldName += "_" + c.ToString().ToUpper();
						}
						else
						{
							_fieldName += c.ToString().ToUpper();
						}
					}
				}else{
					_fieldName=p.Name;
				}
				if (_fieldName[0] == '_')
				{
					_fieldName =_fieldName[1].ToString().ToUpper()+ _fieldName.Substring(2).ToUpper();
				}
				if (_fieldName[_fieldName.Length-1]=='_')
					_fieldName=_fieldName.Substring(0,_fieldName.Length-1);
				if (!_fieldType.HasValue)
					_fieldType = GetFieldType(p.PropertyType);
				if ((_fieldLength==int.MinValue)&&((_fieldType == FieldType.STRING) || (_fieldType == FieldType.BYTE)))
				{
					if ((_fieldType== FieldType.BYTE)&&(!p.PropertyType.IsArray))
						_fieldLength=1;
					else
						_fieldLength = -1;
				}
                if ((_fieldType == FieldType.STRING) && (this is PrimaryKeyField))
                {
                    if (((PrimaryKeyField)this).AutoGen)
                    {
                        _fieldLength = 38;
                    }
                }
				Logger.LogLine("Located Field Name: "+_fieldName);
			}
		}

		private FieldType GetFieldType(Type propertyType)
		{
            if (propertyType.IsGenericType && propertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
                propertyType = propertyType.GetGenericArguments()[0];
			switch (propertyType.Name.ToUpper().Replace("[]",""))
			{
				case "INT64":
				case "LONG":
					return FieldType.LONG;
                case "UINT64":
                case "ULONG":
                    _fieldLength = 8;
                    return FieldType.UNSIGNED_LONG;
				case "INT16":
                case "SHORT":
					return FieldType.SHORT;
                case "UINT16":
                case "USHORT":
                    _fieldLength = 2;
                    return FieldType.UNSIGNED_SHORT;
				case "DATETIME":
					return FieldType.DATETIME;
				case "BOOLEAN":
					return FieldType.BOOLEAN;
				case "CHAR":
					return FieldType.CHAR;
				case "BYTE":
					return FieldType.BYTE;
				case "DECIMAL":
					return FieldType.DECIMAL;
				case "DOUBLE":
					return FieldType.DOUBLE;
				case "FLOAT":
					return FieldType.FLOAT;
				case "INT":
				case "INT32":
					return FieldType.INTEGER;
                case "UINT":
                case "UINT32":
                    _fieldLength = 4;
                    return FieldType.UNSIGNED_INTEGER;
				default:
					if (propertyType.IsEnum)
						return FieldType.ENUM;
					else
						return FieldType.STRING;
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
