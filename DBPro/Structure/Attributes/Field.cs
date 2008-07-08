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

		private string _fieldName=null ;
		private FieldType _fieldType;
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
				return _fieldType;
			}
		}

		public string Name
		{
			get
			{
                if (_fieldName == null)
                {
                    Assembly asm = Assembly.GetEntryAssembly();
                    foreach (Type t in asm.GetTypes())
                    {
                        if (t.IsSubclassOf(typeof(Org.Reddragonit.Dbpro.Structure.Table)))
                        {
                            foreach (MemberInfo m in t.GetMembers(BindingFlags.Public |      //Get public members
                                                                                 BindingFlags.NonPublic |   //Get private/protected/internal members
                                                                                 BindingFlags.Static |      //Get static members
                                                                                 BindingFlags.Instance |    //Get instance members
                                                                                 BindingFlags.DeclaredOnly))
                            {
                                foreach (object obj in m.GetCustomAttributes(this.GetType(), true))
                                {
                                    if (obj.Equals(this))
                                    {
                                        _fieldName = "";
                                        foreach (char c in m.Name.ToCharArray())
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
                                        if (_fieldName[0] == '_')
                                        {
                                        	_fieldName =_fieldName[1].ToString().ToUpper()+ _fieldName.Substring(2).ToUpper();
                                        }
                                        string type = m.ToString().Split(" ".ToCharArray())[0];
                                        if (type.IndexOf(".") >= 0)
                                        {
                                            type = type.Substring(type.LastIndexOf(".") + 1);
                                        }
                                        type = type.Replace(" ", "").Replace("[]", "");
                                        _fieldType = GetFieldType(type);
                                        if ((_fieldType == FieldType.STRING) || (_fieldType == FieldType.BYTE))
                                        {
                                            _fieldLength = -1;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
				return _fieldName;
			}
		}

        private FieldType GetFieldType(string name)
        {
            switch (name)
            {
                case "Int":
                    return FieldType.INTEGER;
                default:
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
