/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 21/03/2008
 * Time: 11:33 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */

using Org.Reddragonit.Dbpro.Connections;
using System;
using System.Reflection;
using Org.Reddragonit.Dbpro.Structure.Attributes;

namespace Org.Reddragonit.Dbpro.Structure.Mapping
{
	/// <summary>
	/// Description of InternalFieldMap.
	/// </summary>
	internal class InternalFieldMap : FieldMap
	{
		private int _fieldLength=0;
		private string _fieldName=null;
		private FieldType _fieldType;
				
		public InternalFieldMap(PropertyInfo info) : base(info)
		{
			foreach (object obj in info.GetCustomAttributes(true))
			{
				if (obj is IField)
				{
					IField f = (IField)obj;
					((Field)f).InitFieldName(info);
					_fieldLength=f.Length;
					_fieldName=f.Name;
					_fieldType=f.Type;
				}
				if (obj is VersionField)
				{
					_versionable=true;
				}
			}
            Type propertyType = info.PropertyType;
            if (propertyType.IsGenericType && propertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
                propertyType = propertyType.GetGenericArguments()[0];
            if (propertyType.Name.EndsWith("[]"))
                propertyType = propertyType.GetElementType();
            if ((FieldType == FieldType.ENUM) && (!propertyType.IsEnum))
                throw new Exception("Unable to cast a field that is not of the type enum to use the ENUM field type.");
            if ((FieldType == FieldType.STRING) && (this.PrimaryKey) && (this.AutoGen))
                _fieldLength = 38;
		}
		
		internal InternalFieldMap(InternalFieldMap map,bool primary) : base(primary,map.Nullable,false,map.IsArray,map.ObjectType)
		{
			_fieldLength=map.FieldLength;
			_fieldName=map.FieldName;
			_fieldType=map.FieldType;
		}
		
		public override bool Equals(object obj)
		{
			if ((obj==null)||!(obj is InternalFieldMap))
				return false;
			InternalFieldMap ifm = (InternalFieldMap)obj;
			return base.Equals(obj)&&(ifm.FieldName==FieldName)&&(ifm.FieldLength==FieldLength)&&(ifm.FieldType==FieldType);
		}
		
		public InternalFieldMap(int fieldLength, string fieldName, FieldType fieldType,bool primaryKey, bool autogen, bool nullable,bool versionable,Type objectType) : base(primaryKey,autogen,nullable,versionable,objectType)
		{
			this._fieldLength = fieldLength;
			this._fieldName = fieldName;
			this._fieldType = fieldType;
		}
		
		public void CorrectName(ConnectionPool pool)
		{
			_fieldName=Utility.CorrectName(pool,_fieldName);
		}
		
		
		public int FieldLength{
			get{
				if ((_fieldLength==0)||(_fieldLength==int.MinValue))
					ExtractFieldLengthForType();
				return _fieldLength;
			}
		}
		
		public string FieldName{
			get{
				return _fieldName;
			}
		}
		
		public FieldType FieldType{
			get{
				return _fieldType;
			}
		}
		
		private void ExtractFieldLengthForType()
		{
			switch(FieldType)
			{
				case FieldType.BOOLEAN:
					_fieldLength=1;
					break;
				case FieldType.DATE:
				case FieldType.DATETIME:
				case FieldType.TIME:
				case FieldType.DOUBLE:
				case FieldType.FLOAT:
				case FieldType.LONG:
				case FieldType.MONEY:
					_fieldLength=8;
					break;
				case FieldType.INTEGER:
				case FieldType.ENUM:
					_fieldLength=4;
					break;
				case FieldType.SHORT:
					_fieldLength=2;
					break;
			}
		}
	}
}
