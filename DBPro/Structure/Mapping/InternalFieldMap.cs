/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 21/03/2008
 * Time: 11:33 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */

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
		private Field.FieldType _fieldType;
				
		public InternalFieldMap(MemberInfo info) : base(info)
		{
			foreach (object obj in info.GetCustomAttributes(true))
			{
				if (obj is IField)
				{
					IField f = (IField)obj;
					_fieldLength=f.Length;
					_fieldName=f.Name;
					_fieldType=f.Type;
				}
				if (obj is VersionField)
				{
					_versionable=true;
				}
			}
		}
		
		internal InternalFieldMap(InternalFieldMap map,bool primary) : base(primary,map.Nullable,false)
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
		
		public InternalFieldMap(int fieldLength, string fieldName, Field.FieldType fieldType,bool primaryKey, bool autogen, bool nullable,bool versionable) : base(primaryKey,autogen,nullable,versionable)
		{
			this._fieldLength = fieldLength;
			this._fieldName = fieldName;
			this._fieldType = fieldType;
		}
		
		
		public int FieldLength{
			get{
				return _fieldLength;
			}
		}
		
		public string FieldName{
			get{
				return _fieldName;
			}
		}
		
		public Field.FieldType FieldType{
			get{
				return _fieldType;
			}
		}
	}
}
