/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 07/09/2008
 * Time: 11:06 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */

using System;

namespace Org.Reddragonit.Dbpro.Structure.Attributes
{
	/// <summary>
	/// Description of VersionField.
	/// </summary>
	public class VersionField : Field,IVersionField 
	{
		public enum VersionTypes
		{
			NUMBER,
			DATESTAMP
		}
		
		private VersionTypes _versionType=VersionTypes.DATESTAMP;
		
		public VersionTypes VersionType
		{
			get{return _versionType;}
		}
		
		
		public VersionField()
		{
		}
		
		public VersionField(int fieldLength) : this(fieldLength,VersionTypes.DATESTAMP)
        {
        }
		
		public VersionField(int fieldLength,VersionTypes VersionType) :base(fieldLength,true)
        {
        }

		public VersionField(bool nullable) : this(nullable,VersionTypes.DATESTAMP)
        {
        }
        
        public VersionField(bool nullable,VersionTypes VersionType) : base(int.MinValue,nullable)
        {
        	_versionType=VersionType;
        }
		
		public VersionField(string FieldName,FieldType type):this(FieldName,type,true,0)
		{
		}
		
		public VersionField(string FieldName,FieldType type,VersionTypes VersionType):this(FieldName,type,true,0,VersionType)
		{
		}


		public VersionField(string FieldName,FieldType type,bool Nullable):this(FieldName,type,Nullable,0)
		{
		}
		
		public VersionField(string FieldName,FieldType type,bool Nullable,VersionTypes VersionType):this(FieldName,type,Nullable,0,VersionType)
		{
		}
		
		public VersionField(string FieldName,FieldType type,int fieldLength) :this(FieldName,type,true,fieldLength)
		{
		}
		
		public VersionField(string FieldName,FieldType type,int fieldLength,VersionTypes VersionType) :this(FieldName,type,true,fieldLength,VersionType)
		{
		}

		public VersionField(string FieldName,FieldType type,bool Nullable,int fieldLength):this(FieldName,type,Nullable,fieldLength,VersionTypes.DATESTAMP)
		{
		}
		
		public VersionField(string FieldName,FieldType type,bool Nullable,int fieldLength,VersionTypes VersionType):base(FieldName,type,Nullable,fieldLength)
		{
			_versionType=VersionType;
		}
	}
}
