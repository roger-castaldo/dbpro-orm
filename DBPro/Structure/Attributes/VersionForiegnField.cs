/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 15/09/2008
 * Time: 10:35 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */

using System;

namespace Org.Reddragonit.Dbpro.Structure.Attributes
{
	/// <summary>
	/// Description of VersionForeignField.
	/// </summary>
	public class VersionForeignField : ForeignField,IVersionField  
	{
		
		private VersionField.VersionTypes _versionType=VersionField.VersionTypes.DATESTAMP;
		
		public VersionField.VersionTypes VersionType
		{
			get{return _versionType;}
		}
		
		public VersionForeignField() : this(true,UpdateDeleteAction.NO_ACTION,UpdateDeleteAction.NO_ACTION)
		{
		}
		
		public VersionForeignField(VersionField.VersionTypes VersionType) : this(true,UpdateDeleteAction.NO_ACTION,UpdateDeleteAction.NO_ACTION,VersionType)
		{
		}
		
		public VersionForeignField(bool NullAble) : this(NullAble,UpdateDeleteAction.NO_ACTION,UpdateDeleteAction.NO_ACTION)
		{}

		public VersionForeignField(bool NullAble,VersionField.VersionTypes VersionType) : this(NullAble,UpdateDeleteAction.NO_ACTION,UpdateDeleteAction.NO_ACTION,VersionType)
		{}
		
		public VersionForeignField(UpdateDeleteAction OnUpdate,UpdateDeleteAction OnDelete) : this(true,OnUpdate,OnDelete)
		{}
		
		public VersionForeignField(UpdateDeleteAction OnUpdate,UpdateDeleteAction OnDelete,VersionField.VersionTypes VersionType) : this(true,OnUpdate,OnDelete,VersionType)
		{}

		public VersionForeignField(bool NullAble,UpdateDeleteAction OnUpdate,UpdateDeleteAction OnDelete) : this(NullAble,OnUpdate,OnDelete,VersionField.VersionTypes.DATESTAMP)
		{
		}
		
		public VersionForeignField(bool NullAble,UpdateDeleteAction OnUpdate,UpdateDeleteAction OnDelete,VersionField.VersionTypes VersionType) : base(NullAble,OnUpdate,OnDelete)
		{
			_versionType = VersionType;
		}
	}
}
