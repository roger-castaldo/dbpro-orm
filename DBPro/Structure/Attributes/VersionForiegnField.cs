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
	/// Description of VersionForiegnField.
	/// </summary>
	public class VersionForiegnField : ForiegnField,IVersionField  
	{
		
		private VersionField.VersionTypes _versionType=VersionField.VersionTypes.DATESTAMP;
		
		public VersionField.VersionTypes VersionType
		{
			get{return _versionType;}
		}
		
		public VersionForiegnField() : this(true,UpdateDeleteAction.NO_ACTION,UpdateDeleteAction.NO_ACTION)
		{
		}
		
		public VersionForiegnField(VersionField.VersionTypes VersionType) : this(true,UpdateDeleteAction.NO_ACTION,UpdateDeleteAction.NO_ACTION,VersionType)
		{
		}
		
		public VersionForiegnField(bool NullAble) : this(NullAble,UpdateDeleteAction.NO_ACTION,UpdateDeleteAction.NO_ACTION)
		{}

		public VersionForiegnField(bool NullAble,VersionField.VersionTypes VersionType) : this(NullAble,UpdateDeleteAction.NO_ACTION,UpdateDeleteAction.NO_ACTION,VersionType)
		{}
		
		public VersionForiegnField(UpdateDeleteAction OnUpdate,UpdateDeleteAction OnDelete) : this(true,OnUpdate,OnDelete)
		{}
		
		public VersionForiegnField(UpdateDeleteAction OnUpdate,UpdateDeleteAction OnDelete,VersionField.VersionTypes VersionType) : this(true,OnUpdate,OnDelete,VersionType)
		{}

		public VersionForiegnField(bool NullAble,UpdateDeleteAction OnUpdate,UpdateDeleteAction OnDelete) : this(NullAble,OnUpdate,OnDelete,VersionField.VersionTypes.DATESTAMP)
		{
		}
		
		public VersionForiegnField(bool NullAble,UpdateDeleteAction OnUpdate,UpdateDeleteAction OnDelete,VersionField.VersionTypes VersionType) : base(NullAble,OnUpdate,OnDelete)
		{
			_versionType = VersionType;
		}
	}
}
