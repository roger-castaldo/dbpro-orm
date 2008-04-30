using System;
using UpdateDeleteAction = Org.Reddragonit.Dbpro.Structure.Attributes.ForiegnField.UpdateDeleteAction;

namespace Org.Reddragonit.Dbpro.Structure.Attributes
{
	/// <summary>
	/// Summary description for ForiegnPrimaryKeyField.
	/// </summary>
	public class ForiegnPrimaryKeyField : ForiegnField,IPrimaryKeyField
	{
		public ForiegnPrimaryKeyField() : this(true,UpdateDeleteAction.NO_ACTION,UpdateDeleteAction.NO_ACTION)
		{
		}
		
		public ForiegnPrimaryKeyField(bool NullAble) : this(NullAble,UpdateDeleteAction.NO_ACTION,UpdateDeleteAction.NO_ACTION)
		{}


		public ForiegnPrimaryKeyField(bool NullAble,UpdateDeleteAction OnUpdate,UpdateDeleteAction OnDelete) : base(NullAble,OnUpdate,OnDelete)
		{
		}
		
		public bool AutoGen
		{
			get
			{
				return false;
			}
		}
	}
}
