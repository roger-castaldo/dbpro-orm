using System;
using UpdateDeleteAction = Org.Reddragonit.Dbpro.Structure.Attributes.ForiegnField.UpdateDeleteAction;

namespace Org.Reddragonit.Dbpro.Structure.Attributes
{
	/// <summary>
	/// Summary description for ForiegnPrimaryKeyField.
	/// </summary>
	public class ForiegnPrimaryKeyField : ForiegnField,IPrimaryKeyField
	{
		public ForiegnPrimaryKeyField() : this(UpdateDeleteAction.NO_ACTION,UpdateDeleteAction.NO_ACTION)
		{
		}


		public ForiegnPrimaryKeyField(UpdateDeleteAction OnUpdate,UpdateDeleteAction OnDelete) : base(false,OnUpdate,OnDelete)
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
