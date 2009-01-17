using System;
using UpdateDeleteAction = Org.Reddragonit.Dbpro.Structure.Attributes.ForeignField.UpdateDeleteAction;

namespace Org.Reddragonit.Dbpro.Structure.Attributes
{
	/// <summary>
	/// Summary description for ForeignPrimaryKeyField.
	/// </summary>
	public class ForeignPrimaryKeyField : ForeignField,IPrimaryKeyField
	{
		public ForeignPrimaryKeyField() : this(UpdateDeleteAction.NO_ACTION,UpdateDeleteAction.NO_ACTION)
		{
		}


		public ForeignPrimaryKeyField(UpdateDeleteAction OnUpdate,UpdateDeleteAction OnDelete) : base(false,OnUpdate,OnDelete)
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
