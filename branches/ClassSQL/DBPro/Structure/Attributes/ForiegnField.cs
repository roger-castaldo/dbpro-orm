using System;
using System.Reflection;

namespace Org.Reddragonit.Dbpro.Structure.Attributes
{
	/// <summary>
	/// Summary description for ForiegnField.
	/// </summary>
	[AttributeUsage(AttributeTargets.Property)]
	public class ForiegnField : Attribute,IForiegnField
	{
		public enum UpdateDeleteAction
		{
			CASCADE,
			NO_ACTION,
			SET_NULL,
			SET_DEFAULT
		};

		private UpdateDeleteAction _onUpdate=UpdateDeleteAction.NO_ACTION;
		private UpdateDeleteAction _onDelete=UpdateDeleteAction.NO_ACTION;
		private bool _nullable;

		public ForiegnField() : this(true,UpdateDeleteAction.NO_ACTION,UpdateDeleteAction.NO_ACTION)
		{
		}
		
		public ForiegnField(bool NullAble) : this(NullAble,UpdateDeleteAction.NO_ACTION,UpdateDeleteAction.NO_ACTION)
		{}

		public ForiegnField(UpdateDeleteAction OnUpdate,UpdateDeleteAction OnDelete) : this(true,OnUpdate,OnDelete)
		{}

		public ForiegnField(bool NullAble,UpdateDeleteAction OnUpdate,UpdateDeleteAction OnDelete)
		{
			_onDelete=OnDelete;
			_onUpdate=OnUpdate;
			_nullable=Nullable;
		}

		public UpdateDeleteAction OnUpdate
		{
			get
			{
				return _onUpdate;
			}
		}

		public UpdateDeleteAction OnDelete
		{
			get
			{
				return _onDelete;
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
