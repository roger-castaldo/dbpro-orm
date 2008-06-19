using System;

namespace Org.Reddragonit.Dbpro.Structure.Attributes
{
	/// <summary>
	/// Summary description for IForiegnField.
	/// </summary>
	public interface IForiegnField : INullable
	{
		ForiegnField.UpdateDeleteAction OnUpdate
		{
			get;
		}

		ForiegnField.UpdateDeleteAction OnDelete
		{
			get;
		}
	}
}
