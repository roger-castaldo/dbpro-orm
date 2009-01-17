using System;

namespace Org.Reddragonit.Dbpro.Structure.Attributes
{
	/// <summary>
	/// Summary description for IForeignField.
	/// </summary>
	public interface IForeignField : INullable
	{
		ForeignField.UpdateDeleteAction OnUpdate
		{
			get;
		}

		ForeignField.UpdateDeleteAction OnDelete
		{
			get;
		}
	}
}
