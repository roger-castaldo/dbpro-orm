using System;

namespace Org.Reddragonit.Dbpro.Structure.Attributes
{
	/// <summary>
	/// Summary description for IPrimaryKeyField.
	/// </summary>
	public interface IPrimaryKeyField : INullable
	{

		bool AutoGen
		{
			get;
		}
	}
}
