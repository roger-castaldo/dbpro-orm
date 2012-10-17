using System;

namespace Org.Reddragonit.Dbpro.Structure.Attributes
{
	/// <summary>
	/// Summary description for IField.
	/// </summary>
	public interface IField : INullable
	{

		int Length
		{
			get;
		}

		FieldType Type
		{
			get;
		}

		string Name
		{
			get;
		}
		
	}
}
