using System;
using System.Collections.Generic;
using System.Text;

namespace Org.Reddragonit.Dbpro.Connections.Parameters
{
    public class StartsWithParameter: CompareParameter
	{
		public StartsWithParameter(string fieldName,string fieldValue) : base(fieldName,fieldValue.ToString().ToUpper()+"%")
		{
		}
		
		protected override string ComparatorString {
			get {return "LIKE";}
		}

        protected override bool CaseInsensitive
        {
            get
            {
                return true;
            }
        }
	}
}
