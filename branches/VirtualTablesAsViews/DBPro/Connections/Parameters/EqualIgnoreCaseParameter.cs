using System;
using System.Collections.Generic;
using System.Text;

namespace Org.Reddragonit.Dbpro.Connections.Parameters
{
    public class EqualIgnoreCaseParameter: CompareParameter
	{
        public EqualIgnoreCaseParameter(string fieldName, string fieldValue)
            : base(fieldName, (fieldValue == null ? null : fieldValue.ToUpper()))
		{}
		
		protected override string ComparatorString {
			get {return "=";}
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
