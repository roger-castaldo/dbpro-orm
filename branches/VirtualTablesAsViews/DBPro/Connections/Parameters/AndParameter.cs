/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 05/04/2009
 * Time: 7:56 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using System;
using System.Collections.Generic;

namespace Org.Reddragonit.Dbpro.Connections.Parameters
{
	/// <summary>
	/// Description of AndParameter.
	/// </summary>
	public class AndParameter : JoinParameter 
	{
        public AndParameter(SelectParameter[] parameters)
            : base(parameters)
		{
		}

        internal override List<string> Fields
        {
            get { 
                List<string> ret = new List<string>();
                foreach (SelectParameter par in Parameters)
                    ret.AddRange(par.Fields);
                return ret;
            }
        }
		
		protected override string JoinString {
			get {return "AND";}
		}
	}
}
