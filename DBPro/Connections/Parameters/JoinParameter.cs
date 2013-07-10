/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 05/04/2009
 * Time: 7:51 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using System;
using System.Collections.Generic;
using System.Data;
using Org.Reddragonit.Dbpro.Connections.PoolComponents;
using Org.Reddragonit.Dbpro.Virtual;

namespace Org.Reddragonit.Dbpro.Connections.Parameters
{
	/// <summary>
	/// Description of JoinParameter.
	/// </summary>
	public abstract class JoinParameter : SelectParameter 
	{
        private SelectParameter[] _parameters;
		
		public JoinParameter(SelectParameter[] parameters)
		{
            _parameters = parameters;
		}
		
		public SelectParameter[] Parameters{
			get{return _parameters;}
		}
		
		protected abstract string JoinString{
			get;
		}
		
		internal sealed override string ConstructString(Type tableType, ConnectionPool pool, QueryBuilder builder, ref List<IDbDataParameter> queryParameters, ref int parCount)
		{
            string ret = "( ";
            foreach (SelectParameter par in _parameters)
            {
                ret += " (" + par.ConstructString(tableType, pool, builder, ref queryParameters, ref parCount) + ") " + JoinString;
            }
            if (_parameters.Length > 0)
                return ret.Substring(0, ret.Length - JoinString.Length) + ")";
            else
                return "";
		}

        internal sealed override string ConstructClassViewString(ClassViewAttribute cva, ConnectionPool pool, QueryBuilder builder, ref List<IDbDataParameter> queryParameters, ref int parCount)
        {
            string ret = "( ";
            foreach (SelectParameter par in _parameters)
            {
                ret += " (" + par.ConstructClassViewString(cva,pool, builder, ref queryParameters, ref parCount) + ") " + JoinString;
            }
            if (_parameters.Length > 0)
                return ret.Substring(0, ret.Length - JoinString.Length) + ")";
            else
                return "";
        }
	}
}
