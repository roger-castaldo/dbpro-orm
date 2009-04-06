/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 05/04/2009
 * Time: 7:51 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using System;
using Org.Reddragonit.Dbpro.Structure.Mapping;
using System.Collections.Generic;
using System.Data;

namespace Org.Reddragonit.Dbpro.Connections.Parameters
{
	/// <summary>
	/// Description of JoinParameter.
	/// </summary>
	public abstract class JoinParameter : SelectParameter 
	{
		private SelectParameter _leftPar;
		private SelectParameter _rightPar;
		
		public JoinParameter(SelectParameter leftPar,SelectParameter rightPar)
		{
			_rightPar=rightPar;
			_leftPar=leftPar;
		}
		
		public SelectParameter RightPar{
			get{return _rightPar;}
		}
		
		public SelectParameter LeftPar{
			get{return _leftPar;}
		}
		
		protected abstract string JoinString{
			get;
		}
		
		internal sealed override string ConstructString(TableMap map, Connection conn, QueryBuilder builder, ref List<IDbDataParameter> queryParameters, ref int parCount)
		{
			return "( ("+LeftPar.ConstructString(map,conn,builder,ref queryParameters,ref parCount)+" ) "+JoinString+" ( "+RightPar.ConstructString(map,conn,builder,ref queryParameters,ref parCount)+" ) )";
		}
	}
}
