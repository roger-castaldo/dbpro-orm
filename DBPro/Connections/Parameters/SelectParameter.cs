/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 05/04/2009
 * Time: 6:10 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using System;
using System.Collections.Generic;
using System.Data;
using Org.Reddragonit.Dbpro.Connections.PoolComponents;

namespace Org.Reddragonit.Dbpro.Connections.Parameters
{
	public abstract class SelectParameter
	{
        internal abstract List<string> Fields
        {
            get;
        }
		internal abstract string ConstructString(Type tableType,Connection conn,QueryBuilder builder,ref List<IDbDataParameter> queryParameters,ref int parCount);
        internal abstract string ConstructVirtualTableString(sTable tbl, Connection conn,QueryBuilder builder, ref List<IDbDataParameter> queryParameters, ref int parCount);
	}
}
