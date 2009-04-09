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
using Org.Reddragonit.Dbpro.Structure.Mapping;
using System.Data;

namespace Org.Reddragonit.Dbpro.Connections.Parameters
{
	public abstract class SelectParameter
	{
		internal abstract string ConstructString(TableMap map,Connection conn,QueryBuilder builder,ref List<IDbDataParameter> queryParameters,ref int parCount);
	}
}
