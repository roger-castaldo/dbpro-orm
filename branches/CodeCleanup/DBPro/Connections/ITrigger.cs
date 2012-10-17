using System;
using System.Collections.Generic;
using System.Text;
using Org.Reddragonit.Dbpro.Structure;
using Org.Reddragonit.Dbpro.Connections.Parameters;

namespace Org.Reddragonit.Dbpro.Connections
{
    public interface ITrigger
    {
        void PreInsert(Connection conn,Table table, out bool abort);
        void PreUpdate(Connection conn, Table originalValue, Table newValue, List<string> changedFields, out bool abort);
        void PreUpdate(Connection conn, Type tableType, Dictionary<string, object> updateFields, SelectParameter[] parameters, out bool abort);
        void PreDelete(Connection conn, Table table, out bool abort);
        void PreDelete(Connection conn, Type tableType, SelectParameter[] parameters, out bool abort);
        void PreDeleteAll(Connection conn, out bool abort);
        
        void PostDeleteAll(Connection conn);
        void PostInsert(Connection conn, Table table);
        void PostUpdate(Connection conn, Table originalValue, Table newValue, List<string> changedFields);
        void PostUpdate(Connection conn, Type tableType, Dictionary<string, object> updateFields, SelectParameter[] parameters);
        void PostDelete(Connection conn, Table table);
        void PostDelete(Connection conn, Type tableType, SelectParameter[] parameters);
    }
}
