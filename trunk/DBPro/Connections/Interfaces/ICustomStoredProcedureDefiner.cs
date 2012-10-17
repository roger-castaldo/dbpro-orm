using System;
using System.Collections.Generic;
using System.Text;

namespace Org.Reddragonit.Dbpro.Connections.Interfaces
{
    interface ICustomStoredProcedureDefiner
    {
        List<StoredProcedure> GetStoredProceduresForConnectionPool(ConnectionPool pool);
    }
}
