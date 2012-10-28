using System;
using System.Collections.Generic;
using System.Text;

namespace Org.Reddragonit.Dbpro.Connections.Interfaces
{
    public interface ICustomViewDefiner
    {
        List<View> GetViewsForConnectionPool(ConnectionPool pool);
    }
}
