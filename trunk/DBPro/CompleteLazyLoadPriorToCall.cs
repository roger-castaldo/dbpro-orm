using System;
using System.Collections.Generic;
using System.Text;

namespace Org.Reddragonit.Dbpro
{
    //this attribute is simply to mark methos and properties in a class
    //to force the lazy loading to compelte when they are called.
    [AttributeUsage(AttributeTargets.Property|AttributeTargets.Method)]
    public class CompleteLazyLoadPriorToCall : Attribute
    {
    }
}
