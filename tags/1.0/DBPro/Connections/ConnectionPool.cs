/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 23/03/2008
 * Time: 8:58 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */

using System;
using System.Collections.Generic;
using System.Threading;

namespace Org.Reddragonit.Dbpro.Connections
{
	/// <summary>
	/// Description of ConnectionPool.
	/// </summary>
	public abstract class ConnectionPool
	{
		private List<Connection> locked=new List<Connection>();
		private Queue<Connection> unlocked=new Queue<Connection>();
		private Mutex mut = new Mutex(false);
		protected string connectionString;
		
		private int minPoolSize=0;
		private int maxPoolSize=0;
		private long maxKeepAlive=0;
		
		private bool isClosed=false;
		private bool isReady=false;
		
		protected abstract Connection CreateConnection();
		
		
		public ConnectionPool(string connectionString,int minPoolSize,int maxPoolSize,long maxKeepAlive)
		{
			this.connectionString=connectionString;
			this.minPoolSize=minPoolSize;
			this.maxPoolSize=maxPoolSize;
			this.maxKeepAlive=maxKeepAlive;
			for (int x=0;x<minPoolSize;x++)
				unlocked.Enqueue(CreateConnection());
			isReady=true;
		}
		
		public Connection getConnection()
		{
			if (isClosed)
				return null;
			Connection ret=null;
			while(true)
			{
				mut.WaitOne();
				if (unlocked.Count>0)
				{
					ret=unlocked.Dequeue();
					if (ret.isPastKeepAlive(maxKeepAlive))
					{
						ret.Disconnect();
						ret=null;
					}
					else
						break;
				}
				if (isClosed)
					break;
				if (!checkMin())
				{
					ret=CreateConnection();
					break;
				}
				mut.ReleaseMutex();
				try{
					Thread.Sleep(100);
				}catch (Exception e){}
			}
			if (ret!=null)
				locked.Add(ret);
			mut.ReleaseMutex();
			return ret;
		}
		
		public void ClosePool()
		{
			mut.WaitOne();
			while (unlocked.Count>0)
				unlocked.Dequeue().Disconnect();
			foreach (Connection conn in locked)
				conn.Disconnect();
			isClosed=true;
			mut.ReleaseMutex();
		}
		
		internal void returnConnection(Connection conn)
		{
			mut.WaitOne();
			locked.Remove(conn);
			mut.ReleaseMutex();
			if (!checkMax()&&!isClosed&&!conn.isPastKeepAlive(maxKeepAlive))
			{
				mut.WaitOne();
				unlocked.Enqueue(conn);
				mut.ReleaseMutex();
			}else
			{
				conn.Disconnect();
			}
		}
		
		private bool checkMax()
		{
			if (maxPoolSize<=0) return false;
			else return maxPoolSize>(locked.Count+unlocked.Count);
		}
		
		private bool checkMin()
		{
			if (minPoolSize<=0) return true;
			else return minPoolSize<(locked.Count+unlocked.Count);
		}
	}
}
