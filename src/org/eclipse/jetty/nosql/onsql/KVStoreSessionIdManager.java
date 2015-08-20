package org.eclipse.jetty.nosql.onsql;


import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.SessionManager;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.session.AbstractSessionIdManager;
import org.eclipse.jetty.server.session.SessionHandler;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;
import org.eclipse.jetty.util.thread.ScheduledExecutorScheduler;
import org.eclipse.jetty.util.thread.Scheduler;

import oracle.kv.Depth;
import oracle.kv.Direction;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.KeyRange;
import oracle.kv.ValueVersion;

/**
 * Based partially on the JDBCSessionIdManager.
 *
 * Theory is that we really only need the session id manager for the local 
 * instance so we have something to scavenge on, namely the list of known ids
 * 
 * This class has a timer that runs a periodic scavenger thread to query
 *  for all id's known to this node whose precalculated expiry time has passed.
 *  
 * These found sessions are then run through the invalidateAll(id) method that 
 * is a bit hinky but is supposed to notify all handlers this id is now DOA and 
 * ought to be cleaned up.  this ought to result in a save operation on the session
 * that will change the valid field to false (this conjecture is unvalidated atm)
 */
public class KVStoreSessionIdManager extends AbstractSessionIdManager
{
    private final static Logger __log = Log.getLogger("org.eclipse.jetty.server.session");

    final static long __defaultScavengePeriod = 30 * 60 * 1000; // every 30 minutes
    
    private KVStore _kvstorehandler = null;
    private String _kvstorehosts = null;
    private String _kvstorename = null;
    protected Server _server;
    private Scheduler _scheduler;
    private boolean _ownScheduler;
    private Scheduler.Task _scavengerTask;
    private Scheduler.Task _purgerTask;
    
    private long _scavengePeriod = __defaultScavengePeriod;
    

    /** 
     * purge process is enabled by default
     */
    private boolean _purge = true;

    /**
     * purge process would run daily by default
     */
    private long _purgeDelay = 24 * 60 * 60 * 1000; // every day
    
    /**
     * how long do you want to persist sessions that are no longer
     * valid before removing them completely
     */
    private long _purgeInvalidAge = 24 * 60 * 60 * 1000; // default 1 day

    /**
     * how long do you want to leave sessions that are still valid before
     * assuming they are dead and removing them
     */
    private long _purgeValidAge = 7 * 24 * 60 * 60 * 1000; // default 1 week

    
    /**
     * the collection of session ids known to this manager
     * 
     * TODO consider if this ought to be concurrent or not
     */
    protected final Set<String> _sessionsIds = new HashSet<String>();
    
    
    /**
     * Scavenger
     *
     */
    protected class Scavenger implements Runnable
    {
        @Override
        public void run()
        {
            try
            {
                scavenge();
            }
            finally
            {
                if (_scheduler != null && _scheduler.isRunning())
                    _scavengerTask = _scheduler.schedule(this, _scavengePeriod, TimeUnit.MILLISECONDS);
            }
        } 
    }
    
    
    /**
     * Purger
     *
     */
    protected class Purger implements Runnable
    {
        @Override
        public void run()
        {
            try
            {
                purge();
            }
            finally
            {
                if (_scheduler != null && _scheduler.isRunning())
                    _purgerTask = _scheduler.schedule(this, _purgeDelay, TimeUnit.MILLISECONDS);
            }
        }
    }
    
    
    

    /* ------------------------------------------------------------ */
    public void setKvstorehosts(String hosts) {
    	_kvstorehosts = hosts;
    }
    public void setKvstorename(String kvstorename) {
    	_kvstorename = kvstorename;
    }
    public String getKvstorehosts() {
    	return _kvstorehosts;
    }
    public String getKvstorename() {
    	return _kvstorename;
    }

    public KVStoreSessionIdManager(Server server)
    {
        super(new Random());
        _server = server;
        __log.debug("inside KVStoreSessionIdManager constructor");
        
        /*  
        _sessions.ensureIndex(
                BasicDBObjectBuilder.start().add("id",1).get(),
                BasicDBObjectBuilder.start().add("unique",true).add("sparse",false).get());
        _sessions.ensureIndex(
                BasicDBObjectBuilder.start().add("id",1).add("version",1).get(),
                BasicDBObjectBuilder.start().add("unique",true).add("sparse",false).get());
         */       

    }
 
    /* ------------------------------------------------------------ */
    /**
     * Scavenge is a process that periodically checks the tracked session
     * ids of this given instance of the session id manager to see if they 
     * are past the point of expiration.
     */
    protected void scavenge()
    {
        String now = Base62Converter.longToLexiSortableBase62(System.currentTimeMillis());
        __log.debug("SessionIdManager:scavenge:at {}", now);        
        synchronized (_sessionsIds)
        {         
            /*
             * run a query returning results that:
             *  - are in the known list of sessionIds
             *  - the expiry time has passed
             *  
             *  we limit the query to return just the __ID so we are not sucking back full sessions
             *  for kvstore we going to use linkback with expiry time set as major key and session_id as minor
             *  also we need to split table per day to limit table partition size
             *  
             *  for table api indexes are being scanned using scatter-gather technique
             *  based on theory info from oracle.kv.impl.api.table.IndexScan class
             *  
             *  we going to execute scavenge on daily basic, what if we partition linkback 
             *  based on day info, i.e. major key will be day info from expiry date
             *  then minor keys would be expiry date, session id
             *  
             *  seems the scavenge is executed every 30 mins by default, so partitioning over day is not going to work
             */
        	Iterator<Key> expiry_iterator = _kvstorehandler.storeKeysIterator(Direction.UNORDERED, 1000, Key.createKey(
        			Arrays.asList(KVStoreSessionManager.__expirydailyindexprefix)),
        			new KeyRange(Base62Converter.longToLexiSortableBase62(0L),true,
        					Base62Converter.longToLexiSortableBase62(System.currentTimeMillis()),true),
        			Depth.PARENT_AND_DESCENDANTS);
        	while (expiry_iterator.hasNext()) {
        		String session_id = expiry_iterator.next().getMinorPath().get(0);
                __log.debug("SessionIdManager:scavenge: expiring session {}", session_id);
                expireAll(session_id);
        	}
        }      
    }
    
    /* ------------------------------------------------------------ */
    /**
     * ScavengeFully will expire all sessions. In most circumstances
     * you should never need to call this method.
     * 
     * <b>USE WITH CAUTION</b>
     */
    protected void scavengeFully()
    {        
        __log.debug("SessionIdManager:scavengeFully");

    	Iterator<Key> expiry_iterator = _kvstorehandler.storeKeysIterator(Direction.UNORDERED, 1000, Key.createKey(
    			Arrays.asList(KVStoreSessionManager.__expirydailyindexprefix)),
    			null,
    			Depth.PARENT_AND_DESCENDANTS);
    	while (expiry_iterator.hasNext()) {
    		String session_id = expiry_iterator.next().getMinorPath().get(0);           
            expireAll(session_id);
    	}

    }

    protected void purge()
    {
        __log.debug("PURGING");
        Iterator<Key> purge_iterator = _kvstorehandler.storeKeysIterator(Direction.UNORDERED, 1000,
        							  Key.createKey(Arrays.asList(KVStoreSessionManager.__purgeindexprefix,
        									  	    "0")),new KeyRange(Base62Converter.longToLexiSortableBase62(0L),true,
        									  	    				  Base62Converter.longToLexiSortableBase62(System.currentTimeMillis() - _purgeInvalidAge),true
        																		),Depth.PARENT_AND_DESCENDANTS);
        while (purge_iterator.hasNext()) {
        	Key purge_key = purge_iterator.next();
        	String session_id = purge_key.getMinorPath().get(0);
        	ValueVersion vval_expiry = _kvstorehandler.get(Key.createKey(Arrays.asList(
					KVStoreSessionManager.__storeprefix,
					session_id),Arrays.asList(KVStoreSessionManager.__EXPIRY)));
        	__log.debug("KVStoreSessionIdManager:purging invalid session {}", session_id);        	
        	if (vval_expiry != null && vval_expiry.getValue() != null && vval_expiry.getValue().getValue() != null) {
        		__log.debug("purge expiry index for session id = {}",session_id);
        		try {
					String old_expiry = new String(vval_expiry.getValue().getValue(),"UTF-8");
					_kvstorehandler.delete(Key.createKey(Arrays.asList(KVStoreSessionManager.__expirydailyindexprefix,old_expiry),
														 Arrays.asList(session_id)));
				} catch (UnsupportedEncodingException e) {
					__log.debug("UnsupportedEncoding exception",e);
				}
        	} else __log.debug("no expiry value for session {} found, nothing to purge", session_id);
        	_kvstorehandler.delete(purge_key);
        	_kvstorehandler.multiDelete(Key.createKey(Arrays.asList(
					KVStoreSessionManager.__storeprefix,
					session_id)),null,Depth.PARENT_AND_DESCENDANTS);
        }
        if (_purgeValidAge != 0)
        {
            Iterator<Key> purge_iterator2 = _kvstorehandler.storeKeysIterator(Direction.UNORDERED, 1000,
					  Key.createKey(KVStoreSessionManager.__purgeindexprefix,
							  	    "1"),new KeyRange(Base62Converter.longToLexiSortableBase62(0L),true,
							  	    				  Base62Converter.longToLexiSortableBase62(System.currentTimeMillis() - _purgeValidAge),true
																),Depth.PARENT_AND_DESCENDANTS);

            while (purge_iterator2.hasNext()) {
            	Key purge_key = purge_iterator2.next();
            	String session_id = purge_key.getMinorPath().get(0);
            	__log.debug("KVStoreSessionIdManager:purging outdated valid session {}", session_id);
            	ValueVersion vval_expiry = _kvstorehandler.get(Key.createKey(Arrays.asList(
    					KVStoreSessionManager.__storeprefix,
    					session_id),Arrays.asList(KVStoreSessionManager.__EXPIRY)));
            	if (vval_expiry != null && vval_expiry.getValue() != null && vval_expiry.getValue().getValue() != null) {
            		try {
    					String old_expiry = new String(vval_expiry.getValue().getValue(),"UTF-8");
    					_kvstorehandler.delete(Key.createKey(Arrays.asList(KVStoreSessionManager.__expirydailyindexprefix,old_expiry),
    														 Arrays.asList(session_id)));
    				} catch (UnsupportedEncodingException e) {
    					__log.debug("UnsupportedEncoding exception",e);
    				}
            	}
            	_kvstorehandler.delete(purge_key);
            	_kvstorehandler.multiDelete(Key.createKey(Arrays.asList(
    					KVStoreSessionManager.__storeprefix,
    					session_id)),null,Depth.PARENT_AND_DESCENDANTS);
            }    
        }

    }
    
    protected void purgeFully()
    {
    	Iterator<Key> purge_iterator = _kvstorehandler.storeKeysIterator(Direction.UNORDERED, 1000,
    			Key.createKey(KVStoreSessionManager.__purgeindexprefix,
    					"0"),null,Depth.PARENT_AND_DESCENDANTS);
    	while (purge_iterator.hasNext()) {
    		Key purge_key = purge_iterator.next();
    		String session_id = purge_key.getMinorPath().get(0);
    		__log.debug("KVStoreSessionIdManager:purging invalid session {}", session_id);
    		_kvstorehandler.multiDelete(Key.createKey(Arrays.asList(
    				KVStoreSessionManager.__storeprefix,
    				session_id)),null,Depth.PARENT_AND_DESCENDANTS);
    		_kvstorehandler.delete(purge_key);
    		ValueVersion vval_expiry = _kvstorehandler.get(Key.createKey(Arrays.asList(
    				KVStoreSessionManager.__storeprefix,
    				session_id),Arrays.asList(KVStoreSessionManager.__EXPIRY)));
    		if (vval_expiry != null && vval_expiry.getValue() != null && vval_expiry.getValue().getValue() != null) {
    			try {
    				String old_expiry = new String(vval_expiry.getValue().getValue(),"UTF-8");
    				_kvstorehandler.delete(Key.createKey(Arrays.asList(KVStoreSessionManager.__expirydailyindexprefix,old_expiry),
    						Arrays.asList(session_id)));
    			} catch (UnsupportedEncodingException e) {
    				__log.debug("UnsupportedEncoding exception",e);
    			}
    		}
    	}

    }
    
    
    /* ------------------------------------------------------------ */
    public KVStore getKVStore()
    {
        return this._kvstorehandler;
    }
    
    
    /* ------------------------------------------------------------ */
    public boolean isPurgeEnabled()
    {
        return _purge;
    }
    
    /* ------------------------------------------------------------ */
    public void setPurge(boolean purge)
    {
        this._purge = purge;
    }


    /* ------------------------------------------------------------ */
    /** 
     * The period in seconds between scavenge checks.
     * 
     * @param scavengePeriod
     */
    public void setScavengePeriod(long scavengePeriod)
    {
        if (scavengePeriod <= 0)
            _scavengePeriod = __defaultScavengePeriod;
        else
            _scavengePeriod = TimeUnit.SECONDS.toMillis(scavengePeriod);
    }

    /* ------------------------------------------------------------ */
    public void setPurgeDelay(long purgeDelay)
    {
        if ( isRunning() )
        {
            throw new IllegalStateException();
        }
        
        this._purgeDelay = purgeDelay;
    }
 
    /* ------------------------------------------------------------ */
    public long getPurgeInvalidAge()
    {
        return _purgeInvalidAge;
    }

    /* ------------------------------------------------------------ */
    /**
     * sets how old a session is to be persisted past the point it is
     * no longer valid
     */
    public void setPurgeInvalidAge(long purgeValidAge)
    {
        this._purgeInvalidAge = purgeValidAge;
    } 
    
    /* ------------------------------------------------------------ */
    public long getPurgeValidAge()
    {
        return _purgeValidAge;
    }

    /* ------------------------------------------------------------ */
    /**
     * sets how old a session is to be persist past the point it is 
     * considered no longer viable and should be removed
     * 
     * NOTE: set this value to 0 to disable purging of valid sessions
     */
    public void setPurgeValidAge(long purgeValidAge)
    {
        this._purgeValidAge = purgeValidAge;
    } 

    /* ------------------------------------------------------------ */
    @Override
    protected void doStart() throws Exception
    {
        __log.debug("KVStoreSessionIdManager:starting");

        synchronized (this)
        {
        	String[] hhosts = _kvstorehosts.split(",");
            KVStoreConfig kconfig = new KVStoreConfig(_kvstorename, hhosts);
            KVStore kvstore = KVStoreFactory.getStore(kconfig);
            if (kvstore == null) throw new IllegalStateException("cannot connect to kvstore, hosts="+_kvstorehosts+";storename="+_kvstorename);
            else __log.debug("succesfully connected to the kvstore instance");
            this._kvstorehandler = kvstore;
            //try and use a common scheduler, fallback to own
            _scheduler =_server.getBean(Scheduler.class);
            if (_scheduler == null)
            {
                _scheduler = new ScheduledExecutorScheduler();
                _ownScheduler = true;
                _scheduler.start();
            }   
            else if (!_scheduler.isStarted())
                throw new IllegalStateException("Shared scheduler not started");
            

            //setup the scavenger thread
            if (_scavengePeriod > 0)
            {
                if (_scavengerTask != null)
                {
                    _scavengerTask.cancel();
                    _scavengerTask = null;
                }

                _scavengerTask = _scheduler.schedule(new Scavenger(), _scavengePeriod, TimeUnit.MILLISECONDS);
            }
            else if (__log.isDebugEnabled())
                __log.debug("Scavenger disabled");


            //if purging is enabled, setup the purge thread
            if ( _purge )
            { 
                if (_purgerTask != null)
                {
                    _purgerTask.cancel();
                    _purgerTask = null;
                }
                _purgerTask = _scheduler.schedule(new Purger(), _purgeDelay, TimeUnit.MILLISECONDS);
            }
            else if (__log.isDebugEnabled())
                __log.debug("Purger disabled");
        }
    }

    /* ------------------------------------------------------------ */
    @Override
    protected void doStop() throws Exception
    {
        synchronized (this)
        {
            if (_scavengerTask != null)
            {
                _scavengerTask.cancel();
                _scavengerTask = null;
            }
 
            if (_purgerTask != null)
            {
                _purgerTask.cancel();
                _purgerTask = null;
            }
            
            if (_ownScheduler && _scheduler != null)
            {
                _scheduler.stop();
                _scheduler = null;
            }
            if (_kvstorehandler != null) {
            	_kvstorehandler.close();
            }
        }
        super.doStop(); 
    }

    /* ------------------------------------------------------------ */
    /**
     * Searches database to find if the session id known to kvstore, and is it valid
     */
    @Override
    public boolean idInUse(String sessionId)
    {        
        /*
         * optimize this query to only return the valid variable
         */
    	ValueVersion vver_valid = _kvstorehandler.get(Key.createKey(Arrays.asList(
				KVStoreSessionManager.__storeprefix,
				sessionId),Arrays.asList(KVStoreSessionManager.__VALID)));
    	try {
			if (vver_valid != null && vver_valid.getValue() != null && vver_valid.getValue().getValue() != null
				&& "1".equals(new String(vver_valid.getValue().getValue(),"UTF-8")))
				return true;
			
		} catch (UnsupportedEncodingException e) {
			__log.debug("KVStoreSessionIdManager:idInUse :: error {}", sessionId,e);
		}
    	return false;
    }

    /* ------------------------------------------------------------ */
    @Override
    public void addSession(HttpSession session)
    {
        if (session == null)
        {
            return;
        }
        
        /*
         * already a part of the index in kvstore 
         */
        
        __log.debug("KVStoreSessionIdManager:addSession {}", session.getId());
        
        synchronized (_sessionsIds)
        {
            _sessionsIds.add(session.getId());
        }
        
    }

    /* ------------------------------------------------------------ */
    @Override
    public void removeSession(HttpSession session)
    {
        if (session == null)
        {
            return;
        }
        
        synchronized (_sessionsIds)
        {
            _sessionsIds.remove(session.getId());
        }
    }

    /* ------------------------------------------------------------ */
    /** Remove the session id from the list of in-use sessions.
     * Inform all other known contexts that sessions with the same id should be
     * invalidated.
     * @see org.eclipse.jetty.server.SessionIdManager#invalidateAll(java.lang.String)
     */
    @Override
    public void invalidateAll(String sessionId)
    {
        synchronized (_sessionsIds)
        {
            _sessionsIds.remove(sessionId);
                
            //tell all contexts that may have a session object with this id to
            //get rid of them
            Handler[] contexts = _server.getChildHandlersByClass(ContextHandler.class);
            for (int i=0; contexts!=null && i<contexts.length; i++)
            {
                SessionHandler sessionHandler = ((ContextHandler)contexts[i]).getChildHandlerByClass(SessionHandler.class);
                if (sessionHandler != null) 
                {
                    SessionManager manager = sessionHandler.getSessionManager();

                    if (manager != null && manager instanceof KVStoreSessionManager)
                    {
                        ((KVStoreSessionManager)manager).invalidateSession(sessionId);
                    }
                }
            }
        }      
    } 

    /* ------------------------------------------------------------ */
    /**
     * Expire this session for all contexts that are sharing the session 
     * id.
     * @param sessionId
     */
    public void expireAll (String sessionId)
    {
        synchronized (_sessionsIds)
        {
            _sessionsIds.remove(sessionId);
            
            
            //tell all contexts that may have a session object with this id to
            //get rid of them
            Handler[] contexts = _server.getChildHandlersByClass(ContextHandler.class);
            for (int i=0; contexts!=null && i<contexts.length; i++)
            {
                SessionHandler sessionHandler = ((ContextHandler)contexts[i]).getChildHandlerByClass(SessionHandler.class);
                if (sessionHandler != null) 
                {
                    SessionManager manager = sessionHandler.getSessionManager();

                    if (manager != null && manager instanceof KVStoreSessionManager)
                    {
                        ((KVStoreSessionManager)manager).expire(sessionId);
                    }
                }
            }
        }      
    }
    
    /* ------------------------------------------------------------ */
    @Override
    public void renewSessionId(String oldClusterId, String oldNodeId, HttpServletRequest request)
    {
        //generate a new id
        String newClusterId = newSessionId(request.hashCode());

        synchronized (_sessionsIds)
        {
            _sessionsIds.remove(oldClusterId);//remove the old one from the list
            _sessionsIds.add(newClusterId); //add in the new session id to the list

            //tell all contexts to update the id 
            Handler[] contexts = _server.getChildHandlersByClass(ContextHandler.class);
            for (int i=0; contexts!=null && i<contexts.length; i++)
            {
                SessionHandler sessionHandler = ((ContextHandler)contexts[i]).getChildHandlerByClass(SessionHandler.class);
                if (sessionHandler != null) 
                {
                    SessionManager manager = sessionHandler.getSessionManager();

                    if (manager != null && manager instanceof KVStoreSessionManager)
                    {
                        ((KVStoreSessionManager)manager).renewSessionId(oldClusterId, oldNodeId, newClusterId, getNodeId(newClusterId, request));
                    }
                }
            }
        }
    }

}
