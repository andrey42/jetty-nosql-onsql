package org.eclipse.jetty.nosql.onsql;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;

import org.eclipse.jetty.nosql.NoSqlSession;
import org.eclipse.jetty.nosql.NoSqlSessionManager;
import org.eclipse.jetty.server.SessionIdManager;
import org.eclipse.jetty.util.annotation.ManagedOperation;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;

import oracle.kv.Depth;
import oracle.kv.Direction;
import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.KeyRange;
import oracle.kv.Operation;
import oracle.kv.OperationExecutionException;
import oracle.kv.OperationFactory;
import oracle.kv.Value;
import oracle.kv.ValueVersion;

public class KVStoreSessionManager extends NoSqlSessionManager
{
	private static final Logger LOG = Log.getLogger(KVStoreSessionManager.class);

	private final static Logger __log = Log.getLogger("org.eclipse.jetty.server.session");

	/*
	 * strings used as keys or parts of keys 
	 */


	public final static String __storeprefix = "jettysessions";
	public final static String __expirydailyindexprefix = "jettyexpireindex";
	public final static String __purgeindexprefix = "jettypurgeindex";

	/**
	 * Session id
	 */
	public final static String __ID = "id";

	/**
	 * Time of session creation
	 */
	private final static String __CREATED = "created";

	/**
	 * Whether or not session is valid
	 */
	public final static String __VALID = "valid";

	/**
	 * Time at which session was invalidated
	 */
	public final static String __INVALIDATED = "invalidated";

	/**
	 * Last access time of session
	 */
	public final static String __ACCESSED = "accessed";

	/**
	 * Time this session will expire, based on last access time and maxIdle
	 */
	public final static String __EXPIRY = "expiry";

	/**
	 * The max idle time of a session (smallest value across all contexts which has a session with the same id)
	 */
	public final static String __MAX_IDLE = "maxIdle";

	/**
	 * Name of nested document field containing 1 sub document per context for which the session id is in use
	 */
	private final static String __CONTEXT = "context";   


	/**
	 * Special attribute per session per context, incremented each time attributes are modified
	 */
	public final static String __VERSION = "_version_";

	/**
	 * the context id is only set when this class has been started
	 */
	private String _contextId = null;


	/**
	 * Access to KVStore
	 */
	private KVStore _kvstorehandler;    
	private List<Operation> kvstore_object_ops = null;
	private OperationFactory kvstore_opfactory = null;
	private KVStoreSessionIdManager _session_id_manager = null; 

	/* ------------------------------------------------------------ */
	public KVStoreSessionManager(Object mgr)
	{
		this._session_id_manager = (KVStoreSessionIdManager)mgr;
	}



	/*------------------------------------------------------------ */
	@Override
	public void doStart() throws Exception
	{
		super.doStart();
		__log.debug("KVStoreSessionManager::doStart method");
		String kvstorename = this._session_id_manager.getKvstorename();
		if (kvstorename == null) throw new IllegalStateException("kvstore name is null");
		String kvstorehosts = this._session_id_manager.getKvstorehosts();
		if (kvstorehosts == null) throw new IllegalStateException("kvstore hosts list is null");
		this._kvstorehandler = this._session_id_manager.getKVStore();
		String[] hhosts = this._session_id_manager.getKvstorehosts().split(",");
		KVStoreConfig kconfig = new KVStoreConfig(this._session_id_manager.getKvstorename(), hhosts);
		KVStore kvstore = KVStoreFactory.getStore(kconfig);
		if (kvstore == null) throw new IllegalStateException("cannot connect to kvstore, hosts="+kvstorehosts+";storename="+kvstorename);
		else __log.debug("succesfully connected to the kvstore instance");
		this._kvstorehandler = kvstore;
		

		if (this._kvstorehandler == null) throw new IllegalStateException("kvstore handler passed from session id manager is null");
		String[] hosts = getContextHandler().getVirtualHosts();

		if (hosts == null || hosts.length == 0)
			hosts = new String[]
					{ "::" }; // IPv6 equiv of 0.0.0.0

		String contextPath = getContext().getContextPath();
		if (contextPath == null || "".equals(contextPath))
		{
			contextPath = "*";
		}

		_contextId = createContextId(hosts,contextPath);        
		this.kvstore_object_ops = new ArrayList<Operation>();        
	}

	/*
    @Override
    public void setSessionIdManager(SessionIdManager mgr)
    {      

        super.setSessionIdManager(mgr);
        __log.warn("session mgr instance class name = "+mgr.getClass().getCanonicalName());
        this._session_id_manager = (org.eclipse.jetty.nosql.onsql.KVStoreSessionIdManager) mgr;
    }
	 */

	/* ------------------------------------------------------------ */
	@Override
	protected synchronized Object save(NoSqlSession session, Object version, boolean activateAfterSave)
	{
		try
		{
			__log.debug("KVStoreSessionManager:save session {}", session.getClusterId());
			session.willPassivate();            
			this.kvstore_object_ops.clear();
			if (this.kvstore_opfactory == null) this.kvstore_opfactory = this._kvstorehandler.getOperationFactory(); 
			/*
            // Form query for upsert
            BasicDBObject key = new BasicDBObject(__ID,session.getClusterId());

            // Form updates
            BasicDBObject update = new BasicDBObject();

            BasicDBObject sets = new BasicDBObject();
            BasicDBObject unsets = new BasicDBObject();
			 */
			boolean upsert = false;
			boolean need_to_purge_attributes = false;
			// handle valid or invalid
			String new_valid = null;
			String new_expiry = null;
			String new_accessed = Base62Converter.longToLexiSortableBase62(session.getAccessed());
			if (session.isValid())
			{
				new_valid = "1";
				long expiry = (session.getMaxInactiveInterval() > 0?(session.getAccessed()+(1000L*getMaxInactiveInterval())):0);
				new_expiry = Base62Converter.longToLexiSortableBase62(expiry);
				__log.debug("KVStoreSessionManager: calculated expiry {} for session {}", expiry, session.getId());
				// handle new or existing
				if (version == null)
				{
					__log.debug("New session"); 
					upsert = true;
					version = new Long(1);
					kvstore_object_ops.add(this.kvstore_opfactory.createPut(
							Key.createKey(Arrays.asList(
									__storeprefix,
									session.getClusterId()),
									Arrays.asList(__CREATED)
									),Value.createValue(Base62Converter.fromBase10(session.getCreationTime()).getBytes("UTF-8"))));					
					kvstore_object_ops.add(this.kvstore_opfactory.createPut(
							Key.createKey(Arrays.asList(
									__storeprefix,
									session.getClusterId()),
									Arrays.asList(__VALID)
									),Value.createValue(new_valid.getBytes("UTF-8"))));                    
					kvstore_object_ops.add(this.kvstore_opfactory.createPut(
							Key.createKey(Arrays.asList(
									__storeprefix,
									session.getClusterId()),
									Arrays.asList(__CONTEXT, _contextId,__VERSION)
									),Value.createValue(Base62Converter.fromBase10(((Long)version).longValue()).getBytes("UTF-8"))));
					kvstore_object_ops.add(this.kvstore_opfactory.createPut(
							Key.createKey(Arrays.asList(
									__storeprefix,
									session.getClusterId()),
									Arrays.asList(__MAX_IDLE)
									),Value.createValue(Base62Converter.fromBase10(getMaxInactiveInterval()).getBytes("UTF-8"))));					
					kvstore_object_ops.add(this.kvstore_opfactory.createPut(
							Key.createKey(Arrays.asList(
									__storeprefix,
									session.getClusterId()),
									Arrays.asList(__EXPIRY)
									),Value.createValue(new_expiry.getBytes("UTF-8"))));
				}
				else
				{
					version = new Long(((Number)version).longValue() + 1);
					kvstore_object_ops.add(this.kvstore_opfactory.createPut(
							Key.createKey(Arrays.asList(
									__storeprefix,
									session.getClusterId()),
									Arrays.asList(__CONTEXT, _contextId,__VERSION)
									),Value.createValue(Base62Converter.fromBase10(((Long)version).longValue()).getBytes("UTF-8"))));

					//if max idle time and/or expiry is smaller for this context, then choose that for the whole session doc
					ValueVersion vval_max_idle = this._kvstorehandler.get(Key.createKey(Arrays.asList(
							__storeprefix,
							session.getClusterId()),
							Arrays.asList(__MAX_IDLE)
							));
					ValueVersion vval_expiry = this._kvstorehandler.get(Key.createKey(Arrays.asList(
							__storeprefix,
							session.getClusterId()),
							Arrays.asList(__EXPIRY)
							));
					if (vval_max_idle != null && vval_max_idle.getValue().getValue() != null) {
						long currentMaxIdle = Base62Converter.toBase10(new String(vval_max_idle.getValue().getValue(),"UTF-8"));
						if (getMaxInactiveInterval() > 0 && getMaxInactiveInterval() < currentMaxIdle)
							kvstore_object_ops.add(this.kvstore_opfactory.createPut(
									Key.createKey(Arrays.asList(
											__storeprefix,
											session.getClusterId()),
											Arrays.asList(__MAX_IDLE)
											),Value.createValue(Base62Converter.fromBase10(getMaxInactiveInterval()).getBytes("UTF-8"))));
					}

					if (vval_expiry != null && vval_expiry.getValue().getValue() != null) {
						long currentExpiry = Base62Converter.toBase10(new String(vval_expiry.getValue().getValue(),"UTF-8"));
						if (expiry > 0 && expiry != currentExpiry) {
							new_expiry = Base62Converter.longToLexiSortableBase62(expiry);
							kvstore_object_ops.add(this.kvstore_opfactory.createPut(
									Key.createKey(Arrays.asList(
											__storeprefix,
											session.getClusterId()),
											Arrays.asList(__EXPIRY)
											),Value.createValue(new_expiry.getBytes("UTF-8"))));
						}
					}
				}                
				kvstore_object_ops.add(this.kvstore_opfactory.createPut(
						Key.createKey(Arrays.asList(
								__storeprefix,
								session.getClusterId()),
								Arrays.asList(__ACCESSED)
								),Value.createValue(new_accessed.getBytes("UTF-8"))));

				__log.debug("session.isDirty="+session.isDirty()+";getSavePeriod="+this.getSavePeriod());
				
				Set<String> names = session.takeDirty();
				if (isSaveAllAttributes() || upsert)
				{
					names.addAll(session.getNames()); // note dirty may include removed names
				}

				for (String name : names)
				{
					Object value = session.getAttribute(name);
					if (value == null)
						kvstore_object_ops.add(this.kvstore_opfactory.createDelete(
								Key.createKey(Arrays.asList(
										__storeprefix,
										session.getClusterId()),
										Arrays.asList(__CONTEXT,_contextId,name)
										)));
					else
						kvstore_object_ops.add(this.kvstore_opfactory.createPut(
								Key.createKey(Arrays.asList(
										__storeprefix,
										session.getClusterId()),
										Arrays.asList(__CONTEXT,_contextId,name)
										),                        		
								Value.createValue(encodeValue(value))));
				}
			}
			else
			{
				need_to_purge_attributes = true;
				new_valid = "0";
				kvstore_object_ops.add(this.kvstore_opfactory.createPut(
						Key.createKey(Arrays.asList(
								__storeprefix,
								session.getClusterId()),
								Arrays.asList(__VALID)
								),Value.createValue(new_valid.getBytes("UTF-8"))));
				kvstore_object_ops.add(this.kvstore_opfactory.createPut(
						Key.createKey(Arrays.asList(
								__storeprefix,
								session.getClusterId()),
								Arrays.asList(__INVALIDATED)
								),Value.createValue(Base62Converter.fromBase10(System.currentTimeMillis()).getBytes("UTF-8"))));                
				//unsets.put(getContextKey(),1); 
			}

			// update indexes
			// delete old entries, if present
			ValueVersion old_valid = _kvstorehandler.get(
					Key.createKey(Arrays.asList(
							__storeprefix,
							session.getClusterId()),
							Arrays.asList(__VALID)));
			ValueVersion old_accessed = _kvstorehandler.get(
					Key.createKey(Arrays.asList(
							__storeprefix,
							session.getClusterId()),
							Arrays.asList(__ACCESSED)));
			ValueVersion old_expiry = _kvstorehandler.get(
					Key.createKey(Arrays.asList(
							__storeprefix,
							session.getClusterId()),
							Arrays.asList(__EXPIRY)));
			
			if (old_valid != null && old_valid.getValue() != null && old_valid.getValue().getValue() != null &&
					old_accessed != null && old_accessed.getValue() != null && old_accessed.getValue().getValue() != null) {
				__log.debug("delete purgeindex entry for old_valid="+new String(old_valid.getValue().getValue(),"UTF-8")
						+";old_accessed="+new String(old_accessed.getValue().getValue(),"UTF-8"));
				_kvstorehandler.delete(
						Key.createKey(Arrays.asList(
								__purgeindexprefix,
								new String(old_valid.getValue().getValue(),"UTF-8"),
								new String(old_accessed.getValue().getValue(),"UTF-8")
								),
								Arrays.asList(session.getClusterId())));
			} else __log.debug("nothing to delete from purge index, either accessed or valid keys are missing");            
			__log.debug("new_valid="+new_valid+";new_accessed="+new_accessed+";new_expiry="+new_expiry);
			
			_kvstorehandler.put(
					Key.createKey(Arrays.asList(
							__purgeindexprefix,
							new_valid,
							new_accessed
							),
							Arrays.asList(session.getClusterId())),Value.EMPTY_VALUE);
			if (new_expiry != null) {
				if (old_expiry != null && old_expiry.getValue() != null && old_expiry.getValue().getValue() != null) {
					_kvstorehandler.delete(
							Key.createKey(Arrays.asList(
									__expirydailyindexprefix,
									new String(old_expiry.getValue().getValue(),"UTF-8")),
									Arrays.asList(session.getClusterId())
									));            	
				} else __log.debug("nothing to delete from expiry index, old expiry entry key is missing");         	

				_kvstorehandler.put(
						Key.createKey(Arrays.asList(
								__expirydailyindexprefix,
								new_expiry),
								Arrays.asList(session.getClusterId())
								),Value.EMPTY_VALUE);
			}
			// Done with indexes, do the upsert
			if (!kvstore_object_ops.isEmpty()) {
				_kvstorehandler.execute(kvstore_object_ops);
				kvstore_object_ops.clear();
			}			
			if (need_to_purge_attributes) {
				/*
				__log.debug("before need to purge attrubutes");
			    Iterator<Key> it = _kvstorehandler.multiGetKeysIterator(Direction.FORWARD, 10, 
			    		Key.createKey(Arrays.asList(
								__storeprefix,
								session.getClusterId())), 
			    		null,//new KeyRange("minorpart1",true,"minorpart1",true),
			    		Depth.PARENT_AND_DESCENDANTS);
			    while (it.hasNext()) {
			    	Key key = it.next();    	 
			    	__log.debug("key="+key.toString());
			    }
			    */
				_kvstorehandler.multiDelete(Key.createKey(Arrays.asList(
						__storeprefix,
						session.getClusterId())						
						), new KeyRange(__CONTEXT,true,__CONTEXT,true), Depth.PARENT_AND_DESCENDANTS);
				/*
				__log.debug("after need to purge attrubutes");
			    Iterator<Key> it = _kvstorehandler.multiGetKeysIterator(Direction.FORWARD, 10, 
			    		Key.createKey(Arrays.asList(
								__storeprefix,
								session.getClusterId())), 
			    		null,//new KeyRange("minorpart1",true,"minorpart1",true),
			    		Depth.PARENT_AND_DESCENDANTS);
			    while (it.hasNext()) {
			    	Key key = it.next();    	 
			    	__log.debug("key="+key.toString());
			    }
			    */

			}

			if (__log.isDebugEnabled())
				__log.debug("KVStoreSessionManager:save:db.sessions.updated");
			if (activateAfterSave)
				session.didActivate();
			return version;
		}
		catch (Exception e)
		{
			LOG.warn(e);
		}
		return null;
	}

	/*------------------------------------------------------------ */
	@Override
	protected Object refresh(NoSqlSession session, Object version)
	{
		__log.debug("KVStoreSessionManager:refresh session {}", session.getId());

		// check if our in memory version is the same as what is on the disk
		if (version != null)
		{
			ValueVersion vval_version = this._kvstorehandler.get(Key.createKey(Arrays.asList(
					__storeprefix,
					session.getClusterId()),
					Arrays.asList(__CONTEXT, _contextId, __VERSION)
					));

			if (vval_version != null && vval_version.getValue().getValue() != null)
			{

				Long saved_version = new Long(-1L);
				try {
					saved_version = new Long(Base62Converter.toBase10(new String(vval_version.getValue().getValue(),"UTF-8")));
				} catch (UnsupportedEncodingException e) {}

				if (saved_version.equals(version))
				{
					__log.debug("KVStoreSessionManager:refresh not needed, session {}", session.getId());
					return version;
				}
				version = saved_version;
			}
		}

		// If we are here, we have to load the object
		ValueVersion vver_valid = _kvstorehandler.get(Key.createKey(Arrays.asList(
				__storeprefix,
				session.getClusterId()),Arrays.asList(__VALID)));

		// If it doesn't exist, invalidate
		if (vver_valid == null || vver_valid.getValue() == null || vver_valid.getValue().getValue() == null)
		{
			__log.debug("KVStoreSessionManager:refresh:marking session {} invalid, no object", session.getClusterId());
			session.invalidate();
			return null;
		}
		try
		{

			// If it has been flagged invalid, invalidate
			String valid_str = new String(vver_valid.getValue().getValue(),"UTF-8");
			Boolean valid = valid_str=="1"?true:false;
			if (!valid)
			{
				__log.debug("KVstoreSessionManager:refresh:marking session {} invalid, valid flag {}", session.getClusterId(), valid);
				session.invalidate();
				return null;
			}

			// We need to update the attributes. We will model this as a passivate,
			// followed by bindings and then activation.
			session.willPassivate();        	
			SortedMap<Key, ValueVersion> context_keyvalues = _kvstorehandler.multiGet(Key.createKey(Arrays.asList(
					__storeprefix,
					session.getClusterId()),
					Arrays.asList(__CONTEXT, _contextId)
					),null,Depth.CHILDREN_ONLY);

			//if disk version now has no attributes, get rid of them
			if (context_keyvalues == null || context_keyvalues.size() == 0)
			{
				session.clearAttributes();
			}
			else
			{
				//iterate over the names of the attributes on the disk version, updating the value
				ArrayList<String> stored_attrs = new ArrayList<String>();
				for (Entry<Key, ValueVersion> attr_entry : context_keyvalues.entrySet())
				{
					List<String> attr_minorpath = attr_entry.getKey().getMinorPath();
					String attribute_name = attr_minorpath.get(attr_minorpath.size()-1); 
					String attr = decodeName(attribute_name);
					Object value = null;                    
					if (attr_entry.getValue() != null && attr_entry.getValue().getValue() != null && attr_entry.getValue().getValue().getValue() != null)
						value = decodeValue(attr_entry.getValue().getValue().getValue());

					//session does not already contain this attribute, so bind it
					if (value != null) {
						stored_attrs.add(attr);
						if (session.getAttribute(attr) == null)
						{ 
							session.doPutOrRemove(attr,value);
							session.bindValue(attr,value);
						}
						else //session already contains this attribute, update its value
							session.doPutOrRemove(attr,value);
					}                    
				}
				// cleanup, remove values from session, that don't exist in data anymore:
				for (String sess_attr_entry : session.getNames()) {
					if (!stored_attrs.contains(sess_attr_entry))
					{
						session.doPutOrRemove(sess_attr_entry,null);
						session.unbindValue(sess_attr_entry,session.getAttribute(sess_attr_entry));
					}

				}
			}

			/*
			 * We are refreshing so we should update the last accessed time.
			 */
			// update purge index too, if we here we assume valid is set to 1, so we do not need to check for old value
			ValueVersion old_accessed = _kvstorehandler.get(
					Key.createKey(Arrays.asList(
							__storeprefix,
							session.getClusterId()),
							Arrays.asList(__ACCESSED)));
			if (old_accessed != null && old_accessed.getValue() != null && old_accessed.getValue().getValue() != null) {
				_kvstorehandler.delete(
						Key.createKey(Arrays.asList(
								__purgeindexprefix,
								"1",
								new String(old_accessed.getValue().getValue(),"UTF-8")
								),
								Arrays.asList(session.getClusterId())));
			}
			String new_accessed = Base62Converter.longToLexiSortableBase62(System.currentTimeMillis());
			_kvstorehandler.put(
					Key.createKey(Arrays.asList(
							__purgeindexprefix,
							"1",
							new_accessed
							),
							Arrays.asList(session.getClusterId())),Value.EMPTY_VALUE);

			_kvstorehandler.put(
					Key.createKey(Arrays.asList(
							__storeprefix,
							session.getClusterId()),
							Arrays.asList(__ACCESSED)
							),Value.createValue(new_accessed.getBytes("UTF-8")));
			session.didActivate();

			return version;
		}
		catch (Exception e)
		{
			LOG.warn(e);
		}

		return null;
	}

	/*------------------------------------------------------------ */
	@Override
	protected synchronized NoSqlSession loadSession(String clusterId)
	{    	
		__log.debug("KVStoreSessionManager::loadSession id={}", clusterId);
		SortedMap<Key, ValueVersion> context_keyvalues = _kvstorehandler.multiGet(
				Key.createKey(Arrays.asList(__storeprefix,clusterId)
						),null,Depth.PARENT_AND_DESCENDANTS);

		//DBObject o = _kvstorehandler.findOne(new BasicDBObject(__ID,clusterId));		
		if (context_keyvalues == null || context_keyvalues.size()==0)
			return null;        

		ValueVersion vver_valid = context_keyvalues.get(Key.createKey(Arrays.asList(
				__storeprefix,clusterId),Arrays.asList(__VALID)));


		Boolean valid = null;
		if (vver_valid != null && vver_valid.getValue() != null && vver_valid.getValue().getValue() != null)
			try { valid = "1".equals(new String(vver_valid.getValue().getValue(),"UTF-8")); }
		catch (UnsupportedEncodingException e) {}
		__log.debug("KVStoreSessionManager:id={} valid={}", clusterId, valid);
		if (valid == null || !valid)  return null;


		try
		{


			Object version = new Long(Base62Converter.toBase10(new String(context_keyvalues.get(Key.createKey(Arrays.asList(
					__storeprefix,clusterId),Arrays.asList(__CONTEXT, _contextId,__VERSION))).getValue().getValue(),"UTF-8")));
			Long created = new Long(Base62Converter.toBase10(new String(context_keyvalues.get(Key.createKey(Arrays.asList(
					__storeprefix,clusterId),Arrays.asList(__CREATED))).getValue().getValue(),"UTF-8")));
			Long accessed = new Long(Base62Converter.toBase10(new String(context_keyvalues.get(Key.createKey(Arrays.asList(
					__storeprefix,clusterId),Arrays.asList(__ACCESSED))).getValue().getValue(),"UTF-8")));

			NoSqlSession session = null;

			for (Entry<Key,ValueVersion> attr_entry  : context_keyvalues.entrySet()) {            	
				if (attr_entry.getKey().toString().startsWith(__storeprefix+"/"+clusterId+"/"+__CONTEXT+"/"+_contextId)) {
					// got our attribute

					if (session == null) {
						__log.debug("KVStoreSessionManager: session {} present for context {}", clusterId, _contextId);
						session = new NoSqlSession(this,created,accessed,clusterId,version);
					}
					List<String> attr_minorpath = attr_entry.getKey().getMinorPath();
					String attribute_name = attr_minorpath.get(attr_minorpath.size()-1); 
					String attr = decodeName(attribute_name);
					Object value = null;                    
					if (attr_entry.getValue() != null && attr_entry.getValue().getValue() != null && attr_entry.getValue().getValue().getValue() != null) {
						value = decodeValue(attr_entry.getValue().getValue().getValue());
						session.doPutOrRemove(attr,value);
						session.bindValue(attr,value);
					}
				}
			}
			if (session != null) session.didActivate();
			else __log.debug("KVStoreSessionManager: session  {} not present for context {}",clusterId, _contextId);
			return session;
		}
		catch (Exception e)
		{
			LOG.warn(e);
		}
		return null;
	}



	/*------------------------------------------------------------ */
	/** 
	 * Remove the per-context sub document for this session id.
	 * @see org.eclipse.jetty.nosql.NoSqlSessionManager#remove(org.eclipse.jetty.nosql.NoSqlSession)
	 */
	@Override
	protected boolean remove(NoSqlSession session)
	{
		__log.debug("KVStoreSessionManager:remove:session {} for context {}",session.getClusterId(), _contextId);

		/*
		 * Check if the session exists and if it does remove the context
		 * associated with this session
		 */
		int deleted_count = _kvstorehandler.multiDelete(
				Key.createKey(Arrays.asList(__storeprefix,session.getClusterId()),
							  Arrays.asList(__CONTEXT,_contextId)),
				null, Depth.PARENT_AND_DESCENDANTS);
		if (deleted_count > 0) return true;
		else return false;

	}



	/** 
	 * @see org.eclipse.jetty.nosql.NoSqlSessionManager#expire(java.lang.String)
	 */
	@Override
	protected void expire (String idInCluster)
	{
		__log.debug("KVStoreSessionManager:expire session {} ", idInCluster);

		//Expire the session for this context
		super.expire(idInCluster);


		//If the outer session document has not already been marked invalid, do so.
		ValueVersion vver_valid = _kvstorehandler.get(Key.createKey(Arrays.asList(
				__storeprefix,
				idInCluster),Arrays.asList(__VALID)));

		try {
			if (vver_valid != null && vver_valid.getValue() != null && vver_valid.getValue().getValue() != null
					&& "0".equals(new String(vver_valid.getValue().getValue(),"UTF-8")))
			{
				if (this.kvstore_opfactory == null) this.kvstore_opfactory = this._kvstorehandler.getOperationFactory();
				kvstore_object_ops.clear();
				kvstore_object_ops.add(this.kvstore_opfactory.createPut(
						Key.createKey(Arrays.asList(
								__storeprefix,
								idInCluster),
								Arrays.asList(__VALID)
								),Value.createValue("0".getBytes("UTF-8"))));
				kvstore_object_ops.add(this.kvstore_opfactory.createPut(
						Key.createKey(Arrays.asList(
								__storeprefix,
								idInCluster),
								Arrays.asList(__INVALIDATED)
								),Value.createValue(Base62Converter.fromBase10(System.currentTimeMillis()).getBytes("UTF-8"))));

				// update indexes
				// delete old entries, if present
				ValueVersion old_valid = _kvstorehandler.get(
						Key.createKey(Arrays.asList(
								__storeprefix,
								idInCluster),
								Arrays.asList(__VALID)));
				__log.debug("old_valid="+(old_valid!=null?"notnull":"null"));
				ValueVersion old_accessed = _kvstorehandler.get(
						Key.createKey(Arrays.asList(
								__storeprefix,
								idInCluster),
								Arrays.asList(__ACCESSED)));
				__log.debug("old_accessed="+(old_accessed!=null?"notnull":"null"));
				if (old_valid != null && old_valid.getValue() != null && old_valid.getValue().getValue() != null &&
						old_accessed != null && old_accessed.getValue() != null && old_accessed.getValue().getValue() != null) {
					__log.debug("deleting old purgeindex entry");
					_kvstorehandler.delete(
							Key.createKey(Arrays.asList(
									__purgeindexprefix,
									new String(old_valid.getValue().getValue(),"UTF-8"),
									new String(old_accessed.getValue().getValue(),"UTF-8")
									),
									Arrays.asList(idInCluster)));
				} else __log.debug("some keys for purge index missing, nothing to delete");            
				
				_kvstorehandler.put(
						Key.createKey(Arrays.asList(
								__purgeindexprefix,
								"0",
								new String(old_accessed.getValue().getValue(),"UTF-8")
								),
								Arrays.asList(idInCluster)),Value.EMPTY_VALUE);

				
				
				if (kvstore_object_ops.size() >0) {
					_kvstorehandler.execute(kvstore_object_ops);
					kvstore_object_ops.clear();
				}
			}
		} catch (UnsupportedEncodingException | OperationExecutionException | FaultException e) {
			__log.debug("KVStoreSessionManager: expire :: error :: session {} context {} ",idInCluster, _contextId);
		}       
	}


	/*------------------------------------------------------------ */
	/** 
	 * Change the session id. Note that this will change the session id for all contexts for which the session id is in use.
	 * @see org.eclipse.jetty.nosql.NoSqlSessionManager#update(org.eclipse.jetty.nosql.NoSqlSession, java.lang.String, java.lang.String)
	 */
	@Override
	protected void update(NoSqlSession session, String newClusterId, String newNodeId) throws Exception
	{
		__log.debug("KVStoreSessionManager:update session {} to {}", session.getClusterId(),newClusterId);
		SortedMap<Key, ValueVersion> old_keyvalues = _kvstorehandler.multiGet(
				Key.createKey(Arrays.asList(__storeprefix,session.getClusterId())
						),null,Depth.PARENT_AND_DESCENDANTS);

		if (old_keyvalues != null && old_keyvalues.size() >0) {
			this.kvstore_object_ops.clear();    		
			ValueVersion expiry_old_val = null;
			for (Entry<Key, ValueVersion> old_entry :  old_keyvalues.entrySet()) {
				List<String> major_keypart = old_entry.getKey().getMajorPath();
				List<String> minor_keypart = old_entry.getKey().getMinorPath();
				if (old_entry.getKey().toString().startsWith(__storeprefix+"/"+session.getClusterId()+"/"
						+__EXPIRY)) {
					//got expiry value, save it for linkback
					expiry_old_val = old_entry.getValue();    				
				} 
				this.kvstore_object_ops.add(this.kvstore_opfactory.createPut(
						Key.createKey(major_keypart,minor_keypart),old_entry.getValue().getValue()));

				major_keypart.set(1, newClusterId);
			}
			if (this.kvstore_object_ops.size() >0 ) {
				_kvstorehandler.execute(this.kvstore_object_ops);
				this.kvstore_object_ops.clear();
				_kvstorehandler.multiDelete(Key.createKey(Arrays.asList(__storeprefix,session.getClusterId())
						),null,Depth.PARENT_AND_DESCENDANTS);
			}
			if (expiry_old_val != null) {
				_kvstorehandler.put(Key.createKey(Arrays.asList(
						__expirydailyindexprefix,
						new String(expiry_old_val.getValue().getValue()),"UTF-8"),
						Arrays.asList(newClusterId)
						),Value.EMPTY_VALUE);
				_kvstorehandler.delete(Key.createKey(Arrays.asList(
						__expirydailyindexprefix,
						new String(expiry_old_val.getValue().getValue()),"UTF-8"),
						Arrays.asList(session.getClusterId())
						));
			}

		}

	}

	/*------------------------------------------------------------ */
	protected String encodeName(String name)
	{
		return name.replace("%","%25").replace(".","%2E").replace("//","%2F");
	}

	/*------------------------------------------------------------ */
	protected String decodeName(String name)
	{
		return name.replace("%2E",".").replace("%2F", "//").replace("%25","%");
	}

	/*------------------------------------------------------------ */
	@SuppressWarnings("resource")
	protected Object decodeValue(final byte[] valueToDecode) throws IOException, ClassNotFoundException
	{
		final ByteArrayInputStream bais = new ByteArrayInputStream(valueToDecode);
		final ClassLoadingObjectInputStream objectInputStream = new ClassLoadingObjectInputStream(bais);
		return objectInputStream.readUnshared();
	}

	public static byte[] encodeValue(Object value) throws IOException {
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		ObjectOutputStream out = new ObjectOutputStream(bout);
		out.reset();
		out.writeUnshared(value);
		out.flush();
		return bout.toByteArray();
	}


	/*------------------------------------------------------------ */

	private String getContextKey()
	{
		return __CONTEXT + "." + _contextId;
	}


	/*------------------------------------------------------------ */
	@ManagedOperation(value="purge invalid sessions in the session store based on normal criteria", impact="ACTION")
	public void purge()
	{   
		((KVStoreSessionIdManager)_sessionIdManager).purge();
	}


	/*------------------------------------------------------------ */
	@ManagedOperation(value="full purge of invalid sessions in the session store", impact="ACTION")
	public void purgeFully()
	{   
		((KVStoreSessionIdManager)_sessionIdManager).purgeFully();
	}

	/*------------------------------------------------------------ */
	@ManagedOperation(value="scavenge sessions known to this manager", impact="ACTION")
	public void scavenge()
	{
		((KVStoreSessionIdManager)_sessionIdManager).scavenge();
	}

	/*------------------------------------------------------------ */
	@ManagedOperation(value="scanvenge all sessions", impact="ACTION")
	public void scavengeFully()
	{
		((KVStoreSessionIdManager)_sessionIdManager).scavengeFully();
	}


	private String createContextId(String[] virtualHosts, String contextPath)
	{
		String contextId = virtualHosts[0] + contextPath;

		contextId.replace('/', '_');
		contextId.replace('.','_');
		contextId.replace('\\','_');

		return contextId;
	}


	/*------------------------------------------------------------ */
	/**
	 * ClassLoadingObjectInputStream
	 *
	 *
	 */
	protected class ClassLoadingObjectInputStream extends ObjectInputStream
	{
		public ClassLoadingObjectInputStream(java.io.InputStream in) throws IOException
		{
			super(in);
		}

		public ClassLoadingObjectInputStream () throws IOException
		{
			super();
		}

		@Override
		public Class<?> resolveClass (java.io.ObjectStreamClass cl) throws IOException, ClassNotFoundException
		{
			try
			{
				return Class.forName(cl.getName(), false, Thread.currentThread().getContextClassLoader());
			}
			catch (ClassNotFoundException e)
			{
				return super.resolveClass(cl);
			}
		}
	}


}
