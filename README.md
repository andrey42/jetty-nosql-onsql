# jetty-nosql-onsql
SessionManager implementation for Jetty, supporting Oracle NoSQL database

## Overview

SessionManager implementation for Jetty, based on jetty nosql support made for MongoDB


## Install

jetty-nosql-onsql is an extension for jetty nosql package, available withih standard Jetty releases under name

To install this session manager, You have to install both siandard jetty nosql library, available as jetty-nosql-<version>.jar
and prebuilt jar jetty-nosql-onsql.jar from dist folder, into jetty's `${jetty.home}/lib/ext`.

## Configuration

You need to configure both "session manager" and "session ID manager".


### Configuring "session ID manager"

SessionIdManagers can be configured in files under `${JETTY_HOME}/etc`.  In following example, using `${JETTY_HOME}/etc/jetty.xml`.

  <?xml version="1.0"?>
  <Configure id="Server" class="org.eclipse.jetty.server.Server">
      
  (... cut ...)
      
  <Set name="sessionIdManager">
    <New class="org.eclipse.jetty.nosql.onsql.KVStoreSessionIdManager">
      <Arg><Ref id="Server"/></Arg>
      <Set name="kvstorehosts">localhost:5000</Set>
      <Set name="kvstorename">omegastore</Set>
    </New>
  </Set>

  </Configure>

#### Extra options for "session id manager"

You can configure the behavior of session manager with following setters, either via jetty-web.xml or via app code
* setScavengePeriod(long scavengePeriod)
 * The period in seconds between scavenge checks.
 
 * setPurgeDelay(long purgeDelay)
  * period in seconds to purge invalud sessions 

* setPurgeInvalidAge(long purgeValidAge)
  * sets how old a session is to be persist past the point it has became invalud
  
setPurgeValidAge(long purgeValidAge)
     * sets how old a session is to be persist past the point it is  considered no longer viable and should be removed
     * NOTE: set this value to 0 to disable purging of valid sessions

### Configuring "session manager"

SessionManagers can be configured by either `${APP_ROOT}/WEB-INF/jetty-web.xml` or `${JETTY_HOME}/context/${APP_NAME}.xml`.

Sample configuration for `${APP_ROOT}/WEB-INF/jetty-web.xml`:

<?xml version="1.0" encoding="UTF-8"?>
<Configure class="org.eclipse.jetty.webapp.WebAppContext">
  <Get name="server">
    <Get id="IdManager" name="sessionIdManager" />
  </Get>
  <Set name="sessionHandler">
    <New class="org.eclipse.jetty.server.session.SessionHandler">
      <Arg>
        <New class="org.eclipse.jetty.nosql.onsql.KVStoreSessionManager">
          <Arg>
            <Ref id="IdManager" />
          </Arg>
          <Set name="savePeriod">1</Set>
        </New>
      </Arg>
    </New>
  </Set>
</Configure>


#### Extra options for "session manager"

You can configure the behavior of session manager with following setters, either via jetty-web.xml or via app code

* savePeriod(SessionIdManager idManager)
  * session id manager you created.
* setSessionFactory(AbstractSessionFactory sf)
  * set session serializer. org.eclipse.jetty.nosql.kvs.session.serializable.SerializableSessionFactory is used by default.

## License

* Copyright (c) 2015 Andrey Prokopenko <<mm87642@gmail.com>>
* Copyright (c) 2013 Yamashita, Yuu <<yamashita@geishatokyo.com>>

All rights reserved. This program and the accompanying materials
are made available under the terms of the Eclipse Public License v1.0
and Apache License v2.0 which accompanies this distribution.

The Eclipse Public License is available at http://www.eclipse.org/legal/epl-v10.html

The Apache License v2.0 is available at http://www.opensource.org/licenses/apache2.0.php

You may elect to redistribute this code under either of these licenses.
