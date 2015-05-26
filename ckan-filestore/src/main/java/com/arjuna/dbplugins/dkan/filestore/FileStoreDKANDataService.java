/*
 * Copyright (c) 2014-2015, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.dkan.filestore;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.arjuna.databroker.data.DataConsumer;
import com.arjuna.databroker.data.DataFlow;
import com.arjuna.databroker.data.DataProvider;
import com.arjuna.databroker.data.DataService;
import com.arjuna.databroker.data.jee.annotation.DataConsumerInjection;
import com.arjuna.databroker.data.jee.annotation.PostConfig;
import com.arjuna.databroker.data.jee.annotation.PostCreated;
import com.arjuna.databroker.data.jee.annotation.PostRecovery;
import com.arjuna.databroker.data.jee.annotation.PreConfig;
import com.arjuna.databroker.data.jee.annotation.PreDelete;
import org.risbic.dkan.DKANClient;
import org.risbic.dkan.DKANConnection;

public class FileStoreDKANDataService implements DataService
{
    private static final Logger logger = Logger.getLogger(FileStoreDKANDataService.class.getName());

    public static final String DKANROOTURL_PROPERTYNAME = "DKAN Root URL";
    public static final String PACKAGEID_PROPERTYNAME   = "Package Id";
    public static final String USERNAME_PROPERTYNAME    = "Username";
    public static final String PASSWORD_PROPERTYNAME    = "Password";

    private String _dkanRootURL;
    private String _packageId;
    private String _username;
    private String _password;

    private DataFlow             _dataFlow;
    private String               _name;
    private Map<String, String>  _properties;

    @DataConsumerInjection(methodName="consumeString")
    private DataConsumer<String> _dataConsumerString;

    @DataConsumerInjection(methodName="consumeBytes")
    private DataConsumer<byte[]> _dataConsumerBytes;

    @DataConsumerInjection(methodName="consumeMap")
    private DataConsumer<Map>    _dataConsumerMap;

    public FileStoreDKANDataService()
    {
        logger.log(Level.FINE, "FileStoreDKANDataService");
    }

    public FileStoreDKANDataService(String name, Map<String, String> properties)
    {
        logger.log(Level.FINE, "FileStoreDKANDataService: " + name + ", " + properties);

        _name       = name;
        _properties = properties;
    }

    @Override
    public DataFlow getDataFlow()
    {
        return _dataFlow;
    }

    @Override
    public void setDataFlow(DataFlow dataFlow)
    {
        _dataFlow = dataFlow;
    }

    @Override
    public String getName()
    {
        return _name;
    }

    @Override
    public void setName(String name)
    {
        _name = name;
    }

    @Override
    public Map<String, String> getProperties()
    {
        return Collections.unmodifiableMap(_properties);
    }

    @Override
    public void setProperties(Map<String, String> properties)
    {
        _properties = properties;
    }

    @PostConfig
    @PostCreated
    @PostRecovery
    public void setup()
    {
        _dkanRootURL = _properties.get(DKANROOTURL_PROPERTYNAME);
        _packageId   = _properties.get(PACKAGEID_PROPERTYNAME);
        _username    = _properties.get(USERNAME_PROPERTYNAME);
        _password    = _properties.get(PASSWORD_PROPERTYNAME);
    }

    @PreConfig
    @PreDelete
    public void teardown()
    {
    }

    public void consumeString(String data)
    {
        logger.log(Level.FINE, "FileStoreDKANDataService.consumeString");

        try
        {
            uploadResource(data.getBytes(), null, null, null, null);
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Problems with dkan filestore api invoke", throwable);
        }
    }

    public void consumeBytes(byte[] data)
    {
        logger.log(Level.FINE, "FileStoreDKANDataService.consumeBytes");

        try
        {
            uploadResource(data, null, null, null, null);
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Problems with dkan filestore api invoke", throwable);
        }
    }

    public void consumeMap(Map map)
    {
        logger.log(Level.FINE, "FileStoreDKANDataService.consumeMap");

        try
        {
            byte[] data                = (byte[]) map.get("data");
            String fileName            = (String) map.get("filename");
            String resourceName        = (String) map.get("resourcename");
            String resourceFormat      = (String) map.get("resourceformat");
            String resourceDescription = (String) map.get("resourcedescription");

            uploadResource(data, fileName, resourceName, resourceFormat, resourceDescription);
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Problems with dkan filestore api invoke", throwable);
        }
    }

    private void uploadResource(byte[] data, String fileName, String resourceName, String resourceFormat, String resourceDescription)
    {
        logger.log(Level.FINE, "FileStoreDKANDataService.consume");

        // Set Defaults
        fileName = setDefault(fileName, false);
        resourceName = setDefault(resourceName, false);
        resourceDescription = setDefault(resourceDescription, true);
        resourceFormat = setDefault(resourceFormat, true);

        if (data.length == 0)
        {
            logger.log(Level.WARNING, "Unable to upload resource with empty data");
            return;
        }

        try
        {
            DKANConnection connection = DKANClient.connect(_username, _password, _dkanRootURL);
            String fileId = connection.createFile(data, fileName);
            connection.createResourceByDataSetTitle(resourceName, resourceDescription, _packageId, fileId);
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Problems with dkan filestore api invoke", throwable);
        }
    }

    private String setDefault(String value, boolean allowEmpty)
    {
        if (allowEmpty)
        {
            return (value != null) ? value : "";
        }
        return (value != null && value.length() > 0) ? value : UUID.randomUUID().toString();
    }

    @Override
    public Collection<Class<?>> getDataProviderDataClasses()
    {
        Set<Class<?>> dataConsumerDataClasses = new HashSet<Class<?>>();

        return dataConsumerDataClasses;
    }

    @Override
    public <T> DataProvider<T> getDataProvider(Class<T> dataClass)
    {
        return null;
    }

    @Override
    public Collection<Class<?>> getDataConsumerDataClasses()
    {
        Set<Class<?>> dataConsumerDataClasses = new HashSet<Class<?>>();

        dataConsumerDataClasses.add(String.class);
        dataConsumerDataClasses.add(byte[].class);
        dataConsumerDataClasses.add(Map.class);

        return dataConsumerDataClasses;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> DataConsumer<T> getDataConsumer(Class<T> dataClass)
    {
        if (dataClass == String.class)
            return (DataConsumer<T>) _dataConsumerString;
        else if (dataClass == byte[].class)
            return (DataConsumer<T>) _dataConsumerBytes;
        else if (dataClass == Map.class)
            return (DataConsumer<T>) _dataConsumerMap;
        else
            return null;
    }
}
