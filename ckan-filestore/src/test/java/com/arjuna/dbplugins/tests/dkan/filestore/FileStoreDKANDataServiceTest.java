/*
 * Copyright (c) 2014-2015, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.tests.dkan.filestore;

import com.arjuna.databroker.data.connector.ObservableDataProvider;
import com.arjuna.databroker.data.connector.ObserverDataConsumer;
import com.arjuna.databroker.data.core.DataFlowNodeLifeCycleControl;
import com.arjuna.dbplugins.dkan.filestore.FileStoreDKANDataService;
import com.arjuna.dbutils.testsupport.dataflownodes.dummy.DummyDataSource;
import com.arjuna.dbutils.testsupport.dataflownodes.lifecycle.TestJEEDataFlowNodeLifeCycleControl;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.fail;

public class FileStoreDKANDataServiceTest
{
    private static final Logger logger = Logger.getLogger(FileStoreDKANDataServiceTest.class.getName());

    @Test
    public void createResourceAsString()
    {
        try
        {
            DKANAPIProperties dkanAPIProperties = new DKANAPIProperties("dkanapi.properties");

            if (! dkanAPIProperties.isLoaded())
            {
                logger.log(Level.INFO, "SKIPPING TEST 'createResource', no propertiles file");
                return;
            }

            DataFlowNodeLifeCycleControl dataFlowNodeLifeCycleControl = new TestJEEDataFlowNodeLifeCycleControl();

            String              name       = "FileStoreDKANDataService";
            Map<String, String> properties = new HashMap<String, String>();
            properties.put(FileStoreDKANDataService.DKANROOTURL_PROPERTYNAME, dkanAPIProperties.getDKANRootURL());
            properties.put(FileStoreDKANDataService.PACKAGEID_PROPERTYNAME, dkanAPIProperties.getPackageId());
            properties.put(FileStoreDKANDataService.USERNAME_PROPERTYNAME, dkanAPIProperties.getUsername());
            properties.put(FileStoreDKANDataService.PASSWORD_PROPERTYNAME, dkanAPIProperties.getPassword());

            DummyDataSource          dummyDataSource          = new DummyDataSource("Dummy Data Source", Collections.<String, String>emptyMap());
            FileStoreDKANDataService fileStoreDKANDataService = new FileStoreDKANDataService(name, properties);

            dataFlowNodeLifeCycleControl.completeCreationAndActivateDataFlowNode(UUID.randomUUID().toString(), dummyDataSource, null);
            dataFlowNodeLifeCycleControl.completeCreationAndActivateDataFlowNode(UUID.randomUUID().toString(), fileStoreDKANDataService, null);

            ((ObservableDataProvider<String>) dummyDataSource.getDataProvider(String.class)).addDataConsumer((ObserverDataConsumer<String>) fileStoreDKANDataService.getDataConsumer(String.class));

            dummyDataSource.sendData("Test Data, Test Text");

            dataFlowNodeLifeCycleControl.removeDataFlowNode(dummyDataSource);
            dataFlowNodeLifeCycleControl.removeDataFlowNode(fileStoreDKANDataService);
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Problem in 'createResource'", throwable);
            fail("Problem in 'createResource': " + throwable);
        }
    }

    @Test
    public void createResourceAsBytes()
    {
        try
        {
            DKANAPIProperties dkanAPIProperties = new DKANAPIProperties("dkanapi.properties");

            if (! dkanAPIProperties.isLoaded())
            {
                logger.log(Level.INFO, "SKIPPING TEST 'createResource', no propertiles file");
                return;
            }

            DataFlowNodeLifeCycleControl dataFlowNodeLifeCycleControl = new TestJEEDataFlowNodeLifeCycleControl();

            String              name       = "FileStoreDKANDataService";
            Map<String, String> properties = new HashMap<String, String>();
            properties.put(FileStoreDKANDataService.DKANROOTURL_PROPERTYNAME, dkanAPIProperties.getDKANRootURL());
            properties.put(FileStoreDKANDataService.PACKAGEID_PROPERTYNAME, dkanAPIProperties.getPackageId());
            properties.put(FileStoreDKANDataService.USERNAME_PROPERTYNAME, dkanAPIProperties.getUsername());
            properties.put(FileStoreDKANDataService.PASSWORD_PROPERTYNAME, dkanAPIProperties.getPassword());

            DummyDataSource          dummyDataSource          = new DummyDataSource("Dummy Data Source", Collections.<String, String>emptyMap());
            FileStoreDKANDataService fileStoreDKANDataService = new FileStoreDKANDataService(name, properties);

            dataFlowNodeLifeCycleControl.completeCreationAndActivateDataFlowNode(UUID.randomUUID().toString(), dummyDataSource, null);
            dataFlowNodeLifeCycleControl.completeCreationAndActivateDataFlowNode(UUID.randomUUID().toString(), fileStoreDKANDataService, null);

            ((ObservableDataProvider<byte[]>) dummyDataSource.getDataProvider(byte[].class)).addDataConsumer((ObserverDataConsumer<byte[]>) fileStoreDKANDataService.getDataConsumer(byte[].class));

            dummyDataSource.sendData("Test Data, Test Text".getBytes());

            dataFlowNodeLifeCycleControl.removeDataFlowNode(dummyDataSource);
            dataFlowNodeLifeCycleControl.removeDataFlowNode(fileStoreDKANDataService);
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Problem in 'createResource'", throwable);
            fail("Problem in 'createResource': " + throwable);
        }
    }

    @Test
    public void createResourceMap()
    {
        try
        {
            DKANAPIProperties dkanAPIProperties = new DKANAPIProperties("dkanapi.properties");

            if (! dkanAPIProperties.isLoaded())
            {
                logger.log(Level.INFO, "SKIPPING TEST 'createResource', no propertiles file");
                return;
            }

            DataFlowNodeLifeCycleControl dataFlowNodeLifeCycleControl = new TestJEEDataFlowNodeLifeCycleControl();

            String              name       = "FileStoreDKANDataService";
            Map<String, String> properties = new HashMap<String, String>();
            properties.put(FileStoreDKANDataService.DKANROOTURL_PROPERTYNAME, dkanAPIProperties.getDKANRootURL());
            properties.put(FileStoreDKANDataService.PACKAGEID_PROPERTYNAME, dkanAPIProperties.getPackageId());
            properties.put(FileStoreDKANDataService.USERNAME_PROPERTYNAME, dkanAPIProperties.getUsername());
            properties.put(FileStoreDKANDataService.PASSWORD_PROPERTYNAME, dkanAPIProperties.getPassword());

            DummyDataSource          dummyDataSource          = new DummyDataSource("Dummy Data Source", Collections.<String, String>emptyMap());
            FileStoreDKANDataService fileStoreDKANDataService = new FileStoreDKANDataService(name, properties);

            dataFlowNodeLifeCycleControl.completeCreationAndActivateDataFlowNode(UUID.randomUUID().toString(), dummyDataSource, null);
            dataFlowNodeLifeCycleControl.completeCreationAndActivateDataFlowNode(UUID.randomUUID().toString(), fileStoreDKANDataService, null);

            ((ObservableDataProvider<Map>) dummyDataSource.getDataProvider(Map.class)).addDataConsumer((ObserverDataConsumer<Map>) fileStoreDKANDataService.getDataConsumer(Map.class));

            Map<String, Object> dataMap = new HashMap<String, Object>();
            dataMap.put("data", "Test Data, Test Text".getBytes());
            dataMap.put("filename", "filename");
            dataMap.put("resourcename", "resourcename");
            dataMap.put("resourceformat", "TEXT");
            dataMap.put("resourcedescription", "A description");
            dummyDataSource.sendData(dataMap);

            dataFlowNodeLifeCycleControl.removeDataFlowNode(dummyDataSource);
            dataFlowNodeLifeCycleControl.removeDataFlowNode(fileStoreDKANDataService);
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Problem in 'createResource'", throwable);
            fail("Problem in 'createResource': " + throwable);
        }
    }
}
