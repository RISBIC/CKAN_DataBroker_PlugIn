/*
 * Copyright (c) 2014-2015, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.tests.dkan.filestore;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.fail;

public class DKANAPIProperties
{
    private boolean    _loaded;
    private Properties _dkanAPIProperties = new Properties();

    public DKANAPIProperties(String dkanAPIPropertiesFilename)
    {
        _dkanAPIProperties = new Properties();

        try
        {
            FileReader dkanAPIFileReader = new FileReader(dkanAPIPropertiesFilename);
            _dkanAPIProperties.load(dkanAPIFileReader);
            dkanAPIFileReader.close();
            _loaded = true;
        }
        catch (IOException ioException)
        {
            _dkanAPIProperties = null;
            _loaded = false;
        }
    }

    public boolean isLoaded()
    {
        return _loaded;
    }

    public String getDKANRootURL()
    {
        if (_dkanAPIProperties != null)
        {
            String dkanRootURL = _dkanAPIProperties.getProperty("dkanrooturl");

            if (dkanRootURL != null)
                return dkanRootURL;
            else
            {
                fail("Failed to obtain \"dkanrooturl\" property");
                return null;
            }
        }
        else
        {
            fail("Failed to obtain \"dkanrooturl\" property, no property file");
            return null;
        }
    }

    public String getPackageId()
    {
        if (_dkanAPIProperties != null)
        {
            String packageId = _dkanAPIProperties.getProperty("package_id");

            if (packageId != null)
                return packageId;
            else
            {
                fail("Failed to obtain \"package_id\" property");
                return null;
            }
        }
        else
        {
            fail("Failed to obtain \"package_id\" property, no property file");
            return null;
        }
    }

    public String getUsername()
    {
        if (_dkanAPIProperties != null)
        {
            String username = _dkanAPIProperties.getProperty("username");

            if (username != null)
                return username;
            else
            {
                fail("Failed to obtain \"username\" property");
                return null;
            }
        }
        else
        {
            fail("Failed to obtain \"username\" property, no property file");
            return null;
        }
    }

    public String getPassword()
    {
        if (_dkanAPIProperties != null)
        {
            String password = _dkanAPIProperties.getProperty("password");
            if (password != null)
                return password;
            else
            {
                fail("Failed to obtain \"password\" property");
                return null;
            }
        }
        else
        {
            fail("Failed to obtain \"password\" property, no property file");
            return null;
        }
    }

}
