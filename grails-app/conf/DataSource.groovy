dataSource {
    pooled = true
    driverClassName = "org.gjt.mm.mysql.Driver"
    username = "miax"
    password = "miax"


    /*
                DriverAdapterCPDS cpds = new DriverAdapterCPDS();
            cpds.setDriver("org.gjt.mm.mysql.Driver");
            cpds.setUrl("jdbc:mysql://localhost:3306/miax");
            cpds.setUser("miax");
            cpds.setPassword("miax");
     */
}
hibernate {
    cache.use_second_level_cache = true
    cache.use_query_cache = true
    cache.provider_class = 'net.sf.ehcache.hibernate.EhCacheProvider'
}
// environment specific settings
environments {
    development {
        dataSource {
            url = "jdbc:mysql://localhost:3306/miax"
        }
    }
    test {
        dataSource {
            url = "jdbc:mysql://localhost:3306/miax"
         }
    }
    production {
        dataSource {
            dbCreate = "update"
            url = "jdbc:mysql://localhost:3306/miax"
        }
    }
}
