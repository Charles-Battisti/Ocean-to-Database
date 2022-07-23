# Ocean-to_Database
Part of an automatic system to update an internal database with Sea Surface Temperature (SST) and Sea Surface Salinity (SSS) from external partner data.

Internal database is updated from currently two different data sources. The first data source, NDBC, is through an internal file share system. The second, SOFS, uses webscrapping. Both are updated daily.

The database update method uses the metadata of the processed data, the site, and the type of data to select the appropriate tables to update.
