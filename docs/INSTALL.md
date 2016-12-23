# Installation

## Requirements

* [svgexport](https://github.com/shakiba/svgexport), for creating the previews of the hypermodels
* [jpegoptim](https://github.com/tjko/jpegoptim), for shrinking in size the output JPEG preview images 

The server uses [PostgreSQL](https://www.postgresql.org/) as the underlying RDBMS, and it's compatible with version 9.5 and above. After the installation of PostgreSQL, the `hme` user and database need to be created, as follows:

```sql
CREATE USER hme WITH PASSWORD 'hme';
CREATE DATABASE hme OWNER hme;
```

The database schema is at `dbschema.sql` so you need to run it e.g.

```bash
psql -U hme hme < dbschema.sql
```

## Configuration

In the classpath there should be a `HmeServerConfig.properties` file that contains the following contents:


```ini
port=9090
hostname=0.0.0.0
serviceAccountName=
serviceAccountPassword=
serviceUrl=https://hme.example.com/

secureTokenService = https://ciam.chic-vph.eu/sts/services/STS
mrServiceUrl=https://mr.chic-vph.eu/
sparqlRicordo=http://open-physiology.org:3033/chicprot/query

staticDir = /Users/ssfak/hme-server/static

dbHost=localhost
dbName=hme
dbUser=hme
dbPwd=hme
```

* `port` is the TCP port used for accepting HTTP requests
* `hostname` is the IP address (interface) to listen on
* `serviceAccountName` is the CHIC username for this service
* `serviceAccountPassword` is the CHIC password for this service
* `serviceUrl` is the external URI of the service
* `secureTokenService` is the URI/endpoint for the Secure Token Service (STS) of CHIC
* `mrServiceUrl` is the URI/endpoint for the CHIC Model Repository
* `sparqlRicordo` is the URI/endpoint for the CHIC Semantic Repository (SPARQL/Triplestore)
* `staticDir` is the path to the directory that contains the HTML/JS/etc assets
* `dbHost`, `dbName`, `dbUser`, and `dbPwd` refer to the DB connection parameters (host, database name, username, and password)

