# Install Postgres and proceed on Docker and Jupyternotebook

## I. Install on Docker as a Docker Image
In order to create Postgres on Docker, install Docker first on docker with with options of [Mac OS and Window](https://docs.docker.com/desktop/mac/install/). 
1. Pull the Postgres image to the docker via Terminal/Shell command
Pull the Postgres image via [Docker Hub](https://hub.docker.com/_/postgres) and run the: `docker pull postgres` on the Shell command 
2. Modify file docker-compose.yml
Create docker-compose.yml with command `touch docker-compose.yml` and edit as the file `docker-compose.yml` above the file in code editor
3. Run the file docker-compose.yml and proceed the localhost
- Using the command: `docker-compose up` to run the code, at default, there are 2 containers being created, 1 is the pgadmin, 2 is the Postgres
- To proceed the PgAdmin page, which is `0.0.0.0:5050`or `localhost:5050`, using the Default User and Default Password


## II. Run docker image and create a database
1. Run image:
```
 docker run --name postgres -e POSTGRES_PASSWORD=secret -d postgres
```
2. Run postgres:
```
docker exec -it postgres psql -U postgres
```
For example:
- To see the name of docker image: use `docker ps`

![Docker](img/docker.png)

- Run the postgres PSQL

![PSQL](img/psql.png)

3. Create database:
```
psql (13.1 (Debian 13.1-1.pgdg100+1))
Type "help" for help.

postgres=# CREATE DATABASE pagila;
postgres-# CREATE DATABASE
postgres=\q
```

## III. Connect Postgres Database to JupyterNotebook
1. Run the sql extension
- load ipython-sql:`%load_ext sql`

- To execute SQL queries you write one of the following atop of your cell: 
    - `%sql`
        - For a one-liner SQL query
        - You can access a python var using `$`    
    - `%%sql`
        - For a multi-line SQL query
        - You can **NOT** access a python var using `$`
2. Set up the postgres url:
```
DB_ENDPOINT ="127.0.0.1"
DB='pagila'
DB_USER = 'your user which set in docker-composer.yml'
DB_PASSWORD = 'your password which set in docker-composer.yml'
DB_PORT = '5432' #port used in 

conn_string="postgresql://{}:{}@{}:{}/{}"\
                        .format(DB_USER,DB_PASSWORD,DB_ENDPOINT,DB_PORT,DB)
                      # e.g: postgresql://thesis:thesis1234@127.0.0.1:5432/pagila
```
3. Use the SQL extension to execute query:
`%sql $conn_string`
## IV. Import from existing SQL file
1. To import the SQL file into a available database, access to the PSQL 
2. Create all schema objetcs (tables, etc) replace ```<local-repo>``` by your local directory :

```
cat <local-repo>/<sql-schema-file>.sql | docker exec -i <postgres_image_name> psql -U <server_name> -d pagila
```

3. Insert all data:
```
cat <local-repo>/<sql-data-file>.sql | docker exec -i <postgres_image_name> psql -U <server_name> -d pagila
```

4. Done! Just use:
```
docker exec -it <postgres_image_name> psql -U <server_name>
```
````
postgres
psql (13.1 (Debian 13.1-1.pgdg100+1))
Type "help" for help.

postgres=# \c pagila
You are now connected to database "pagila" as user "postgres".
pagila=# \dt
                    List of relations
 Schema |       Name       |       Type        |  Owner
--------+------------------+-------------------+----------
 public | actor            | table             | postgres
 public | address          | table             | postgres
 public | category         | table             | postgres
 public | city             | table             | postgres
 public | country          | table             | postgres
 public | customer         | table             | postgres
 public | film             | table             | postgres
 public | film_actor       | table             | postgres
 public | film_category    | table             | postgres
 public | inventory        | table             | postgres
 public | language         | table             | postgres
 public | payment          | partitioned table | postgres
 public | payment_p2020_01 | table             | postgres
 public | payment_p2020_02 | table             | postgres
 public | payment_p2020_03 | table             | postgres
 public | payment_p2020_04 | table             | postgres
 public | payment_p2020_05 | table             | postgres
 public | payment_p2020_06 | table             | postgres
 public | rental           | table             | postgres
 public | staff            | table             | postgres
 public | store            | table             | postgres
(21 rows)

pagila=#
```
