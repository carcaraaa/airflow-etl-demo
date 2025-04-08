# airflow-etl-demo


## Software needed

- docker
- docker compose or docker-compose (old version)
- python 3.11


## Running airflow

Download the official docker-compose.yaml file:

```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.10.5/docker-compose.yaml
```

Configure .env file:

```bash
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

Start containers

```bash
docker compose up -d
```

or

```bash
docker-compose up -d
```

See container status

```bash
docker-compose ps
```

Airflow web interface uses port 8080, if port is already being used the container will not start. You can change the port by altering the line number 122 from the [docker-compose.yaml](docker-compose.yaml) like this:

```yaml
- "new_port:8080"
```


## Configure connections via web

Go to `localhost:port`, the default login and password is `airflow`. Then go Admin -> Connections and add a new connection.

For this demo we need to create two connections, a http connection to get the data and a postgres connection to save the data. Be sure to configure then like the examples below.

### Configuring the poke api end point
![http connection](/imgs/http_connection.png)

### Configuring the postgres connection

Using the default airflow login and database

![postgres connection](/imgs/postgres_connection.png)


## Running a DAG

The file [dag.py](/dags/dag.py) has the code that describes the pipeline that will be executed and, since it already is inside the *dags* folder, it should be visible in the web page like so:

![dag](/imgs/dag.png)

Click on the blue play button to run the DAG.


## Checking the output

If the tasks run without failure you'll be able to see the results in the database. Here's an example of how to do it via command line:

- access the postgres database
```bash
docker compose exec postgres psql -U airflow
```

- query the output table
```sql
select * from test_output;
```

