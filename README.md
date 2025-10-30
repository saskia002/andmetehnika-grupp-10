## Python venv (virtual environment) setup

### Prerequisites:

Required:

-   Python 3.13

### Auto (Windows):

Run the setup script:

```bash
./setup_venv.bat
```

### Auto (UNIX):

Run the setup script:

```bash
{
    chmod +x ./setup_venv.sh;
    ./setup_venv.sh;
}
```

### IDE setup:

Make sure your IDE is using the created virtual environment as runtime interpreter.

## Useful commands:

1. Activate the virtual environment:

    Windows:

    ```bash
    ./venv/Scripts/activate
    ```

    UNIX:

    ```bash
    source ./venv/bin/activate
    ```

2. Deactivate the virtual environment:

    ```bash
    deactivate
    ```

3. Freeze the current dependencies to `requirements.txt`:

    venv must be activated!

    ```bash
    pip freeze > requirements.txt
    ```
4. Ingest data from MongoDB to ClickHouse (Bronze layer)

   ```
   python "hw2/clickhouse/ingest_from_mongo.py"
   ```

5. Connect to ClickHouse

   ```
   docker exec -it clickhouse clickhouse-client
   ```
   
