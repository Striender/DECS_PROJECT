# CS744 Project: HTTP-based Key-Value Server (C++)

## Overview

This project implements a multi-tier HTTP key-value server with:

- A multi-threaded C++ HTTP server using cpp-httplib
- An in-memory LRU cache
- A persistent MySQL database
- A load generator for performance testing

It supports POST (create/update), GET (read), and DELETE operations on key-value pairs using REST APIs.

---

## System Requirements

- Ubuntu 20.04+ with CMake ≥ 3.10
- C++17
- MySQL Server

Install dependencies:

```bash
sudo apt update
sudo apt install build-essential cmake libmysqlclient-dev mysql-server curl
```

---

## Project Directory Structure

```
HTTP-KV-Server/
├── image
├── lib/
│   └── httplib.h    # from https://github.com/yhirose/cpp-httplib
├── src/
│   ├── server.cpp
│   ├── load_generator.cpp
├── result
|   ├── put.txt
│   ├── popular.txt
├── plot.ipynb
├── Makefile
└── README.md
```

---

## MySQL Setup

```bash
sudo systemctl enable mysql
sudo systemctl start mysql
sudo mysql_secure_installation
```

Then we create a dedicated database and user:

```bash
sudo mysql -u root -p
# If prompted for a password, simply press enter if you did not create a password
```

Then we use the MySQL prompt:

```sql
-- To see which databases exist already
-- SHOW DATABASES;
CREATE DATABASE kv_server_db;
```

Testing the connection:

```bash
mysql -u <username> -p 
# password:  ****
# You may then exit mysql
```

---

## Build Instructions

```bash
make all
```

This will then generate two executables 1) kv_server and 2) load_generator

---

## Running the Server

Run the server (default port 8080):

```bash
taskset -c 1 ./kv_server
# Expected output: Starting server at 0.0.0.0:8080
```

---

## Testing the API

### Create (POST)

```bash
curl -X POST -d "key=foo" -d "value=bar" http://127.0.0.1:9090/kv
# Output: OK
```

### Read (GET)

```bash
curl http://127.0.0.1:9090/kv/foo
# Output: bar
```

### Delete

```bash
curl -X DELETE http://127.0.0.1:8080/kv/foo
# Output: Deleted
```

### Verify in MySQL

```bash
mysql -u Striender -pkvpass  <passwword> -e "SELECT * FROM kv_store LIMIT 10;"
```

---

## Running the Load Generator

```
taskset -c 3,4,5,6  ./load_generator <no_of _threads> <duration_seconds> <workload>
```



### Example

#### 1. CPU-bound cache workload (Get Popular)

```bash
taskset -c 3,4,5,6 ./load_generator 10 300 popular
```

#### 2. Disk-bound workload (Put = create + delete)

```bash
taskset -c 3,4,5,6 ./load_generator 10 300 put
```



Expected output:

```
Pinging server at 127.0.0.1:9090...
Server connection successful.
Initialized 10 popular keys for workload.
Preloading popular keys into server/cache...
  ✅ Inserted key_5
  ✅ Inserted key_8
  ✅ Inserted key_2
  ✅ Inserted key_7
  ✅ Inserted key_9
  ✅ Inserted key_1
  ✅ Inserted key_10
  ✅ Inserted key_3
  ✅ Inserted key_4
  ✅ Inserted key_6
Preloading complete. Waiting briefly before test...
Starting 'popular' workload with 10 threads for 300 seconds...
Test running...
Time is up. Detaching threads and calculating results...

--- Results ---
Workload: popular
Test ran for: 300 seconds
Total requests completed: 1959150
Total requests failed (timeout/error): 0
Throughput: 6530.5 successful requests/second
Average response time: 0.0821969 ms
```

---

## SQL Verification Steps

You can also verify persistence by doing:

```sql
SHOW TABLES;
SELECT * FROM kv_pairs;
SELECT COUNT(*) FROM kv_store_db;
```

