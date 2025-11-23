#define CPPHTTPLIB_THREAD_POOL_COUNT 10

#include "../lib/httplib.h"
#include <iostream>
#include <fstream>
#include <string>
#include <map>
#include <list>
#include <unordered_map>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <chrono>
#include <atomic>
#include <thread>
#include <sstream>
#include <stdexcept>

#include <mysql_connection.h>
#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/statement.h>
#include <cppconn/resultset.h>

// Use this namespace to avoid typing std:: often in examples
using namespace std;

// -------------------- Configuration & Globals --------------------
size_t MAX_CACHE_SIZE ;
int DB_POOL_SIZE ;
int SERVER_PORT ;

std::atomic<long long> total_requests{0};
std::atomic<long long> total_failures{0};
std::atomic<long long> cache_hits{0};
std::atomic<long long> cache_misses{0};
std::atomic<long long> db_calls{0};

sql::Driver *driver_instance = nullptr;

// -------------------- Simple config reader --------------------
map<string, string> read_config(const string &filename)
{
    map<string, string> config;
    ifstream config_file(filename);
    string line;
    if (config_file.is_open())
    {
        while (getline(config_file, line))
        {
            // ignore empty lines & comments
            if (line.empty() || line[0] == '#')
                continue;
            size_t separator_pos = line.find('=');
            if (separator_pos != string::npos)
            {
                string key = line.substr(0, separator_pos);
                string value = line.substr(separator_pos + 1);
                // trim whitespace (simple)libmysqlcppconnx.
                auto trim = [](string &s)
                {
                    size_t a = s.find_first_not_of(" \t\r\n");
                    size_t b = s.find_last_not_of(" \t\r\n");
                    if (a == string::npos)
                    {
                        s = "";
                        return;
                    }
                    s = s.substr(a, b - a + 1);
                };
                trim(key);
                trim(value);
                config[key] = value;
            }
        }
    }
    else
    {
        cerr << "Error: Could not open config file " << filename << endl;
    }
    return config;
}

// -------------------- LRU Cache (thread-safe via cache_mutex) --------------------
list<pair<string, string>> lru_list; // front = oldest, back = newest
unordered_map<string, list<pair<string, string>>::iterator> cache_map;
std::mutex cache_mutex;

void move_to_back(const string &key)
{
    auto it = cache_map.find(key);
    if (it != cache_map.end())
    {
        lru_list.splice(lru_list.end(), lru_list, it->second);
    }
}

void add_to_cache(const string &key, const string &value)
{
    if (cache_map.size() >= MAX_CACHE_SIZE)
    {
        string lru_key = lru_list.front().first;
        lru_list.pop_front();
        cache_map.erase(lru_key);
        cout << "[CACHE EVICT] Evicted key: " << lru_key << endl;
    }
    lru_list.push_back({key, value});
    cache_map[key] = --lru_list.end();
    cout << "[CACHE] Stored key: " << key << endl;
}

bool cache_get(const string &key, string &out_value)
{
    std::lock_guard<std::mutex> lk(cache_mutex);
    auto it = cache_map.find(key);
    if (it != cache_map.end())
    {
        out_value = it->second->second;
        move_to_back(key);
        cache_hits++;
        return true;
    }
    cache_misses++;
    return false;
}

void cache_put(const string &key, const string &value)
{
    std::lock_guard<std::mutex> lk(cache_mutex);
    auto it = cache_map.find(key);
    if (it != cache_map.end())
    {
        it->second->second = value;
        move_to_back(key);
    }
    else
    {
        add_to_cache(key, value);
    }
}

void cache_delete(const string &key)
{
    std::lock_guard<std::mutex> lk(cache_mutex);
    auto it = cache_map.find(key);
    if (it != cache_map.end())
    {
        lru_list.erase(it->second);
        cache_map.erase(it);
        cout << "[CACHE] Deleted key: " << key << endl;
    }
}

// -------------------- Connection Pool --------------------
class ConnectionPool
{
private:
    vector<sql::Connection *> pool;
    vector<bool> in_use;
    mutex pool_mutex;
    condition_variable pool_cv;

    string host, user, pass, schema;

public:
    ConnectionPool() = default;

    // Initialize pool with given size
    void init(const string &db_host, const string &db_user, const string &db_pass, const string &db_name, int pool_size)
    {
        host = db_host;
        user = db_user;
        pass = db_pass;
        schema = db_name;

        if (!driver_instance)
        {
            driver_instance = get_driver_instance(); // may throw
        }

        pool.clear();
        in_use.clear();
        pool.reserve(pool_size);
        in_use.reserve(pool_size);

        for (int i = 0; i < pool_size; ++i)
        {
            try
            {
                sql::Connection *con = driver_instance->connect(host, user, pass);
                con->setSchema(schema);
                pool.push_back(con);
                in_use.push_back(false);
                cout << "[POOL] Created connection " << i << endl;
            }
            catch (const sql::SQLException &e)
            {
                cerr << "[POOL ERROR] Failed to create DB connection " << i << ": " << e.what() << endl;
                // try to continue; the pool may have fewer connections
            }
        }

        if (pool.empty())
        {
            throw runtime_error("ConnectionPool: Could not create any DB connections");
        }
    }

    // Acquire a free connection (blocking)
    sql::Connection *acquire()
    {
        unique_lock<mutex> lk(pool_mutex);
        pool_cv.wait(lk, [&]()
                     {
            for (size_t i = 0; i < pool.size(); ++i)
            {
                if (!in_use[i])
                    return true;
            }
            return false; });

        // find first free
        for (size_t i = 0; i < pool.size(); ++i)
        {
            if (!in_use[i])
            {
                in_use[i] = true;
                sql::Connection *c = pool[i];

                // Simple liveness check; try to reconnect if invalid
                try
                {
                    if (!c->isValid()) // may throw
                    {
                        // attempt reconnect
                        try
                        {
                            delete c;
                        }
                        catch (...)
                        {
                        }
                        try
                        {
                            c = driver_instance->connect(host, user, pass);
                            c->setSchema(schema);
                            pool[i] = c;
                        }
                        catch (const sql::SQLException &e)
                        {
                            cerr << "[POOL] Reconnect failed: " << e.what() << endl;
                            // leave connection pointer null, but still mark in_use true temporarily
                        }
                    }
                }
                catch (const exception &e)
                {
                    // Some connectors may throw on isValid; attempt a reconnect
                    try
                    {
                        delete c;
                        c = driver_instance->connect(host, user, pass);
                        c->setSchema(schema);
                        pool[i] = c;
                    }
                    catch (const sql::SQLException &se)
                    {
                        cerr << "[POOL] Reconnect exception: " << se.what() << endl;
                    }
                }

                return pool[i];
            }
        }

        // Shouldn't reach here
        return nullptr;
    }

    // Release the previously acquired connection back to the pool
    void release(sql::Connection *con)
    {
        unique_lock<mutex> lk(pool_mutex);
        for (size_t i = 0; i < pool.size(); ++i)
        {
            if (pool[i] == con)
            {
                in_use[i] = false;
                pool_cv.notify_one();
                return;
            }
        }

        // If not found in pool (rare), just delete and ignore
        try
        {
            delete con;
        }
        catch (...)
        {
        }
    }

    // Cleanup
    void cleanup()
    {
        unique_lock<mutex> lk(pool_mutex);
        for (auto c : pool)
        {
            try
            {
                delete c;
            }
            catch (...)
            {
            }
        }
        pool.clear();
        in_use.clear();
    }

    ~ConnectionPool()
    {
        cleanup();
    }
};

ConnectionPool db_pool;

// -------------------- Database operations (use pool) --------------------

bool save_to_database(const string &key, const string &value)
{
    db_calls++;
    sql::Connection *con = nullptr;
    try
    {
        con = db_pool.acquire();
        if (!con)
        {
            cerr << "DB acquire failed" << endl;
            return false;
        }

        unique_ptr<sql::Statement> stmt(con->createStatement());
        // Very small sanitization: escape single quotes by doubling them
        auto esc = [](const string &s)
        {
            string out;
            out.reserve(s.size());
            for (char c : s)
            {
                if (c == '\'')
                    out.push_back('\'');
                out.push_back(c);
            }
            return out;
        };

        string qkey = esc(key);
        string qval = esc(value);

        string query = "INSERT INTO kv_pairs(item_key, item_value) VALUES('" + qkey + "', '" + qval +
                       "') ON DUPLICATE KEY UPDATE item_value='" + qval + "'";
        stmt->execute(query);
    }
    catch (const sql::SQLException &e)
    {
        cerr << "DATABASE ERROR (save): " << e.what() << endl;
        if (con)
            db_pool.release(con);
        return false;
    }
    catch (const exception &e)
    {
        cerr << "DATABASE ERROR (save unknown): " << e.what() << endl;
        if (con)
            db_pool.release(con);
        return false;
    }

    if (con)
        db_pool.release(con);

    // Update cache
    cache_put(key, value);
    return true;
}

pair<int, string> get_from_database(const string &key)
{
    // First try cache
    string val;
    if (cache_get(key, val))
    {
        // cache_get already increments cache_hits
        return {200, val};
    }

    // Cache miss -> check DB
    db_calls++;
    sql::Connection *con = nullptr;
    string value = "";
    try
    {
        con = db_pool.acquire();
        if (!con)
        {
            cerr << "DB acquire failed (get)" << endl;
            return {500, ""};
        }

        unique_ptr<sql::Statement> stmt(con->createStatement());
        // simple escape
        auto esc = [](const string &s)
        {
            string out;
            out.reserve(s.size());
            for (char c : s)
            {
                if (c == '\'')
                    out.push_back('\'');
                out.push_back(c);
            }
            return out;
        };

        string qkey = esc(key);
        string query = "SELECT item_value FROM kv_pairs WHERE item_key='" + qkey + "'";
        unique_ptr<sql::ResultSet> res(stmt->executeQuery(query));

        if (res->next())
        {
            value = res->getString("item_value");
        }
    }
    catch (const sql::SQLException &e)
    {
        cerr << "DATABASE ERROR (get): " << e.what() << endl;
        if (con)
            db_pool.release(con);
        return {500, ""};
    }
    catch (const exception &e)
    {
        cerr << "DATABASE ERROR (get unknown): " << e.what() << endl;
        if (con)
            db_pool.release(con);
        return {500, ""};
    }

    if (con)
        db_pool.release(con);

    if (!value.empty())
    {
        cache_put(key, value);
        return {200, value};
    }
    else
    {
        return {404, ""};
    }
}

int delete_from_database(const string &key)
{
    db_calls++;
    sql::Connection *con = nullptr;
    int update_count = 0;
    try
    {
        con = db_pool.acquire();
        if (!con)
        {
            cerr << "DB acquire failed (delete)" << endl;
            return 500;
        }

        unique_ptr<sql::Statement> stmt(con->createStatement());
        // simple escape
        auto esc = [](const string &s)
        {
            string out;
            out.reserve(s.size());
            for (char c : s)
            {
                if (c == '\'')
                    out.push_back('\'');
                out.push_back(c);
            }
            return out;
        };

        string qkey = esc(key);
        string query = "DELETE FROM kv_pairs WHERE item_key='" + qkey + "'";
        update_count = stmt->executeUpdate(query);
    }
    catch (const sql::SQLException &e)
    {
        cerr << "DATABASE ERROR (delete): " << e.what() << endl;
        if (con)
            db_pool.release(con);
        return 500;
    }
    catch (const exception &e)
    {
        cerr << "DATABASE ERROR (delete unknown): " << e.what() << endl;
        if (con)
            db_pool.release(con);
        return 500;
    }

    if (con)
        db_pool.release(con);

    if (update_count > 0)
    {
        cache_delete(key);
        return 200;
    }
    else
    {
        return 404;
    }
}

// -------------------- HTTP Handlers --------------------

void create_key_handler(const httplib::Request &req, httplib::Response &res)
{
    // Expect key as query param and body as value (keeps compatibility with your load generator)
    string key = req.get_param_value("key");
    string value = req.body;

    cout << "[REQ] Create key: " << key << " (len=" << value.size() << ")" << endl;

    if (key.empty())
    {
        res.status = 400;
        res.set_content("Missing key parameter", "text/plain");
        total_failures++;
        return;
    }

    bool ok = save_to_database(key, value);
    if (ok)
    {
        res.set_content("Successfully saved the key.", "text/plain");
        res.status = 200;
        total_requests++;
    }
    else
    {
        res.set_content("Failed to save the key to the database.", "text/plain");
        res.status = 500;
        total_failures++;
    }
}

void read_key_handler(const httplib::Request &req, httplib::Response &res)
{
    string key = req.get_param_value("key");
    cout << "[REQ] Read key: " << key << endl;

    if (key.empty())
    {
        res.status = 400;
        res.set_content("Missing key parameter", "text/plain");
        total_failures++;
        return;
    }

    auto result = get_from_database(key);
    int status = result.first;
    string value = result.second;

    if (status == 200)
    {
        res.set_content(value, "text/plain");
        res.status = 200;
        total_requests++;
    }
    else if (status == 404)
    {
        res.set_content("Key not found.", "text/plain");
        res.status = 404;
        total_requests++; // count as completed request
    }
    else
    {
        res.set_content("Internal server error.", "text/plain");
        res.status = 500;
        total_failures++;
    }
}

void delete_key_handler(const httplib::Request &req, httplib::Response &res)
{
    string key = req.get_param_value("key");
    cout << "[REQ] Delete key: " << key << endl;

    if (key.empty())
    {
        res.status = 400;
        res.set_content("Missing key parameter", "text/plain");
        total_failures++;
        return;
    }

    int status = delete_from_database(key);

    if (status == 200)
    {
        res.set_content("Key successfully deleted.", "text/plain");
        res.status = 200;
        total_requests++;
    }
    else if (status == 404)
    {
        res.set_content("Key not found or error during deletion.", "text/plain");
        res.status = 404;
        total_requests++;
    }
    else
    {
        res.set_content("Internal server error.", "text/plain");
        res.status = 500;
        total_failures++;
    }
}

void popular_read_handler(const httplib::Request &req, httplib::Response &res)
{
    string key = req.get_param_value("key");
    cout << "[REQ] Popular read key: " << key << endl;

    if (key.empty())
    {
        res.status = 400;
        res.set_content("Missing key parameter", "text/plain");
        total_failures++;
        return;
    }

    // Only check cache for popular reads (no DB hit)
    string value;
    {
        if (cache_get(key, value))
        {
            res.set_content(value, "text/plain");
            res.status = 200;
            total_requests++;
            return;
        }
        else
        {
            // Popular handler intentionally avoids DB to create memory-bound workload
            res.set_content("Key not found in cache for popular access.", "text/plain");
            res.status = 404;
            total_requests++;
            return;
        }
    }
}

// Return a JSON of metrics
void stats_handler(const httplib::Request & /*req*/, httplib::Response &res)
{
    std::ostringstream ss;
    ss << "{";
    ss << "\"total_requests\":" << total_requests.load() << ",";
    ss << "\"total_failures\":" << total_failures.load() << ",";
    ss << "\"cache_hits\":" << cache_hits.load() << ",";
    ss << "\"cache_misses\":" << cache_misses.load() << ",";
    ss << "\"db_calls\":" << db_calls.load() << ",";
    {
        std::lock_guard<std::mutex> lk(cache_mutex);
        ss << "\"cache_size\":" << cache_map.size() << ",";
    }
    ss << "\"pool_size\":" << DB_POOL_SIZE;
    ss << "}";
    res.set_content(ss.str(), "application/json");
    res.status = 200;
}

// -------------------- Main --------------------

int main(void)
{
    auto db_config = read_config("db.conf");
    if (db_config.empty())
    {
        cerr << "Error: db.conf not found or empty" << endl;
        return 1;
    }

    try
    {
        // read config values
        string db_host = db_config.at("DB_HOST");
        string db_user = db_config.at("DB_USER");
        string db_pass = db_config.at("DB_PASS");
        string db_name = db_config.at("DB_NAME");

        if (db_config.count("MAX_CACHE_SIZE"))
            MAX_CACHE_SIZE = stoi(db_config.at("MAX_CACHE_SIZE"));
        if (db_config.count("DB_POOL_SIZE"))
            DB_POOL_SIZE = stoi(db_config.at("DB_POOL_SIZE"));
        if (db_config.count("SERVER_PORT"))
            SERVER_PORT = stoi(db_config.at("SERVER_PORT"));

        cout << "CONFIG: host=" << db_host << " user=" << db_user << " schema=" << db_name << " pool=" << DB_POOL_SIZE << " cache=" << MAX_CACHE_SIZE << endl;

        // initialize the driver once
        driver_instance = get_driver_instance();

        // Initialize connection pool (this will create DB_POOL_SIZE connections)
        db_pool.init(db_host, db_user, db_pass, db_name, DB_POOL_SIZE);

        // Optional: pre-warm cache from DB or via other mechanism if desired (not done automatically)
    }
    catch (const exception &e)
    {
        cerr << "FATAL: Could not initialize DB or driver: " << e.what() << endl;
        return 1;
    }

    httplib::Server svr;

    svr.Post("/kv", [&](const httplib::Request &req, httplib::Response &res)
             { create_key_handler(req, res); });
    svr.Get("/kv", [&](const httplib::Request &req, httplib::Response &res)
            { read_key_handler(req, res); });
    svr.Delete("/kv", [&](const httplib::Request &req, httplib::Response &res)
               { delete_key_handler(req, res); });
    svr.Get("/kv_popular", [&](const httplib::Request &req, httplib::Response &res)
            { popular_read_handler(req, res); });
    svr.Get("/stats", [&](const httplib::Request &req, httplib::Response &res)
            { stats_handler(req, res); });

    cout << "Server with " << MAX_CACHE_SIZE << "-item LRU cache and DB pool size " << DB_POOL_SIZE << ". Starting on port " << SERVER_PORT << endl;

    if (!svr.listen("0.0.0.0", SERVER_PORT))
    {
        cerr << "\nFATAL ERROR: Server failed to listen on 0.0.0.0:" << SERVER_PORT << endl;
        cerr << "This is most likely a port conflict. Check with 'sudo lsof -i :" << SERVER_PORT << "'" << endl;
        return 1;
    }

    // cleanup (never reached normally)
    db_pool.cleanup();
    return 0;
}
