// Define the thread pool count BEFORE including the library
#define CPPHTTPLIB_THREAD_POOL_COUNT 10

#include "../lib/httplib.h"
#include <iostream>
#include <fstream>
#include <string>
#include <map>
#include <mutex>
#include <list>
#include <unordered_map>

#include <mysql_connection.h>
#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/prepared_statement.h>
#include <cppconn/resultset.h>

using namespace std;

// --- Global Cache and Mutexes ---
const size_t MAX_CACHE_SIZE = 1000;
list<pair<string, string>> lru_list;
unordered_map<string, list<pair<string, string>>::iterator> cache_map;
mutex cache_mutex;
mutex db_mutex;

// --- NEW: A single, global database connection ---
sql::Connection *global_con;

// --- Helper function to read the config file ---
map<string, string> read_config(const string &filename)
{ /* ... (Same as before) ... */
    map<string, string> config;
    ifstream config_file(filename);
    string line;
    if (config_file.is_open())
    {
        while (getline(config_file, line))
        {
            size_t separator_pos = line.find('=');
            if (separator_pos != string::npos)
            {
                string key = line.substr(0, separator_pos);
                string value = line.substr(separator_pos + 1);
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

// --- Cache helper functions ---
void move_to_back(const string &key)
{
    lru_list.splice(lru_list.end(), lru_list, cache_map[key]);
}

void add_to_cache(const string &key, const string &value)
{
    if (cache_map.size() >= MAX_CACHE_SIZE)
    {
        string lru_key = lru_list.front().first;
        lru_list.pop_front();
        cache_map.erase(lru_key);
        // cout << "  [CACHE EVICT] Evicted key: " << lru_key << endl;
    }
    lru_list.push_back({key, value});
    cache_map[key] = --lru_list.end();
    // cout << "  [CACHE] Stored key: " << key << endl;
}

// --- Database Function (CREATE/UPDATE) ---
// --- UPDATED: No longer takes db_config, as it uses the global connection ---
bool save_to_database(const string &key, const string &value)
{
    lock_guard<mutex> db_guard(db_mutex); // Lock the database first
    try
    {
        // --- REMOVED: All connection logic (driver, connect, etc.) ---
        sql::PreparedStatement *pstmt;

        // --- UPDATED: Use the global connection ---
        pstmt = global_con->prepareStatement("INSERT INTO kv_pairs(item_key, item_value) VALUES(?, ?) ON DUPLICATE KEY UPDATE item_value = VALUES(item_value)");

        pstmt->setString(1, key);
        pstmt->setString(2, value);
        pstmt->executeUpdate();
        delete pstmt;
        // --- DO NOT delete the connection ---
    }
    catch (const exception &e)
    {
        cerr << "DATABASE ERROR (save): " << e.what() << endl;
        return false;
    }

    // Now update the cache
    lock_guard<mutex> cache_guard(cache_mutex);
    if (cache_map.count(key))
    {
        cache_map[key]->second = value;
        move_to_back(key);
    }
    else
    {
        add_to_cache(key, value);
    }
    return true;
}

// --- Database Function (READ) ---
// --- UPDATED: No longer takes db_config ---
string get_from_database(const string &key)
{
    {
        lock_guard<mutex> cache_guard(cache_mutex);
        if (cache_map.count(key))
        {
            // cout << "  [CACHE HIT] Found key: " << key << endl;
            move_to_back(key);
            return cache_map[key]->second;
        }
    }

    // cout << "  [CACHE MISS] Key not found, checking DB for: " << key << endl;

    string value = "";
    lock_guard<mutex> db_guard(db_mutex);
    try
    {
        // --- REMOVED: All connection logic ---
        sql::PreparedStatement *pstmt;
        sql::ResultSet *res;

        // --- UPDATED: Use the global connection ---
        pstmt = global_con->prepareStatement("SELECT item_value FROM kv_pairs WHERE item_key = ?");
        pstmt->setString(1, key);
        res = pstmt->executeQuery();

        if (res->next())
        {
            value = res->getString("item_value");
        }
        delete res;
        delete pstmt;
    }
    catch (const exception &e)
    {
        cerr << "DATABASE ERROR (get): " << e.what() << endl;
        return "";
    }

    if (!value.empty())
    {
        lock_guard<mutex> cache_guard(cache_mutex);
        add_to_cache(key, value);
    }
    return value;
}

// --- Database Function (DELETE) ---
// --- UPDATED: No longer takes db_config ---
bool delete_from_database(const string &key)
{
    lock_guard<mutex> db_guard(db_mutex);
    int update_count = 0;
    try
    {
        // --- REMOVED: All connection logic ---
        sql::PreparedStatement *pstmt;

        // --- UPDATED: Use the global connection ---
        pstmt = global_con->prepareStatement("DELETE FROM kv_pairs WHERE item_key = ?");
        pstmt->setString(1, key);
        update_count = pstmt->executeUpdate();
        delete pstmt;
    }
    catch (const exception &e)
    {
        cerr << "DATABASE ERROR (delete): " << e.what() << endl;
        return false;
    }

    if (update_count > 0)
    {
        lock_guard<mutex> cache_guard(cache_mutex);
        if (cache_map.count(key))
        {
            lru_list.erase(cache_map[key]);
            cache_map.erase(key);
            // cout << "  [CACHE] Deleted key: " << key << endl;
        }
        return true;
    }
    return false;
}

// --- HTTP Handlers (PERFORMANCE VERSION) ---
// --- UPDATED: All logging is removed from handlers for max performance ---
void create_key_handler(const httplib::Request &req, httplib::Response &res)
{
    string key = req.get_param_value("key");
    string value = req.body;
    cout << "Received request to create key: " << key << endl;
    bool success = save_to_database(key, value);
    if (success)
    {
        res.set_content("Successfully saved the key.", "text/plain");
    }
    else
    {
        res.set_content("Failed to save the key to the database.", "text/plain");
        res.status = 500;
    }
}
void read_key_handler(const httplib::Request &req, httplib::Response &res)
{
    string key = req.get_param_value("key");
    cout << "Received request to read key: " << key << endl;
    string value = get_from_database(key);
    if (!value.empty())
    {
        res.set_content(value, "text/plain");
    }
    else
    {
        res.set_content("Key not found.", "text/plain");
        res.status = 404;
    }
}
void delete_key_handler(const httplib::Request &req, httplib::Response &res)
{
    string key = req.get_param_value("key");
    cout << "Received request to delete key: " << key << endl;
    bool success = delete_from_database(key);
    if (success)
    {
        res.set_content("Key successfully deleted.", "text/plain");
    }
    else
    {
        res.set_content("Key not found or error during deletion.", "text/plain");
        res.status = 404;
    }
}
void popular_read_handler(const httplib::Request &req, httplib::Response &res)
{
    string key = req.get_param_value("key");
    cout << "Received popular request for key: " << key << endl;

    lock_guard<mutex> cache_guard(cache_mutex);
    if (cache_map.count(key))
    {
        // --- CACHE HIT ---
        cout << "  [CACHE HIT] Found popular key: " << key << endl;
        move_to_back(key); // Mark as recently used
        string value = cache_map[key]->second;
        res.set_content(value, "text/plain");
        res.status = 200;
    }
    else
    {
        // --- CACHE MISS ---
        cout << "  [CACHE MISS] Popular key not in cache: " << key << endl;
        res.set_content("Key not found in cache.", "text/plain");
        res.status = 404;
    }
}

// --- Main Function ---
int main(void)
{
    auto db_config = read_config("db.conf");
    if (db_config.empty())
    {
        return 1;
    }

    // --- NEW: Create the single, shared DB connection on startup ---
    try
    {
        sql::Driver *driver;
        driver = get_driver_instance();
        global_con = driver->connect(db_config.at("DB_HOST"), db_config.at("DB_USER"), db_config.at("DB_PASS"));
        global_con->setSchema(db_config.at("DB_NAME"));
        cout << "Successfully connected to the database." << endl;
    }
    catch (const exception &e)
    {
        cerr << "FATAL: Could not connect to database on startup: " << e.what() << endl;
        return 1;
    }
    // --- END OF NEW BLOCK ---

    httplib::Server svr;

    svr.Post("/kv", [&](const httplib::Request &req, httplib::Response &res)
             { create_key_handler(req, res); });
    svr.Get("/kv", [&](const httplib::Request &req, httplib::Response &res)
            { read_key_handler(req, res); });
    svr.Delete("/kv", [&](const httplib::Request &req, httplib::Response &res)
               { delete_key_handler(req, res); });
    svr.Get("/kv_popular", [&](const httplib::Request &req, httplib::Response &res)
            { popular_read_handler(req, res); });

    cout << "Server with " << MAX_CACHE_SIZE << "-item LRU cache starting on port 8080" << endl;
    svr.listen("0.0.0.0", 8080);

    delete global_con;
    return 0;
}