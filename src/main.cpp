// Define the thread pool count BEFORE including the library
#define CPPHTTPLIB_THREAD_POOL_COUNT 10

#include "../lib/httplib.h"
#include <iostream>
#include <fstream>
#include <string>
#include <map>
#include <mutex>

#include <mysql_connection.h>
#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/prepared_statement.h>
#include <cppconn/resultset.h>

using namespace std;

// --- Global Cache and Mutexes ---
map<string, string> cache;
mutex cache_mutex;
mutex db_mutex; // <-- This is the dedicated mutex for the database

// --- Helper function to read the config file ---
map<string, string> read_config(const string &filename)
{
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

// --- Database Function (CREATE) ---
bool save_to_database(const string &key, const string &value, const map<string, string> &db_config)
{
    lock_guard<mutex> guard(db_mutex);
    try
    {
        sql::Driver *driver;
        sql::Connection *con;
        sql::PreparedStatement *pstmt;
        driver = get_driver_instance();
        con = driver->connect(db_config.at("DB_HOST"), db_config.at("DB_USER"), db_config.at("DB_PASS"));
        con->setSchema(db_config.at("DB_NAME"));

        // --- THIS IS THE ONLY LINE THAT CHANGES ---
        // It now updates the key if it already exists.
        pstmt = con->prepareStatement("INSERT INTO kv_pairs(item_key, item_value) VALUES(?, ?) ON DUPLICATE KEY UPDATE item_value = VALUES(item_value)");

        pstmt->setString(1, key);
        pstmt->setString(2, value);
        pstmt->executeUpdate();
        delete pstmt;
        delete con;

        lock_guard<mutex> cache_guard(cache_mutex);
        cache[key] = value;
        cout << "  [CACHE] Stored/Updated key: " << key << endl;
        return true;
    }
    catch (const exception &e)
    {
        cerr << "DATABASE ERROR (save): " << e.what() << endl;
        return false;
    }
}
// --- Database Function (READ) ---
string get_from_database(const string &key, const map<string, string> &db_config)
{
    {
        lock_guard<mutex> cache_guard(cache_mutex);
        if (cache.count(key))
        {
            cout << "  [CACHE HIT] Found key: " << key << endl;
            return cache[key];
        }
    }

    cout << "  [CACHE MISS] Key not found, checking DB for: " << key << endl;
    lock_guard<mutex> guard(db_mutex); // Lock the database for this entire function
    try
    {
        sql::Driver *driver;
        sql::Connection *con;
        sql::PreparedStatement *pstmt;
        sql::ResultSet *res;
        driver = get_driver_instance();
        con = driver->connect(db_config.at("DB_HOST"), db_config.at("DB_USER"), db_config.at("DB_PASS"));
        con->setSchema(db_config.at("DB_NAME"));
        pstmt = con->prepareStatement("SELECT item_value FROM kv_pairs WHERE item_key = ?");
        pstmt->setString(1, key);
        res = pstmt->executeQuery();
        string value = "";
        if (res->next())
        {
            value = res->getString("item_value");
            lock_guard<mutex> cache_guard(cache_mutex);
            cache[key] = value;
            cout << "  [CACHE] Stored key from DB: " << key << endl;
        }
        delete res;
        delete pstmt;
        delete con;
        return value;
    }
    catch (const exception &e)
    {
        cerr << "DATABASE ERROR (get): " << e.what() << endl;
        return "";
    }
}

// --- Database Function (DELETE) ---
bool delete_from_database(const string &key, const map<string, string> &db_config)
{
    lock_guard<mutex> guard(db_mutex); // Lock the database for this entire function
    try
    {
        sql::Driver *driver;
        sql::Connection *con;
        sql::PreparedStatement *pstmt;
        driver = get_driver_instance();
        con = driver->connect(db_config.at("DB_HOST"), db_config.at("DB_USER"), db_config.at("DB_PASS"));
        con->setSchema(db_config.at("DB_NAME"));
        pstmt = con->prepareStatement("DELETE FROM kv_pairs WHERE item_key = ?");
        pstmt->setString(1, key);
        int update_count = pstmt->executeUpdate();
        delete pstmt;
        delete con;

        if (update_count > 0)
        {
            lock_guard<mutex> cache_guard(cache_mutex);
            cache.erase(key);
            cout << "  [CACHE] Deleted key: " << key << endl;
            return true;
        }
        return false;
    }
    catch (const exception &e)
    {
        cerr << "DATABASE ERROR (delete): " << e.what() << endl;
        return false;
    }
}

// --- HTTP Handlers ---
void create_key_handler(const httplib::Request &req, httplib::Response &res, const map<string, string> &db_config)
{
    string key = req.get_param_value("key");
    string value = req.body;
    cout << "Received request to create key: " << key << endl;
    bool success = save_to_database(key, value, db_config);
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
void read_key_handler(const httplib::Request &req, httplib::Response &res, const map<string, string> &db_config)
{
    string key = req.get_param_value("key");
    cout << "Received request to read key: " << key << endl;
    string value = get_from_database(key, db_config);
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
void delete_key_handler(const httplib::Request &req, httplib::Response &res, const map<string, string> &db_config)
{
    string key = req.get_param_value("key");
    cout << "Received request to delete key: " << key << endl;
    bool success = delete_from_database(key, db_config);
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

// --- Main Function ---
int main(void)
{
    auto db_config = read_config("db.conf");
    if (db_config.empty())
    {
        return 1;
    }
    httplib::Server svr;
    svr.Post("/kv", [&](const httplib::Request &req, httplib::Response &res)
             { create_key_handler(req, res, db_config); });
    svr.Get("/kv", [&](const httplib::Request &req, httplib::Response &res)
            { read_key_handler(req, res, db_config); });
    svr.Delete("/kv", [&](const httplib::Request &req, httplib::Response &res)
               { delete_key_handler(req, res, db_config); });
    cout << "Server with cache and thread pool starting on port 8080" << endl;
    svr.listen("0.0.0.0", 8080);
    return 0;
}