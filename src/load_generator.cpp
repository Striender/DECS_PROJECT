#include "../lib/httplib.h"
#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>
#include <random>
#include <ctime>

using namespace std;

long long total_requests = 0;
long long total_failures = 0;
bool time_is_up = false;
long long total_response_time_ms = 0;

vector<string> popular_keys;


// Function to check if server is reachable before starting
bool ping_server(string host, int port)
{
    try
    {
        httplib::Client cli(host, port);
        cli.set_connection_timeout(2);
        auto res = cli.Head("/kv?key=ping_test");
        if (res)
        {
            // Even if key=ping_test doesn't exist, a 200 or 404 indicates the server is up
            if (res->status == 200 || res->status == 404)
            {
                return true;
            }
         }
        return false;
       
    }
    catch (const std::exception &e)
    {
        return false;
    }
}

void preload_popular_keys(const string &host, int port, const vector<string> &popular_keys)
{
    cout << "Preloading popular keys into server/cache..." << endl;

    httplib::Client preload_cli(host, port);
    preload_cli.set_connection_timeout(2);
    preload_cli.set_read_timeout(2);
    preload_cli.set_write_timeout(2);

    for (const auto &key : popular_keys)
    {
        string url = "/kv?key=" + key;
        auto res = preload_cli.Post(url.c_str(), "preload_value_" + key, "text/plain");

        if (res && res->status == 200)
            cout << "  ✅ Inserted " << key << endl;
        else
            cout << "  ⚠️  Failed to insert " << key << endl;
    }

    cout << "Preloading complete. Waiting briefly before test..." << endl;
    this_thread::sleep_for(chrono::seconds(2)); // Small delay for stability
}

void client_thread_function(string host, int port, string workload_type)
{
    httplib::Client cli(host, port);

    cli.set_connection_timeout(2);
    cli.set_read_timeout(2);
    cli.set_write_timeout(2);

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> large_set_dis(1, 10000);
    std::uniform_int_distribution<> popular_dis(0, popular_keys.size() - 1);


    while (!time_is_up)
    {
        string url;
        httplib::Result res;

        auto start_time = chrono::steady_clock::now(); // Start timer

        if (workload_type == "get")
        {
            string key = "key_" + to_string(large_set_dis(gen));
            url = "/kv?key=" + key;
            res = cli.Get(url.c_str());
        }
        else if (workload_type == "put") // This is the "Put all" (POST/DELETE mix)
        {
            string key = "key_" + to_string(large_set_dis(gen));
            url = "/kv?key=" + key;

            if (large_set_dis(gen) % 2 == 0)
            {
                res = cli.Post(url.c_str(), "some_random_value_" + key , "text/plain");
            }
            else
            {
                res = cli.Delete(url.c_str());
            }
        }
        
        else if (workload_type == "popular")
        {
            
            string key = popular_keys[popular_dis(gen)];
            url = "/kv?key=" + key; // Using the standard /kv endpoint for reads
            res = cli.Get(url.c_str());
        }
        else 
        {
            string key = "key_" + to_string(large_set_dis(gen));
            url = "/kv?key=" + key;
            // Randomly choose 0 (GET), 1 (POST), or 2 (DELETE)
            int choice = large_set_dis(gen) % 3;

            if (choice == 0)
            {
                res = cli.Get(url.c_str());
            }
            else if (choice == 1)
            {
                res = cli.Post(url.c_str(), "some_random_value", "text/plain");
            }
            else
            {
                res = cli.Delete(url.c_str());
            }
        }

        auto end_time = chrono::steady_clock::now(); // Stop timer

        if (res)
        {
            auto duration_ms = chrono::duration_cast<chrono::milliseconds>(end_time - start_time).count();

            if (workload_type == "popular")
            {
                if (res->status == 200 || res->status == 404)
                { // 404 is fine if the popular key isn't in DB/cache yet
                    total_requests++;
                    total_response_time_ms += duration_ms;
                }
                else
                {
                    total_failures++;
                }
            }
            else if (workload_type == "get")
            {
                if (res->status == 200 || res->status == 404) // 404 is a valid "not found" response
                {
                    total_requests++;
                    total_response_time_ms += duration_ms;
                }
                else
                {
                    total_failures++;
                }
            }
            else if (workload_type == "put")
            {
                if (res->status == 200)
                {
                    total_requests++;
                    total_response_time_ms += duration_ms;
                }
                else
                {
                    total_failures++;
                }
            }
            
            else if (workload_type == "mix")
            {
                if (res->status == 200 || res->status == 404)
                {
                    total_requests++;
                    total_response_time_ms += duration_ms;
                }
                else
                {
                    total_failures++;
                }
            }
            else
            {
                total_failures++;
            }
        }
        else
        {
            total_failures++;
        }
    }
}

int main(int argc, char *argv[])
{
    if (argc != 4)
    {
        cerr << "Usage: ./load_generator <num_threads> <duration_seconds> <workload_type>" << endl;
        cerr << "  workload_type can be 'get', 'put', 'mix', or 'popular'" << endl;
        return 1;
    }

    int num_threads = stoi(argv[1]);
    int duration = stoi(argv[2]);
    string workload_type = argv[3];

    if (workload_type != "get" && workload_type != "put" && workload_type != "mix" && workload_type != "popular")
    {
        cerr << "Invalid workload type. Choose 'get', 'put', 'mix', or 'popular'." << endl;
        return 1;
    }

    string host = "127.0.0.1";
    int port = 9000;

    cout << "Pinging server at " << host << ":" << port << "..." << endl;
    if (!ping_server(host, port))
    {
        cerr << "\nError: Unable to connect to the server." << endl;
        cerr << "Please ensure the server is running on " << host << ":" << port << endl;
        return 1;
    }
    cout << "Server connection successful." << endl;

    if (workload_type == "popular")
    {
        // Assuming MAX_CACHE_SIZE is 100 for the server (from server code)
        // We'll use 10 popular keys, which is a small subset guaranteed to be in cache.
        // It's assumed the cache will warm up naturally during the test, as per your request.
        for (int i = 1; i <= 10; ++i)
        { // Using keys 1-10 as popular
            popular_keys.push_back("key_" + to_string(i));
        }
        std::mt19937 g(std::chrono::system_clock::now().time_since_epoch().count());
        std::shuffle(popular_keys.begin(), popular_keys.end(), g);

        cout << "Initialized " << popular_keys.size() << " popular keys for workload." << endl;
        preload_popular_keys(host, port, popular_keys);
    }

   
    vector<thread> threads;
    cout << "Starting '" << workload_type << "' workload with " << num_threads << " threads for " << duration << " seconds..." << endl;

    for (int i = 0; i < num_threads; ++i)
    {
        threads.emplace_back(client_thread_function, host, port, workload_type);
    }

    cout << "Test running..." << endl;

    this_thread::sleep_for(chrono::seconds(duration));
    int actual_duration_ran = duration;

    time_is_up = true;

    cout << "Time is up. Detaching threads and calculating results..." << endl;

    for (auto &th : threads)
    {
        th.detach();
    }

    // Calculate both metrics
    double throughput = static_cast<double>(total_requests) / actual_duration_ran;
    double avg_response_time = 0.0;
    if (total_requests > 0)
    {
        avg_response_time = static_cast<double>(total_response_time_ms) / total_requests;
    }

    cout << "\n--- Results ---" << endl;
    cout << "Workload: " << workload_type << endl;
    cout << "Test ran for: " << actual_duration_ran << " seconds" << endl;
    cout << "Total requests completed: " << total_requests << endl;
    cout << "Total requests failed (timeout/error): " << total_failures << endl;
    cout << "Throughput: " << throughput << " successful requests/second" << endl;
    cout << "Average response time: " << avg_response_time << " ms" << endl;


    cout<<"================================================================================================"<<endl;
    return 0;
}