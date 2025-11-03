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

atomic<long long> total_requests = 0;
atomic<long long> total_failures = 0;
atomic<bool> time_is_up = false;

vector<string> popular_keys = {
    "key0", "key1", "key2", "key3", "key4",
    "key5", "key6", "key7", "key8", "key9"};

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
            return true;
        }
        else
        {
            return false;
        }
    }
    catch (const std::exception &e)
    {
        return false;
    }
}

void client_thread_function(string host, int port, string workload_type)
{
    httplib::Client cli(host, port);

    // Short timeouts are still good practice
    cli.set_connection_timeout(2);
    cli.set_read_timeout(2);
    cli.set_write_timeout(2);

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> popular_dis(0, 9);
    std::uniform_int_distribution<> large_set_dis(1, 15000);

    while (!time_is_up)
    {
        string url;
        httplib::Result res;

        if (workload_type == "popular")
        {
            string key_to_get = popular_keys[popular_dis(gen)];
            url = "/kv_popular?key=" + key_to_get;
            res = cli.Get(url.c_str());
        }
        else if (workload_type == "get")
        {
            string key = "key_" + to_string(large_set_dis(gen));
            url = "/kv?key=" + key;
            res = cli.Get(url.c_str());
        }
        else // "put"
        {
            string key = "key_" + to_string(large_set_dis(gen));
            url = "/kv?key=" + key;
            res = cli.Post(url.c_str(), "some_random_value", "text/plain");
        }

        if (res)
        {
            if (workload_type == "popular" && res->status == 200)
            {
                total_requests++;
            }
            else if (workload_type == "get")
            {
                if (res->status == 200 || res->status == 404)
                {
                    total_requests++;
                }
            }
            else if (workload_type == "put" && res->status == 200)
            {
                total_requests++;
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
        cerr << "  workload_type can be 'popular', 'get', or 'put'" << endl;
        return 1;
    }

    int num_threads = stoi(argv[1]);
    int duration = stoi(argv[2]);
    string workload_type = argv[3];

    if (workload_type != "popular" && workload_type != "get" && workload_type != "put")
    {
        cerr << "Invalid workload type. Choose 'popular', 'get', or 'put'." << endl;
        return 1;
    }

    string host = "127.0.0.1";
    int port = 8080;

    cout << "Pinging server at " << host << ":" << port << "..." << endl;
    if (!ping_server(host, port))
    {
        cerr << "\nError: Unable to connect to the server." << endl;
        cerr << "Please ensure the server is running on " << host << ":" << port << endl;
        return 1;
    }
    cout << "Server connection successful." << endl;

    // --- NO CACHE WARM-UP ---

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

    // --- THIS IS THE FIX ---
    // We replace join() with detach()
    // This will stop the main thread from waiting.
    for (auto &th : threads)
    {
        th.detach(); // <-- CHANGED FROM join()
    }
    // --- END OF FIX ---

    double throughput = static_cast<double>(total_requests) / actual_duration_ran;

    cout << "\n--- Results ---" << endl;
    cout << "Workload: " << workload_type << endl;
    cout << "Test ran for: " << actual_duration_ran << " seconds" << endl;
    cout << "Total requests completed: " << total_requests << endl;
    cout << "Total requests failed (timeout/error): " << total_failures << endl;
    cout << "Throughput: " << throughput << " successful requests/second" << endl;

    return 0; // Main thread exits, OS kills all detached threads.
}