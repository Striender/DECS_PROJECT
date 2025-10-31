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
atomic<bool> time_is_up = false;
atomic<bool> server_is_down = false;

vector<string> popular_keys = {
    "key0", "key1", "key2", "key3", "key4",
    "key5", "key6", "key7", "key8", "key9"};

void client_thread_function(string host, int port, string workload_type)
{
    httplib::Client cli(host, port);
    cli.set_connection_timeout(2);
    cli.set_read_timeout(2);
    cli.set_write_timeout(2);

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> popular_dis(0, 9);
    std::uniform_int_distribution<> large_set_dis(1, 10000);

    while (!time_is_up && !server_is_down)
    {
        string url;
        httplib::Result res;

        if (workload_type == "popular")
        {
            string key_to_get = popular_keys[popular_dis(gen)];
            url = "/kv?key=" + key_to_get;
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
            if (res->status == 200 || res->status == 404)
            {
                total_requests++;
            }
        }
        else
        {
            // If res is false, a connection error occurred
            server_is_down = true;
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

    // Make sure these match your server!
    string host = "127.0.0.1";
    int port = 8080;

    vector<thread> threads;
    cout << "Starting '" << workload_type << "' workload with " << num_threads << " threads for " << duration << " seconds..." << endl;

    for (int i = 0; i < num_threads; ++i)
    {
        threads.emplace_back(client_thread_function, host, port, workload_type);
    }

    // --- THIS IS THE UPDATED LOGIC ---
    // Sleep in 1-second naps and check if the server died
    cout << "Test running..." << endl;
    for (int i = 0; i < duration; i++)
    {
        if (server_is_down)
        {
            cout << "Connection to server lost! Stopping test early." << endl;
            break; // Exit the sleep loop immediately
        }
        this_thread::sleep_for(chrono::seconds(1));
    }
    // --- END OF UPDATED LOGIC ---

    time_is_up = true; // Tell all threads to stop

    cout << "Time is up. Waiting for threads to finish..." << endl;
    for (auto &th : threads)
    {
        th.join();
    }

    if (server_is_down)
    {
        cout << "\nWARNING: Test stopped early because the server connection was lost." << endl;
    }

    // Calculate throughput based on how long the test *actually* ran
    // This is a more complex calculation, but for now, we'll just report the raw numbers.
    double throughput = static_cast<double>(total_requests) / duration;

    cout << "\n--- Results ---" << endl;
    cout << "Workload: " << workload_type << endl;
    cout << "Total requests completed: " << total_requests << endl;
    cout << "Throughput: " << throughput << " requests/second (based on full duration)" << endl;

    return 0;
}