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
atomic<long long> total_response_time_ms = 0; // To sum up response times

// --- REMOVED popular_keys vector ---

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

    cli.set_connection_timeout(2);
    cli.set_read_timeout(2);
    cli.set_write_timeout(2);

    std::random_device rd;
    std::mt19937 gen(rd());
    // --- REMOVED popular_dis ---
    std::uniform_int_distribution<> large_set_dis(1, 15000);

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
                res = cli.Post(url.c_str(), "some_random_value", "text/plain");
            }
            else
            {
                res = cli.Delete(url.c_str());
            }
        }
        else // --- NEW: "mix" workload (Get+Put) ---
        {
            string key = "key_" + to_string(large_set_dis(gen));
            url = "/kv?key=" + key;

            // Randomly choose 0 (GET), 1 (POST), or 2 (DELETE)
            int choice = large_set_dis(gen) % 3;

            if (choice == 0)
            {
                // Do a GET
                res = cli.Get(url.c_str());
            }
            else if (choice == 1)
            {
                // Do a POST
                res = cli.Post(url.c_str(), "some_random_value", "text/plain");
            }
            else
            {
                // Do a DELETE
                res = cli.Delete(url.c_str());
            }
        }
        // --- END OF NEW LOGIC ---

        auto end_time = chrono::steady_clock::now(); // Stop timer

        if (res)
        {
            auto duration_ms = chrono::duration_cast<chrono::milliseconds>(end_time - start_time).count();

            // --- REMOVED 'popular' workload logic ---

            if (workload_type == "get")
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
            // --- NEW: Success logic for "mix" ---
            else if (workload_type == "mix")
            {
                // We'll count any 200 (POST, GET-hit, DELETE-hit)
                // or 404 (GET-miss, DELETE-miss) as a successful operation.
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
            // --- END OF NEW LOGIC ---
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
        // --- UPDATED Usage Message ---
        cerr << "Usage: ./load_generator <num_threads> <duration_seconds> <workload_type>" << endl;
        cerr << "  workload_type can be 'get', 'put', or 'mix'" << endl;
        return 1;
    }

    int num_threads = stoi(argv[1]);
    int duration = stoi(argv[2]);
    string workload_type = argv[3];

    // --- UPDATED Workload Check ---
    if (workload_type != "get" && workload_type != "put" && workload_type != "mix")
    {
        cerr << "Invalid workload type. Choose 'get', 'put', or 'mix'." << endl;
        return 1;
    }

    string host = "127.0.0.1";
    int port = 9090;

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

    return 0;
}