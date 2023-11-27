#include <chrono>
#define __stgcommon(name, line, status) {auto t = std::chrono::high_resolution_clock::now(); std::cerr << name << "," << status << "," << line << "," << std::chrono::duration_cast<std::chrono::nanoseconds>(t.time_since_epoch()).count() << std::endl;}
#define __stgst(name, line) {__stgcommon(name, line, "start")}
#define __stged(name, line) {__stgcommon(name, line, "end")}
// Write a simple multithreading program
#include <iostream>
#include <thread>
#include <vector>
#include <mutex>
#include <chrono>
#include <atomic>
#include <condition_variable>
#include <future>
#include <functional>
#include <queue>
#include <memory>
#include <string>
#include <sstream>
#include <fstream>

using namespace std;
const int N = 1000;

void work(int rank, int size){
    cout << "Hello from thread " << rank << " of " << size << endl;
    int a = 0;
    
    for (int i = 0; i < N; i++){
        for (int j = 0; j < N; j++){
            a += i*j * rand();
        }
    }
    cout << "Thread " << rank << " finished" << endl;
}


int main(int argc, char* argv[]){

    // Create 32 threads and each run a for loop
{__stgst("1", 36);}    cout << "Hello from main thread" << endl;{__stged("1", 36);}
{__stgst("2", 37);}    vector<thread> threads;{__stged("2", 37);}
{__stgst("3", 38);}    int size = 32;{__stged("3", 38);}
{__stgst("4", 39);}    for (int i = 0; i < size; i++){
        threads.push_back(thread(work, i, size));
    }{__stged("4", 41);}

    // Wait for all threads to finish
{__stgst("5", 44);}    for (int i = 0; i < size; i++){
        threads[i].join();
    }{__stged("5", 46);}

{__stgst("6", 48);}    cout << "Main thread finished" << endl;{__stged("6", 48);}

    return 0;


}
