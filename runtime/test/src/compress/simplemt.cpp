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
    cout << "Hello from main thread" << endl;
    vector<thread> threads;
    int size = 32;
    for (int i = 0; i < size; i++){
        threads.push_back(thread(work, i, size));
    }

    // Wait for all threads to finish
    for (int i = 0; i < size; i++){
        threads[i].join();
    }

    cout << "Main thread finished" << endl;

    return 0;


}