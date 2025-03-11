#pragma once
#include <chrono>

void simulateWorkload(const long timeSpanMcs)
{
    auto start = std::chrono::steady_clock::now();

    do
    {
        volatile double x = 0.0;
        for (int i = 0; i < 10000; ++i) {
            x += i * 0.001;
        }

        // small sleep to prevent full CPU utilization
        std::this_thread::sleep_for(std::chrono::microseconds(5));

    } while (std::chrono::steady_clock::now() - start < std::chrono::microseconds(timeSpanMcs));
}