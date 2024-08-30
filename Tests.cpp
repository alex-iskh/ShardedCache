#include <thread>
#include <functional>
#include <condition_variable>
#include <random>
#include <chrono>
#include <iostream>
#include <fstream>
#include <array>

#include "SynchronizedContainers.h"

const auto hardware_concurrency = (size_t)std::thread::hardware_concurrency();

class TaskPool
{
public:
    template <typename Callable>
    TaskPool(size_t poolSize, Callable task)
    {
        for (auto i = 0; i < poolSize; ++i)
        {
            _workers.emplace_back(task);
        }
    }

    ~TaskPool()
    {
        for (auto& worker : _workers)
        {
            if (worker.joinable())
                worker.join();
        }
    }
private:
    std::vector<std::thread> _workers;
};

template <typename CacheImpl>
class Test
{
public:
    template <typename CacheImpl = ShardedCache, typename ... CacheArgs>
    Test(const int testrunsNum, const size_t writeWorkersNum, const size_t popWorkersNum,
        const std::string& resultsFile, CacheArgs&& ... cacheArgs) :
        _cache(std::forward<CacheArgs>(cacheArgs)...),
        _writeWorkersNum(writeWorkersNum), _popWorkersNum(popWorkersNum),
        _resultsFile(resultsFile),
        _testrunsNum(testrunsNum), _testStarted (false)
    {
        std::random_device rd;
        _randomGenerator = std::mt19937(rd());
    }


    void run()
    {
        for (auto i = 0; i < _testrunsNum; ++i)
        {
            runSingleTest();
            logResults();
        }
    }

private:
    void runSingleTest()
    {
        {
            std::lock_guard<std::mutex> lock(_testStartSync);
            _testStarted = false;
        }

        // these pools won't just fire as many operations as they can,
        // but will emulate real-time occuring requests to the cache in multithreaded environment
        auto writeTestPool = TaskPool(_writeWorkersNum, std::bind(&Test::writeTransactions, this));
        auto popTestPool = TaskPool(_popWorkersNum, std::bind(&Test::popTransactions, this));

        _writeTime = 0;
        _writeOpNum = 0;
        _popTime = 0;
        _popOpNum = 0;

        {
            std::lock_guard<std::mutex> lock(_testStartSync);
            _testStarted = true;
            _testStartCv.notify_all();
        }
    }

    void logResults()
    {
        std::cout << "===============================================" << std::endl;
        std::cout << "Writing operations number per sec:\t" << _writeOpNum / 60. << std::endl;
        std::cout << "Writing operations avg time (mcsec):\t" << (double)_writeTime / _writeOpNum << std::endl;
        std::cout << "Pop operations number per sec:    \t" << _popOpNum / 60. << std::endl;
        std::cout << "Pop operations avg time (mcsec):  \t" << (double)_popTime / _popOpNum << std::endl;

        std::ofstream resultsFilestream;
        resultsFilestream.open(_resultsFile, std::ios_base::app);
        resultsFilestream << _writeOpNum / 60. << "," << (double)_writeTime / _writeOpNum << ","
            << _popOpNum / 60. << "," << (double)_popTime / _popOpNum << std::endl;

        std::cout << "Results saved to file " << _resultsFile << std::endl;
    }

    void writeTransactions()
    {
        {
            std::unique_lock<std::mutex> lock(_testStartSync);
            _testStartCv.wait(lock, [this] { return _testStarted; });
        }
        std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();

        // hypothetical system has around 100k currently active users
        std::uniform_int_distribution<> userDistribution(1, 100000);

        // delay up to 5 ms for every thread not to start simultaneously
        std::uniform_int_distribution<> waitTimeDistribution(0, 5000);
        std::this_thread::sleep_for(std::chrono::microseconds(waitTimeDistribution(_randomGenerator)));

        for (
            auto iterationStart = std::chrono::steady_clock::now();
            iterationStart - start < std::chrono::minutes(1);
            iterationStart = std::chrono::steady_clock::now())
        {
            auto generatedUser = userDistribution(_randomGenerator);
            TransactionData dummyTransaction = {
                5477311,
                generatedUser,
                1824507435,
                8055.05,
                0,
                "regular transaction by " + std::to_string(generatedUser)};

            std::chrono::steady_clock::time_point operationStart = std::chrono::steady_clock::now();
            _cache.write(dummyTransaction);
            std::chrono::steady_clock::time_point operationEnd = std::chrono::steady_clock::now();

            ++_writeOpNum;
            _writeTime += std::chrono::duration_cast<std::chrono::microseconds>(operationEnd - operationStart).count();

            // make span between iterations at least 5ms
            std::this_thread::sleep_for(iterationStart + std::chrono::milliseconds(5) - std::chrono::steady_clock::now());
        }
    }

    void popTransactions()
    {
        {
            std::unique_lock<std::mutex> lock(_testStartSync);
            _testStartCv.wait(lock, [this] { return _testStarted; });
        }
        std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();

        // hypothetical system has around 100k currently active users
        std::uniform_int_distribution<> userDistribution(1, 100000);

        // delay up to 100 ms for every thread not to start simultaneously
        std::uniform_int_distribution<> waitTimeDistribution(0, 100000);
        std::this_thread::sleep_for(std::chrono::microseconds(waitTimeDistribution(_randomGenerator)));

        for (
            auto iterationStart = std::chrono::steady_clock::now();
            iterationStart - start < std::chrono::minutes(1);
            iterationStart = std::chrono::steady_clock::now())
        {
            auto requestedUser = userDistribution(_randomGenerator);

            std::chrono::steady_clock::time_point operationStart = std::chrono::steady_clock::now();
            auto userTransactions = _cache.pop(requestedUser);
            std::chrono::steady_clock::time_point operationEnd = std::chrono::steady_clock::now();

            ++_popOpNum;
            _popTime += std::chrono::duration_cast<std::chrono::microseconds>(operationEnd - operationStart).count();

            // make span between iterations at least 100ms
            std::this_thread::sleep_for(iterationStart + std::chrono::milliseconds(100) - std::chrono::steady_clock::now());
        }
    }

    CacheImpl _cache;

    std::atomic<long> _writeTime;
    std::atomic<long> _writeOpNum;
    std::atomic<long> _popTime;
    std::atomic<long> _popOpNum;

    size_t _writeWorkersNum;
    size_t _popWorkersNum;
    std::string _resultsFile;
    int _testrunsNum;
    bool _testStarted;
    std::mutex _testStartSync;
    std::condition_variable _testStartCv;
    std::mt19937 _randomGenerator;
};

void testCaches(const size_t testedShardSize, const size_t workersNum)
{
    if (testedShardSize == 1)
    {
        auto simpleImplTest = Test<SimpleSynchronizedCache>(
            10, workersNum, workersNum, "simple_cache_tests(" + std::to_string(workersNum) + "_workers).csv");

        simpleImplTest.run();
    }
    else
    {
        auto shardedImpl4Test = Test<ShardedCache>(
            10, workersNum, workersNum, "sharded_cache_" + std::to_string(testedShardSize) + "_tests(" + std::to_string(workersNum) + "_workers).csv", 4);

        shardedImpl4Test.run();
    }
}

int main()
{
    std::cout << "Hardware concurrency: " << hardware_concurrency << std::endl;

    std::array<size_t, 7> testPlan = { 1, 4, 8, 32, 128, 4096, 100000 };

    for (auto i = 0; i < testPlan.size(); ++i)
    {
        testCaches(testPlan[i], 4 * hardware_concurrency);
    }

    // additional tests with diminished load to show limits of optimization advantage
    std::array<size_t, 4> additionalTestPlan = { 1, 8, 128, 100000 };

    for (auto i = 0; i < additionalTestPlan.size(); ++i)
    {
        testCaches(additionalTestPlan[i], hardware_concurrency);
    }
}
