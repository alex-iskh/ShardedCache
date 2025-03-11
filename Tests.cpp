#include <thread>
#include <functional>
#include <condition_variable>
#include <random>
#include <chrono>
#include <iostream>
#include <fstream>
#include <array>

#include "SynchronizedContainers.h"
#include "ReferenceContainers.h"

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
    template <typename CacheImpl = ShardedCache<std::map>, typename ... CacheArgs>
    Test(const int testrunsNum, const size_t writeWorkersNum, const size_t popWorkersNum,
        const std::string& testHeader, CacheArgs&& ... cacheArgs) :
        _cache(std::forward<CacheArgs>(cacheArgs)...),
        _writeWorkersNum(writeWorkersNum), _popWorkersNum(popWorkersNum),
        _resultsFile("results.csv"),
        _testHeader(testHeader),
        _testrunsNum(testrunsNum), _testStarted (false)
    {
        std::random_device rd;
        _randomGenerator = std::mt19937(rd());
    }

    template <typename CacheImpl = SimpleSynchronizedCache<std::map>>
    Test(const int testrunsNum, const size_t writeWorkersNum, const size_t popWorkersNum,
        const std::string& testHeader) :
        _cache(),
        _writeWorkersNum(writeWorkersNum), _popWorkersNum(popWorkersNum),
        _resultsFile("results.csv"),
        _testHeader(testHeader),
        _testrunsNum(testrunsNum), _testStarted(false)
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
        auto writeTestPool = TaskPool(_writeWorkersNum, std::bind(&Test::writeOperations, this));
        auto popTestPool = TaskPool(_popWorkersNum, std::bind(&Test::popOperations, this));

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
        resultsFilestream << _testHeader << ","
            << _writeWorkersNum << "," << _writeOpNum / 60. << "," << (double)_writeTime / _writeOpNum << ","
            << _popWorkersNum << "," << _popOpNum / 60. << "," << (double)_popTime / _popOpNum << std::endl;

        std::cout << "Results saved to file " << _resultsFile << std::endl;
    }

    void writeOperations()
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

    void popOperations()
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

    std::atomic<long long> _writeTime;
    std::atomic<long long> _writeOpNum;
    std::atomic<long long> _popTime;
    std::atomic<long long> _popOpNum;

    size_t _writeWorkersNum;
    size_t _popWorkersNum;
    std::string _resultsFile;
    std::string _testHeader;
    int _testrunsNum;
    bool _testStarted;
    std::mutex _testStartSync;
    std::condition_variable _testStartCv;
    std::mt19937 _randomGenerator;
};

template <template <typename ...> typename MapImpl>
void testCaches(const size_t testsNum, const size_t testedShardSize, const size_t workersNum)
{
    if (testedShardSize == 1)
    {
        auto simpleImplTest = Test<SimpleSynchronizedCache<MapImpl>>(
            testsNum, workersNum, workersNum, "SimpleSynchronizedCache w/ " + std::string(typeid(MapImpl).name()));

        simpleImplTest.run();
    }
    else
    {
        auto shardedImplTest = Test<ShardedCache<MapImpl>>(
            testsNum, workersNum, workersNum,
            "ShardedCache w/ " + std::string(typeid(MapImpl).name()) + " " + std::to_string(testedShardSize) + " shards",
            testedShardSize);

        shardedImplTest.run();
    }
}

#ifndef NO_REFERENCE_CONTAINERS
void testReferenceCaches(const size_t testsNum, const size_t workersNum)
{
    auto boostTest = Test<BoostConcurrentFlatMap>(
        testsNum, workersNum, workersNum, "BoostConcurrentFlatMap");

    boostTest.run();

    auto tbbTest = Test<TbbConcurrentHashMap>(
        testsNum, workersNum, workersNum, "TbbConcurrentHashMap");

    tbbTest.run();
}
#endif // !NO_REFERENCE_CONTAINERS

int main()
{
    std::cout << "Hardware concurrency: " << hardware_concurrency << std::endl;

    std::array<size_t, 10> testPlan = { 4, 8, 16, 32, 64, 128, 1024, 4096, 100000, 1 };
    size_t testsNum = 20;

    size_t workersNum = 32 * hardware_concurrency;
    size_t reducedWorkersNum = hardware_concurrency;

    // comparing simple cache implementation with sharded cache with differend shard sizes
    // (all caches built on std::map)

    for (auto i = 0; i < testPlan.size(); ++i)
    {
        testCaches<std::map>(testsNum, testPlan[i], workersNum);
    }

    // additional tests with diminished load to show limits of optimization advantage

    std::array<size_t, 4> additionalTestPlan = { 1, 8, 128, 100000 };

    for (auto i = 0; i < additionalTestPlan.size(); ++i)
    {
        testCaches<std::map>(testsNum, additionalTestPlan[i], reducedWorkersNum);
    }

    // comparing simple cache implementation with sharded cache with differend shard sizes
    // (all caches built on std::unordered_map)

    for (auto i = 0; i < testPlan.size(); ++i)
    {
        testCaches<std::unordered_map>(testsNum, testPlan[i], workersNum);
    }

#ifndef NO_REFERENCE_CONTAINERS
    // running the same tests with some popular concurrent maps for reference

    testReferenceCaches(testsNum, workersNum);
#endif // !NO_REFERENCE_CONTAINERS
}
