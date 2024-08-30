#pragma once
#include <vector>
#include <map>
#include <set>
#include <mutex>
#include <shared_mutex>
#include <memory>

#include "TransactionData.h"

class SimpleSynchronizedCache
{
public:
    void write(const TransactionData& transaction)
    {
        std::lock_guard<std::mutex> lock(_cacheMutex);
        _transactionCache[transaction.userId].push_back(transaction);
    }

    std::vector<TransactionData> read(const long& userId)
    {
        std::lock_guard<std::mutex> lock(_cacheMutex);

        try
        {
            return _transactionCache.at(userId);
        }
        catch (const std::out_of_range& ex)
        {
            return std::vector<TransactionData>();
        }
    }

    std::vector<TransactionData> pop(const long& userId)
    {
        std::lock_guard<std::mutex> lock(_cacheMutex);
        auto userNode = _transactionCache.extract(userId);
        return userNode.empty() ? std::vector<TransactionData>() : std::move(userNode.mapped());
    }

private:
    std::map<int, std::vector<TransactionData>> _transactionCache;
    std::mutex _cacheMutex;
};

class CacheWithSharedMutex
{
public:
    void write(const TransactionData& transaction)
    {
        std::lock_guard<std::shared_mutex> lock(_cacheMutex);
        _transactionCache[transaction.userId].push_back(transaction);
    }

    std::vector<TransactionData> read(const long& userId)
    {
        std::shared_lock<std::shared_mutex> lock(_cacheMutex);

        try
        {
            return _transactionCache.at(userId);
        }
        catch (const std::out_of_range& ex)
        {
            return std::vector<TransactionData>();
        }
    }

    std::vector<TransactionData> pop(const long& userId)
    {
        std::lock_guard<std::shared_mutex> lock(_cacheMutex);
        auto userNode = _transactionCache.extract(userId);
        return userNode.empty() ? std::vector<TransactionData>() : std::move(userNode.mapped());
    }

private:
    std::map<int, std::vector<TransactionData>> _transactionCache;
    std::shared_mutex _cacheMutex;
};

class ShardedCache
{
public:
    ShardedCache(size_t shardSize):
        _shardSize(shardSize),
        _transactionCaches(shardSize)
    {
        std::generate(
            _transactionCaches.begin(),
            _transactionCaches.end(),
            []() { return std::make_unique<SimpleSynchronizedCache>(); });
    }

    void write(const TransactionData& transaction)
    {
        _transactionCaches[transaction.userId % _shardSize]->write(transaction);
    }

    std::vector<TransactionData> read(const long& userId)
    {
        _transactionCaches[userId % _shardSize]->read(userId);
    }

    std::vector<TransactionData> pop(const long& userId)
    {
        return std::move(_transactionCaches[userId % _shardSize]->pop(userId));
    }

private:
    const size_t _shardSize;
    std::vector<std::unique_ptr<SimpleSynchronizedCache>> _transactionCaches;
};