#pragma once
#ifndef NO_REFERENCE_CONTAINERS

#include <vector>
#include <boost/unordered/concurrent_flat_map.hpp>
#include <oneapi/tbb/concurrent_hash_map.h>

#include "TransactionData.h"

class BoostConcurrentFlatMap
{
public:
    void write(const TransactionData& transaction)
    {
        _transactionCache.emplace_or_visit(transaction.userId, std::vector<TransactionData>{ transaction },
            [&](auto& kv)
            {
                kv.second.push_back(transaction);
            });
    }

    //it's not actually a pop operation - it replaces existing vector with an empty one, because there's no transactional "retrieve and erase" for concurrent_flat_map
    std::vector<TransactionData> pop(const long long& userId)
    {
        auto userTransactions = std::vector<TransactionData>();

        _transactionCache.visit(userId, [&](auto& kv)
            {
                std::swap(userTransactions, kv.second);
            });

        return userTransactions;
    }

private:
    boost::concurrent_flat_map<long long, std::vector<TransactionData>> _transactionCache;
};

class TbbConcurrentHashMap
{
    typedef oneapi::tbb::concurrent_hash_map<long long, std::vector<TransactionData>> MapType;
public:
    void write(const TransactionData& transaction)
    {
        MapType::accessor acc;
        
        if (!_transactionCache.emplace(acc, transaction.userId, std::vector<TransactionData>{ transaction }))
        {
            acc->second.push_back(transaction);
        }
    }

    std::vector<TransactionData> pop(const long long& userId)
    {
        MapType::const_accessor acc;
        
        auto userTransactions =
            _transactionCache.find(acc, userId) ? acc->second : std::vector<TransactionData>();

        _transactionCache.erase(acc);

        return userTransactions;
    }

private:
    MapType _transactionCache;
};

#endif // !NO_REFERENCE_CONTAINERS