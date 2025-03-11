#pragma once

#include <string>

struct TransactionData
{
    long long transactionId;
    long long userId;
    unsigned long long date;
    double amount;
    int type;
    std::string description;
};
