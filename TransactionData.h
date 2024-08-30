#pragma once

#include <string>

struct TransactionData
{
    long transactionId;
    long userId;
    unsigned long date;
    double amount;
    int type;
    std::string description;
};
