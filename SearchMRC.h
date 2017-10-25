//
// Created by cabby333 on 5/3/17.
//

#ifndef EX3_SEARCHMRC_H
#define EX3_SEARCHMRC_H

#include "MapReduceClient.h"
#include <stdlib.h>
#include <iostream>

class SearchKey : public k1Base, public k2Base, public k3Base
{
public:

    SearchKey() {}

    SearchKey(std::string key);

    ~SearchKey();

    bool operator<(const k1Base &other) const;
    bool operator<(const k2Base &other) const;
    bool operator<(const k3Base &other) const;
    bool operator<(const SearchKey& other) const;

    std::string get_key();

private:
    std::string _key;
};


class SearchValue : public v1Base, public v2Base, public v3Base
{
public:

    SearchValue() {}

    SearchValue(std::string value);

    ~SearchValue();

    std::string get_value();

private:
    std::string _value;
};

class SearchMapReduce : public MapReduceBase {

public:

    void Map(const k1Base *const key, const v1Base *const val) const;
    void Reduce(const k2Base *const key, const V2_VEC &vals) const;
};

#endif //EX3_SEARCHMRC_H
