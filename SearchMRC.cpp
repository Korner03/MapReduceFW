//
// Created by cabby333 on 5/3/17.
//

#include <dirent.h>
#include "SearchMRC.h"
#include "MapReduceFramework.h"

extern std::string g_substring;

SearchKey::SearchKey(std::string key) {
    this->_key = key;
}

SearchKey::~SearchKey() { }

std::string SearchKey::get_key() {
    return this->_key;
}

bool SearchKey::operator<(const k1Base &other) const {
    return this->operator<((SearchKey&) other);
}
bool SearchKey::operator<(const k2Base &other) const {
    return this->operator<((SearchKey&) other);
}
bool SearchKey::operator<(const k3Base &other) const {
    return this->operator<((SearchKey&) other);
}
bool SearchKey::operator<(const SearchKey& other) const {
    return this->_key.compare(other._key) < 0;
}

SearchValue::SearchValue(std::string value) {
    this->_value = value;
}

SearchValue::~SearchValue() { }

std::string SearchValue::get_value() {
    return this->_value;
}


// key is directory
void SearchMapReduce::Map(const k1Base *const key, const v1Base *const val) const {

    if (val == NULL) {}

    SearchKey* search_key = (SearchKey*) key;
    DIR* dir_pointer;
    struct dirent *entry;

    // checking that the file name is a directory
    if ((dir_pointer = opendir(search_key->get_key().c_str())) == NULL) {
        return;
    }

    while ((entry = readdir(dir_pointer)) != NULL) {
        if (std::string(".").compare(entry->d_name) == 0 ||
            std::string("..").compare(entry->d_name) == 0) {
            continue;
        }
        try {
            Emit2(new SearchKey(search_key->get_key()), new SearchValue(entry->d_name));

        } catch (std::bad_alloc& e) {
            std::cout << "Failure: new failed.\n";
            std::exit(1);
        }
    }

    closedir(dir_pointer);

}


void SearchMapReduce::Reduce(const k2Base *const key, const V2_VEC &vals) const {

    if (key != nullptr) {} // make the warning disapper....

    // iterating the values list (file names)
    for (auto iter = vals.begin(); iter != vals.end(); iter++) {
        SearchValue* search_value = (SearchValue *) *iter;

        // Checks if the substring to be found is contained in each value (file name)
        if (search_value->get_value().find(g_substring) == std::string::npos)
            continue;

        try {
            Emit3(new SearchKey(search_value->get_value()), NULL);
        } catch (std::bad_alloc& e) {
            std::cout << "Failure: new failed.\n";
            std::exit(1);
        }

    }
}
