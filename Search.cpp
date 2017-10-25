#include <iostream>
#include <stdlib.h>
#include <list>
#include "MapReduceFramework.h"
#include "SearchMRC.h"

using namespace std;

string g_substring;

/**
 * Initializes a list of file paths from given list of directorys paths, and calls the map-reduce
 * function, which will find the substring in each file path
 * @param substring - the string to search (string)
 * @param paths - a list of directorys (strings)
 */
void search(string substring, list<string>& paths)
{
    g_substring = substring;
    SearchMapReduce search_map_reduce;

    IN_ITEMS_VEC items;

    // creating a vector of pairs<k1base, v1base> from paths
    for (auto &curr_path : paths) {
        try {
            items.push_back(std::make_pair(new SearchKey(curr_path), nullptr));

        } catch (bad_alloc& e) {
            std::cout << "Failure: new failed.\n";
            std::exit(1);
        }
    }

    // initializes the map-shuffle-reduce framework
    OUT_ITEMS_VEC output_items = RunMapReduceFramework(search_map_reduce, items, 5, true);

    // freeing memory k1 and v1
    for (auto kvp_iter = items.begin(); kvp_iter != items.end(); kvp_iter++) {
        delete kvp_iter->first;
    }

    // freeing memory k3 and v3
    for (auto kv3_iter = output_items.begin(); kv3_iter != output_items.end(); kv3_iter++) {
        cout << ((SearchKey*) (*kv3_iter).first)->get_key() << ' ';
        delete kv3_iter->first;
        delete kv3_iter->second;
    }
    cout << endl;
}


int main(int argc, char* argv[]) {
    if (argc < 3) {
        cerr << "Usage: <substring to search> <folders, separated by space>" << endl;
        exit(1);
    }

    list<string> paths;
    for (int i=2; i < argc; ++i) {
        paths.push_back((string) argv[i]);
    }

    search((string) argv[1], paths);
    return 0;
}

