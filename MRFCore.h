
#ifndef EX3_MRFCORE_H
#define EX3_MRFCORE_H


#include "MapReduceFramework.h"
#include <list>
#include <map>
#include "pthread.h"
#include <semaphore.h>
#include <iostream>
#include <set>
#include <time.h>

#define CHUNCK_SIZE 10
#define MICRO_TO_NANO_FACTOR 1000
#define LOG_FILE_NAME ".MapReduceFramework.log"

typedef struct Key2Comparator {
    bool operator()(k2Base* a, k2Base* b) {
        return *a < *b;
    }
} Key2Comparator;


typedef struct Key3Comparator {
    bool operator()(k3Base* a, k3Base* b) {
        return *a < *b;
    }
} Key3Comparator;


typedef std::pair<k2Base*, v2Base*> SHUFFLE_ITEM;
typedef std::vector<SHUFFLE_ITEM> EXEC_MAP_CONTAINER;
typedef std::map<k2Base*, V2_VEC, Key2Comparator> SHUFFLE_CONTAINER;
typedef std::map<k2Base*, V2_VEC, Key2Comparator>::iterator SHUFFLE_CONTAINER_ITER;

void error_handle(std::string function_name);


class ExecMap {
public:
    ExecMap();
    ~ExecMap();
    pthread_t thread_p;
    EXEC_MAP_CONTAINER k2_v2_vec;
    int last_pulled;
    pthread_mutex_t vec_mutex;
    static void * map_routine(void*);
};


class ShuffleThread {
public:
    ShuffleThread();
    pthread_t thread_p;
    SHUFFLE_CONTAINER k2_v2list_map;
    std::vector<std::pair<k2Base*, V2_VEC>> shuffle_keys_vec;

    static void * shuffle_routine(void*);
};

class ExecReduce {
public:
    ExecReduce();
    pthread_t thread_p;
    std::list<OUT_ITEM> k3_v3_list;

    static void * reduce_routine(void*);
};


class MRFCore {
public:

    MRFCore();

    ~MRFCore() {}

    void run_program(MapReduceBase& mapReduce, IN_ITEMS_VEC& itemsVec,
                     int multiThreadLevel, bool autoDeleteV2K2);

    OUT_ITEMS_VEC& get_result();

    void get_input_index(int& index);

    MapReduceBase *mapReduce;

    int input_index;

    void write_to_log_file(std::string msg, bool add_time);

    IN_ITEM get_input_item(int index);

    std::pair<k2Base*,V2_VEC>& get_shuffle_item(int index);

    std::map<unsigned long int, ExecMap*> thread_to_execMap;

    bool map_threads_alive;

    ShuffleThread* shuffle_obj;

    std::map<unsigned long int, ExecReduce*> thread_to_execReduce;


    OUT_ITEMS_VEC out_items_vec;

    FILE* log_file_p;

    std::list<SHUFFLE_ITEM> to_delete_list;


private:

    IN_ITEMS_VEC _itemsVec;
};



#endif //EX3_MRFCORE_H
