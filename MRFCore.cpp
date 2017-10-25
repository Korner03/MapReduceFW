#include <algorithm>
#include <fcntl.h>
#include <stdlib.h>
#include <malloc.h>
#include <sys/time.h>
#include "MRFCore.h"

extern MRFCore *g_mrf_core;

pthread_mutex_t pthread_to_container_mutex ;

pthread_mutex_t log_write_mutex;

pthread_mutex_t input_index_mutex;

pthread_mutex_t input_item_mutex;

pthread_mutex_t still_alive_mutex;

pthread_mutex_t to_delete_mutex;

sem_t shuffle_semaphore;

/* ----------------- EXEC MAP IMPLEMENTATION -------------------- */

ExecMap::ExecMap() {

    last_pulled = 0;
    if (pthread_mutex_init(&vec_mutex, NULL) != 0) {
        error_handle("pthread_mutex_init");
    }

    int success = pthread_create(&this->thread_p, NULL, ExecMap::map_routine, NULL);
    if (success != 0) {
        error_handle("pthread_create");
    }

    g_mrf_core->write_to_log_file(std::string("Thread ExecMap created"), true);
}

ExecMap::~ExecMap() {
    pthread_mutex_destroy(&vec_mutex);
}


void MRFCore::get_input_index(int& index) {
    pthread_mutex_lock(&input_index_mutex);

    index = input_index;
    input_index += CHUNCK_SIZE;

    pthread_mutex_unlock(&input_index_mutex);
}


void* ExecMap::map_routine(void*) {

    // putting the mapExec threads on hold untill pthreadToConatiner is initialized
    pthread_mutex_lock(&pthread_to_container_mutex);
    pthread_mutex_unlock(&pthread_to_container_mutex);

    int counter = 0; //counter of how many (k1,v1) threads we need to take (starts from chunck size)
    int index = 0; // index for the proper position to get (k1,v1) pairs from, for each thread

    IN_ITEM input_item;

    bool finished_chuncked_flag = false;

    // pulling (k1, v1) from user input
    while(true) {

        if (finished_chuncked_flag) {
            if (sem_post(&shuffle_semaphore) != 0) {
                error_handle("sem_post");
            }
        }

        // condition for trying to get index from which to get new k1,v1
        if (counter == 0) {
            counter = CHUNCK_SIZE;
            g_mrf_core->get_input_index(index);

        } else { // get the (k1,v1) pair at index

            try {
                pthread_mutex_lock(&input_item_mutex);
                input_item = g_mrf_core->get_input_item(index);
                pthread_mutex_unlock(&input_item_mutex);

                g_mrf_core->mapReduce->Map(input_item.first, input_item.second);

                counter--;
                index++;
                if (counter == 0) {
                    finished_chuncked_flag = true;
                }

            } catch (int e) {
                pthread_mutex_unlock(&input_item_mutex);
                break;
            }

        }
    }

    if (sem_post(&shuffle_semaphore) != 0) {
        error_handle("sem_post");
    }

    g_mrf_core->write_to_log_file(std::string("Thread ExecMap terminated"), true);
    return nullptr;
}

/* ----------------- SHUFFLE IMPLEMENTATION -------------------- */

ShuffleThread::ShuffleThread() {

    // init the semaphore for shuffle
    if (sem_init(&shuffle_semaphore, 0, 0) != 0) {
        error_handle("sem_init");
    }
    int success = pthread_create(&this->thread_p, NULL, ShuffleThread::shuffle_routine, NULL);
    if (success != 0) {
        error_handle("pthread_create");
    }

    g_mrf_core->write_to_log_file(std::string("Thread Shuffle created"), true);
}



void* ShuffleThread::shuffle_routine(void *) {

    pthread_mutex_lock(&pthread_to_container_mutex);
    pthread_mutex_unlock(&pthread_to_container_mutex);

    int shuffle_sem_val;

    // if entered the loop then a execMap pulled a new data
    while (true) {

        sem_wait(&shuffle_semaphore);

        // iterating all the execMap threads
        for (auto &exec_map_obj : g_mrf_core->thread_to_execMap) {

            pthread_mutex_lock(&exec_map_obj.second->vec_mutex);

            // if a execMap thread didn't add new data, continue
            if ((unsigned int) exec_map_obj.second->last_pulled >=
                    exec_map_obj.second->k2_v2_vec.size()) {
                pthread_mutex_unlock(&exec_map_obj.second->vec_mutex);
                continue;
            }

            //if a execMap thread DID add new data, add it to shuffle container
            int index;
            for (index = exec_map_obj.second->last_pulled;
                 index < (int) exec_map_obj.second->k2_v2_vec.size();
                 index++) {

                g_mrf_core->shuffle_obj->k2_v2list_map[
                        exec_map_obj.second->k2_v2_vec[index].first
                ].push_back(exec_map_obj.second->k2_v2_vec[index].second);

            }

            // update the index of the last data extracted from current execMap thread
            exec_map_obj.second->last_pulled = index;

            pthread_mutex_unlock(&exec_map_obj.second->vec_mutex);
        }

        pthread_mutex_lock(&still_alive_mutex);
        bool is_alive = g_mrf_core->map_threads_alive;

        sem_getvalue(&shuffle_semaphore, &shuffle_sem_val);
        pthread_mutex_unlock(&still_alive_mutex);

        if ((!is_alive) && (shuffle_sem_val == 0)) {
            for (auto &k2_item : g_mrf_core->shuffle_obj->k2_v2list_map) {
                g_mrf_core->shuffle_obj->shuffle_keys_vec.push_back(
                        std::make_pair(const_cast<k2Base *>(k2_item.first), k2_item.second));
            }
            break;
        }
    }


    return nullptr;

}

/* ----------------- EXEC REDUCE IMPLEMENTATION -------------------- */

ExecReduce::ExecReduce() {

    int success = pthread_create(&this->thread_p, NULL, ExecReduce::reduce_routine, NULL);
    if (success != 0) {
        error_handle("pthread_create");
    }

    g_mrf_core->write_to_log_file(std::string("Thread ExecReduce created"), true); // TODO
}

void* ExecReduce::reduce_routine(void*) {

    pthread_mutex_lock(&pthread_to_container_mutex);
    pthread_mutex_unlock(&pthread_to_container_mutex);

    int counter = 0; //counter of how many (k2,list<v2>) threads we need to take (starts from chunck size)
    int index = 0; // index for the proper position to get (k2,list<v2>) pairs from, for each thread

    // pulling (k2, list<v2>) from shuffle container
    while(true) {
        // condition for trying to get index from which to get new (k2,list<v2>)
        if (counter == 0) {
            counter = CHUNCK_SIZE;

            g_mrf_core->get_input_index(index);

        } else {
            // get the (k2,list<v2>) pair at index
            try {

                pthread_mutex_lock(&input_item_mutex);
                std::pair<k2Base*, V2_VEC>& input_item = g_mrf_core->get_shuffle_item(index);
                pthread_mutex_unlock(&input_item_mutex);

                g_mrf_core->mapReduce->Reduce(
                        input_item.first, input_item.second);
                counter--;
                index++;
            } catch (int e) {
                pthread_mutex_unlock(&input_item_mutex);
                break;
            }

        }
    }

    return nullptr;
}

/* ----------------- MRF CORE IMPLEMENTATION -------------------- */

MRFCore::MRFCore() {
    g_mrf_core = this;
}


void MRFCore::run_program(MapReduceBase &mapReduce, IN_ITEMS_VEC &itemsVec, int multiThreadLevel,
                 bool autoDeleteV2K2) {

    struct timeval start_map, end_map;
    if (gettimeofday(&start_map, NULL)) {
        error_handle("gettimeofday");
    }
    //open log file
    log_file_p = fopen(LOG_FILE_NAME, "a+");
    if (log_file_p == NULL) {
        error_handle("fopen");
    }

    // init mutex for log writing
    if (pthread_mutex_init(&log_write_mutex, NULL) != 0) {
        error_handle("pthread_mutex_init");
    }

    write_to_log_file(
            std::string("RunMapReduceFramework started with ") + std::to_string(multiThreadLevel)
            + std::string(" threads"),
            false
    );

    input_index = 0;
    this->_itemsVec = itemsVec;
    this->mapReduce = &mapReduce;
    this->map_threads_alive = true;

    // init mutex for still alive
    if (pthread_mutex_init(&still_alive_mutex, NULL) != 0) {
        error_handle("pthread_mutex_init");
    }

    if (pthread_mutex_init(&to_delete_mutex, NULL) != 0) {
        error_handle("pthread_mutex_init");
    }

    if (pthread_mutex_init(&input_item_mutex, NULL) != 0) {
        error_handle("pthread_mutex_init");
    }

    // init mutex for k1 v1
    if (pthread_mutex_init(&input_index_mutex, NULL) != 0) {
        error_handle("pthread_mutex_init");
    }

    // init mutex for stopping execMap threads before emitting2 and locking it
    if (pthread_mutex_init(&pthread_to_container_mutex, NULL) != 0)  {
        error_handle("pthread_mutex_init");
    }

    if (pthread_mutex_lock(&pthread_to_container_mutex) != 0) {
        error_handle("pthread_mutex_lock");
    }

    //init exec maps
    ExecMap maps_array[multiThreadLevel];

    for (int i = 0; i < multiThreadLevel; ++i) {
        g_mrf_core->thread_to_execMap[
                (unsigned long int) (maps_array[i].thread_p)] = &maps_array[i];
    }

    //init shuffle
    ShuffleThread shuffle_obj;
    g_mrf_core->shuffle_obj = &shuffle_obj;


    // allowing execMap threads to run
    pthread_mutex_unlock(&pthread_to_container_mutex);

    // wait for termination of all map threads
    for (auto &curr_map : maps_array) {
        if (pthread_join(curr_map.thread_p, NULL) != 0) {
            error_handle("pthread_join");
        }
    }

    // kill shuffle thread
    pthread_mutex_lock(&still_alive_mutex);
    map_threads_alive = false;
    pthread_mutex_unlock(&still_alive_mutex);

    if (sem_post(&shuffle_semaphore) != 0) {
        error_handle("sem_post");
    }

    if (pthread_join(shuffle_obj.thread_p, NULL) != 0) {
        error_handle("pthread_join");
    }

    if (gettimeofday(&end_map, NULL)) {
        error_handle("gettimeofday");
    }


    double time_elapsed_map_shuffle =
            (double) MICRO_TO_NANO_FACTOR * ((double)end_map.tv_usec - (double)start_map.tv_usec);
    write_to_log_file(
            std::string("Map and Shuffle took ") +
                    std::to_string(time_elapsed_map_shuffle) +
                    std::string(" ns"), false);

    struct timeval reduce_start, reduce_end;
    if (gettimeofday(&reduce_start, NULL)) {
        error_handle("gettimeofday");
    }

    // create threads for ExecReduce
    input_index = 0;


    pthread_mutex_lock(&pthread_to_container_mutex);

    //init exec reduces
    ExecReduce reduces_array[multiThreadLevel];

    for (int i = 0; i < multiThreadLevel; ++i) {
        g_mrf_core->thread_to_execReduce[
                (unsigned long int) (reduces_array[i].thread_p)] = &reduces_array[i];
    }

    pthread_mutex_unlock(&pthread_to_container_mutex);

    // wait for termination of all reduce threads
    for (auto &curr_reduce : reduces_array) {
        if (pthread_join(curr_reduce.thread_p, NULL) != 0) {
            error_handle("pthread_join");
        }
    }

    // kill mutexes and semaphore
    if (pthread_mutex_destroy(&input_index_mutex) != 0) {
        error_handle("pthread_mutex_destroy");
    }

    if (pthread_mutex_destroy(&pthread_to_container_mutex) != 0) {
        error_handle("pthread_mutex_destroy");
    }

    if (pthread_mutex_destroy(&input_item_mutex) != 0) {
        error_handle("pthread_mutex_destroy");
    }

    if (pthread_mutex_destroy(&still_alive_mutex) != 0) {
        error_handle("pthread_mutex_destroy");
    }

    if (pthread_mutex_destroy(&to_delete_mutex) != 0) {
        error_handle("pthread_mutex_destroy");
    }

    if (sem_destroy(&shuffle_semaphore)) {
        error_handle("semaphore_destroy");
    }

    // removing k3,v3 pairs from ExecReduce objects and storing them in a single vector
    for (auto &curr_reduce : reduces_array) {
        while (!curr_reduce.k3_v3_list.empty()) {
            out_items_vec.push_back(curr_reduce.k3_v3_list.front());
            curr_reduce.k3_v3_list.pop_front();
        }
    }

    // sorting the output by k3
    std::sort(out_items_vec.begin(), out_items_vec.end(), [](const OUT_ITEM &a, const OUT_ITEM &b) {
        return *(a.first) < *(b.first);
    });

    if (gettimeofday(&reduce_end, NULL)) {
        error_handle("gettimeofday");
    }

    double time_elapsed_reduce = (double) MICRO_TO_NANO_FACTOR * (
            ((double)(reduce_end.tv_usec) - (double)(reduce_start.tv_usec)));
    write_to_log_file(std::string("Reduce took ") + std::to_string(time_elapsed_reduce)
                      + std::string(" ns"), false);

    write_to_log_file(std::string("RunMapReduceFramework finished"), false);

    if (pthread_mutex_destroy(&log_write_mutex) != 0) {
        error_handle("pthread_mutex_destroy");
    }

    // freeing memory
    if (autoDeleteV2K2) {
        for (auto &curr_k2v2: to_delete_list) {
            delete curr_k2v2.first;
            delete curr_k2v2.second;
        }
    }

    fclose(log_file_p);
}

IN_ITEM MRFCore::get_input_item(int index) {
    if ((int) _itemsVec.size() <= index) {
        throw 0;
    }

    return _itemsVec[index];
}

std::pair<k2Base*,V2_VEC>& MRFCore::get_shuffle_item(int index) {
    if ((int) shuffle_obj->shuffle_keys_vec.size() <= index) {
        throw 0;
    }
    return shuffle_obj->shuffle_keys_vec[index];
}



OUT_ITEMS_VEC& MRFCore::get_result() {
    return out_items_vec;
}

void MRFCore::write_to_log_file(std::string msg, bool add_time) {

    pthread_mutex_lock(&log_write_mutex);

    if (add_time) {
        char time_string[26];

        time_t timer;
        time(&timer);
        struct tm* tm_info = localtime(&timer);


        strftime(time_string, 26, " [%d.%m.%Y %H:%M:%S]", tm_info);

        if (!fprintf(log_file_p, "%s%s\n", msg.c_str(), time_string)) {
            error_handle("fprintf");
        }

    } else {
        if (!fprintf(log_file_p, "%s\n", msg.c_str())) {
            error_handle("fprintf");
        }
    }
    if (fflush(log_file_p) != 0) {
        error_handle("fflush");
    }

    pthread_mutex_unlock(&log_write_mutex);
}

void error_handle(std::string function_name) {

    std::cout << "MapReduceFramework Failure: " << function_name << " failed.\n";
    std::exit(1);

}