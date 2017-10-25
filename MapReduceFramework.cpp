
#include "MapReduceFramework.h"
#include "MRFCore.h"

MRFCore* g_mrf_core;
extern sem_t shuffle_semaphore;
extern pthread_mutex_t to_delete_mutex;

OUT_ITEMS_VEC RunMapReduceFramework(MapReduceBase& mapReduce, IN_ITEMS_VEC& itemsVec,
                                    int multiThreadLevel, bool autoDeleteV2K2) {
    MRFCore mrf;
    mrf.run_program(mapReduce, itemsVec, multiThreadLevel, autoDeleteV2K2);
    return mrf.get_result();

}

void Emit2 (k2Base* key, v2Base* value) {

    pthread_t curr_thread = pthread_self();

    // locking current thread vector of k2,v2 items to prevent shuffle-map_thread collision
    pthread_mutex_lock(&g_mrf_core->thread_to_execMap[(unsigned long int) curr_thread]->vec_mutex);

    g_mrf_core->thread_to_execMap[(unsigned long int) curr_thread]->k2_v2_vec.push_back(
            SHUFFLE_ITEM(key, value)
    );

    pthread_mutex_unlock(&g_mrf_core->thread_to_execMap[(unsigned long int) curr_thread]->vec_mutex);

    pthread_mutex_lock(&to_delete_mutex);
    g_mrf_core->to_delete_list.push_back(std::make_pair(key, value));
    pthread_mutex_unlock(&to_delete_mutex);

}


void Emit3 (k3Base* key, v3Base* value) {

    pthread_t curr_thread = pthread_self();

    g_mrf_core->thread_to_execReduce[(unsigned long int) curr_thread]->k3_v3_list.push_back(
            OUT_ITEM(key, value));

}
