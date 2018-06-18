#include "utils.h"
#include "debug.h"
#include "hashmap.h"
#include <stdio.h>
#include <string.h>
#include <errno.h>
#define MAP_KEY(base, len) (map_key_t) {.key_base = base, .key_len = len}
#define MAP_VAL(base, len) (map_val_t) {.val_base = base, .val_len = len}
#define MAP_NODE(key_arg, val_arg, tombstone_arg) (map_node_t) {.key = key_arg, .val = val_arg, .tombstone = tombstone_arg}

int exist(hashmap_t *self, map_key_t key);
int linear_probe(int start, map_key_t key, map_node_t *nodes, int capacity);
hashmap_t *create_map(uint32_t capacity, hash_func_f hash_function, destructor_f destroy_function) {
    struct hashmap_t *map = calloc(1,sizeof(struct hashmap_t));
    if (map == NULL){
        return NULL;
    }
    else if (capacity == 0){
        errno = EINVAL;
        return NULL;
    }
    map_node_t *nodes = calloc(capacity,sizeof(struct map_node_t));
    map->capacity = capacity;
    map->size = 0;
    map->nodes = nodes;
    map->hash_function = hash_function;
    map->destroy_function = destroy_function;
    map->num_readers = 0;
    pthread_mutex_t write_lock = map -> write_lock;
    pthread_mutex_t fields_lock = map ->fields_lock;
    pthread_mutex_init(&write_lock, NULL);
    pthread_mutex_init(&fields_lock, NULL);
    map->invalid = false;
    return map;
}

bool
put(hashmap_t *self, map_key_t key, map_val_t val, bool force){
    if (self == NULL){
        errno = EINVAL;
        return false;
    }

    pthread_mutex_lock(&self->write_lock);
    if (self->invalid == true){
        errno = EINVAL;
        pthread_mutex_unlock(&self->write_lock);
        return false;
    }
    else if (key.key_base == NULL || key.key_len == 0){
        errno = EINVAL;
        pthread_mutex_unlock(&self->write_lock);
        return false;
    }
    else if (val.val_base == NULL || val.val_len == 0){
        errno = EINVAL;
        pthread_mutex_unlock(&self->write_lock);
        return false;
    }
    map_node_t *nodes = self->nodes;
    int index = exist(self,key);
    if (index == -1){
        if (self->size == self->capacity && force == true){
            int newIndex = get_index(self,key);
            nodes[newIndex].key = key;
            nodes[newIndex].val = val;
            pthread_mutex_unlock(&self->write_lock);
            return true;
        }
        else if (self->size == self->capacity && force == false){
            pthread_mutex_unlock(&self->write_lock);
            errno = ENOMEM;
            return false;
        }
        else{
            int newIndex = linear_probe(get_index(self,key),key,nodes,self->capacity);
            nodes[newIndex].key = key;
            nodes[newIndex].val = val;
            self->size = self->size + 1;
            pthread_mutex_unlock(&self->write_lock);
            return true;
        }
    }
    else{
        nodes[index].key = key;
        nodes[index].val = val;
        pthread_mutex_unlock(&self->write_lock);
        return true;
    }

}

int linear_probe(int start, map_key_t key, map_node_t *nodes, int capacity){
    int index = start;
    for (int i = 0; i < capacity ; i++){
        if (index == capacity){
            index = 0;
        }
        map_node_t node = nodes[index];
        if (node.tombstone == true){
            return index;
        }
        else if (node.key.key_base == NULL){
            return index;
        }
        index++;
    }
    return -1;
}


map_val_t get(hashmap_t *self, map_key_t key) {

    //pthread_mutex_t field_lock = self -> fields_lock;
    pthread_mutex_lock(&self->fields_lock);
    self->num_readers++;
        if(self->num_readers == 1){
            //pthread_mutex_lock(&self->write_lock);
        }
        //pthread_mutex_unlock(&self->fields_lock);

    int index = exist(self,key);
    if (index == -1){
        errno = EINVAL;
        //pthread_mutex_lock(&field_lock);
            //self->num_readers --;
            //if(self->num_readers == 0){
                //pthread_mutex_unlock(&self->write_lock);
            //}
        pthread_mutex_unlock(&self->fields_lock);

        return MAP_VAL(NULL, 0);
    }
    map_node_t *nodes = self->nodes;
    nodes = nodes + index;
    //pthread_mutex_lock(&field_lock);

    self->num_readers--;
    if(self->num_readers == 0){
            //pthread_mutex_unlock(&self->write_lock);
        }
    pthread_mutex_unlock(&self->fields_lock);
    return nodes->val;
}

int exist(hashmap_t *self, map_key_t key){
    map_node_t *nodes = self->nodes;
    for (int i = 0 ; i < self->capacity ; i++){
        map_node_t temp = nodes[i];
        //map_key_t k = temp.key;
        //if ((k.key_len == key.key_len)){
            //if (memcmp(k.key_base,key.key_base,k.key_len) == 0){
                //if (temp.tombstone == false){
                    //return i;
                //}
            //}
        //}
        if (temp.key.key_base == key.key_base){
            return i;
        }
    }
    return -1;
}

map_node_t delete(hashmap_t *self, map_key_t key) {
    if (self == NULL){
        errno = EINVAL;
        return MAP_NODE(MAP_KEY(NULL, 0), MAP_VAL(NULL, 0), false);
    }
    if (key.key_base == NULL || key.key_len == 0){
        errno = EINVAL;
        return MAP_NODE(MAP_KEY(NULL, 0), MAP_VAL(NULL, 0), false);
    }
    pthread_mutex_t write_lock = self -> write_lock;
    pthread_mutex_lock(&write_lock);
    int index = exist(self,key);
    if (index == -1){
        errno = EINVAL;
        pthread_mutex_unlock(&write_lock);

        return MAP_NODE(MAP_KEY(NULL, 0), MAP_VAL(NULL, 0), false);
    }
    map_node_t *nodes = self->nodes;
    nodes = nodes + index;
    nodes->tombstone = true;
    //self->destroy_function(nodes->key,nodes->val);
    if (self->size > 0){
        self->size = self->size -1;
    }
    pthread_mutex_unlock(&write_lock);
    return nodes[index];
}

bool clear_map(hashmap_t *self) {
    if (self == NULL){
        errno = EINVAL;
        return false;
    }
    pthread_mutex_lock(&self->write_lock);
	map_node_t *nodes = self->nodes;
    for (int i = 0; i < self->capacity ; i++){
        if (nodes + i != NULL){
            map_node_t n = nodes[i];
            map_key_t key = n.key;
            map_val_t val = n.val;
            self-> destroy_function(key,val);
        }
    }
    self->size = 0;
    pthread_mutex_unlock(&self->write_lock);
    return true;
}

bool invalidate_map(hashmap_t *self) {
    if (self == NULL){
        errno = EINVAL;
        return false;
    }
    pthread_mutex_lock(&self->write_lock);
    if (self->invalid == true){
        errno = EINVAL;
        return false;
    }
    map_node_t *nodes = self->nodes;
    for (int i = 0; i < self->capacity ; i++){
        if (nodes + i != NULL){
            map_node_t n = nodes[i];
            map_key_t key = n.key;
            map_val_t val = n.val;
            self-> destroy_function(key,val);
        }
    }
    self->size = 0;
    free(self->nodes);
    self->invalid = true;
    pthread_mutex_unlock(&self->write_lock);
    return true;
}
