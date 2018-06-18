#include "cream.h"
#include "utils.h"
#include "csapp.h"
#include "hashmap.h"
#include "debug.h"
#include "queue.h"
#include <string.h>
#include <sys/socket.h>
void helpMenu();
void doStuff(int connfd);
void map_free_function(map_key_t key, map_val_t val);
void handleGET(hashmap_t *map, int key_size);
void handleEVICT(hashmap_t *map, int key_size);
void handleCLEAR(hashmap_t *map);

queue_t *global_queue;
hashmap_t *global_map;

void *thread(void *vargp);
typedef struct queNmap {
    queue_t *global_queue;
    hashmap_t *global_map;
} queNmap;

int main(int argc, char *argv[]) {
    if (argc == 1){
        exit(EXIT_FAILURE);
    }
    char *firstArg = argv[1];
    if (strcmp(firstArg,"-h") == 0) {
        helpMenu();
        exit(EXIT_SUCCESS);
    }
    else if (argc < 4){
        exit(EXIT_FAILURE);
    }
    int MAX_ENTRIES = atoi(argv[3]);
    int NUM_WORKERS = atoi(argv[1]);
    pthread_t thread_ids[NUM_WORKERS];
    global_queue = create_queue();
    global_map = create_map(MAX_ENTRIES, jenkins_one_at_a_time_hash, map_free_function);
    queNmap *qm = malloc(sizeof(struct queNmap));
    qm -> global_queue = global_queue;
    qm -> global_map = global_map;
    for (int i = 0; i < NUM_WORKERS; i++){ /* Create worker threads */
        Pthread_create(&thread_ids[i], NULL, thread, qm);
    }
    int listenfd, connfd;
    struct sockaddr_in clientaddr;
    //struct hostent *hp;
    //char *haddrp;
    signal(SIGPIPE, SIG_IGN);
    int PORT_NUMBER = atoi(argv[2]);
    listenfd = Open_listenfd(PORT_NUMBER);
    socklen_t clientlen = sizeof(struct sockaddr_in);
    while (1) {
        clientlen = sizeof(clientaddr);
        connfd = Accept(listenfd, (SA *)&clientaddr, &clientlen);
        /* Determine the domain name and IP address of the client */
        //hp = Gethostbyaddr((const char *)&clientaddr.sin_addr.s_addr,
        //sizeof(clientaddr.sin_addr.s_addr), AF_INET);
        //haddrp = inet_ntoa(clientaddr.sin_addr);
        //printf("server connected to %s (%s)\n", hp->h_name, haddrp);
        //printf("sdawdad");
        //printf("CONNFD %d\n",connfd);
        enqueue(global_queue, &connfd);
        //doStuff(connfd);
        //Close(connfd);
    }
}

void *thread(void *vargp){
    signal(SIGPIPE, SIG_IGN);
    while(1){
        //queNmap *qm = (struct  queNmap*)vargp;
        void *item = dequeue(global_queue);
        int fd = *((int *)item);
        //printf("%d\n",fd);
        //request_header_t *request_header = malloc(sizeof(request_header));

        struct request_header_t request_header;
        ///request_header.key_size = key.key_len;
        //request_header.value_size = val.val_len;

        Rio_readn(fd, &request_header, sizeof(request_header));

        //debug("Request Code: %d", request_header.request_code);
        //Rio_readn(fd, request_header + sizeof(int), sizeof(int));
        //uint8_t request_code = request_header->request_code;
        //uint32_t kz = request_header->key_size;
        //uint32_t vz =  request_header->value_size;
        //free(request_header);
        //printf("%d\n",request_code);
        int kz = request_header.key_size;
        int vz = request_header.value_size;
        int request_code = request_header.request_code;
        //debug("kz: %d", request_header.key_size);
        //debug("vz: %d", request_header.value_size);
        switch(request_code){
            case 1:{
                if (kz < MIN_KEY_SIZE){
                    response_header_t response_header = {BAD_REQUEST, 0};
                    Rio_writen(fd, &response_header, sizeof(response_header));
                }
                else if (kz > MAX_KEY_SIZE){
                    response_header_t response_header = {BAD_REQUEST, 0};
                    Rio_writen(fd, &response_header, sizeof(response_header));
                }
                else if (vz < MIN_VALUE_SIZE){
                    response_header_t response_header = {BAD_REQUEST, 0};
                    Rio_writen(fd, &response_header, sizeof(response_header));
                }
                else if (vz > MAX_KEY_SIZE){
                    response_header_t response_header = {BAD_REQUEST, 0};
                    Rio_writen(fd, &response_header, sizeof(response_header));
                }
                else{
                    map_key_t key;
                    key.key_len = kz;
                    Rio_readn(fd, key.key_base, kz);
                    map_val_t val;
                    val.val_len = kz;
                    Rio_readn(fd, val.val_base, vz);
                    bool result = put(global_map,key,val,true);
                    if (result == false){
                        response_header_t response_header = {BAD_REQUEST, 0};
                        Rio_writen(fd, &response_header, sizeof(response_header));
                    }
                    else{
                        response_header_t response_header = {OK, 0};
                        Rio_writen(fd, &response_header, sizeof(response_header));
                    }
                }
                break;
            }
                case 2:{
                    if (kz < MIN_KEY_SIZE){
                        response_header_t response_header = {BAD_REQUEST, 0};
                        Rio_writen(fd, &response_header, sizeof(response_header));
                    }
                    else if (kz > MAX_KEY_SIZE){
                        response_header_t response_header = {BAD_REQUEST, 0};
                        Rio_writen(fd, &response_header, sizeof(response_header));
                    }
                    else{
                        int zero = 0;
                        map_key_t key;
                        key.key_base = &zero;
                        key.key_len = kz;
                        Rio_readn(fd, key.key_base, kz);
                        map_val_t item = get(global_map,key);
                        if (item.val_base == NULL){
                            response_header_t response_header = {NOT_FOUND, 0};
                            Rio_writen(fd, &response_header, sizeof(response_header));
                        }
                        else{
                            void* value = item.val_base;
                            int result = *(int*)value;
                            response_header_t response_header = {OK, result};
                            Rio_writen(fd, &response_header, sizeof(response_header));
                        }
                    }
                break;
            }
                case 4:{
                    if (kz < MIN_KEY_SIZE){
                        response_header_t response_header = {BAD_REQUEST, 0};
                        Rio_writen(fd, &response_header, sizeof(response_header));
                    }
                    else if (kz > MAX_KEY_SIZE){
                        response_header_t response_header = {BAD_REQUEST, 0};
                        Rio_writen(fd, &response_header, sizeof(response_header));
                    }
                    else{
                        response_header_t response_header = {OK, 0};
                        Rio_writen(fd, &response_header, sizeof(response_header));
                        }
                }
                case 8:{
                    clear_map(global_map);
                    response_header_t response_header = {OK, 0};
                    Rio_writen(fd, &response_header, sizeof(response_header));
                }
                default:{
                    response_header_t response_header = {UNSUPPORTED, 0};
                    Rio_writen(fd, &response_header, sizeof(response_header));
                    break;
                }
        }

        //response_header_t response_header = {OK, 0};
        //Rio_writen(fd, &response_header, sizeof(response_header));
        Close(fd);
    }
    return NULL;
}





void map_free_function(map_key_t key, map_val_t val) {
    free(key.key_base);
    free(val.val_base);
}




void helpMenu(){
    printf("./cream [-h] NUM_WORKERS PORT_NUMBER MAX_ENTRIES\n");
    printf("-h                 Displays this help menu and returns EXIT_SUCCESS.\n");
    printf("NUM_WORKERS        The number of worker threads used to service requests.\n");
    printf("PORT_NUMBER        Port number to listen on for incoming connections.\n");
    printf("MAX_ENTRIES        The maximum number of entries that can be stored in `cream`'s underlying data store.\n");
    exit(EXIT_SUCCESS);
}
