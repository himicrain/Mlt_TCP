/* Server program for key-value store. */
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include<sys/poll.h>
#include <netdb.h>
#include <unistd.h>
#include<pthread.h>
#include <stdlib.h>
#include "kv.h"
#include "parser.h"

#define NTHREADS  4
#define BACKLOG 10


typedef struct tpool_work{
    void* (*work)(void*);
    int arg;
    struct tpool_work *next;
}tpool_work_t;

typedef struct tpool{
    int isShutdown;
    int max_thread;
    pthread_t *t_id;
    tpool_work_t *queue_head;
    pthread_mutex_t lock;
    pthread_cond_t isReady;
}tpool_t;


tpool_t *tpool = NULL;






/* Add anything you want here. */

/* A worker thread. You should write the code of this function. */
/*工作线程，作为一个被委托的函数，存进线程中，通过调用该工作线程函数执行任务*/
void* worker(void* arg) {
    //创建一个任务对象
    tpool_work_t *do_work;

    while(1){
        //上锁
        pthread_mutex_lock(&(tpool->lock));
        //如果线程池中没有可使用 且线程或者线程池没有关闭，那么等待
        while(!tpool->queue_head && !tpool->isShutdown){
            printf("%s","wait\n");
            pthread_cond_wait(&tpool->isReady,&(tpool->lock));
        }
        //如果线程池已经关闭，那么解锁
        if(tpool->isShutdown){
            pthread_mutex_unlock(&(tpool->lock));
            pthread_exit(NULL);
        }
        //如果存在可用线程，那么将任务对象付给一个线程处理
        do_work = (tpool->queue_head);
        //线程池中head线程指针向前移动
        tpool->queue_head = tpool->queue_head->next;
        //解锁
        pthread_mutex_unlock(&(tpool->lock));
        //任务对象工作
        do_work->work(do_work->arg);
        //释放任务对象
        free(do_work);
    }

    return NULL;

}


int create_tpool(){
    int i;
    //为线程池申请空间
    tpool = calloc(1,sizeof(tpool_t));
    //设置线程池的线程数
    tpool->max_thread = NTHREADS;
    //设置线程池状态，0为开启，1为关闭
    tpool->isShutdown = 0;
    tpool->queue_head = NULL;
    //申请一个锁
    if(pthread_mutex_init(&(tpool->lock),NULL) !=0){
        printf("%s","init mutex failed\n");
        exit(1);
    }

    //创建一个消息量
    if (pthread_cond_init(&tpool->isReady, NULL) !=0 ) {
        printf("%s","init cond failed\n");
        exit(1);
    }
    //为线程申请空间
    tpool->t_id = calloc(NTHREADS,sizeof(pthread_t));
    if(!tpool->t_id){
        printf("%s","init thread pool failed\n");
        exit(2);
    }
    //初始化各个线程
    for(i=0;i<NTHREADS;i++){
        if(pthread_create(&tpool->t_id[i],NULL,worker,NULL) != 0 ){
            printf(" %s  %d\n","create thread",i);
            exit(3);
        }
        printf(" %s  %d\n","create thread",i);

    }
    return 0;

}



void destroy_tpool(){
    if(tpool->isShutdown){
        printf("%s","have been shutdown\n");
        return ;
    }
    //设置tpool为关闭状态
    tpool->isShutdown = 1;
    //上锁
    pthread_mutex_lock(&(tpool->lock));
    //广播唤醒所有线程
    pthread_cond_broadcast(&tpool->isReady);
    pthread_mutex_unlock(&(tpool->lock));
    /*int i;
    for(i=0;i<NTHREADS;i++){
        pthread_join(tpool->t_id[i],NULL);
        printf("%s %d \n","join",i);
    }*/
    //释放线程
    free(tpool->t_id);

    tpool_work_t *temp;
    //释放任务队列
    while(tpool->queue_head){
        temp = tpool->queue_head;
        
        tpool->queue_head = tpool->queue_head->next;
        free(temp);
    }
    printf("%s","destroy pool\n");
    //销毁锁
    pthread_mutex_destroy(&(tpool->lock));
    //销毁信号量
    pthread_cond_destroy(&tpool->isReady);
    //释放线程池
    free(tpool);

}

/*向任务队列添加任务，work函数为需要处理的任务*/
int add_work(void*(*work)(void*),int arg){
    tpool_work_t *do_work,*temp;

    if(!work){
        printf("%","invalid func\n");
        return -1;
    }
    //创建一个任务对象
    do_work = malloc(sizeof(tpool_work_t));
    if(!do_work){
        printf("%","init do_work failed\n");
    }
    //对任务对象赋值
    do_work->work = work;
    do_work->arg = arg;
    do_work->next = NULL;
    //上锁
    pthread_mutex_lock(&(tpool->lock));
    //将任务对象添加到任务队列
    temp = tpool->queue_head;
    if(!temp){
        tpool->queue_head = do_work;
    }else{
        while(temp->next){
            temp = temp->next;
        }
        temp->next = do_work;
    }
    //通知线程有任务添加
    pthread_cond_signal(&tpool->isReady);
    //解锁
    pthread_mutex_unlock(&(tpool->lock));
    return 0;
}



void* conn_socket(int p){

    char buffer[255];
    char t = 'o';
    char *tt = &t;
    char **key=&tt;
    char b='i';
    char *bb = &b;
    char **text=&bb;
    int a=0;
    enum DATA_CMD *cmd = &a;
    send(p,"welcome\n",8,0);
    while(1){
        memset(buffer,0,sizeof(buffer));
        int ret = recv(p,buffer,255,0);
        if(ret > 0){
            buffer[ret] = 0x00;

            int res = parse_d( buffer, cmd, key, text);

            switch(*cmd){
                case 0:{
                    int ie = itemExists(*key);
                    if(ie==1){ /*如果存在此key，更新*/
                        int iu = updateItem(*key,*text);
                        if(iu==0){/*如果更新成功*/
                            send(p,"successfully storage",20,0);
                        }else{  /*如果更新失败*/
                            send(p,"error storage",15,0);
                        }
                    }else if(ie ==0 ){ /*如果不存在key*/
                        int ret=0;
                        //char str[20];
                        ret = createItem(*key,*text);
                        if(ret == 0){   /*如果创建成功*/
                            //sprintf(str,"%d",ret);
                            send(p,"successfully storage",20,0);
                        }else{  /*如果创建失败*/
                            send(p,"error storage",15,0);
                        }
                    }

                    break;
                }
                case 1:{ // 查找key对应text
                    char* d ;
                    d = findValue(*key);
                    if(!d){
                       send(p,"no such key",12,0); 
                    }else{
                        send(p,d,strlen(d),0);
                    }
                    break;
                }
                case 2:{//计数
                    int count=0;

                    count = countItems();
                    char str[20];
                    sprintf(str,"%d",count);
                    send(p,str,strlen(str),0);
                    break;
                }
                case 3:{ //删除key对应text
                    int ret =  deleteItem(*key,1);

                    if(ret == 0){
                        send(p,"success delete\n",15,0);
                    }else{
                        send(p,"error delete\n",13,0);
                    }
                    break;
                }
                case 4:{ //检查key对应item是否存在
                    int ret = itemExists(*key);
                    char str[20];
                    sprintf(str,"%d",ret);
                    send(p,str,strlen(str),0);
                    break;
                }
                case 5:{ //空行
                    printf("%s","goodbye\n");
                    close(p);
                    return 0;
                }
                default:
                    send(p,"error cmd\n",10,0);
            }

            send(p,"\n",2,0);
        }else if(ret == 0){
            break;
        }
    }

    close(p);
}





/* You may add code to the main() function. */
int main(int argc, char** argv) {
    int cport, dport; /* control and data ports. */
    
	if (argc < 3) {
        printf("Usage: %s data-port control-port\n", argv[0]);
        exit(1);
	} else {
        cport = atoi(argv[2]);
        dport = atoi(argv[1]);
	}

    int s_ctrl = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP); 
    int s_data = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    
    struct sockaddr_in adrin_ctrl,adrin_data;
    adrin_ctrl.sin_family = AF_INET;
    adrin_ctrl.sin_port = htons(cport);
    adrin_ctrl.sin_addr.s_addr = INADDR_ANY;

    adrin_data.sin_family = AF_INET;
    adrin_data.sin_port = htons(dport);
    adrin_data.sin_addr.s_addr = INADDR_ANY;

    if(bind(s_data,(struct sockaddr *)&adrin_data,sizeof(adrin_data)) == -1){
        printf("error bind");
        exit(1);
    }
    if(bind(s_ctrl,(struct sockaddr *)&adrin_ctrl,sizeof(adrin_ctrl)) == -1){
        printf("error bind ctrl");
        exit(1);
    }
    if(listen(s_data,20) == -1){
        printf("error listen");
        exit(1);
    }
    if(listen(s_ctrl,20) == -1){
        printf("error listen");
        exit(1);
    }

    struct pollfd fds[2];
    fds[0].fd = s_data;
    fds[0].events = POLLRDNORM;
    fds[1].fd = s_ctrl;
    fds[1].events = POLLRDNORM;


    if (create_tpool() != 0) {
          printf("tpool_create failed\n");
       exit(1);
     }
     
   // pthread_mutex_t lk;


    while(1){
        int ret = poll(fds,2,-1);
        if(fds[0].revents & POLLRDNORM ){
            struct sockaddr_in remoteAddr;  
            int nAddrlen = sizeof(remoteAddr);
            printf("waiting connect...\n");  
            int conn = accept(s_data, (struct sockaddr*)&remoteAddr, &nAddrlen); 

            add_work(conn_socket,conn);

        }else if(fds[1].revents & POLLRDNORM){
            struct sockaddr_in remoteAddr;  
            int nAddrlen = sizeof(remoteAddr);
            printf("waiting connect...\n");  
            int conn = accept(s_ctrl, (struct sockaddr*)&remoteAddr, &nAddrlen); 

            char buffer[255];
            memset(buffer,0,sizeof(buffer));
            int ret = recv(conn,buffer,255,0);
            if(ret > 0){
                buffer[ret] = 0x00;
                int flag = parse_c(buffer);

                if(flag == 0){
                    destroy_tpool();
                    close(conn);
                    return 0;
                }else if(flag == 1){
                    int count = 0;
                    count = countItems();
                    char str[20];
                    sprintf(str,"%d",count);
                    send(conn,str,strlen(str),0);
                    send(conn,"\n",2,0);
                    close(conn);
                }else{
                    send(conn,"error cmd\n",10,0);
                    close(conn);
                }

            }else if(ret == 0){
                close(conn);
                break;
            }
        }
    }

    return 0;
}

