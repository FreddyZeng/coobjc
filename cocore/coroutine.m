//
//  coroutine.c
//  coobjc
//
//  Copyright © 2018 Alibaba Group Holding Limited All rights reserved.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

#include "coroutine.h"
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <stddef.h>
#include <string.h>
#include <stdint.h>
#include "coroutine_context.h"
#import <pthread/pthread.h>
#import <mach/mach.h>
#import "co_queuedebugging_support.h"


#define COROUTINE_DEAD 0
#define COROUTINE_READY 1
#define COROUTINE_RUNNING 2
#define COROUTINE_SUSPEND 3


#define STACK_SIZE      (512*1024)
#define DEFAULT_COROUTINE_COUNT     64

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wincompatible-pointer-types"


void scheduler_queue_push(coroutine_scheduler_t *scheduler, coroutine_t *co);
coroutine_t *scheduler_queue_pop(coroutine_scheduler_t *scheduler);
coroutine_scheduler_t *coroutine_scheduler_new(void);
void coroutine_scheduler_free(coroutine_scheduler_t *schedule);
void coroutine_resume_im(coroutine_t *co);
void *coroutine_memory_malloc(size_t s);
void  coroutine_memory_free(void *ptr, size_t size);

static pthread_key_t coroutine_scheduler_key = 0;

void *coroutine_memory_malloc(size_t s) {
    vm_address_t address;
    
    vm_size_t size = s;
    kern_return_t ret = vm_allocate((vm_map_t)mach_task_self(), &address, size,  VM_MAKE_TAG(VM_MEMORY_STACK) | VM_FLAGS_ANYWHERE);
    // VM_FLAGS_ANYWHERE "VM_FLAGS_ANYWHERE – This indicates that it’s ok to ignore the input address, and simply allocate the pages wherever they’d fit (and return the base address in address)" https://perpendiculo.us/2011/12/page-tables-and-you/
    // VM_MAKE_TAG(VM_MEMORY_STACK) 这个可以声明为栈的方式创建内存，并且不会调用__syscall_logger日志，https://opensource.apple.com/source/xnu/xnu-6153.11.26/libsyscall/mach/mach_vm.c.auto.html
    
    if ( ret != ERR_SUCCESS ) {
        return NULL;
    }
    return (void *)address;
}

void  coroutine_memory_free(void *ptr, size_t size) {
    if (ptr) {
        vm_deallocate((vm_map_t)mach_task_self(), (vm_address_t)ptr, size);
    }
}

coroutine_scheduler_t *coroutine_scheduler_self(void) {
    
    if (!coroutine_scheduler_key) {
        pthread_key_create(&coroutine_scheduler_key, coroutine_scheduler_free);
    }
    
    void *schedule = pthread_getspecific(coroutine_scheduler_key);
    return schedule;
}

coroutine_scheduler_t *coroutine_scheduler_self_create_if_not_exists(void) {
    
    if (!coroutine_scheduler_key) {
        pthread_key_create(&coroutine_scheduler_key, coroutine_scheduler_free);
    }
    
    void *schedule = pthread_getspecific(coroutine_scheduler_key);
    if (!schedule) {
        schedule = coroutine_scheduler_new(); // 创建一个新的scheduler
        pthread_setspecific(coroutine_scheduler_key, schedule); // 在线程数据隔离中保存，任何线程之间不共享
    }
    return schedule;
}

// The main entry of the coroutine's scheduler
// The scheduler is just a special coroutine, so we can use yield.
__attribute__ ((optnone))
void coroutine_scheduler_main(coroutine_t *scheduler_co) {
    
    coroutine_scheduler_t *scheduler = scheduler_co->scheduler;
    for (;;) {
        
        // Pop a coroutine from the scheduler's queue.
        coroutine_t *co = scheduler_queue_pop(scheduler);
        if (co == NULL) {
            // Yield the scheduler, give back cpu to origin thread.
            coroutine_yield(scheduler_co);// 如果任务队列是空，调度器会释放当前线程，让当前线程去执行普通的代码。当添加新任务来了，就会从这里重新执行。
            
            // When some coroutine add to the scheduler's queue,
            // the scheduler will resume again,
            // then will resume here, continue the loop.
            continue;
        }
        // Set scheduler's current running coroutine.
        scheduler->running_coroutine = co;
        // Resume the coroutine
        coroutine_resume_im(co);
        // Set scheduler's current running coroutine to nil.
        scheduler->running_coroutine = nil;
        
        // 如果异步任务执行完成了，就是否内存
        // if coroutine finished, free coroutine.
        if (co->status == COROUTINE_DEAD) {
            coroutine_close_ifdead(co);
        }
    }
}

coroutine_scheduler_t *coroutine_scheduler_new(void) {
    
    coroutine_scheduler_t *scheduler = calloc(1, sizeof(coroutine_scheduler_t));
    // 创建一个特殊的coroutine_t。它执行异步任务coroutine_scheduler_main，它用来调度异步任务的
    coroutine_t *co = coroutine_create((void(*)(void *))coroutine_scheduler_main);
    co->stack_size = 16 * 1024; // scheduler does not need so much stack memory.
    scheduler->main_coroutine = co; // 调度器设置主协程对象，它是一个loop，循环监听队列是否存在co异步任务
    co->scheduler = scheduler;// co协程结构体 设置调度器
    co->is_scheduler = true; // 修改协程结构体 is_scheduler标记，标记为处理调度的特殊协程
    return scheduler;
}

void coroutine_scheduler_free(coroutine_scheduler_t *schedule) {
    // 对 schedule->main_coroutine 释放co结构体的userdata对象（co OC实例），free co结构体
    coroutine_close_ifdead(schedule->main_coroutine);
}

coroutine_t *coroutine_create(coroutine_func func) {
    coroutine_t *co = calloc(1, sizeof(coroutine_t));
    co->entry = func;
    co->stack_size = STACK_SIZE;
    co->status = COROUTINE_READY;
    
    // check debugger is attached, fix queue debugging.
    co_rebind_backtrace();
    return co;
}


void coroutine_setuserdata(coroutine_t* co, void* userdata, coroutine_func ud_dispose) {
    if (co->userdata && co->userdata_dispose) {
        co->userdata_dispose(co->userdata);
    }
    co->userdata = userdata;
    co->userdata_dispose = ud_dispose;
}

void *coroutine_getuserdata(coroutine_t* co) {
    
    return co->userdata;
}

void coroutine_close(coroutine_t *co) {
    
    coroutine_setuserdata(co, nil, nil);
    if (co->stack_memory) {
        coroutine_memory_free(co->stack_memory, co->stack_size);
    }
    free(co->context);
    free(co->pre_context);
    free(co);
}

void coroutine_close_ifdead(coroutine_t *co) {
    
    if (co->status == COROUTINE_DEAD) {
        coroutine_close(co);
    }
}
__attribute__ ((optnone))
static void coroutine_main(coroutine_t *co) {
    co->status = COROUTINE_RUNNING;// 异步任务开始执行的状态
    co->entry(co);
    co->status = COROUTINE_DEAD;// 异步任务执行完成的状态
    coroutine_setcontext(co->pre_context);
}

// use optnone to keep the `skip` not be optimized.
__attribute__ ((optnone))
void coroutine_resume_im(coroutine_t *co) {
    switch (co->status) {
        case COROUTINE_READY:
        {
            co->stack_memory = coroutine_memory_malloc(co->stack_size);// 创建一个栈内存
            co->stack_top = co->stack_memory + co->stack_size - 3 * sizeof(void *);
            // get the pre context
            co->pre_context = malloc(sizeof(coroutine_ucontext_t));
            BOOL skip = false;
            // 保存当前的环境, 当执行co协程任务后，会从这里返回
            coroutine_getcontext(co->pre_context);
            if (skip) {
                // when proccess reenter(resume a coroutine), skip the remain codes, just return to pre func.
                return;
            }
#pragma unused(skip)
            skip = true;
            
            free(co->context);
            co->context = calloc(1, sizeof(coroutine_ucontext_t));
            coroutine_makecontext(co->context, (IMP)coroutine_main, co, (void *)co->stack_top);
            // setcontext
            
            coroutine_begin(co->context);
            // 执行 coroutine_main的代码，并且可能会执行 entry 的时候，发生yield
            break;
        }
        case COROUTINE_SUSPEND:
        {
            BOOL skip = false;
            // 保存当前的环境
            coroutine_getcontext(co->pre_context);
            if (skip) {
                // when proccess reenter(resume a coroutine), skip the remain codes, just return to pre func.
                return;
            }
#pragma unused(skip)
            skip = true;
            // setcontext
            coroutine_setcontext(co->context);
            
            break;
        }
        default:
            assert(false);
            break;
    }
}

__attribute__ ((optnone))
void coroutine_resume(coroutine_t *co) {
    if (!co->is_scheduler) {
        coroutine_scheduler_t *scheduler = coroutine_scheduler_self_create_if_not_exists();
        co->scheduler = scheduler; // 给需要执行的普通co协程，设置调度器
        
        scheduler_queue_push(scheduler, co);// 把需要启动的co添加到调度器scheduler的队列中
        
        if (scheduler->running_coroutine) {
            // resume a sub coroutine.
            /*
             // Set scheduler's current running coroutine.
             scheduler->running_coroutine = co;
             // Resume the coroutine
             coroutine_resume_im(co);// 这里是执行当前任务，如果任务是同步添加到当前队列，scheduler->running_coroutine不会为空
             // Set scheduler's current running coroutine to nil.
             scheduler->running_coroutine = nil;
             */
            
            /*
             从co异步任务中，添加co同步任务，就会来到这里。会挂起主任务从新入队，执行新添加的任务
             co_launch(^{
                 NSLog(@"co_launch");
                 co_launch_now(^{
                     这里是执行当前任务，如果任务是同步添加到当前队列，scheduler->running_coroutine不会为空
                     NSLog(@"co_launch_now");
                 });
             });
             co_launch 和 co_launch 的嵌套添加任务，是不会进入这里的，因为是异步添加。
             co_launch(^{
                 co_launch(^{
             
                 });
             });
             */
            scheduler_queue_push(scheduler, scheduler->running_coroutine);
            coroutine_yield(scheduler->running_coroutine);
        } else {
            // scheduler is idle 启动main_coroutine，也就是loop循环处理scheduler队列中的co任务
            coroutine_resume_im(co->scheduler->main_coroutine);
        }
    }
}

void coroutine_add(coroutine_t *co) {
    if (!co->is_scheduler) {
        coroutine_scheduler_t *scheduler = coroutine_scheduler_self_create_if_not_exists();
        co->scheduler = scheduler;
        if (scheduler->main_coroutine->status == COROUTINE_DEAD) {
            coroutine_close_ifdead(scheduler->main_coroutine);
            coroutine_t *main_co = coroutine_create(coroutine_scheduler_main);
            main_co->is_scheduler = true;
            main_co->scheduler = scheduler;
            scheduler->main_coroutine = main_co;
        }
        scheduler_queue_push(scheduler, co);
        
        if (!scheduler->running_coroutine) {
            coroutine_resume_im(co->scheduler->main_coroutine);
        }
    }
}

// use optnone to keep the `skip` not be optimized.
__attribute__ ((optnone))
void coroutine_yield(coroutine_t *co)
{
    if (co == NULL) {
        // if null
        co = coroutine_self();
    }
    BOOL skip = false;
    coroutine_getcontext(co->context);// 保存当前异步任务的执行状态
    if (skip) {
        return;
    }
#pragma unused(skip)
    skip = true;
    co->status = COROUTINE_SUSPEND;
    coroutine_setcontext(co->pre_context);
}

__attribute__ ((optnone))
coroutine_t *coroutine_self() {
    coroutine_scheduler_t *schedule = coroutine_scheduler_self();
    if (schedule) {
        return schedule->running_coroutine;
    } else {
        return nil;
    }
}

#pragma mark - linked lists

__attribute__ ((optnone))
void scheduler_queue_push(coroutine_scheduler_t *scheduler, coroutine_t *co) {
    coroutine_list_t *queue = &scheduler->coroutine_queue;
    if(queue->tail) {
        // 如果存在队尾就入队
        queue->tail->next = co;
        co->prev = queue->tail;
    } else {
        // 如果不存在队尾，就是设置队头
        queue->head = co;
        co->prev = nil;
    }
    queue->tail = co; // 设置队尾为新添加的对象
    co->next = nil; // 队尾的next置空
}
__attribute__ ((optnone))
coroutine_t *scheduler_queue_pop(coroutine_scheduler_t *scheduler) {
    coroutine_list_t *queue = &scheduler->coroutine_queue;
    coroutine_t *co = queue->head;
    if (co) {
        // 出队
        queue->head = co->next;
        // Actually, co->prev is nil now.
        if (co->next) {
            // 如果队列第二个元素是有值的，就把第二个元素的pre置空
            co->next->prev = co->prev;
        } else {
            // 否则，当前对头元素出队后，列表是空队列，队尾置空
            queue->tail = co->prev;
        }
    }
    return co;
}

#pragma clang diagnostic pop

