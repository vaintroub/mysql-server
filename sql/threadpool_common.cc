/* Copyright (C) 2012 Monty Program Ab

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */

#include <my_global.h>
#include <violite.h>
#include <sql_class.h>
#include <sql_connect.h>
#include <sql_audit.h>
#include <debug_sync.h>
#include <threadpool.h>
#include <probes_mysql.h>
#include <my_thread_local.h>
#include <mysql/psi/mysql_idle.h>
#include <conn_handler/channel_info.h>
#include <conn_handler/connection_handler_manager.h>
#include <conn_handler/connection_handler_impl.h>
#include <mysqld_thd_manager.h>
#include <mysql/thread_pool_priv.h>


/* Threadpool parameters */

uint threadpool_min_threads;
uint threadpool_idle_timeout;
uint threadpool_size;
uint threadpool_max_size;
uint threadpool_stall_limit;
uint threadpool_max_threads;
uint threadpool_oversubscribe;

/* Stats */
TP_STATISTICS tp_stats;


extern bool do_command(THD*);

/*
  Worker threads contexts, and THD contexts.
  =========================================
  
  Both worker threads and connections have their sets of thread local variables 
  At the moment it is mysys_var (this has specific data for dbug, my_error and 
  similar goodies), and PSI per-client structure.

  Whenever query is executed following needs to be done:

  1. Save worker thread context.
  2. Change TLS variables to connection specific ones using thread_attach(THD*).
     This function does some additional work , e.g setting up 
     thread_stack/thread_ends_here pointers.
  3. Process query
  4. Restore worker thread context.

  Connection login and termination follows similar schema w.r.t saving and 
  restoring contexts. 

  For both worker thread, and for the connection, mysys variables are created 
  using my_thread_init() and freed with my_thread_end().

*/
#ifndef DBUG_OFF
extern "C" thread_local_key_t THR_KEY_mysys;
#endif

struct Worker_thread_context
{
#ifdef HAVE_PSI_THREAD_INTERFACE
  PSI_thread *psi_thread;
#endif

#ifndef DBUG_OFF
  my_thread_id thread_id;
#endif

  void save()
  {
#ifdef HAVE_PSI_THREAD_INTERFACE
    psi_thread= PSI_THREAD_CALL(get_thread)();
#endif
#ifndef DBUG_OFF
    thread_id= my_thread_var_id();
#endif
  }

  void restore()
  {
#ifdef HAVE_PSI_THREAD_INTERFACE
    PSI_THREAD_CALL(set_thread)(psi_thread);
#endif
#ifndef DBUG_OFF
    set_my_thread_var_id(thread_id);
#endif
    my_set_thread_local(THR_THD, 0);
    my_set_thread_local(THR_MALLOC, 0);
  }
};


/*
Attach/associate the connection with the OS thread,
*/
static bool thread_attach(THD* thd)
{
#ifndef DBUG_OFF
  set_my_thread_var_id(thd->thread_id());
#endif
  thd->thread_stack=(char*)&thd;
  thd->store_globals();
#ifdef HAVE_PSI_THREAD_INTERFACE
  PSI_THREAD_CALL(set_thread)(thd->get_psi());
#endif
  mysql_socket_set_thread_owner(thd->get_protocol_classic()->get_vio()->mysql_socket);
  return 0;
}



#ifdef HAVE_PSI_INTERFACE

/*
  The following fixes PSI "idle" psi instrumentation. The server assumes that connection
  becomes idle just before net_read_packet() and switches to active after it.
*/

extern void net_before_header_psi(struct st_net *net, void *user_data, size_t /* unused: count */);

static void dummy_net_before_header_psi(struct st_net *, void *, size_t)
{
}

static void re_init_net_server_extension(THD *thd)
{
  NET *net= thd->get_protocol_classic()->get_net();
  if (net->extension)
  {
    DBUG_ASSERT(net->extension == &thd->m_net_server_extension);
    DBUG_ASSERT(thd->m_net_server_extension.m_before_header == net_before_header_psi);
    thd->m_net_server_extension.m_before_header = dummy_net_before_header_psi;
  }
}

#else

#define re_init_net_server_extension(thd)

#endif /* HAVE_PSI_INTERFACE */


static inline void set_thd_idle(THD *thd)
{
  thd_set_net_read_write(thd, 1);
  thd->m_server_idle = true;
#ifdef HAVE_PSI_INTERFACE
  NET* net= thd->get_protocol_classic()->get_net();
  if (net->extension)
  {
    DBUG_ASSERT(net->extension == &thd->m_net_server_extension);
    DBUG_ASSERT(thd->m_net_server_extension.m_before_header == dummy_net_before_header_psi);
    net_before_header_psi(net, thd, 0);
  }
#endif
}

/* Abort a connection which does not have a corresponding THD */
THD* threadpool_create_thd(Channel_info **channel_info)
{
  THD *thd = (*channel_info)->create_thd();
  if (!thd)
  {
    (*channel_info)->send_error_and_close_channel(ER_OUT_OF_RESOURCES, 0, false);
    Connection_handler_manager::get_instance()->inc_aborted_connects();
    Connection_handler_manager::dec_connection_count();
  }
  delete *channel_info;
  *channel_info= 0;
  return thd;
}


int threadpool_add_connection(THD *thd)
{
  int retval=1;

  Worker_thread_context worker_context;
  worker_context.save();
  /*
    Create a new connection context: mysys_thread_var and PSI thread
    Store them in THD.
  */
  //my_thread_init();

  thd->set_new_thread_id();

  /* Create new PSI thread for use with the THD. */
#ifdef HAVE_PSI_THREAD_INTERFACE
  thd->set_psi(PSI_THREAD_CALL(new_thread)(key_thread_one_connection, thd,
    thd->thread_id()));
#endif

  /* Login. */
  thread_attach(thd);
  re_init_net_server_extension(thd);
  ulonglong now= my_micro_time();
  thd->start_utime= now;
  thd->thr_create_utime= now;

  if (thd->store_globals())
  {
    close_connection(thd, ER_OUT_OF_RESOURCES);
    goto end;
  }

  Global_THD_manager::get_instance()->add_thd(thd);

  if (thd_prepare_connection(thd))
    goto end;

   /* 
    Check if THD is ok, as prepare_new_connection_state()
    can fail, for example if init command failed.
   */
  if (!thd_connection_alive(thd))
    goto end;

  retval= 0;
  set_thd_idle(thd);

end:
  if (retval)
  {
    Connection_handler_manager::get_instance()->inc_aborted_connects();
  }
  worker_context.restore();
  return retval;
}


void threadpool_remove_connection(THD *thd)
{

  Worker_thread_context worker_context;
  worker_context.save();

  thread_attach(thd);
  thd_set_net_read_write(thd, 0);

  end_connection(thd);
  close_connection(thd, 0);

  thd->get_stmt_da()->reset_diagnostics_area();
  thd->release_resources();
  Global_THD_manager::get_instance()->remove_thd(thd);
  dec_connection_count();

  delete thd;

  worker_context.restore();
}

/**
 Process a single client request or a single batch.
*/
int threadpool_process_request(THD *thd)
{
  int retval= 0;
  Worker_thread_context  worker_context;
  worker_context.save();

  thread_attach(thd);
  thd_set_net_read_write(thd, 0);

  if (thd->killed == THD::KILL_CONNECTION)
  {
    /* 
      killed flag was set by timeout handler 
      or KILL command. Return error.
    */
    retval= 1;
    goto end;
  }


  /*
    In the loop below, the flow is essentially the copy of thead-per-connections
    logic, see do_handle_one_connection() in sql_connect.c

    The goal is to execute a single query, thus the loop is normally executed 
    only once. However for SSL connections, it can be executed multiple times 
    (SSL can preread and cache incoming data, and vio->has_data() checks if it 
    was the case).
  */
  for(;;)
  {
    Vio *vio;
    thd_set_net_read_write(thd, 0);
    mysql_audit_release(thd);

    if ((retval= do_command(thd)) != 0)
      goto end;
    if (!thd_connection_alive(thd))
    {
      retval= 1;
      goto end;
    }

    set_thd_idle(thd);

    vio= thd->get_protocol_classic()->get_net()->vio;
    if (!vio->has_data(vio))
    { 
      /* More info on this debug sync is in sql_parse.cc*/
      DEBUG_SYNC(thd, "before_do_command_net_read");
      goto end;
    }
  }

end:
  worker_context.restore();
  return retval;
}

void tp_post_kill_notification(THD *thd)
{
  if (thd == current_thd || thd->system_thread)
    return;
  Vio *vio= thd->get_protocol_classic()->get_net()->vio;
  if (vio)
    vio_shutdown(vio,  SHUT_RD);
}

THD_event_functions tp_event_functions=
{
  tp_wait_begin, tp_wait_end,0
};


/* Connection handler class*/
Thread_pool_connection_handler::Thread_pool_connection_handler()
{
  if (tp_init())
    abort();
  Connection_handler_manager::event_functions= &tp_event_functions;
}

Thread_pool_connection_handler::~Thread_pool_connection_handler()
{
  tp_end();
}

bool Thread_pool_connection_handler::add_connection(Channel_info *channel_info)
{
  return tp_add_connection(channel_info);
}

uint Thread_pool_connection_handler::get_max_threads() const
{
  return threadpool_max_threads;
}

