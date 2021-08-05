#ifndef _DATA_H_
#define _DATA_H_

#include <time.h>
#include <ctype.h>

#include "session_manager.h"
#include "dataextractor_htable.h"


/**
 * \brief defines
*/
#define _TIER_1_ 1
#define _TIER_2_ 2
#define _TIER_3_ 3
#define _TIER_4_ 4
#define _TIER_5_ 5

#define _DATA_EXTRACTOR_TIER_ 2
#define _DATA_EXTRACTOR_LOAD_POINT_ 1
#define _DATA_EXTRACTOR_EVENTS_MAX_QTD_ 10

#define _DB_EXTRACTOR_CREATE_ 1
#define _DB_EXTRACTOR_INSERT_ 2
#define _DB_EXTRACTOR_ALTER_ 3
#define _DB_EXTRACTOR_DELETE_ 4

#define GET_VAR_NAME(var)  #var
#define NUMBER_DATA_VARS(x)  (sizeof(x) / sizeof((x)[0]))

/**
 * \brief Typedefs
*/
typedef struct htable      STRUCT_DATA_EXTRACTOR_VARS;
typedef struct data        STRUCT_DATA_EXTRACTOR;
typedef struct queue       STRUCT_DATA_EXTRACTOR_QUEUE;
typedef struct event       STRUCT_DATA_EXTRACTOR_EVENT;
typedef struct event_args  STRUCT_DATA_EXTRACTOR_EVENT_ARGS;


typedef enum{
   EVENTS_priority_0 = 0, //HIGH
   EVENTS_priority_1,
   EVENTS_priority_2,
   EVENTS_priority_3,
   EVENTS_priority_4,    //LOW
} EVENTS_priority;

/**
 * \brief MAIN VARIABLE (DATASET)
*/
//-------------------------------            -------------------------------
//------     HEY DEVS,     ------            ----------  VAR TYPES  --------
//------   PUT YOUR DATA   ------            ---                         ---
//------       HERE!       ------            ---            int          ---
//-------------------------------            ---       unsigned int      ---
//-------------------------------            ---         long int        ---
//-------------------------------            ---     unsigned long int   ---
//-------       MODEL      ------            ---         short int       ---
//-------    {key, type}   ------            ---   unsigned short int    ---
//-------------------------------            ---          float          ---
//-------------------------------            ---          double         ---
//------                    -----            ---           char          ---
//------      POSSIBEL      -----            ---      unsigned char      ---
//------   VARIABLE TYPES   -----            ---         bool(0-1)       ---
//------     -------->      -----            ---          string         ---
//-------------------------------            -------------------------------
//-------------------------------            -------------------------------

static char* data_labels[][2] = {
#if (_DATA_EXTRACTOR_TIER_ >= _TIER_1_)
   {"string"       ,  "SESSION_ID"},
   {"string"       ,  "SESSION_DURATION" },
   {"int"          ,  "SESSION_CASHIN"},
   {"string"       ,  "SESSION_DATE"  },
   {"bool"         ,  "SESSION_SHOW" }
#endif
#if (_DATA_EXTRACTOR_TIER_ >= _TIER_2_)
  ,{"int"          ,  "cash_out"},
   {"int"          ,  "bet_avg" },
   {"int"          ,  "card_avg"},
   {"unsigned int" ,  "clicks"  },
   {"unsigned int" ,  "touches" }
#endif
#if (_DATA_EXTRACTOR_TIER_ >= _TIER_3_)
   ,{"int"          ,  "new_var1"},
    {"int"          ,  "new_var2" },
    {"int"          ,  "new_var3"},
    {"unsigned int" ,  "new_var4"  },
    {"unsigned int" ,  "new_var5" }
#endif
#if (_DATA_EXTRACTOR_TIER_ >= _TIER_4_)
   ,{"int"          ,  "4cash_out"},
   {"int"          ,  "4bet_avg" },
   {"int"          ,  "4card_avg"},
   {"unsigned int" ,  "4clicks"  },
   {"unsigned int" ,  "4touches" }
#endif
#if (_DATA_EXTRACTOR_TIER_ >= _TIER_5_)
   ,{"int"          ,  "5cash_out"},
   {"int"          ,  "5bet_avg" },
   {"int"          ,  "5card_avg"},
   {"unsigned int" ,  "5clicks"  },
   {"unsigned int" ,  "5touches" }
#endif
                               };

/**
 * \brief Struct of data session
*/
struct data
{
   STRUCT_CURRENT_SESSION *session;          //Session infos
   STRUCT_DATA_EXTRACTOR_VARS *data_vars;    //List of data vars
};

/**
 * \brief Struct of data queue
*/
struct queue
{
   STRUCT_DATA_EXTRACTOR *data;          //data infos
   STRUCT_DATA_EXTRACTOR_QUEUE *next;    //next one
};

/**
 * \brief Struct of data list
*/
struct event_args
{
   int from_row;
   int to_row;
   BOOL player;
   BOOL show;
   char* list_column;
   char* custom_sql_append;

};

struct event
{
   EVENTS_priority priority;
   STRUCT_DATA_EXTRACTOR_EVENT_ARGS args;
};



//DATA_EXTRACTOR PROTOTYPES
void DATAEXTRACTOR_Init(void);
void DATAEXTRACTOR_InitDataSession(void);
void DATAEXTRACTOR_FinishDataSession(void);
void DATAEXTRACTOR_UpdateVar(char* label, void* value);
struct htable_entry* DATAEXTRACTOR_Find(char* label, STRUCT_DATA_EXTRACTOR *extractor);

void DATAEXTRACTOR_QUEUE_Insert(void);
void DATAEXTRACTOR_QUEUE_Print(void);
void DATAEXTRACTOR_QUEUE_LoadSql(void);
char* DATAEXTRACTOR_QUEUE_CreateQuery(int operation, char column_name[NUMBER_DATA_VARS(data_labels)][MAX_BIND_NAME], int column_counter);

void DATAEXTRACTOR_EVENT_EventLoop(void);
void DATAEXTRACTOR_EVENT_CreateEvent(EVENTS_priority priority, int send_from, int send_to, BOOL show, BOOL player, char* list_column, char* custom_sql_append);
#endif //_DATA_H_
