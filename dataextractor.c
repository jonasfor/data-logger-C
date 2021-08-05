#include "dataextractor.h"
#include <sqlite3.h>

STRUCT_DATA_EXTRACTOR* current_extractor = NULL;
STRUCT_DATA_EXTRACTOR_QUEUE* queue_extractor = NULL;
STRUCT_DATA_EXTRACTOR_EVENT events_extractor[_DATA_EXTRACTOR_EVENTS_MAX_QTD_];
int queue_counter = 0;
int event_counter = 0;

//#########################################################################################################
//#########################################################################################################
//############################          HASH SYSTEM FUNCTIONS         #####################################
//#########################################################################################################
//#########################################################################################################

static size_t key_mapper(const void *key)
{
   return (size_t) key;
}

static int key_cmp(const void *k1, const void *k2)
{
   const intptr_t res = (intptr_t) k1 - (intptr_t) k2;
   return res < 0 ? -1 : res > 0 ? 1 : 0;
}

//#########################################################################################################
//#########################################################################################################
//############################          DATA SYSTEM FUNCTIONS         #####################################
//#########################################################################################################
//#########################################################################################################

void DATAEXTRACTOR_Init(void)
{
   current_extractor = calloc(1, sizeof(STRUCT_DATA_EXTRACTOR));
   queue_extractor = calloc(1, sizeof(STRUCT_DATA_EXTRACTOR_QUEUE));
   DATAEXTRACTOR_InitDataSession();
   memset(&events_extractor, -1, sizeof(events_extractor));

}

void DATAEXTRACTOR_InitDataSession(void)
{
   memset(current_extractor, 0, sizeof(STRUCT_DATA_EXTRACTOR));

   //Get Session_struct information
   current_extractor->session = SESSION_GetSession();
   //Get data hash
   current_extractor->data_vars = htable_make(key_mapper, key_cmp, NULL, NULL);
   //Insert all data variables on hash
   for(int i = 0; i < NUMBER_DATA_VARS(data_labels); i++)
   {
       htable_insert(current_extractor->data_vars, (void *) (char *) data_labels[i][1], NULL);
   }
}

void DATAEXTRACTOR_FinishDataSession(void)
{
   //Insert one dataset on queue
   DATAEXTRACTOR_QUEUE_Insert();

   if(queue_counter == _DATA_EXTRACTOR_LOAD_POINT_)
   {
       DATAEXTRACTOR_QUEUE_LoadSql();
   }

   //Clear current dataset
   memset(current_extractor, 0, sizeof(STRUCT_DATA_EXTRACTOR));
}

struct htable_entry* DATAEXTRACTOR_Find(char* label, STRUCT_DATA_EXTRACTOR *extractor)
{
   struct htable_entry *e = htable_find(extractor->data_vars, (void *) (intptr_t) label);

   if(e) return e;
   else return NULL;

}

void DATAEXTRACTOR_UpdateVar(char* label, void* value)
{
   int index = -1;

   for(int i; i<NUMBER_DATA_VARS(data_labels); i++)
   {
       if(!strcmp(data_labels[i][1], label))
       {
           index = i;
           break;
       }
   }

   if(DATAEXTRACTOR_Find(data_labels[index][1], current_extractor))
   {

       if(!strcmp(data_labels[index][0], "int"))
       {
         DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->int_p = calloc(1, sizeof(int));
         memcpy(DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->int_p, value, sizeof(int));
         DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->data = DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->int_p;
       }
       else if(!strcmp(data_labels[index][0], "unsigned int"))
       {
         DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->uint_p = calloc(1, sizeof(unsigned int));
         memcpy(DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->uint_p, value, sizeof(unsigned int));
         DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->data = DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->uint_p;
       }
       else if(!strcmp(data_labels[index][0], "long int"))
       {
         DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->lint_p = calloc(1, sizeof(long int));
         memcpy(DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->lint_p, value, sizeof(long int));
         DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->data = DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->lint_p;
       }
       else if(!strcmp(data_labels[index][0], "unsigned long int"))
       {
         DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->ulint_p = calloc(1, sizeof(unsigned long int));
         memcpy(DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->ulint_p, value, sizeof(unsigned long int));
         DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->data = DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->ulint_p;
       }
       else if(!strcmp(data_labels[index][0], "short int"))
       {
         DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->sint_p = calloc(1, sizeof(short int));
         memcpy(DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->sint_p, value, sizeof(short int));
         DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->data = DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->sint_p;
       }
       else if(!strcmp(data_labels[index][0], "unsigned short int"))
       {
         DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->usint_p = calloc(1, sizeof(unsigned short int));
         memcpy(DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->usint_p, value, sizeof(unsigned short int));
         DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->data = DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->usint_p;
       }
       else if(!strcmp(data_labels[index][0], "float"))
       {
         DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->float_p = calloc(1, sizeof(float));
         memcpy(DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->float_p, value, sizeof(float));
         DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->data = DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->float_p;
       }
       else if(!strcmp(data_labels[index][0], "double"))
       {
         DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->double_p = calloc(1, sizeof(double));
         memcpy(DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->double_p, value, sizeof(double));
         DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->data = DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->double_p;
       }
       else if(!strcmp(data_labels[index][0], "char"))
       {
         DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->char_p = calloc(1, sizeof(char));
         memcpy(DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->char_p, value, sizeof(char));
         DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->data = DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->char_p;
       }
       else if(!strcmp(data_labels[index][0], "unsigned char"))
       {
         DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->uchar_p = calloc(1, sizeof(unsigned char));
         memcpy(DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->uchar_p, value, sizeof(unsigned char));
         DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->data = DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->uchar_p;
       }
       else if(!strcmp(data_labels[index][0], "string"))
       {
         DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->char_p = calloc(MAX_BIND_NAME, sizeof(char));
         memcpy(DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->char_p, value, MAX_BIND_NAME * sizeof(char));
         DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->data = DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->char_p;
       }
       if(!strcmp(data_labels[index][0], "bool"))
       {
         DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->char_p = calloc(5, sizeof(char));
         if(!value || *(int*)value == 0) strcpy(DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->char_p, "false");
         else strcpy(DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->char_p, "true");
         DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->data = DATAEXTRACTOR_Find(data_labels[index][1], current_extractor)->char_p;
       }

   }
   else
   {
       printf("DATAEXTRACTOR Error: key not finded !");
   }
}

//#########################################################################################################
//#########################################################################################################
//############################         QUEUE SYSTEM FUNCTIONS         #####################################
//#########################################################################################################
//#########################################################################################################

void DATAEXTRACTOR_QUEUE_Insert(void)
{
   if(!current_extractor->data_vars) return;

   STRUCT_DATA_EXTRACTOR_QUEUE *tmp = queue_extractor;
   STRUCT_DATA_EXTRACTOR_QUEUE *new_node = calloc(1, sizeof(STRUCT_DATA_EXTRACTOR_QUEUE));

   while(tmp->next != NULL)
   {
       tmp = tmp->next;
   }

   new_node->next = NULL;
   new_node->data = current_extractor;
   tmp->next = new_node;
   queue_counter++;
}

void DATAEXTRACTOR_QUEUE_GarbageCollector(void)
{
    //Controle de tamanho de arquivos de debug
   FILE *p = fopen("./analytics/debug/querys_data_extractor.txt", "a+");
   fseek(p, 0, SEEK_END);
   if (ftell(p) > 10000)
   {
       remove("./analytics/debug/querys_data_extractor.txt");
   }
   fclose(p);
}

void DATAEXTRACTOR_QUEUE_LoadSql(void)
{

   sqlite3 *database;
   char    *ErrMsg = 0;
   int      rc;
   char    *sql_query = calloc(1000, sizeof(char));;
   static char column_name[NUMBER_DATA_VARS(data_labels)][MAX_BIND_NAME];
   int column_counter = 0;



   queue_counter = 0;

   //  OPEN OR CREATE IF NOT EXIST SQLITE3 DATABASE
   rc = sqlite3_open("./analytics/db/extractor.db", &database);
   if( rc )
   {
       FILE *p = fopen("./analytics/debug/querys_data_extractor.txt", "a+");
       fprintf(p, "%s\n\n",  sqlite3_errmsg(database));
       fclose(p);
       return;
   }
   else
   {
       sqlite3_stmt* stmt;

       column_counter = 0;
       memset(column_name, 0, sizeof(column_name));
       rc = sqlite3_prepare_v2(database, "PRAGMA table_info('EXTRACTOR');", -1, &stmt, 0);


       while( sqlite3_step(stmt)==SQLITE_ROW )
       {
           for(int  i=0; i<sqlite3_column_count(stmt); i++)
           {
                 if(strcmp(sqlite3_column_name(stmt,i), "name") == 0)
                 {
                     strcpy(column_name[column_counter], (const char *)sqlite3_column_text(stmt,i));
                     column_counter++;
                     break;
                 }
           }
       }

       sqlite3_finalize(stmt);

   }

   //  OPEN OR CREATE IF NOT EXIST SQLITE3 TABLE
   strcpy(sql_query, DATAEXTRACTOR_QUEUE_CreateQuery(_DB_EXTRACTOR_CREATE_, column_name, column_counter));
   rc = sqlite3_exec(database, sql_query, NULL, NULL, &ErrMsg);
   if( rc != SQLITE_OK )
   {
          FILE *p = fopen("./analytics/debug/querys_data_extractor.txt", "a+");
          fprintf(p, "ERROR : %s\n",  ErrMsg);
          fprintf(p, "QUERY (CREATE) : %s\n\n",  sql_query);
          fclose(p);
          sqlite3_free(ErrMsg);
   }

   // INSERT INTO DB IF SOME COLUMN NOT EXIST ALTER TABLE TO ADD NEW COLUMN
   memset(sql_query, 0, sizeof(&sql_query));
   strcpy(sql_query, DATAEXTRACTOR_QUEUE_CreateQuery(_DB_EXTRACTOR_INSERT_, column_name, column_counter));
   rc = sqlite3_exec(database, sql_query, NULL, NULL, &ErrMsg);
   if( rc != SQLITE_OK )
   {
       if(strstr(ErrMsg, "has no column") == NULL)
       {
           FILE *p = fopen("./analytics/debug/querys_data_extractor.txt", "a+");
           fprintf(p, "ERROR : %s\n",  ErrMsg);
           fprintf(p, "QUERY (INSERT) : %s\n\n",  sql_query);
           fclose(p);
           sqlite3_free(ErrMsg);
       }
       else
       {
           strcpy(sql_query, DATAEXTRACTOR_QUEUE_CreateQuery(_DB_EXTRACTOR_ALTER_, column_name, column_counter));
           rc = sqlite3_exec(database, sql_query, NULL, NULL, &ErrMsg);
           if( rc != SQLITE_OK )
           {
               FILE *p = fopen("./analytics/debug/querys_data_extractor.txt", "a+");
               fprintf(p, "ERROR : %s\n",  ErrMsg);
               fprintf(p, "QUERY (ALTER) : %s\n\n",  sql_query);
               fclose(p);
               sqlite3_free(ErrMsg);
           }
       }
   }




//  CLOSE SQLITE3 DATABASE
   sqlite3_close(database);
   DATAEXTRACTOR_QUEUE_GarbageCollector();

}

char* DATAEXTRACTOR_UpperCase(char* var_name, int size){

   int i = 0;
   char chr;
   char *upper_var_name = calloc(size, sizeof(char));

       // Loop
       while (var_name[i]) {
           chr = var_name[i];
           upper_var_name[i] = toupper(chr);
           i++;
       }

       return upper_var_name;
}

char* DATAEXTRACTOR_QUEUE_CreateQuery(int operation, char column_name[NUMBER_DATA_VARS(data_labels)][MAX_BIND_NAME], int column_counter){
   char *sql = calloc(1000, sizeof(char));
   STRUCT_DATA_EXTRACTOR_QUEUE *tmp = queue_extractor;


   switch (operation) {
   case _DB_EXTRACTOR_CREATE_:

       sprintf(sql , "%s", "CREATE TABLE IF NOT EXISTS EXTRACTOR(");

       for(int i=0; i<NUMBER_DATA_VARS(data_labels); i++)
       {
           char* type;
           char* var_name = DATAEXTRACTOR_UpperCase(data_labels[i][1], strlen(data_labels[i][1]));


           if(strcmp(var_name, "SESSION_ID")==0)
           {
               strcat(sql, " SESSION_ID TEXT PRIMARY KEY NOT NULL,");
               continue;
           }
           if(strcmp(var_name, "SESSION_DURATION")==0)
           {
               strcat(sql, " SESSION_DURATION TEXT NOT NULL,");
               continue;
           }
           if(strcmp(var_name, "SESSION_DATE")==0)
           {
               strcat(sql, " SESSION_DATE DATETIME,");
               continue;
           }

           if(!strcmp(data_labels[i][0], "int") ||
              !strcmp(data_labels[i][0], "unsigned int "))              type = "INT";

           else if(!strcmp(data_labels[i][0], "long int"))              type = "BIGINT";
           else if(!strcmp(data_labels[i][0], "unsigned long int"))     type = "UNSIGNED BIG INT";

           else if(!strcmp(data_labels[i][0], "short int") ||
                   !strcmp(data_labels[i][0], "unsigned short int"))    type = "SMALLINT";

           else if(!strcmp(data_labels[i][0], "float"))                 type = "FLOAT";
           else if(!strcmp(data_labels[i][0], "double"))                type = "DOUBLE";

           else if(!strcmp(data_labels[i][0], "char") ||
                   !strcmp(data_labels[i][0], "unsigned char") ||
                   !strcmp(data_labels[i][0], "bool") ||
                   !strcmp(data_labels[i][0], "string"))                type = "TEXT";


           strcat(sql, var_name);
           strcat(sql, " ");
           strcat(sql, type);

           if(i == NUMBER_DATA_VARS(data_labels)-1)
               strcat(sql, ");");
           else
               strcat(sql, ",");
       }

       break;


   case _DB_EXTRACTOR_INSERT_:


       while(tmp != NULL)
       {
           if(tmp->data)
           {
               sprintf(sql , "%s", "INSERT INTO EXTRACTOR(");

               for(int i=0; i<NUMBER_DATA_VARS(data_labels); i++)
               {
                   char* var_name = DATAEXTRACTOR_UpperCase(data_labels[i][1], strlen(data_labels[i][1]));
                   strcat(sql, var_name);

                   if(i == NUMBER_DATA_VARS(data_labels)-1)
                       strcat(sql, ") ");
                   else
                       strcat(sql, ", ");
               }

               char* int_char = calloc(100, sizeof(char));

               strcat(sql, "VALUES(");


               for(int i = 0; i < NUMBER_DATA_VARS(data_labels); i++)
               {
                   if(strcmp(data_labels[i][1], "SESSION_ID")==0)
                   {
                       sprintf(int_char, "'%s'", tmp->data->session->session_id);
                       strcat(sql, int_char);
                       strcat(sql, ", ");
                       continue;
                   }
                   if(strcmp(data_labels[i][1], "SESSION_DURATION")==0)
                   {
                       sprintf(int_char, "%ld", SESSION_Get_session_duration());
                       strcat(sql, int_char);
                       strcat(sql, ", ");
                       continue;
                   }
                   if(strcmp(data_labels[i][1], "SESSION_CASHIN")==0)
                   {
                       sprintf(int_char, "'%d'", tmp->data->session->total_cashIn);
                       strcat(sql, int_char);
                       strcat(sql, ", ");
                       continue;
                   }
                   if(strcmp(data_labels[i][1], "SESSION_DATE")==0)
                   {
                       sprintf(int_char, "'%s'", tmp->data->session->date_time);
                       strcat(sql, int_char);
                       strcat(sql, ", ");
                       continue;
                   }
                   if(strcmp(data_labels[i][1], "SESSION_SHOW")==0)
                   {
                       sprintf(int_char, "%d", tmp->data->session->is_show);
                       strcat(sql, int_char);
                       strcat(sql, ", ");
                       continue;
                   }
                   if(DATAEXTRACTOR_Find(data_labels[i][1], tmp->data))
                   {
                       if(DATAEXTRACTOR_Find(data_labels[i][1], tmp->data)->data)
                       {
                           if(!strcmp(data_labels[i][0], "int"))
                                sprintf(int_char, "%d",  *(int *)DATAEXTRACTOR_Find(data_labels[i][1], tmp->data)->data);

                           else if(!strcmp(data_labels[i][0], "unsigned int"))
                                sprintf(int_char, "%u",  *(unsigned int *)DATAEXTRACTOR_Find(data_labels[i][1], tmp->data)->data);

                           else if(!strcmp(data_labels[i][0], "long int"))
                                sprintf(int_char, "%li", *(long int *)DATAEXTRACTOR_Find(data_labels[i][1], tmp->data)->data);

                           else if(!strcmp(data_labels[i][0], "unsigned long int"))
                                sprintf(int_char, "%lu", *(unsigned long int *)DATAEXTRACTOR_Find(data_labels[i][1], tmp->data)->data);

                           else if(!strcmp(data_labels[i][0], "short int"))
                                sprintf(int_char, "%hi", *(short int *)DATAEXTRACTOR_Find(data_labels[i][1], tmp->data)->data);

                           else if(!strcmp(data_labels[i][0], "unsigned short int"))
                                sprintf(int_char, "%hu", *(unsigned short int *)DATAEXTRACTOR_Find(data_labels[i][1], tmp->data)->data);

                           else if(!strcmp(data_labels[i][0], "float"))
                                sprintf(int_char, "%f",  *(float *)DATAEXTRACTOR_Find(data_labels[i][1], tmp->data)->data);

                           else if(!strcmp(data_labels[i][0], "double"))
                               sprintf(int_char, "%lf", *(double *)DATAEXTRACTOR_Find(data_labels[i][1], tmp->data)->data);

                           else if(!strcmp(data_labels[i][0], "char"))
                               sprintf(int_char, "'%c'",  *(char *)DATAEXTRACTOR_Find(data_labels[i][1], tmp->data)->data);

                           else if(!strcmp(data_labels[i][0], "unsigned char"))
                               sprintf(int_char, "'%c'",  *(unsigned char *)DATAEXTRACTOR_Find(data_labels[i][1], tmp->data)->data);

                           else if(!strcmp(data_labels[i][0], "string"))
                               sprintf(int_char, "'%s'", (char *)DATAEXTRACTOR_Find(data_labels[i][1], tmp->data)->data);

                           else if(!strcmp(data_labels[i][0], "bool"))
                               sprintf(int_char, "'%s'",  (char *)DATAEXTRACTOR_Find(data_labels[i][1], tmp->data)->data);

                           strcat(sql, int_char);
                       }
                       else
                       {
                           strcat(sql, "0");
                       }

                   }
                   else
                   {
                       strcat(sql, "0");
                   }
                   if(i == NUMBER_DATA_VARS(data_labels)-1)
                       strcat(sql, ");");
                   else
                       strcat(sql, ", ");

                   memset(int_char, 0, sizeof(&int_char));
               }
           }
           tmp = tmp->next;
           strcat(sql, " ");
       }
       free(tmp);
       free(queue_extractor);
       queue_extractor = calloc(1, sizeof(STRUCT_DATA_EXTRACTOR_QUEUE));
       break;


   case _DB_EXTRACTOR_ALTER_:

       sprintf(sql , "%s", "  BEGIN TRANSACTION;\n");

       for(int i=0; i<NUMBER_DATA_VARS(data_labels); i++)
       {
           int finded = 0;
           char* type;
           char* var_name = DATAEXTRACTOR_UpperCase(data_labels[i][1], strlen(data_labels[i][1]));

           for(int j=0; j<column_counter; j++)
           {
               if(strcmp(var_name, column_name[j]) == 0)
               {
                   finded = 1;
                   break;
               }
           }
           if(finded) continue;


           if(!strcmp(data_labels[i][0], "int") ||
              !strcmp(data_labels[i][0], "unsigned int "))              type = "INT";

           else if(!strcmp(data_labels[i][0], "long int"))              type = "BIGINT";
           else if(!strcmp(data_labels[i][0], "unsigned long int"))     type = "UNSIGNED BIG INT";

           else if(!strcmp(data_labels[i][0], "short int") ||
                   !strcmp(data_labels[i][0], "unsigned short int"))    type = "SMALLINT";

           else if(!strcmp(data_labels[i][0], "float"))                 type = "FLOAT";
           else if(!strcmp(data_labels[i][0], "double"))                type = "DOUBLE";

           else if(!strcmp(data_labels[i][0], "char") ||
                   !strcmp(data_labels[i][0], "unsigned char") ||
                   !strcmp(data_labels[i][0], "bool") ||
                   !strcmp(data_labels[i][0], "string"))                type = "TEXT";

           strcat(sql, "ALTER TABLE EXTRACTOR ADD ");
           strcat(sql, var_name);
           strcat(sql, " ");
           strcat(sql, type);

           if(i == NUMBER_DATA_VARS(data_labels)-1)
               strcat(sql, ";\nCOMMIT");
           else
               strcat(sql, ";\n");
       }

       break;
   case _DB_EXTRACTOR_DELETE_:

       break;
   default:
       break;
   }


   return sql;
}

void DATAEXTRACTOR_QUEUE_Print(void){

   STRUCT_DATA_EXTRACTOR_QUEUE *tmp = queue_extractor;
   queue_counter = 0;

   FILE *p = fopen("./analytics/debug/dataExtractor(table).csv", "a+");

   while(tmp != NULL)
   {
       if(tmp->data)
       {
           if(tmp->data->session)
               fprintf(p, "%s,%ld,%d,", tmp->data->session->session_id, SESSION_Get_session_duration(), tmp->data->session->total_cashIn);
           else
               fprintf(p, "null,null,null,");

           for(int i = 0; i < NUMBER_DATA_VARS(data_labels); i++)
           {
               if(DATAEXTRACTOR_Find(data_labels[i][1], tmp->data))
               {
                   if(DATAEXTRACTOR_Find(data_labels[i][1], tmp->data)->key && DATAEXTRACTOR_Find(data_labels[i][1], tmp->data)->data)
                   {
                       if(!strcmp(data_labels[i][0], "int"))
                            fprintf(p, "%d",  *(int *)DATAEXTRACTOR_Find(data_labels[i][1], tmp->data)->data);

                       else if(!strcmp(data_labels[i][0], "unsigned int"))
                            fprintf(p, "%u",  *(unsigned int *)DATAEXTRACTOR_Find(data_labels[i][1], tmp->data)->data);

                       else if(!strcmp(data_labels[i][0], "long int"))
                            fprintf(p, "%li", *(long int *)DATAEXTRACTOR_Find(data_labels[i][1], tmp->data)->data);

                       else if(!strcmp(data_labels[i][0], "unsigned long int"))
                            fprintf(p, "%lu", *(unsigned long int *)DATAEXTRACTOR_Find(data_labels[i][1], tmp->data)->data);

                       else if(!strcmp(data_labels[i][0], "short int"))
                            fprintf(p, "%hi", *(short int *)DATAEXTRACTOR_Find(data_labels[i][1], tmp->data)->data);

                       else if(!strcmp(data_labels[i][0], "unsigned short int"))
                            fprintf(p, "%hu", *(unsigned short int *)DATAEXTRACTOR_Find(data_labels[i][1], tmp->data)->data);

                       else if(!strcmp(data_labels[i][0], "float"))
                            fprintf(p, "%f",  *(float *)DATAEXTRACTOR_Find(data_labels[i][1], tmp->data)->data);

                       else if(!strcmp(data_labels[i][0], "double"))
                           fprintf(p, "%lf", *(double *)DATAEXTRACTOR_Find(data_labels[i][1], tmp->data)->data);

                       else if(!strcmp(data_labels[i][0], "char"))
                           fprintf(p, "%c",  *(char *)DATAEXTRACTOR_Find(data_labels[i][1], tmp->data)->data);

                       else if(!strcmp(data_labels[i][0], "unsigned char"))
                           fprintf(p, "%c",  *(unsigned char *)DATAEXTRACTOR_Find(data_labels[i][1], tmp->data)->data);

                       else if(!strcmp(data_labels[i][0], "string"))
                           fprintf(p, "%s", (char *)DATAEXTRACTOR_Find(data_labels[i][1], tmp->data)->data);

                       else if(!strcmp(data_labels[i][0], "bool"))
                           fprintf(p, "%s",  (char *)DATAEXTRACTOR_Find(data_labels[i][1], tmp->data)->data);
                   }
                   else if (DATAEXTRACTOR_Find(data_labels[i][1], tmp->data)->key)
                   {
                       fprintf(p, "null");
                   }
                   else
                   {
                       fprintf(p, "error");
                   }
                       fprintf(p, ",");

               }
               else
                   fprintf(p, "error");
           }
           fprintf(p, "\n");
       }
       tmp = tmp->next;
   }
   fclose(p);
   memset(&queue_extractor, 0, sizeof(queue_extractor));
   queue_extractor = calloc(1, sizeof(STRUCT_DATA_EXTRACTOR_QUEUE));

}

//#########################################################################################################
//#########################################################################################################
//############################         EVENT SYSTEM FUNCTIONS         #####################################
//#########################################################################################################
//#########################################################################################################

void DATAEXTRACTOR_EVENT_Sort(void)
{
   STRUCT_DATA_EXTRACTOR_EVENT tmp;

   for (int i = 0; i < _DATA_EXTRACTOR_EVENTS_MAX_QTD_; ++i)
   {
       for (int j = i + 1; j < _DATA_EXTRACTOR_EVENTS_MAX_QTD_; ++j)
       {
           if (events_extractor[i].priority > events_extractor[j].priority  &&  events_extractor[i].priority >= 0 &&  events_extractor[j].priority >= 0)
           {
               tmp = events_extractor[i];
               events_extractor[i] = events_extractor[j];
               events_extractor[j] = tmp;
           }
       }
   }
}

void DATAEXTRACTOR_EVENT_CreateEvent(EVENTS_priority priority, int send_from, int send_to, BOOL show, BOOL player, char* list_column, char* custom_sql_append)
{
   STRUCT_DATA_EXTRACTOR_EVENT tmp;

   tmp.priority = priority;
   tmp.args.show = show;
   tmp.args.player = player;
   tmp.args.to_row = send_to;
   tmp.args.from_row = send_from;

   memcpy(&tmp.args.list_column, &list_column, sizeof(list_column));
   memcpy(&tmp.args.custom_sql_append, &custom_sql_append, sizeof(custom_sql_append));

   memcpy(&events_extractor[event_counter], &tmp, sizeof(tmp));


   event_counter++;

   if(event_counter == 10)
   {
       event_counter = 0;
   }

}


void *DATAEXTRACTOR_EVENT_THREAD_SendData(void *arg)
{
   UNUSED(arg);

   int index = *(int *) arg;
   char query[1000];

   if(events_extractor[index].priority == -1) return 0;

   sprintf(query, "nohup python ./analytics/scripts/send_to_server.py --from_row %d --to_row %d --show %d --player %d \0",
           events_extractor[index].args.from_row,
           events_extractor[index].args.to_row,
           events_extractor[index].args.show,
           events_extractor[index].args.player);


   if (events_extractor[index].args.custom_sql_append != NULL)
   {
       strcat(query, " --custom_sql_append ");
       strcat(query, events_extractor[index].args.custom_sql_append );
   }

   if (events_extractor[index].args.list_column != NULL)
   {
       strcat(query, " --list_column ");
       strcat(query, events_extractor[index].args.list_column );
   }

   system(query);

   memset(&events_extractor[index], -1, sizeof(events_extractor[index]));

   return 0;
}


void DATAEXTRACTOR_EVENT_EventLoop(void)
{
   DATAEXTRACTOR_EVENT_Sort();

   static pthread_t thread = NULL;

   if(events_extractor[0].priority == -1)
   {
      thread = 0;
      return;
   }
   for(int i = 0; i < _DATA_EXTRACTOR_EVENTS_MAX_QTD_; i++)
   {
       if(thread)
       {
           if(pthread_tryjoin_np(thread,  NULL) != 0)
           {
               break;
           }
           else
           {
               pthread_kill(thread, 0);
           }
       }

       pthread_create(&thread, NULL, DATAEXTRACTOR_EVENT_THREAD_SendData, &i); //Cria uma task para tipo de evento 1
   }
}
