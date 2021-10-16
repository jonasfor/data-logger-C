#include <stdlib.h>
#include <string.h>

#include "dataextractor_htable.h"

enum {
   MAX_AVERAGE_ENTRIES_PER_BUCKET = 5
};

static int
next_size(int size)
{
   static int sizes[] = {
       2, 5, 11, 23, 47, 97, 197,
       397, 797, 1597, 3203, 6421,
       12853, 25717, 51437, 102877,
       205759, 411527, 823117,
       1646237, 3292489, 6584983
   };
   size_t i;
   int next;

   next = size*2;

   for (i = 0; i < sizeof sizes/sizeof sizes[0]; i++) {
       if (sizes[i] >= next)
           return sizes[i];
   }

   return next;
}

unsigned
string_hash(const char *str)
{
   unsigned h;
   const unsigned char *p;

   h = 0;

   for (p = (const unsigned char *)str; *p; p++)
       h = h*31 ^ (unsigned)*p;

   return h;
}

static void
delete_entry(struct htable *ht, struct htable_entry *e)
{
   if (ht->free_data)
       ht->free_data(e->data);

   if (ht->free_key)
       ht->free_key(e->key);

   free(e);
}

static void
htable_grow(struct htable *ht)
{
   int new_num_buckets;
   struct htable_entry **new_buckets;
   struct htable_entry **pp;

   new_num_buckets = next_size(ht->num_buckets);
   new_buckets = calloc(new_num_buckets, sizeof *new_buckets);

   /* rehash */

   for (pp = ht->buckets; pp != &ht->buckets[ht->num_buckets]; pp++) {
       struct htable_entry *p;

       p = *pp;

       while (p) {
           struct htable_entry *t, **q;

           q = &new_buckets[ht->calc_hash(p->key)%new_num_buckets];
            
           t = p->next;
           p->next = *q;
           *q = p;

           p = t;
       }
   }

   free(ht->buckets);

   ht->buckets = new_buckets;
   ht->num_buckets = new_num_buckets;
}

struct htable_entry *htable_find(struct htable *ht, void *key)
{
   if(!ht) return NULL;

   const size_t k = ht->calc_hash((char*)key) % ht->num_buckets;
   struct htable_entry *p = ht->buckets[k];

   for (p = ht->buckets[k]; p; p = p->next) {
       if (!ht->key_cmp((char*)p->key, (char*)key))
           return p;
   }

   return NULL;
}

void htable_insert(struct htable *ht, void *key, void *data)
{
   struct htable_entry *p, *e, **q;

   if(!ht) return;

   if (ht->num_entries >= MAX_AVERAGE_ENTRIES_PER_BUCKET*ht->num_buckets)
       htable_grow(ht);
 
   q = &ht->buckets[ht->calc_hash(key)%ht->num_buckets];

   /* check for collision */
   for (e = *q; e; e = e->next) {
       if (!ht->key_cmp(e->key, key)) {
           if (ht->free_data)
               ht->free_data(e->data);

           e->data = data;
           return;
       }
   }

   p = malloc(sizeof *p);

   p->key = key;
   p->next = *q;
   p->data = data;

   *q = p;

   ht->num_entries++;
}

void
htable_remove(struct htable *ht, void *key)
{
   if(!ht) return;

   const size_t k = ht->calc_hash(key) % ht->num_buckets;
   struct htable_entry *prev = NULL, *p = ht->buckets[k];

   for (p = ht->buckets[k]; p; prev = p, p = p->next) {
       if (!ht->key_cmp(p->key, key)) {
           if (prev)
               prev->next = p->next;
           else
               ht->buckets[k] = NULL;
            
           delete_entry(ht, p);
           return;
       }
   }
}

struct htable *
htable_make(key_mapper_t key_mapper, key_cmp_t key_cmp,
       free_data_t free_data, free_key_t free_key)
{
   struct htable *ht = malloc(sizeof *ht);

   ht->num_entries = 0;
   ht->num_buckets = next_size(0);
   ht->buckets = calloc(ht->num_buckets, sizeof *ht->buckets);
   ht->calc_hash = key_mapper;
   ht->free_data = free_data;
   ht->free_key = free_key;
   ht->key_cmp = key_cmp;

   return ht;
}

void
htable_free(struct htable *ht)
{
   struct htable_entry **pp;

   if(!ht) return;

   for (pp = ht->buckets; pp != &ht->buckets[ht->num_buckets]; pp++) {
       struct htable_entry *p;
       p = *pp;

       while (p) {
           struct htable_entry *next = p->next;
           delete_entry(ht, p);
           p = next;
       }
   }

   free(ht->buckets);
   free(ht);
}
