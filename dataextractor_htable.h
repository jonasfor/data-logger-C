#ifndef HTABLE_H
#define HTABLE_H

typedef size_t (*key_mapper_t) (const void *key);
typedef void (*free_data_t) (void *data);
typedef void (*free_key_t) (void *key);
typedef int (*key_cmp_t) (const void *k1, const void *k2);

struct htable_entry {
    struct htable_entry *next;
    void *key;
    void *data;
    int                *int_p;
    unsigned int       *uint_p;
    long int           *lint_p;
    unsigned long int  *ulint_p;
    short int          *sint_p;
    unsigned short int *usint_p;
    float              *float_p;
    double             *double_p;
    char               *char_p;
    unsigned char      *uchar_p;

};

struct htable {
    int num_buckets;
    int num_entries;
    struct htable_entry **buckets;
    key_mapper_t calc_hash;
    free_data_t free_data;
    free_key_t free_key;
    key_cmp_t key_cmp;
};

unsigned string_hash(const char *str);
struct htable_entry * htable_find(struct htable *ht, void *key);
void htable_insert(struct htable *ht, void *key, void *data);
void htable_remove(struct htable *ht, void *key);
struct htable * htable_make(key_mapper_t key_mapper, key_cmp_t key_cmp,
                            free_data_t free_data, free_key_t free_key);
void htable_free(struct htable *ht);

#endif /* HTABLE_H */
