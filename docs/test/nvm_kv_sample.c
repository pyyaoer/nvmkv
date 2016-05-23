
#define _GNU_SOURCE
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include <assert.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>
#include <nvm/nvm_kv.h>
#include <nvm/nvm_error.h>

#define MAX_DEV_NAME_SIZE 256
#define SECTOR_SIZE 512
#define KEY_LEN 9
#define VALUE_LEN 1024


int main(int argc, char **argv)
{
    char                  device_name[MAX_DEV_NAME_SIZE];
    char                  pool_tag[16];
    char                 *key_str = NULL;
    char                 *value_str = NULL;
    char                 *value_str2 = NULL;
    char                 *value_emp = NULL;
    char                 *value_emp2 = NULL;
    nvm_kv_key_info_t     key_info;
    int                   optind = 0;
    int                   ret = 0;
    int                   value_len = 0;
    int                   fd = 0;
    int                   pool_id = 0;
    int                   kv_id = 0;
    uint32_t              key_len = 0;
    uint32_t              version = 0;
    uint64_t              cache_size = 4096;
    bool                  read_exact = false;

	// Open device
    strncpy(device_name, argv[++optind], MAX_DEV_NAME_SIZE);
    fd = open(device_name, O_RDWR | O_DIRECT);

	// Open nvmkv
    kv_id = nvm_kv_open(fd, version, NVM_KV_MAX_POOLS,
                        KV_GLOBAL_EXPIRY, cache_size);

	// Open pool sample_pool
    strncpy(pool_tag, "sample_pool", strlen("sample_pool"));
    pool_id = nvm_kv_pool_create(kv_id, (nvm_kv_pool_tag_t *)pool_tag);

	// Set key = "abc_test"
    key_str = (char *) malloc (sizeof(char) * KEY_LEN);
    strcpy(key_str, "abc_test");

	// Malloc value whose length = VALUE_LEN
    value_len = VALUE_LEN;
    posix_memalign((void**) &value_str, SECTOR_SIZE, value_len);
    memset(value_str, 'a', value_len);

	// Malloc value whose length = VALUE_LEN/2
    posix_memalign((void**) &value_str2, SECTOR_SIZE, value_len);
    memset(value_str2, 'b', value_len/2);

	// Malloc value buffer for get
    posix_memalign((void**) &value_emp, SECTOR_SIZE, value_len);
    memset(value_emp, 0, value_len);

    posix_memalign((void**) &value_emp2, SECTOR_SIZE, value_len);
    memset(value_emp2, 0, value_len);

	// Put
    ret = nvm_kv_put(kv_id, pool_id, (nvm_kv_key_t *) key_str, strlen((char *) key_str), value_str, strlen(value_str), 0,true, 0);
	printf ("\tPut: %s\n", value_str);

	// Get
	ret = nvm_kv_get(kv_id, pool_id, (nvm_kv_key_t *) key_str, strlen((char *) key_str), value_emp, value_len, read_exact, &key_info);
	printf("\tGet: %s\n", value_emp);

	// Put
    ret = nvm_kv_put(kv_id, pool_id, (nvm_kv_key_t *) key_str, strlen((char *) key_str), value_str2, strlen(value_str2), 0, true, 0);
	printf ("\tPut: %s\n", value_str2);

	// Get
	ret = nvm_kv_get(kv_id, pool_id, (nvm_kv_key_t *) key_str, strlen((char *) key_str), value_emp2, value_len, read_exact, &key_info);
	printf("\tGet: %s\n", value_emp2);

	// Close the KV Store
    ret = nvm_kv_close(kv_id);

test_exit:
    free(value_str);
    free(value_str2);
	free(value_emp);
	free(value_emp2);
    free(key_str);
    close(fd);

	return 0;
}

