#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <json-c/json.h>
#include <ctype.h>

typedef struct {
    char name[256];
    int count;
} ItemCount;

int compare_counts(const void *a, const void *b) {
    ItemCount *ia = (ItemCount *)a;
    ItemCount *ib = (ItemCount *)b;
    return ib->count - ia->count;
}

void tokenize_and_count(const char *text, ItemCount **items, int *item_count, int *item_size, int is_word) {
    char token[256];
    int index = 0;
    for (int i = 0; text[i]; i++) {
        if (isalnum(text[i])) {
            if (index < 255) token[index++] = tolower(text[i]);
        } else if (index > 0) {
            token[index] = '\0';
            int found = 0;
            for (int j = 0; j < *item_count; j++) {
                if (strcmp((*items)[j].name, token) == 0) {
                    (*items)[j].count++;
                    found = 1;
                    break;
                }
            }
            if (!found) {
                if (*item_count >= *item_size) {
                    *item_size *= 2;
                    *items = realloc(*items, (*item_size) * sizeof(ItemCount));
                    if (!(*items)) MPI_Abort(MPI_COMM_WORLD, 1);
                }
                strncpy((*items)[*item_count].name, token, 255);
                (*items)[*item_count].name[255] = '\0';
                (*items)[*item_count].count = 1;
                (*item_count)++;
            }
            index = 0;
        }
    }
    if (index > 0) {
        token[index] = '\0';
        int found = 0;
        for (int j = 0; j < *item_count; j++) {
            if (strcmp((*items)[j].name, token) == 0) {
                (*items)[j].count++;
                found = 1;
                break;
            }
        }
        if (!found) {
            if (*item_count >= *item_size) {
                *item_size *= 2;
                *items = realloc(*items, (*item_size) * sizeof(ItemCount));
                if (!(*items)) MPI_Abort(MPI_COMM_WORLD, 1);
            }
            strncpy((*items)[*item_count].name, token, 255);
            (*items)[*item_count].name[255] = '\0';
            (*items)[*item_count].count = 1;
            (*item_count)++;
        }
    }
}

int main(int argc, char *argv[]) {
    MPI_Init(&argc, &argv);
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    MPI_Datatype MPI_ItemCount;
    int block_lengths[2] = {256, 1};
    MPI_Aint displacements[2] = {offsetof(ItemCount, name), offsetof(ItemCount, count)};
    MPI_Datatype types[2] = {MPI_CHAR, MPI_INT};
    MPI_Type_create_struct(2, block_lengths, displacements, types, &MPI_ItemCount);
    MPI_Type_commit(&MPI_ItemCount);

    char *data = NULL;
    long length;
    if (rank == 0) {
        FILE *f = fopen("parte-3/dados_com_etl.json", "rb");
        if (!f) MPI_Abort(MPI_COMM_WORLD, 1);
        fseek(f, 0, SEEK_END);
        length = ftell(f);
        fseek(f, 0, SEEK_SET);
        data = malloc(length + 1);
        if (!data) MPI_Abort(MPI_COMM_WORLD, 1);
        fread(data, 1, length, f);
        data[length] = '\0';
        fclose(f);
    }

    MPI_Bcast(&length, 1, MPI_LONG, 0, MPI_COMM_WORLD);
    if (rank != 0) data = malloc(length + 1);
    MPI_Bcast(data, length + 1, MPI_CHAR, 0, MPI_COMM_WORLD);

    struct json_object *parsed = json_tokener_parse(data);
    if (!parsed) MPI_Abort(MPI_COMM_WORLD, 1);
    int total = json_object_array_length(parsed);
    int chunk = total / size, start = rank * chunk, end = (rank == size - 1) ? total : start + chunk;

    ItemCount *local_artists = malloc(sizeof(ItemCount) * total);
    ItemCount *local_words = malloc(sizeof(ItemCount) * 1024);
    int local_artist_count = 0, local_word_count = 0, local_word_size = 1024;

    for (int i = start; i < end; i++) {
        struct json_object *item = json_object_array_get_idx(parsed, i);
        struct json_object *artist_obj, *text_obj;

        // Count Artists
        if (json_object_object_get_ex(item, "artist", &artist_obj)) {
            const char *artist = json_object_get_string(artist_obj);
            int found = 0;
            for (int j = 0; j < local_artist_count; j++) {
                if (strcmp(local_artists[j].name, artist) == 0) {
                    local_artists[j].count++;
                    found = 1;
                    break;
                }
            }
            if (!found) {
                strncpy(local_artists[local_artist_count].name, artist, 255);
                local_artists[local_artist_count].name[255] = '\0';
                local_artists[local_artist_count].count = 1;
                local_artist_count++;
            }
        }

        // Count Words
        if (json_object_object_get_ex(item, "text", &text_obj)) {
            tokenize_and_count(json_object_get_string(text_obj), &local_words, &local_word_count, &local_word_size, 1);
        }
    }

    int *recv_artist_counts = (rank == 0) ? malloc(sizeof(int) * size) : NULL;
    MPI_Gather(&local_artist_count, 1, MPI_INT, recv_artist_counts, 1, MPI_INT, 0, MPI_COMM_WORLD);

    ItemCount *global_artists = NULL;
    int total_global_artists = 0, *displs_a = NULL;
    if (rank == 0) {
        for (int i = 0; i < size; i++) total_global_artists += recv_artist_counts[i];
        global_artists = malloc(sizeof(ItemCount) * total_global_artists);
        displs_a = malloc(sizeof(int) * size);
        displs_a[0] = 0;
        for (int i = 1; i < size; i++) displs_a[i] = displs_a[i-1] + recv_artist_counts[i-1];
    }
    MPI_Gatherv(local_artists, local_artist_count, MPI_ItemCount,
                global_artists, recv_artist_counts, displs_a, MPI_ItemCount,
                0, MPI_COMM_WORLD);

    int *recv_word_counts = (rank == 0) ? malloc(sizeof(int) * size) : NULL;
    MPI_Gather(&local_word_count, 1, MPI_INT, recv_word_counts, 1, MPI_INT, 0, MPI_COMM_WORLD);

    ItemCount *global_words = NULL;
    int total_global_words = 0, *displs_w = NULL;
    if (rank == 0) {
        for (int i = 0; i < size; i++) total_global_words += recv_word_counts[i];
        global_words = malloc(sizeof(ItemCount) * total_global_words);
        displs_w = malloc(sizeof(int) * size);
        displs_w[0] = 0;
        for (int i = 1; i < size; i++) displs_w[i] = displs_w[i-1] + recv_word_counts[i-1];
    }
    MPI_Gatherv(local_words, local_word_count, MPI_ItemCount,
                global_words, recv_word_counts, displs_w, MPI_ItemCount,
                0, MPI_COMM_WORLD);

    if (rank == 0) {
        // Aggregate Artists
        ItemCount *final_artists = malloc(sizeof(ItemCount) * total_global_artists);
        int final_artist_count = 0;
        for (int i = 0; i < total_global_artists; i++) {
            int found = 0;
            for (int j = 0; j < final_artist_count; j++) {
                if (strcmp(final_artists[j].name, global_artists[i].name) == 0) {
                    final_artists[j].count += global_artists[i].count;
                    found = 1;
                    break;
                }
            }
            if (!found) {
                strncpy(final_artists[final_artist_count].name, global_artists[i].name, 255);
                final_artists[final_artist_count].name[255] = '\0';
                final_artists[final_artist_count].count = global_artists[i].count;
                final_artist_count++;
            }
        }
        qsort(final_artists, final_artist_count, sizeof(ItemCount), compare_counts);

        // Aggregate Words
        ItemCount *final_words = malloc(sizeof(ItemCount) * total_global_words);
        int final_word_count = 0;
        for (int i = 0; i < total_global_words; i++) {
            int found = 0;
            for (int j = 0; j < final_word_count; j++) {
                if (strcmp(final_words[j].name, global_words[i].name) == 0) {
                    final_words[j].count += global_words[i].count;
                    found = 1;
                    break;
                }
            }
            if (!found) {
                strncpy(final_words[final_word_count].name, global_words[i].name, 255);
                final_words[final_word_count].name[255] = '\0';
                final_words[final_word_count].count = global_words[i].count;
                final_word_count++;
            }
        }
        qsort(final_words, final_word_count, sizeof(ItemCount), compare_counts);

        struct json_object *result = json_object_new_object();
        struct json_object *artists_json = json_object_new_array();
        for (int i = 0; i < final_artist_count; i++) {
            struct json_object *entry = json_object_new_object();
            json_object_object_add(entry, "artist", json_object_new_string(final_artists[i].name));
            json_object_object_add(entry, "count", json_object_new_int(final_artists[i].count));
            json_object_array_add(artists_json, entry);
        }
        json_object_object_add(result, "artist_counts", artists_json);

        struct json_object *words_json = json_object_new_array();
        for (int i = 0; i < final_word_count; i++) {
            struct json_object *entry = json_object_new_object();
            json_object_object_add(entry, "word", json_object_new_string(final_words[i].name));
            json_object_object_add(entry, "count", json_object_new_int(final_words[i].count));
            json_object_array_add(words_json, entry);
        }
        json_object_object_add(result, "word_counts", words_json);


        if (json_object_to_file_ext("resultado.json", result, JSON_C_TO_STRING_PRETTY) == 0)
            printf("Resultados salvos em resultado.json\n");
        json_object_put(result);
    }

    free(local_artists);
    free(local_words);
    free(data);
    json_object_put(parsed);
    MPI_Type_free(&MPI_ItemCount);
    MPI_Finalize();
    return 0;
}
