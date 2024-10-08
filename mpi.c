#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <json-c/json.h>
#include <stddef.h>
#include <ctype.h>

typedef struct {
    char artist[256];
    int count;
} ArtistCount;

typedef struct {
    char word[256];
    int count;
} WordCount;

int compare_artist(const void *a, const void *b) {
    ArtistCount *ac1 = (ArtistCount *)a;
    ArtistCount *ac2 = (ArtistCount *)b;
    return ac2->count - ac1->count;
}

int compare_word(const void *a, const void *b) {
    WordCount *wc1 = (WordCount *)a;
    WordCount *wc2 = (WordCount *)b;
    return wc2->count - wc1->count;
}

void tokenize_and_count(const char *text, WordCount **words, int *word_count, int *word_size) {
    char word[256];
    int index = 0;
    for (int i = 0; text[i] != '\0'; i++) {
        if (isalnum(text[i])) {
            if (index < 255) {
                word[index++] = tolower(text[i]);
            }
        } else {
            if (index > 0) {
                word[index] = '\0';
                int found = 0;
                for (int j = 0; j < *word_count; j++) {
                    if (strcmp((*words)[j].word, word) == 0) {
                        (*words)[j].count++;
                        found = 1;
                        break;
                    }
                }
                if (!found) {
                    if (*word_count >= *word_size) {
                        *word_size *= 2;
                        *words = realloc(*words, (*word_size) * sizeof(WordCount));
                        if (!(*words)) {
                            fprintf(stderr, "Erro ao realocar memória para palavras.\n");
                            MPI_Abort(MPI_COMM_WORLD, 1);
                        }
                    }
                    strncpy((*words)[*word_count].word, word, 255);
                    (*words)[*word_count].word[255] = '\0';
                    (*words)[*word_count].count = 1;
                    (*word_count)++;
                }
                index = 0;
            }
        }
    }
    if (index > 0) {
        word[index] = '\0';
        int found = 0;
        for (int j = 0; j < *word_count; j++) {
            if (strcmp((*words)[j].word, word) == 0) {
                (*words)[j].count++;
                found = 1;
                break;
            }
        }
        if (!found) {
            if (*word_count >= *word_size) {
                *word_size *= 2;
                *words = realloc(*words, (*word_size) * sizeof(WordCount));
                if (!(*words)) {
                    fprintf(stderr, "Erro ao realocar memória para palavras.\n");
                    MPI_Abort(MPI_COMM_WORLD, 1);
                }
            }
            strncpy((*words)[*word_count].word, word, 255);
            (*words)[*word_count].word[255] = '\0';
            (*words)[*word_count].count = 1;
            (*word_count)++;
        }
    }
}

int main(int argc, char *argv[]) {
    int rank, size;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    // Definição do tipo MPI personalizado para ArtistCount
    MPI_Datatype MPI_ArtistCount;
    int blocklengths_a[2] = {256, 1};
    MPI_Aint displacements_a[2];
    displacements_a[0] = offsetof(ArtistCount, artist);
    displacements_a[1] = offsetof(ArtistCount, count);
    MPI_Datatype types_a[2] = {MPI_CHAR, MPI_INT};
    MPI_Type_create_struct(2, blocklengths_a, displacements_a, types_a, &MPI_ArtistCount);
    MPI_Type_commit(&MPI_ArtistCount);

    // Definição do tipo MPI personalizado para WordCount
    MPI_Datatype MPI_WordCount;
    int blocklengths_w[2] = {256, 1};
    MPI_Aint displacements_w[2];
    displacements_w[0] = offsetof(WordCount, word);
    displacements_w[1] = offsetof(WordCount, count);
    MPI_Datatype types_w[2] = {MPI_CHAR, MPI_INT};
    MPI_Type_create_struct(2, blocklengths_w, displacements_w, types_w, &MPI_WordCount);
    MPI_Type_commit(&MPI_WordCount);

    char *data = NULL;
    long length;
    if (rank == 0) {
        FILE *f = fopen("parte-3/dados_com_etl.json", "rb");
        if (!f) {
            fprintf(stderr, "Erro ao abrir o arquivo amostra.json.\n");
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
        fseek(f, 0, SEEK_END);
        length = ftell(f);
        fseek(f, 0, SEEK_SET);
        data = malloc(length + 1);
        if (!data) {
            fprintf(stderr, "Erro de alocação de memória para dados.\n");
            fclose(f);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
        fread(data, 1, length, f);
        data[length] = '\0';
        fclose(f);
    }

    MPI_Bcast(&length, 1, MPI_LONG, 0, MPI_COMM_WORLD);

    if (rank != 0) {
        data = malloc(length + 1);
        if (!data) {
            fprintf(stderr, "Erro de alocação de memória para dados.\n");
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
    }

    MPI_Bcast(data, length + 1, MPI_CHAR, 0, MPI_COMM_WORLD);

    struct json_object *parsed_json, *artist_obj, *text_obj;
    parsed_json = json_tokener_parse(data);
    if (parsed_json == NULL) {
        if (rank == 0) fprintf(stderr, "Erro ao parsear JSON.\n");
        free(data);
        MPI_Finalize();
        return 1;
    }

    int total = json_object_array_length(parsed_json);
    int chunk = total / size;
    int start = rank * chunk;
    int end = (rank == size - 1) ? total : start + chunk;

    // Contagem de Músicas por Artista
    ArtistCount *local_artists = malloc(sizeof(ArtistCount) * total);
    if (!local_artists) {
        fprintf(stderr, "Erro de alocação de memória local para artistas.\n");
        json_object_put(parsed_json);
        free(data);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    int local_artist_count = 0;

    // Contagem de Palavras
    WordCount *local_words = malloc(sizeof(WordCount) * 1024);
    if (!local_words) {
        fprintf(stderr, "Erro de alocação de memória local para palavras.\n");
        free(local_artists);
        json_object_put(parsed_json);
        free(data);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    int local_word_size = 1024;
    int local_word_count = 0;

    for (int i = start; i < end; i++) {
        struct json_object *item = json_object_array_get_idx(parsed_json, i);
        
        // Contagem de Artistas
        if (json_object_object_get_ex(item, "artist", &artist_obj)) {
            const char *artist = json_object_get_string(artist_obj);
            int found = 0;
            for (int j = 0; j < local_artist_count; j++) {
                if (strcmp(local_artists[j].artist, artist) == 0) {
                    local_artists[j].count++;
                    found = 1;
                    break;
                }
            }
            if (!found) {
                strncpy(local_artists[local_artist_count].artist, artist, 255);
                local_artists[local_artist_count].artist[255] = '\0';
                local_artists[local_artist_count].count = 1;
                local_artist_count++;
            }
        }

        // Contagem de Palavras
        if (json_object_object_get_ex(item, "text", &text_obj)) {
            const char *text = json_object_get_string(text_obj);
            tokenize_and_count(text, &local_words, &local_word_count, &local_word_size);
        }
    }

    // Coleta das Contagens de Artistas
    int *recv_artist_counts = NULL;
    int *displs_a = NULL;
    if (rank == 0) {
        recv_artist_counts = malloc(sizeof(int) * size);
        if (!recv_artist_counts) {
            fprintf(stderr, "Erro de alocação de recv_artist_counts.\n");
            free(local_artists);
            free(local_words);
            json_object_put(parsed_json);
            free(data);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
    }

    MPI_Gather(&local_artist_count, 1, MPI_INT, recv_artist_counts, 1, MPI_INT, 0, MPI_COMM_WORLD);

    ArtistCount *global_artists = NULL;
    int total_global_artists = 0;
    if (rank == 0) {
        for (int i = 0; i < size; i++) {
            total_global_artists += recv_artist_counts[i];
        }
        global_artists = malloc(sizeof(ArtistCount) * total_global_artists);
        if (!global_artists) {
            fprintf(stderr, "Erro de alocação de global_artists.\n");
            free(recv_artist_counts);
            free(local_artists);
            free(local_words);
            json_object_put(parsed_json);
            free(data);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
        displs_a = malloc(sizeof(int) * size);
        if (!displs_a) {
            fprintf(stderr, "Erro de alocação de displs_a.\n");
            free(global_artists);
            free(recv_artist_counts);
            free(local_artists);
            free(local_words);
            json_object_put(parsed_json);
            free(data);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
        displs_a[0] = 0;
        for (int i = 1; i < size; i++) {
            displs_a[i] = displs_a[i-1] + recv_artist_counts[i-1];
        }
    }

    MPI_Gatherv(local_artists, local_artist_count, MPI_ArtistCount,
                global_artists, recv_artist_counts, displs_a, MPI_ArtistCount,
                0, MPI_COMM_WORLD);

    // Coleta das Contagens de Palavras
    int *recv_word_counts = NULL;
    int *displs_w = NULL;
    if (rank == 0) {
        recv_word_counts = malloc(sizeof(int) * size);
        if (!recv_word_counts) {
            fprintf(stderr, "Erro de alocação de recv_word_counts.\n");
            free(global_artists);
            free(displs_a);
            free(recv_artist_counts);
            free(local_artists);
            free(local_words);
            json_object_put(parsed_json);
            free(data);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
    }

    MPI_Gather(&local_word_count, 1, MPI_INT, recv_word_counts, 1, MPI_INT, 0, MPI_COMM_WORLD);

    WordCount *global_words = NULL;
    int total_global_words = 0;
    if (rank == 0) {
        for (int i = 0; i < size; i++) {
            total_global_words += recv_word_counts[i];
        }
        global_words = malloc(sizeof(WordCount) * total_global_words);
        if (!global_words) {
            fprintf(stderr, "Erro de alocação de global_words.\n");
            free(recv_word_counts);
            free(global_artists);
            free(displs_a);
            free(recv_artist_counts);
            free(local_artists);
            free(local_words);
            json_object_put(parsed_json);
            free(data);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
        displs_w = malloc(sizeof(int) * size);
        if (!displs_w) {
            fprintf(stderr, "Erro de alocação de displs_w.\n");
            free(global_words);
            free(recv_word_counts);
            free(global_artists);
            free(displs_a);
            free(recv_artist_counts);
            free(local_artists);
            free(local_words);
            json_object_put(parsed_json);
            free(data);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
        displs_w[0] = 0;
        for (int i = 1; i < size; i++) {
            displs_w[i] = displs_w[i-1] + recv_word_counts[i-1];
        }
    }

    MPI_Gatherv(local_words, local_word_count, MPI_WordCount,
                global_words, recv_word_counts, displs_w, MPI_WordCount,
                0, MPI_COMM_WORLD);

    if (rank == 0) {
        // Agregação das Contagens de Artistas
        ArtistCount *final_artists = malloc(sizeof(ArtistCount) * total_global_artists);
        if (!final_artists) {
            fprintf(stderr, "Erro de alocação de final_artists.\n");
            free(global_words);
            free(displs_w);
            free(recv_word_counts);
            free(global_artists);
            free(displs_a);
            free(recv_artist_counts);
            free(local_artists);
            free(local_words);
            json_object_put(parsed_json);
            free(data);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
        int final_artist_count = 0;
        for (int i = 0; i < total_global_artists; i++) {
            int found = 0;
            for (int j = 0; j < final_artist_count; j++) {
                if (strcmp(final_artists[j].artist, global_artists[i].artist) == 0) {
                    final_artists[j].count += global_artists[i].count;
                    found = 1;
                    break;
                }
            }
            if (!found) {
                strncpy(final_artists[final_artist_count].artist, global_artists[i].artist, 255);
                final_artists[final_artist_count].artist[255] = '\0';
                final_artists[final_artist_count].count = global_artists[i].count;
                final_artist_count++;
            }
        }
        qsort(final_artists, final_artist_count, sizeof(ArtistCount), compare_artist);

        // Agregação das Contagens de Palavras
        WordCount *final_words = malloc(sizeof(WordCount) * total_global_words);
        if (!final_words) {
            fprintf(stderr, "Erro de alocação de final_words.\n");
            free(final_artists);
            free(global_words);
            free(displs_w);
            free(recv_word_counts);
            free(global_artists);
            free(displs_a);
            free(recv_artist_counts);
            free(local_artists);
            free(local_words);
            json_object_put(parsed_json);
            free(data);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
        int final_word_count = 0;
        for (int i = 0; i < total_global_words; i++) {
            int found = 0;
            for (int j = 0; j < final_word_count; j++) {
                if (strcmp(final_words[j].word, global_words[i].word) == 0) {
                    final_words[j].count += global_words[i].count;
                    found = 1;
                    break;
                }
            }
            if (!found) {
                strncpy(final_words[final_word_count].word, global_words[i].word, 255);
                final_words[final_word_count].word[255] = '\0';
                final_words[final_word_count].count = global_words[i].count;
                final_word_count++;
            }
        }
        qsort(final_words, final_word_count, sizeof(WordCount), compare_word);

        // Criação do Objeto JSON para Resultado
        struct json_object *result_json = json_object_new_object();
        
        // Adiciona Contagem de Artistas
        struct json_object *artists_json = json_object_new_array();
        for (int i = 0; i < final_artist_count; i++) {
            struct json_object *artist_entry = json_object_new_object();
            json_object_object_add(artist_entry, "artist", json_object_new_string(final_artists[i].artist));
            json_object_object_add(artist_entry, "count", json_object_new_int(final_artists[i].count));
            json_object_array_add(artists_json, artist_entry);
        }
        json_object_object_add(result_json, "artist_counts", artists_json);

        // Adiciona Contagem de Palavras
        struct json_object *words_json = json_object_new_array();
        for (int i = 0; i < final_word_count; i++) {
            struct json_object *word_entry = json_object_new_object();
            json_object_object_add(word_entry, "word", json_object_new_string(final_words[i].word));
            json_object_object_add(word_entry, "count", json_object_new_int(final_words[i].count));
            json_object_array_add(words_json, word_entry);
        }
        json_object_object_add(result_json, "word_counts", words_json);

        // Escreve o Resultado no Arquivo resultado.json com Indentação
        if (json_object_to_file_ext("resultado.json", result_json, JSON_C_TO_STRING_PRETTY) != 0) {
            fprintf(stderr, "Erro ao escrever resultado.json.\n");
        } else {
            printf("Resultados salvos em resultado.json\n");
        }

        // Liberação de Memória do Objeto JSON
        json_object_put(result_json);

        // Liberação de Memória
        free(final_artists);
        free(final_words);
        free(global_artists);
        free(global_words);
        free(recv_artist_counts);
        free(recv_word_counts);
        free(displs_a);
        free(displs_w);
    }

    // Liberação de Memória e Finalização
    free(local_artists);
    free(local_words);
    free(data);
    json_object_put(parsed_json);
    MPI_Type_free(&MPI_ArtistCount);
    MPI_Type_free(&MPI_WordCount);
    MPI_Finalize();
    return 0;
}
