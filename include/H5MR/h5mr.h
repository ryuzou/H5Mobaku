//
// Created by ryuzot on 25/05/29.
//

#ifndef H5MR_H
#define H5MR_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif
struct h5r;

typedef struct {
    uint64_t dcol0; /* dataset 側の開始列   */
    uint64_t mcol0; /* memory  側の開始列   */
    uint64_t ncols; /* 列数（連続）         */
} h5r_block_t;

int h5r_open(const char *path, struct h5r **out); /* 初期化 */
int h5r_read_cell(struct h5r *ctx, uint64_t row, uint64_t col, int32_t *value); /* 単一セル読み */
int h5r_read_cells(struct h5r *ctx, uint64_t row, uint64_t *cols, size_t ncols, int32_t *values); /* 複数セル読み */
int h5r_read_column_range(struct h5r *ctx, uint64_t start_row, uint64_t end_row, uint64_t col, int32_t *values);

/* 時系列読み */
int h5r_read_columns_range(struct h5r *ctx, uint64_t *rows, size_t nrows, uint64_t *cols, size_t ncols,
                           int32_t *values); /* 複数メッシュ×複数時系列 */
int h5r_read_blocks_union(struct h5r *ctx, uint64_t row0, uint64_t nrows, const h5r_block_t *blocks, size_t nblk,
                          int32_t *dst, size_t dst_stride);

void h5r_close(struct h5r *ctx); /* 終了 */


// #define ENABLE_H5MOBaku_PROFILE
    /* --- Profiling utilities --- */
#ifdef ENABLE_H5MOBaku_PROFILE
#  include <time.h>
    static inline double now_sec(void)
    {
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        return ts.tv_sec + ts.tv_nsec * 1e-9;
    }
#  define TIC(tag)  double tic_##tag = now_sec()
#  define TOC(tag)  fprintf(stderr, "[PROFILE] %-18s : %.6f s\n", #tag, now_sec() - tic_##tag)
#else
#  define TIC(tag)
#  define TOC(tag)
#endif



#ifdef __cplusplus
}
#endif

#endif //H5MR_H
