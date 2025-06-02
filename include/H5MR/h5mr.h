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

    int  h5r_open(const char *path, struct h5r **out);               /* 初期化 */
    int  h5r_read_cell(struct h5r *ctx, uint64_t row, uint64_t col, int32_t *value); /* 単一セル読み */
    int  h5r_read_cells(struct h5r *ctx, uint64_t row, uint64_t *cols, size_t ncols, int32_t *values); /* 複数セル読み */
    int  h5r_read_column_range(struct h5r *ctx, uint64_t start_row, uint64_t end_row, uint64_t col, int32_t *values); /* 時系列読み */
    void h5r_close(struct h5r *ctx);                                 /* 終了 */

#ifdef __cplusplus
}
#endif

#endif //H5MR_H
