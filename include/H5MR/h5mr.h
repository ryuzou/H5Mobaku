//
// Created by ryuzot on 25/05/29.
//

#ifndef H5MR_H
#define H5MR_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif
    struct h5r;

    int  h5r_open(const char *path, struct h5r **out);               /* 初期化 */
    int  h5r_read_row(struct h5r *ctx, uint64_t row, int32_t *dst);  /* 行読み */
    void h5r_close(struct h5r *ctx);                                 /* 終了 */

#ifdef __cplusplus
}
#endif

#endif //H5MR_H
