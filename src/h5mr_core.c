//
// Created by ryuzot on 25/05/29.
//
#define _GNU_SOURCE
#include "H5MR/h5mr.h"
#include <liburing.h>
#include <hdf5.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#define QD 32           /* キュー深度(行単位なら十分) */
#define ALIGN 4096
struct h5r {
    int fd;                     /* O_DIRECT で開いた HDF5 ファイル */
    struct io_uring ring;       /* uring インスタンス */
    hid_t file, dset;           /* HDF5 ハンドル */
    hsize_t rows, cols;         /* (74160, 1553332) */
    hsize_t crows, ccols;       /* チャンクサイズ 例: (1, 32768) */
    haddr_t base;               /* 先頭オフセット (contiguousなら) */
};
static void *aligned_alloc4k(size_t n)
{
    void *p; posix_memalign(&p, ALIGN, n); return p;
}
int h5r_open(const char *path, struct h5r **out)
{
    struct h5r *ctx = calloc(1,sizeof(*ctx));
    if (!ctx) return -1;
    
    // O_DIRECTは後で実装。まずは通常のファイルディスクリプタを使用
    ctx->fd = open(path, O_RDONLY);
    if (ctx->fd < 0) {
        free(ctx);
        return -1;
    }
    
    // io_uring初期化も一旦スキップ（O_DIRECTと組み合わせて使用するため）
    // if (io_uring_queue_init(QD, &ctx->ring, IORING_SETUP_SQPOLL) < 0) {
    //     close(ctx->fd);
    //     free(ctx);
    //     return -1;
    // }

    /* HDF5 read-only (SWMRモードは削除または適切に設定) */
    hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
    // H5Pset_fapl_swmr_read(fapl, 1); // この関数は存在しない
    ctx->file = H5Fopen(path, H5F_ACC_RDONLY, fapl); // SWMRフラグも削除
    H5Pclose(fapl);
    
    if (ctx->file < 0) {
        io_uring_queue_exit(&ctx->ring);
        close(ctx->fd);
        free(ctx);
        return -1;
    }

    ctx->dset = H5Dopen2(ctx->file, "population_data", H5P_DEFAULT);
    if (ctx->dset < 0) {
        H5Fclose(ctx->file);
        io_uring_queue_exit(&ctx->ring);
        close(ctx->fd);
        free(ctx);
        return -1;
    }
    hid_t sp = H5Dget_space(ctx->dset);
    hsize_t dims[2];
    H5Sget_simple_extent_dims(sp, dims, NULL);
    ctx->rows = dims[0];
    ctx->cols = dims[1];

    /* チャンクサイズの取得を修正 */
    hid_t dcpl = H5Dget_create_plist(ctx->dset);
    int ndims = H5Pget_chunk(dcpl, 2, dims); // 最大2次元
    if (ndims > 0) {
        ctx->crows = dims[0];
        ctx->ccols = dims[1];
    } else {
        // チャンクされていない場合のデフォルト値
        ctx->crows = 1;
        ctx->ccols = ctx->cols;
    }
    H5Pclose(dcpl);
    H5Sclose(sp);

    ctx->base = H5Dget_offset(ctx->dset);   /* chunked でも先頭は取得可能 */
    *out = ctx;
    return 0;
}

int h5r_read_cell(struct h5r *ctx, uint64_t row, uint64_t col, int32_t *value)
{
    /* 単一セルを読み込み */
    hid_t msp = H5Screate_simple(1,(hsize_t[]){1},NULL);
    hid_t fsp = H5Dget_space(ctx->dset);
    H5Sselect_hyperslab(fsp,H5S_SELECT_SET,(hsize_t[]){row,col},NULL,(hsize_t[]){1,1},NULL);
    int ret = H5Dread(ctx->dset,H5T_STD_I32LE,msp,fsp,H5P_DEFAULT,value);
    H5Sclose(msp); H5Sclose(fsp);
    return ret;
}
int h5r_read_cells(struct h5r *ctx, uint64_t row, uint64_t *cols, size_t ncols, int32_t *values)
{
    /* 複数の非連続セルを読み込み */
    hid_t fsp = H5Dget_space(ctx->dset);
    
    /* 最初のセルを選択 */
    H5Sselect_hyperslab(fsp,H5S_SELECT_SET,(hsize_t[]){row,cols[0]},NULL,(hsize_t[]){1,1},NULL);
    
    /* 残りのセルを追加 */
    for (size_t i = 1; i < ncols; i++) {
        H5Sselect_hyperslab(fsp,H5S_SELECT_OR,(hsize_t[]){row,cols[i]},NULL,(hsize_t[]){1,1},NULL);
    }
    
    /* メモリ空間を作成 */
    hid_t msp = H5Screate_simple(1,(hsize_t[]){ncols},NULL);
    
    /* データを読み込み */
    int ret = H5Dread(ctx->dset,H5T_STD_I32LE,msp,fsp,H5P_DEFAULT,values);
    
    H5Sclose(msp); H5Sclose(fsp);
    return ret;
}

void h5r_close(struct h5r *ctx)
{
    if (!ctx) return;
    if (ctx->dset >= 0) H5Dclose(ctx->dset);
    if (ctx->file >= 0) H5Fclose(ctx->file);
    // io_uring_queue_exit(&ctx->ring);
    if (ctx->fd >= 0) close(ctx->fd);
    free(ctx);
}