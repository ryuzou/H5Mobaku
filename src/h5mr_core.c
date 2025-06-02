//
// Created by ryuzot on 25/05/29.
//
#define _GNU_SOURCE
#include "H5MR/h5mr.h"
#include <hdf5.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>

// Define to enable io_uring support (comment out to disable)
#define USE_IO_URING

#ifdef USE_IO_URING
#include <liburing.h>
#define QD 32           /* キュー深度(行単位なら十分) */
#endif

#define ALIGN 4096

struct h5r {
    int fd;                     /* HDF5 ファイル */
#ifdef USE_IO_URING
    struct io_uring ring;       /* uring インスタンス */
    int io_uring_enabled;       /* io_uring が実際に使用可能かどうか */
#endif
    hid_t file, dset;
    hsize_t rows, cols;
    hsize_t crows, ccols;
    haddr_t base;
};
static void *aligned_alloc4k(size_t n)
{
    void *p; posix_memalign(&p, ALIGN, n); return p;
}
#ifdef USE_IO_URING
// io_uring版のファイルオープン
static int h5r_open_io_uring(const char *path, struct h5r *ctx) {
    // O_DIRECTでファイルを開く
    ctx->fd = open(path, O_RDONLY | O_DIRECT);
    if (ctx->fd < 0) {
        // O_DIRECTが失敗した場合は通常のオープンを試行
        ctx->fd = open(path, O_RDONLY);
        if (ctx->fd < 0) {
            return -1;
        }
    }
    
    // io_uring初期化
    ctx->io_uring_enabled = 0;
    if (io_uring_queue_init(QD, &ctx->ring, IORING_SETUP_SQPOLL) == 0) {
        ctx->io_uring_enabled = 1;
    } else if (io_uring_queue_init(QD, &ctx->ring, 0) == 0) {
        // SQPOLL失敗時は通常モードで初期化
        ctx->io_uring_enabled = 1;
    }
    // io_uring初期化に失敗してもエラーとせず、標準I/Oを使用
    
    return 0;
}

// io_uring版のクリーンアップ
static void h5r_cleanup_io_uring(struct h5r *ctx) {
    if (ctx->io_uring_enabled) {
        io_uring_queue_exit(&ctx->ring);
    }
    if (ctx->fd >= 0) close(ctx->fd);
}
#endif

// 標準版のファイルオープン
static int h5r_open_standard(const char *path, struct h5r *ctx) {
    // 通常のファイルディスクリプタを使用
    ctx->fd = open(path, O_RDONLY);
    if (ctx->fd < 0) {
        return -1;
    }
    return 0;
}

// 標準版のクリーンアップ
static void h5r_cleanup_standard(struct h5r *ctx) {
    if (ctx->fd >= 0) close(ctx->fd);
}

int h5r_open(const char *path, struct h5r **out)
{
    struct h5r *ctx = calloc(1, sizeof(*ctx));
    if (!ctx) return -1;
    
    // ファイルディスクリプタとio_uringの初期化
#ifdef USE_IO_URING
    if (h5r_open_io_uring(path, ctx) < 0) {
        // io_uring失敗時は標準版にフォールバック
        if (h5r_open_standard(path, ctx) < 0) {
            free(ctx);
            return -1;
        }
    }
#else
    if (h5r_open_standard(path, ctx) < 0) {
        free(ctx);
        return -1;
    }
#endif

    /* HDF5 read-only */
    hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
    ctx->file = H5Fopen(path, H5F_ACC_RDONLY, fapl);
    H5Pclose(fapl);
    
    if (ctx->file < 0) {
#ifdef USE_IO_URING
        h5r_cleanup_io_uring(ctx);
#else
        h5r_cleanup_standard(ctx);
#endif
        free(ctx);
        return -1;
    }

    ctx->dset = H5Dopen2(ctx->file, "population_data", H5P_DEFAULT);
    if (ctx->dset < 0) {
        H5Fclose(ctx->file);
#ifdef USE_IO_URING
        h5r_cleanup_io_uring(ctx);
#else
        h5r_cleanup_standard(ctx);
#endif
        free(ctx);
        return -1;
    }
    
    hid_t sp = H5Dget_space(ctx->dset);
    hsize_t dims[2];
    H5Sget_simple_extent_dims(sp, dims, NULL);
    ctx->rows = dims[0];
    ctx->cols = dims[1];

    /* チャンクサイズの取得 */
    hid_t dcpl = H5Dget_create_plist(ctx->dset);
    int ndims = H5Pget_chunk(dcpl, 2, dims);
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

    ctx->base = H5Dget_offset(ctx->dset);
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
static int is_contiguous_columns(uint64_t *cols, size_t ncols) {
    if (ncols <= 1) return 1;
    for (size_t i = 1; i < ncols; i++) {
        if (cols[i] != cols[i-1] + 1) return 0;
    }
    return 1;
}

static int read_contiguous_cells(struct h5r *ctx, uint64_t row, uint64_t start_col, size_t ncols, int32_t *values) {
    /* 連続する列の場合、単一のhyperslabで効率的に読み込み */
    hid_t fsp = H5Dget_space(ctx->dset);
    H5Sselect_hyperslab(fsp, H5S_SELECT_SET, 
                       (hsize_t[]){row, start_col}, NULL, 
                       (hsize_t[]){1, ncols}, NULL);
    
    hid_t msp = H5Screate_simple(1, (hsize_t[]){ncols}, NULL);
    int ret = H5Dread(ctx->dset, H5T_STD_I32LE, msp, fsp, H5P_DEFAULT, values);
    
    H5Sclose(msp); H5Sclose(fsp);
    return ret;
}

static int read_chunked_cells(struct h5r *ctx, uint64_t row, uint64_t *cols, size_t ncols, int32_t *values) {
    /* チャンク境界を考慮した最適化読み込み */
    hid_t fsp = H5Dget_space(ctx->dset);
    
    // チャンクサイズ(ccols=16)を考慮してグループ化
    size_t chunk_size = ctx->ccols;
    size_t processed = 0;
    
    while (processed < ncols) {
        // 同一チャンク内の列をグループ化
        size_t chunk_start = processed;
        uint64_t current_chunk = cols[processed] / chunk_size;
        
        while (processed < ncols && cols[processed] / chunk_size == current_chunk) {
            processed++;
        }
        
        size_t chunk_count = processed - chunk_start;
        
        // このチャンク内の列を読み込み
        if (chunk_count == 1) {
            // 単一セルの場合
            H5Sselect_hyperslab(fsp, chunk_start == 0 ? H5S_SELECT_SET : H5S_SELECT_OR,
                               (hsize_t[]){row, cols[chunk_start]}, NULL, 
                               (hsize_t[]){1, 1}, NULL);
        } else {
            // 複数セルの場合、個別に追加
            for (size_t i = chunk_start; i < processed; i++) {
                H5Sselect_hyperslab(fsp, (chunk_start == 0 && i == chunk_start) ? H5S_SELECT_SET : H5S_SELECT_OR,
                                   (hsize_t[]){row, cols[i]}, NULL, 
                                   (hsize_t[]){1, 1}, NULL);
            }
        }
    }
    
    hid_t msp = H5Screate_simple(1, (hsize_t[]){ncols}, NULL);
    int ret = H5Dread(ctx->dset, H5T_STD_I32LE, msp, fsp, H5P_DEFAULT, values);
    
    H5Sclose(msp); H5Sclose(fsp);
    return ret;
}

int h5r_read_cells(struct h5r *ctx, uint64_t row, uint64_t *cols, size_t ncols, int32_t *values)
{
    if (!ctx || !cols || !values || ncols == 0) return -1;
    
    /* 単一セルの場合は専用関数を使用 */
    if (ncols == 1) {
        return h5r_read_cell(ctx, row, cols[0], values);
    }
    
    /* 連続する列の場合は最適化された読み込みを使用 */
    if (is_contiguous_columns(cols, ncols)) {
        return read_contiguous_cells(ctx, row, cols[0], ncols, values);
    }
    
    /* 非連続の場合はチャンク最適化読み込みを使用 */
    return read_chunked_cells(ctx, row, cols, ncols, values);
}

int h5r_read_column_range(struct h5r *ctx, uint64_t start_row, uint64_t end_row, uint64_t col, int32_t *values)
{
    /* 時系列データ読み込み: 連続する行の同一列を一括読み込み */
    if (start_row > end_row) return -1;
    
    uint64_t num_rows = end_row - start_row + 1;
    
    /* ファイル空間を取得し、連続する行範囲を選択 */
    hid_t fsp = H5Dget_space(ctx->dset);
    H5Sselect_hyperslab(fsp, H5S_SELECT_SET, 
                       (hsize_t[]){start_row, col}, NULL, 
                       (hsize_t[]){num_rows, 1}, NULL);
    
    /* メモリ空間を作成 */
    hid_t msp = H5Screate_simple(1, (hsize_t[]){num_rows}, NULL);
    
    /* データを読み込み */
    int ret = H5Dread(ctx->dset, H5T_STD_I32LE, msp, fsp, H5P_DEFAULT, values);
    
    H5Sclose(msp); H5Sclose(fsp);
    return ret;
}

void h5r_close(struct h5r *ctx)
{
    if (!ctx) return;
    if (ctx->dset >= 0) H5Dclose(ctx->dset);
    if (ctx->file >= 0) H5Fclose(ctx->file);
    
#ifdef USE_IO_URING
    h5r_cleanup_io_uring(ctx);
#else
    h5r_cleanup_standard(ctx);
#endif
    
    free(ctx);
}