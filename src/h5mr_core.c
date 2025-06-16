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
#define QD 32           /* Queue depth (sufficient for row-based operations) */
#endif

#define ALIGN 4096

struct h5r {
    int fd;                     /* HDF5 file descriptor */
#ifdef USE_IO_URING
    struct io_uring ring;       /* io_uring instance */
    int io_uring_enabled;       /* Whether io_uring is actually available */
#endif
    hid_t file, dset;
    hsize_t rows, cols;
    hsize_t crows, ccols;
    haddr_t base;
    hid_t dataspace_id;         /* Current dataspace (for writing) */
    hid_t dcpl_id;              /* Dataset creation property list */
    int is_writable;            /* Whether file is open for writing */
    h5r_writer_config_t config; /* Writer configuration */
};



static void *aligned_alloc4k(size_t n)
{
    void *p; posix_memalign(&p, ALIGN, n); return p;
}
#ifdef USE_IO_URING
// Open file with io_uring support
static int h5r_open_io_uring(const char *path, struct h5r *ctx) {
    // Open file with O_DIRECT for bypassing page cache
    ctx->fd = open(path, O_RDONLY | O_DIRECT);
    if (ctx->fd < 0) {
        // Fallback to normal open if O_DIRECT fails
        ctx->fd = open(path, O_RDONLY);
        if (ctx->fd < 0) {
            return -1;
        }
    }
    
    // Initialize io_uring
    ctx->io_uring_enabled = 0;
    if (io_uring_queue_init(QD, &ctx->ring, IORING_SETUP_SQPOLL) == 0) {
        ctx->io_uring_enabled = 1;
    } else if (io_uring_queue_init(QD, &ctx->ring, 0) == 0) {
        // Initialize in normal mode if SQPOLL fails
        ctx->io_uring_enabled = 1;
    }
    // Don't treat io_uring init failure as error, fallback to standard I/O
    
    return 0;
}

// Cleanup io_uring resources
static void h5r_cleanup_io_uring(struct h5r *ctx) {
    if (ctx->io_uring_enabled) {
        io_uring_queue_exit(&ctx->ring);
    }
    if (ctx->fd >= 0) close(ctx->fd);
}
#endif

// Open file with standard I/O
static int h5r_open_standard(const char *path, struct h5r *ctx) {
    // Use standard file descriptor
    ctx->fd = open(path, O_RDONLY);
    if (ctx->fd < 0) {
        return -1;
    }
    return 0;
}

// Cleanup standard I/O resources
static void h5r_cleanup_standard(struct h5r *ctx) {
    if (ctx->fd >= 0) close(ctx->fd);
}

int h5r_open(const char *path, struct h5r **out)
{
    struct h5r *ctx = calloc(1, sizeof(*ctx));
    if (!ctx) return -1;
    
    // Initialize file descriptor and io_uring
#ifdef USE_IO_URING
    if (h5r_open_io_uring(path, ctx) < 0) {
        // Fallback to standard version if io_uring fails
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

    hid_t dapl = H5Pcreate(H5P_DATASET_ACCESS);
    H5Pset_chunk_cache(dapl,
                       /* nslots */ 10007,
                       /* nbytes */ 32 * 1024 * 1024,
                       /* w0     */ 0.75);
    ctx->dset = H5Dopen2(ctx->file, "population_data", dapl);
    H5Pclose(dapl);    if (ctx->dset < 0) {
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

    /* Get chunk dimensions */
    hid_t dcpl = H5Dget_create_plist(ctx->dset);
    int ndims = H5Pget_chunk(dcpl, 2, dims);
    if (ndims > 0) {
        ctx->crows = dims[0];
        ctx->ccols = dims[1];
    } else {
        // Default values for non-chunked datasets
        ctx->crows = 1;
        ctx->ccols = ctx->cols;
    }
    H5Pclose(dcpl);
    H5Sclose(sp);

    ctx->base = H5Dget_offset(ctx->dset);
    ctx->dataspace_id = -1;  // Not used for read-only mode
    ctx->dcpl_id = -1;       // Not used for read-only mode
    ctx->is_writable = 0;    // Read-only
    *out = ctx;
    return 0;
}

int h5r_read_cell(struct h5r *ctx, uint64_t row, uint64_t col, int32_t *value)
{
    /* Read single cell */
    hid_t msp = H5Screate_simple(1,(hsize_t[]){1},NULL);
    hid_t fsp = H5Dget_space(ctx->dset);
    H5Sselect_hyperslab(fsp,H5S_SELECT_SET,(hsize_t[]){row,col},NULL,(hsize_t[]){1,1},NULL);
    int ret = H5Dread(ctx->dset,H5T_NATIVE_INT,msp,fsp,H5P_DEFAULT,value);
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
    /* For contiguous columns, read efficiently with single hyperslab */
    hid_t fsp = H5Dget_space(ctx->dset);
    H5Sselect_hyperslab(fsp, H5S_SELECT_SET, 
                       (hsize_t[]){row, start_col}, NULL, 
                       (hsize_t[]){1, ncols}, NULL);
    
    hid_t msp = H5Screate_simple(1, (hsize_t[]){ncols}, NULL);
    int ret = H5Dread(ctx->dset, H5T_NATIVE_INT, msp, fsp, H5P_DEFAULT, values);
    
    H5Sclose(msp); H5Sclose(fsp);
    return ret;
}

static int read_chunked_cells(struct h5r *ctx, uint64_t row, uint64_t *cols, size_t ncols, int32_t *values) {
    /* Optimized read using H5Sselect_elements() */
    hid_t fsp = H5Dget_space(ctx->dset);
    
    // Create 2D coordinate array (row, col) pairs
    hsize_t *coords = (hsize_t*)malloc(ncols * 2 * sizeof(hsize_t));
    if (!coords) {
        H5Sclose(fsp);
        return -1;
    }
    
    // Set coordinates
    for (size_t i = 0; i < ncols; i++) {
        coords[i * 2] = row;         // row coordinate
        coords[i * 2 + 1] = cols[i]; // column coordinate
    }
    
    // Select all elements at once with H5Sselect_elements()
    herr_t status = H5Sselect_elements(fsp, H5S_SELECT_SET, ncols, coords);
    free(coords);
    
    if (status < 0) {
        H5Sclose(fsp);
        return -1;
    }
    
    // Create memory space
    hid_t msp = H5Screate_simple(1, (hsize_t[]){ncols}, NULL);
    
    // Read data
    int ret = H5Dread(ctx->dset, H5T_NATIVE_INT, msp, fsp, H5P_DEFAULT, values);
    
    H5Sclose(msp); H5Sclose(fsp);
    return ret;
}

int h5r_read_cells(struct h5r *ctx, uint64_t row, uint64_t *cols, size_t ncols, int32_t *values)
{
    if (!ctx || !cols || !values || ncols == 0) return -1;
    
    /* Use dedicated function for single cell */
    if (ncols == 1) {
        return h5r_read_cell(ctx, row, cols[0], values);
    }
    
    /* Use optimized read for contiguous columns */
    if (is_contiguous_columns(cols, ncols)) {
        return read_contiguous_cells(ctx, row, cols[0], ncols, values);
    }
    
    /* Use chunk-optimized read for non-contiguous columns */
    return read_chunked_cells(ctx, row, cols, ncols, values);
}

int h5r_read_column_range(struct h5r *ctx, uint64_t start_row, uint64_t end_row, uint64_t col, int32_t *values)
{
    /* Read time series data: bulk read of same column across contiguous rows */
    if (start_row > end_row) return -1;
    
    uint64_t num_rows = end_row - start_row + 1;
    
    /* Get file space and select contiguous row range */
    hid_t fsp = H5Dget_space(ctx->dset);
    H5Sselect_hyperslab(fsp, H5S_SELECT_SET, 
                       (hsize_t[]){start_row, col}, NULL, 
                       (hsize_t[]){num_rows, 1}, NULL);
    
    /* Create memory space */
    hid_t msp = H5Screate_simple(1, (hsize_t[]){num_rows}, NULL);
    
    /* Read data */
    int ret = H5Dread(ctx->dset, H5T_NATIVE_INT, msp, fsp, H5P_DEFAULT, values);
    
    H5Sclose(msp); H5Sclose(fsp);
    return ret;
}

int h5r_read_columns_range(struct h5r *ctx, uint64_t *rows, size_t nrows, uint64_t *cols, size_t ncols, int32_t *values)
{
    /* Optimized read for multiple meshes x multiple time series
     * rows: time index array (nrows elements)
     * cols: mesh index array (ncols elements) 
     * values: output array (nrows × ncols)
     * Output layout: values[row_idx * ncols + col_idx]
     */
    if (!ctx || !rows || !cols || !values || nrows == 0 || ncols == 0) return -1;
    
    hid_t fsp = H5Dget_space(ctx->dset);
    size_t total_elements = nrows * ncols;
    
    // Create 2D coordinate array
    hsize_t *coords = (hsize_t*)malloc(total_elements * 2 * sizeof(hsize_t));
    if (!coords) {
        H5Sclose(fsp);
        return -1;
    }
    
    // Set coordinates for all (row, col) pairs
    size_t coord_idx = 0;
    for (size_t r = 0; r < nrows; r++) {
        for (size_t c = 0; c < ncols; c++) {
            coords[coord_idx * 2] = rows[r];      // time index
            coords[coord_idx * 2 + 1] = cols[c];  // mesh index
            coord_idx++;
        }
    }
    
    // Select all elements at once with H5Sselect_elements()
    herr_t status = H5Sselect_elements(fsp, H5S_SELECT_SET, total_elements, coords);
    free(coords);
    
    if (status < 0) {
        H5Sclose(fsp);
        return -1;
    }
    
    // Create memory space (as 1D array)
    hid_t msp = H5Screate_simple(1, (hsize_t[]){total_elements}, NULL);
    
    // Read data
    int ret = H5Dread(ctx->dset, H5T_NATIVE_INT, msp, fsp, H5P_DEFAULT, values);
    
    H5Sclose(msp); H5Sclose(fsp);
    return ret;
}


/* Read multiple blocks sharing the same row range (row0 to row0+nrows-1)
 * in a single H5Dread call and store in dst.
 * dst is row-major with row stride = dst_stride.
 * Typically dst_stride = num_meshes.
 *
 * Return: 0 = success, <0 = HDF5 error
 */
int h5r_read_blocks_union(struct h5r           *ctx,
                          uint64_t              row0,
                          uint64_t              nrows,
                          const h5r_block_t    *blocks,
                          size_t                nblk,
                          int32_t              *dst,
                          size_t                dst_stride)
{
    if (!ctx || !blocks || !dst || nblk == 0 || nrows == 0)
        return -1;
    TIC(union_start);
    /* -- Preliminary: Create space objects -- */
    hid_t fsp = H5Dget_space(ctx->dset);

    hsize_t mdims[2] = { nrows, dst_stride };
    hid_t   msp      = H5Screate_simple(2, mdims, NULL);

    for (size_t i = 0; i < nblk; ++i) {
        /* ------------- File space ------------- */
        hsize_t f_start[2] = { row0, blocks[i].dcol0 };
        hsize_t f_count[2] = { nrows, blocks[i].ncols };
        H5S_seloper_t f_op = (i == 0) ? H5S_SELECT_SET : H5S_SELECT_OR;
        if (H5Sselect_hyperslab(fsp, f_op, f_start, NULL, f_count, NULL) < 0) {
            H5Sclose(msp); H5Sclose(fsp);
            return -1;
        }

        /* ------------- Memory space ------------- */
        hsize_t m_start[2] = { 0, blocks[i].mcol0 };
        hsize_t m_count[2] = { nrows, blocks[i].ncols };
        H5S_seloper_t m_op = (i == 0) ? H5S_SELECT_SET : H5S_SELECT_OR;
        if (H5Sselect_hyperslab(msp, m_op, m_start, NULL, m_count, NULL) < 0) {
            H5Sclose(msp); H5Sclose(fsp);
            return -1;
        }
    }
    TIC(UNION_read_start);

    /* -- Read data -- */
    int ret = H5Dread(ctx->dset, H5T_NATIVE_INT, msp, fsp, H5P_DEFAULT, dst);

    H5Sclose(msp);
    H5Sclose(fsp);
    TOC(UNION_read_start);
    TOC(union_start);;
    return ret;   /* ret == 0 on success */
}

void h5r_close(struct h5r *ctx)
{
    if (!ctx) return;
    if (ctx->dset >= 0) H5Dclose(ctx->dset);
    if (ctx->dataspace_id >= 0) H5Sclose(ctx->dataspace_id);
    if (ctx->dcpl_id >= 0) H5Pclose(ctx->dcpl_id);
    if (ctx->file >= 0) H5Fclose(ctx->file);

#ifdef USE_IO_URING
    h5r_cleanup_io_uring(ctx);
#else
    h5r_cleanup_standard(ctx);
#endif

    free(ctx);
}

/* ===============================================
 *  Writing Functions (adapted from h5_writer_ops.c)
 * =============================================== */

int h5r_open_readwrite(const char *path, struct h5r **out) {
    if (!path || !out) return -1;
    
    struct h5r *ctx = calloc(1, sizeof(*ctx));
    if (!ctx) return -1;
    
    ctx->is_writable = 1;
    
    // Open file for read/write
    hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
    ctx->file = H5Fopen(path, H5F_ACC_RDWR, fapl);
    H5Pclose(fapl);
    
    if (ctx->file < 0) {
        free(ctx);
        return -1;
    }
    
    // Open dataset
    ctx->dset = H5Dopen2(ctx->file, "population_data", H5P_DEFAULT);
    if (ctx->dset < 0) {
        H5Fclose(ctx->file);
        free(ctx);
        return -1;
    }
    
    // Get dataspace
    ctx->dataspace_id = H5Dget_space(ctx->dset);
    
    // Get current dimensions
    hsize_t dims[2];
    H5Sget_simple_extent_dims(ctx->dataspace_id, dims, NULL);
    ctx->rows = dims[0];
    ctx->cols = dims[1];
    
    // Get dataset creation property list
    ctx->dcpl_id = H5Dget_create_plist(ctx->dset);
    
    // Initialize other fields
    ctx->fd = -1;
    ctx->base = H5Dget_offset(ctx->dset);
    
    *out = ctx;
    return 0;
}

int h5r_open_readwrite_with_dataset(const char *path, const char *dataset_name, struct h5r **out) {
    if (!path || !dataset_name || !out) return -1;
    
    struct h5r *ctx = calloc(1, sizeof(*ctx));
    if (!ctx) return -1;
    
    ctx->is_writable = 1;
    
    // Open file for read/write
    hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
    ctx->file = H5Fopen(path, H5F_ACC_RDWR, fapl);
    H5Pclose(fapl);
    
    if (ctx->file < 0) {
        free(ctx);
        return -1;
    }
    
    // Open dataset with custom name
    ctx->dset = H5Dopen2(ctx->file, dataset_name, H5P_DEFAULT);
    if (ctx->dset < 0) {
        H5Fclose(ctx->file);
        free(ctx);
        return -1;
    }
    
    // Get dataspace
    ctx->dataspace_id = H5Dget_space(ctx->dset);
    
    // Get current dimensions
    hsize_t dims[2];
    H5Sget_simple_extent_dims(ctx->dataspace_id, dims, NULL);
    ctx->rows = dims[0];
    ctx->cols = dims[1];
    
    // Get dataset creation property list
    ctx->dcpl_id = H5Dget_create_plist(ctx->dset);
    
    // Initialize other fields
    ctx->fd = -1;
    ctx->base = H5Dget_offset(ctx->dset);
    
    *out = ctx;
    return 0;
}

int h5r_extend_time_dimension(struct h5r *ctx, size_t new_time_points) {
    if (!ctx || !ctx->is_writable || new_time_points <= ctx->rows) return -1;
    
    // Extend dataset
    hsize_t new_dims[2] = {new_time_points, ctx->cols};
    herr_t status = H5Dset_extent(ctx->dset, new_dims);
    
    if (status < 0) return -1;
    
    // Update dataspace
    H5Sclose(ctx->dataspace_id);
    ctx->dataspace_id = H5Dget_space(ctx->dset);
    ctx->rows = new_time_points;
    
    return 0;
}

int h5r_write_cell(struct h5r *ctx, uint64_t row, uint64_t col, int32_t value) {
    if (!ctx || !ctx->is_writable || row >= ctx->rows || col >= ctx->cols) {
        return -1;
    }
    
    // Select single element in file
    hsize_t coords[2] = {row, col};
    H5Sselect_elements(ctx->dataspace_id, H5S_SELECT_SET, 1, coords);
    
    // Create memory dataspace for single value
    hid_t memspace = H5Screate_simple(1, (hsize_t[]){1}, NULL);
    
    // Write value
    herr_t status = H5Dwrite(ctx->dset, H5T_NATIVE_INT, memspace, 
                            ctx->dataspace_id, H5P_DEFAULT, &value);
    
    H5Sclose(memspace);
    return (status < 0) ? -1 : 0;
}

int h5r_write_cells(struct h5r *ctx, uint64_t row, const uint64_t *cols, const int32_t *values, size_t ncols) {
    if (!ctx || !ctx->is_writable || !cols || !values || ncols == 0) return -1;
    if (row >= ctx->rows) return -1;
    
    // Validate column indices
    for (size_t i = 0; i < ncols; i++) {
        if (cols[i] >= ctx->cols) return -1;
    }
    
    // Create coordinate array for H5Sselect_elements
    hsize_t* coords = malloc(ncols * 2 * sizeof(hsize_t));
    if (!coords) return -1;
    
    for (size_t i = 0; i < ncols; i++) {
        coords[i * 2] = row;
        coords[i * 2 + 1] = cols[i];
    }
    
    // Select elements in file
    H5Sselect_elements(ctx->dataspace_id, H5S_SELECT_SET, ncols, coords);
    free(coords);
    
    // Create memory dataspace
    hsize_t mem_dims = ncols;
    hid_t memspace = H5Screate_simple(1, &mem_dims, NULL);
    
    // Write values
    herr_t status = H5Dwrite(ctx->dset, H5T_NATIVE_INT, memspace, 
                            ctx->dataspace_id, H5P_DEFAULT, values);
    
    H5Sclose(memspace);
    return (status < 0) ? -1 : 0;
}

int h5r_write_bulk_buffer(struct h5r *ctx, const int32_t *buffer, size_t time_points, size_t mesh_count) {
    if (!ctx || !ctx->is_writable || !buffer) return -1;
    
    // Extend dataset if necessary
    if (time_points > ctx->rows) {
        if (h5r_extend_time_dimension(ctx, time_points) < 0) {
            return -1;
        }
    }
    
    // Create memory dataspace for the entire buffer
    hsize_t mem_dims[2] = {time_points, mesh_count};
    hid_t mem_space = H5Screate_simple(2, mem_dims, NULL);
    if (mem_space < 0) {
        return -1;
    }
    
    // Select hyperslab in file dataspace
    hsize_t start[2] = {0, 0};
    hsize_t count[2] = {time_points, mesh_count};
    
    herr_t status = H5Sselect_hyperslab(ctx->dataspace_id, H5S_SELECT_SET, 
                                       start, NULL, count, NULL);
    if (status < 0) {
        H5Sclose(mem_space);
        return -1;
    }
    
    // Perform bulk write
    status = H5Dwrite(ctx->dset, H5T_NATIVE_INT, mem_space, 
                     ctx->dataspace_id, H5P_DEFAULT, buffer);
    
    H5Sclose(mem_space);
    return (status < 0) ? -1 : 0;
}

int h5r_flush(struct h5r *ctx) {
    if (!ctx || !ctx->is_writable) return -1;
    
    herr_t status = H5Fflush(ctx->file, H5F_SCOPE_LOCAL);
    return (status < 0) ? -1 : 0;
}

int h5r_get_dimensions(struct h5r *ctx, size_t *time_points, size_t *mesh_count) {
    if (!ctx) return -1;
    
    if (time_points) *time_points = ctx->rows;
    if (mesh_count) *mesh_count = ctx->cols;
    
    return 0;
}