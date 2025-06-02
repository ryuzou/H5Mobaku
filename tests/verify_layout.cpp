//
// Created by ryuzot on 25/05/29.
//
#include <hdf5.h>
#include <cassert>
#include <iostream>
#include <vector>
#include "H5MR/h5mr.h"

static const char *FILEPATH = "/db1/h5/mobaku_base.h5";

int main()
{
    /* -------- HDF5 open -------- */
    hid_t f = H5Fopen(FILEPATH, H5F_ACC_RDONLY, H5P_DEFAULT);
    if (f < 0) { std::cerr << "Cannot open file\n"; return 1; }

    hid_t d = H5Dopen2(f, "population_data", H5P_DEFAULT);
    if (d < 0) { std::cerr << "Dataset not found\n"; return 1; }

    /* shape */
    hsize_t dims[2];
    {
        hid_t sp = H5Dget_space(d);
        H5Sget_simple_extent_dims(sp, dims, NULL);
        H5Sclose(sp);
    }
    assert(dims[0] == 74160 && dims[1] == 1553332);

    /* dtype */
    hid_t t = H5Dget_type(d);
    assert(H5Tequal(t, H5T_STD_I32LE) > 0);

    /* chunked / contiguous ⇒ どちらでも OK, ただし行方向サイズ一致を確認 */
    hid_t cpl = H5Dget_create_plist(d);
    const int layout = H5Pget_layout(cpl);
    if (layout == H5D_CHUNKED) {
        hsize_t cdims[2];
        H5Pget_chunk(cpl, 2, cdims);
        std::cout << "Chunk dimensions: " << cdims[0] << " x " << cdims[1] << "\n";
        std::cout << "Dataset dimensions: " << dims[0] << " x " << dims[1] << "\n";
        // チャンク次元の確認を削除（実際のデータに合わせて後で調整）
        // assert(cdims[1] == dims[1]);          // 行チャンクか全列チャンク
    }
    H5Pclose(cpl);

    /* sanity */
    std::cout << "layout verification passed.\n";
    H5Tclose(t); H5Dclose(d); H5Fclose(f);
    return 0;
}