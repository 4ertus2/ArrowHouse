#pragma once
#include <DataStreams/IBlockInputStream.h>
#include "arrow_clickhouse_types.h"
#include "parquet/arrow/reader.h"

namespace AH
{

class ParquetBlockInputStream : public IBlockInputStream
{
public:
    ParquetBlockInputStream(
        std::shared_ptr<arrow::io::RandomAccessFile> file_,
        const std::vector<int> & rg_indices = {},
        const std::vector<int> & column_indices = {},
        const parquet::ReaderProperties & parquet_reader_properties = parquet::default_reader_properties(),
        const parquet::ArrowReaderProperties & arrow_reader_ptoretries = parquet::default_arrow_reader_properties());

    Header getHeader() const override { return header; }

protected:
    Block readImpl() override;

private:
    std::shared_ptr<arrow::io::RandomAccessFile> file;
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    std::shared_ptr<arrow::Schema> header;
    std::unique_ptr<arrow::RecordBatchReader> batch_reader;
};

}
