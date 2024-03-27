#include <DataStreams/ParquetBlockInputStream.h>

namespace AH
{

static std::unique_ptr<parquet::arrow::FileReader> makeArrowReader(
    std::shared_ptr<arrow::io::RandomAccessFile> file,
    const parquet::ReaderProperties & parquet_reader_properties,
    const parquet::ArrowReaderProperties & arrow_reader_ptoretries)
{
    if (!file)
        throw std::runtime_error(__FUNCTION__);
    parquet::arrow::FileReaderBuilder reader_builder;
    auto res = reader_builder.Open(file, parquet_reader_properties);
    reader_builder.memory_pool(arrow::default_memory_pool());
    reader_builder.properties(arrow_reader_ptoretries);
    return *reader_builder.Build();
}

ParquetArrowInputStream::ParquetArrowInputStream(
    std::shared_ptr<arrow::io::RandomAccessFile> file_,
    const std::vector<int> & rg_indices,
    const std::vector<int> & column_indices,
    const parquet::ReaderProperties & parquet_reader_properties,
    const parquet::ArrowReaderProperties & arrow_reader_ptoretries)
    : file(file_), arrow_reader(makeArrowReader(file, parquet_reader_properties, arrow_reader_ptoretries))
{
    if (!arrow_reader)
        throw std::runtime_error(__FUNCTION__);
    if (!arrow_reader->GetSchema(&header).ok() || !header)
        throw std::runtime_error(__FUNCTION__);

    if (rg_indices.empty())
        arrow_reader->GetRecordBatchReader(&batch_reader).ok();
    else if (column_indices.empty())
        arrow_reader->GetRecordBatchReader(rg_indices, &batch_reader).ok();
    else
        arrow_reader->GetRecordBatchReader(rg_indices, column_indices, &batch_reader).ok();
}

Block ParquetArrowInputStream::readImpl()
{
    if (!batch_reader)
        return {};

    std::shared_ptr<arrow::RecordBatch> batch;
    if (!batch_reader->ReadNext(&batch).ok())
    {
        batch_reader.reset();
        return {};
    }

    return batch;
}

}
