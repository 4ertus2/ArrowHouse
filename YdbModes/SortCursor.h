// The code in this file is based on original ClickHouse source code
// which is licensed under Apache license v2.0
// See: https://github.com/ClickHouse/ClickHouse/

#pragma once
#include <algorithm>
#include <arrow/api.h>
#include <DataStreams/FilterColumnsBlockInputStream.h>
#include <YdbModes/CompositeKey.h>
#include <YdbModes/SortDescription.h>
#include "helpers.h"

namespace AHY
{

/// Cursor allows to compare rows in different batches.
/// Cursor moves inside single block. It is used in priority queue.
struct SortCursorImpl
{
    std::shared_ptr<ArrayVec> sort_columns;
    std::shared_ptr<SortDescription> desc;
    uint32_t order = 0; // Number of cursor. It determines an order if comparing columns are equal.
    //
    std::shared_ptr<arrow::RecordBatch> current_batch;
    const ArrayVec * all_columns;
    std::shared_ptr<ArrayVec> replace_columns;

    SortCursorImpl() = default;

    SortCursorImpl(std::shared_ptr<arrow::RecordBatch> batch, std::shared_ptr<SortDescription> desc_, uint32_t order_ = 0)
        : desc(desc_), order(order_)
    {
        Reset(batch);
    }

    bool Empty() const { return Rows() == 0; }
    size_t Rows() const { return (!all_columns || all_columns->empty()) ? 0 : all_columns->front()->length(); }
    size_t LastRow() const { return Rows() - 1; }

    void Reset(std::shared_ptr<arrow::RecordBatch> batch)
    {
        current_batch = batch;
        auto rb_sorting = AH::projection(batch, desc->sorting_key, false);
        sort_columns = std::make_shared<ArrayVec>(rb_sorting->columns());
        all_columns = &batch->columns();
        if (desc->replace_key)
        {
            auto rb_beplace = AH::projection(batch, desc->replace_key, false);
            replace_columns = std::make_shared<ArrayVec>(rb_beplace->columns());
        }
        pos = 0;
    }

    size_t getRow() const { return desc->reverse ? (Rows() - pos - 1) : pos; }

    bool isFirst() const { return pos == 0; }
    bool isLast() const { return pos + 1 >= Rows(); }
    bool isValid() const { return pos < Rows(); }
    void next() { ++pos; }

private:
    size_t pos{0};
};


struct SortCursor
{
    SortCursorImpl * Impl;
    bool not_null;

    SortCursor(SortCursorImpl * impl, bool notNull) : Impl(impl), not_null(notNull) { }

    SortCursorImpl * operator->() { return Impl; }
    const SortCursorImpl * operator->() const { return Impl; }

    bool Greater(const SortCursor & rhs) const { return GreaterAt(rhs, Impl->getRow(), rhs.Impl->getRow()); }

    /// Inverted so that the priority queue elements are removed in ascending order.
    bool operator<(const SortCursor & rhs) const { return Greater(rhs); }

private:
    /// The specified row of this cursor is greater than the specified row of another cursor.
    bool GreaterAt(const SortCursor & rhs, size_t lhs_pos, size_t rhs_pos) const
    {
        RawCompositeKey left(Impl->sort_columns.get(), lhs_pos);
        RawCompositeKey right(rhs.Impl->sort_columns.get(), rhs_pos);

        if (not_null)
        {
            for (size_t i = 0; i < Impl->desc->Size(); ++i)
            {
                auto cmp = left.CompareColumnValueNotNull(i, right, i);
                int res = Impl->desc->Direction(i) * (std::is_eq(cmp) ? 0 : (std::is_lt(cmp) ? -1 : 1));
                if (res > 0)
                    return true;
                if (res < 0)
                    return false;
            }
        }
        else
        {
            for (size_t i = 0; i < Impl->desc->Size(); ++i)
            {
                auto cmp = left.CompareColumnValue(i, right, i);
                int res = Impl->desc->Direction(i) * (std::is_eq(cmp) ? 0 : (std::is_lt(cmp) ? -1 : 1));
                if (res > 0)
                    return true;
                if (res < 0)
                    return false;
            }
        }
        return Impl->order > rhs.Impl->order;
    }
};


/// Allows to fetch data from multiple sort cursors in sorted order (merging sorted data stream).
/// TODO: Replace with "Loser Tree", see https://en.wikipedia.org/wiki/K-way_merge_algorithm
class SortingHeap
{
public:
    SortingHeap() = default;

    template <typename TCursors>
    SortingHeap(TCursors & cursors, bool not_null)
    {
        queue.reserve(cursors.size());
        for (auto & cur : cursors)
            if (!cur.Empty())
                queue.emplace_back(SortCursor(&cur, not_null));
        std::make_heap(queue.begin(), queue.end());
    }

    bool IsValid() const { return !queue.empty(); }
    SortCursor & Current() { return queue.front(); }
    size_t Size() { return queue.size(); }
    SortCursor & NextChild() { return queue[NextChildIndex()]; }

    void Next()
    {
        if (!Current()->isLast())
        {
            Current()->next();
            UpdateTop();
        }
        else
        {
            RemoveTop();
        }
    }

    void ReplaceTop(SortCursor && new_top)
    {
        Current() = new_top;
        UpdateTop();
    }

    void RemoveTop()
    {
        std::pop_heap(queue.begin(), queue.end());
        queue.pop_back();
        next_idx = 0;
    }

    void Push(SortCursor && cursor)
    {
        queue.emplace_back(cursor);
        std::push_heap(queue.begin(), queue.end());
        next_idx = 0;
    }

private:
    std::vector<SortCursor> queue;
    /// Cache comparison between first and second child if the order in queue has not been changed.
    size_t next_idx = 0;

    size_t NextChildIndex()
    {
        if (next_idx == 0)
        {
            next_idx = 1;
            if (queue.size() > 2 && queue[1] < queue[2])
                ++next_idx;
        }

        return next_idx;
    }

    /// This is adapted version of the function __sift_down from libc++.
    /// Why cannot simply use std::priority_queue?
    /// - because it doesn't support updating the top element and requires pop and push instead.
    /// Also look at "Boost.Heap" library.
    void UpdateTop()
    {
        size_t size = queue.size();
        if (size < 2)
            return;

        auto begin = queue.begin();

        size_t child_idx = NextChildIndex();
        auto child_it = begin + child_idx;

        /// Check if we are in order.
        if (*child_it < *begin)
            return;

        next_idx = 0;

        auto curr_it = begin;
        auto top(std::move(*begin));
        do
        {
            /// We are not in heap-order, swap the parent with it's largest child.
            *curr_it = std::move(*child_it);
            curr_it = child_it;

            // recompute the child based off of the updated parent
            child_idx = 2 * child_idx + 1;

            if (child_idx >= size)
                break;

            child_it = begin + child_idx;

            if ((child_idx + 1) < size && *child_it < *(child_it + 1))
            {
                /// Right child exists and is greater than left child.
                ++child_it;
                ++child_idx;
            }

            /// Check if we are in order.
        } while (!(*child_it < top));
        *curr_it = std::move(top);
    }
};

}
