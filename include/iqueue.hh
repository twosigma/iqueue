/*
 *    Copyright 2021 Two Sigma Open Source, LLC
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

#pragma once

#include "stringer.hh"
#include "shash.h"
#include "iqueue.h"
#include <gsl/gsl>
#include <algorithm>
#include <chrono>
#include <string>
#include <optional>

namespace ts { namespace mmia { namespace cpputils {

enum class access_mode { read_only, write };

// Provides an iterator for read-only iqueue iteration.
struct iqueue_const_iterator
{
    using self_type = iqueue_const_iterator;
    using value_type = gsl::span<gsl::byte const>;
    using difference_type = std::int64_t;
    using pointer = value_type const *;
    using reference = value_type const&;
    using iterator_category = std::random_access_iterator_tag;

    iqueue_const_iterator() = default;

    iqueue_const_iterator(self_type const& rhs) = default;

    self_type& operator=(self_type const& rhs) = default;

    friend bool operator == (self_type const& lhs, self_type const& rhs) {
        return lhs._id == rhs._id && lhs._iqueue == rhs._iqueue;
    }
    friend bool operator != (self_type const& lhs, self_type const& rhs) {
        return !(lhs == rhs);
    }

    self_type& operator++() {
        ++_id;
        return *this;
    }
    self_type operator++(int) { return self_type(_iqueue, _id++); }

    friend self_type operator+(self_type const &lhs, difference_type n) {
        return self_type(lhs._iqueue, lhs._id+n);
    }
    friend difference_type operator-(self_type const &lhs, self_type const& rhs) {
        return std::int64_t(lhs._id) - std::int64_t(rhs._id);
    }

    reference operator*() const {
        std::size_t size = 0;
        void const* ptr = iqueue_data(_iqueue, _id, &size);
        _current = {static_cast<gsl::byte const*>(ptr), static_cast<size_t>(size)};
        return _current;
    }

    pointer operator->() const {
        return &(this->operator*());
    }

    iqueue_id_t id() const {return _id;}

private:

    friend struct iqueue;
    iqueue_const_iterator(iqueue_t* iq, iqueue_id_t id)
        : _iqueue(iq)
        , _id(id) {
    }

    iqueue_t* _iqueue = nullptr;
    iqueue_id_t _id = 0;
    mutable value_type _current{};
};

// Provides a type-safe iqueue shash entry with convenient 'update' method.
template<typename ValueType>
struct shash_entry
{
    using self_type = shash_entry<ValueType>;
    using key_type = uint64_t;
    using value_type = ValueType;

    shash_entry(shash_t* table, shash_entry_t* entry) : _table(table), _entry(entry) {}
    shash_entry() = delete;
    shash_entry(self_type const &rhs) = default;
    self_type &operator = (self_type const &rhs) = default;

    void update(value_type value) { iqueue_writer_update(_table, _entry, static_cast<uint64_t>(value)); }
    key_type key() const { return _entry->key; }
    value_type value() const { return value_type{_entry->value}; }
    friend bool operator == (self_type const &lhs, self_type const &rhs) {
        return lhs._table == rhs._table && lhs._entry == rhs._entry;
    }
    friend bool operator != (self_type const &lhs, self_type const &rhs) {
        return lhs._table != rhs._table || lhs._entry != rhs._entry;
    }
    shash_t *table() const {return _table;}
    explicit operator shash_entry_t *() const {return _entry;}
    self_type& operator ++() {++_entry; return *this;}
    self_type operator++(int) {return self_type(_table, _entry++);}

private:
    shash_t* _table = nullptr;
    shash_entry_t* _entry = nullptr;
};

// Provides an iterator over type-safe shash entries in an shash table.
template<typename ValueType>
struct shash_entry_iterator
{
    using self_type = shash_entry_iterator<ValueType>;
    using value_type = std::remove_cv_t<ValueType>;
    using difference_type = std::ptrdiff_t;
    using pointer = ValueType*;
    using reference = ValueType&;
    using iterator_category = std::random_access_iterator_tag;

    shash_entry_iterator() = default;
    shash_entry_iterator(self_type const& rhs) = default;
    self_type& operator=(self_type const& rhs) = default;

    friend bool operator == (shash_entry_iterator<ValueType> const &lhs,
                                    shash_entry_iterator<ValueType> const& rhs) {
        return lhs._current == rhs._current;
    }
    friend bool operator != (shash_entry_iterator<ValueType> const &lhs,
                                    shash_entry_iterator<ValueType> const& rhs) {
        return lhs._current != rhs._current;
    }

    self_type& operator++() { ++_current; return *this; }
    self_type operator++(int) { return self_type(value_type{_current.table(), static_cast<shash_entry_t *>(_current++)}); }

    friend self_type operator+(self_type const &lhs, difference_type n) {
        return self_type(lhs._current.table(), static_cast<shash_entry_t *>(lhs._current)+n);
    }

    friend difference_type operator-(self_type const& lhs, self_type const& rhs) {
        if (lhs._current.table() != rhs._current.table()) {
            throw std::runtime_error("incompatible iterators");
        }
        return static_cast<shash_entry_t *>(lhs._current) - static_cast<shash_entry_t *>(rhs._current);
    }

    reference operator*() const { return _current; }
    pointer operator->() const { return &_current; }

private:
    shash_entry_iterator(shash_t *table, shash_entry_t* entry) : _current{table, entry} {}
    explicit shash_entry_iterator(value_type const &entry) : _current{entry} {}

    mutable value_type _current;
    template <typename EntryValueType>
    friend struct shash_table;
};

// Provide an associative container of type-safe shash_entries.
template <typename EntryValueType>
struct shash_table
{
    using mapped_type = shash_entry<EntryValueType>;
    using const_iterator = shash_entry_iterator<mapped_type const>;
    using iterator = shash_entry_iterator<mapped_type>;
    using mapped_value_type = typename mapped_type::value_type;
    using key_type = typename mapped_type::key_type;
    using value_type = std::pair<key_type const, mapped_type>;
    using size_type = unsigned;

    iterator begin() { return begin_internal<iterator>(); }
    iterator end() { return end_internal<iterator>();}
    const_iterator cbegin() const { return begin_internal<const_iterator>(); }
    const_iterator cend() const { return end_internal<const_iterator>(); }
    const_iterator begin() const { return cbegin(); }
    const_iterator end() const { return cend();}
    size_type size() const {
        return static_cast<size_type>(cend()-cbegin());
    }
    bool empty() const {return size() == 0;}

    // Creates a new heartbeat entry with the specified 'key', using an an initial value of zero.
    mapped_type create(key_type key) {
        return mapped_type{_table, shash_insert(_table, key, 0)};
    }
    // Creates a new heartbeat entry with the specified 'key' and the specified 'value'.
    mapped_type create(key_type key, mapped_value_type value) {
        if (key == 0) {
            throw std::runtime_error("You are not permitted to use key 0! Zero is a special value.");
        }
        return mapped_type{_table, shash_insert(_table, key, static_cast<uint64_t>(value))};
    }
    // Insert the specified 'value', returning a pair, the first element of which is an iterator pointing to the
    // inserted element or the already existing element, the second element of which is a boolean value set to 'true'
    // if the element is newly inserted, or 'false' if the element existed. Note the existing elements remain
    // unchanged by this operation.
    std::pair<iterator, bool> insert(const value_type &value) {
        if (value.first == 0) {
            throw std::runtime_error("You are not permitted to use key 0! Zero is a special value.");
        }
        auto entry = shash_insert(_table, value.first, static_cast<uint64_t>(value.second.value()));
        if (entry) {
            return {iterator{_table, entry}, true};
        }
        entry = shash_get(_table, value.first);
        return {iterator{_table, entry}, false};
    }
    // Insert a range of elements beginning with the specified 'first', and stopping at, though not including, the
    // specified 'last'. Note the the iterators must point to objects conforming to the 'shash_entry'
    // concept, having a the two member functions 'key()' and 'value()' whose signatures match.
    template<typename InputIt>
    void insert(InputIt first, InputIt last) {
        auto&& self = *this;
        std::for_each(first, last, [&self](auto entry){self.create(entry.key(), entry.value());});
    }
    // Return the mapped value for the specified 'key'.
    // Throws: out_of_range if no such key exists in the table.
    //         runtime_error if this table does not exist.
    mapped_type at(key_type key) {
        auto found = shash_get(_table, key);
        if (found == nullptr) {
            throw std::out_of_range(stringer::str("Entry with key ", key,
                                                  " does not exist.",
                                                  " Has the process writing to this iqueue started yet?"));
        }
        return mapped_type{_table, found};
    }
    const_iterator find(key_type key) const { return find_internal<const_iterator>(key); }
    iterator find(key_type key) { return find_internal<iterator>(key); }

    shash_table(shash_table const &other) = default;
    shash_table(shash_table&& other) noexcept = default;
    shash_table() = delete;
    shash_table &operator = (shash_table const &rhs) = default;
    ~shash_table() {_table = nullptr;}
private:
    template<typename IteratorType>
    IteratorType begin_internal() const {
        size_type num_entries;
        return IteratorType{_table, shash_entries(_table, &num_entries)};
    }
    template<typename IteratorType>
    IteratorType end_internal() const {
        size_type num_entries;
        auto end = shash_entries(_table, &num_entries);
        // Heartbeat keys are arranged contiguously. A key of zero signifies an empty entry in the table, and hence
        // the end of the populated area of the table.
        for (size_type i = 0; i < num_entries; ++i) {
            if (end->key == 0) {
                break;
            }
            ++end;
        }
        return IteratorType{_table, end};
    }
    template<typename IteratorType>
    IteratorType find_internal(key_type key) const {
        auto found = shash_get(_table, key);
        if (found == nullptr) {
            return end_internal<IteratorType>();
        }
        return IteratorType{_table, found};
    }
    friend struct iqueue;
    explicit shash_table(shash_t *table) : _table{table} { }

    shash_t* _table;
};

// Provides a wrapper for 'high_resolution_clock::time_point' that adds conversion to/from uint64_t.
struct uint64_convertible_time_point : std::chrono::high_resolution_clock::time_point
{
    template<typename ClockType>
    static uint64_t to_nanos(std::chrono::time_point<ClockType> timestamp) {
        return static_cast<uint64_t>(
            std::chrono::time_point_cast<std::chrono::nanoseconds>(timestamp).time_since_epoch().count());
    }
    using base_type = std::chrono::high_resolution_clock::time_point;
    using base_type::time_point;
    // We want to implicitly convert from the base type.
    uint64_convertible_time_point(base_type t) : base_type(t) {}
    explicit uint64_convertible_time_point(uint64_t value) : base_type(std::chrono::nanoseconds(value)) {}
    explicit operator uint64_t () const { return to_nanos(*this); }
};

using shash_heartbeat_entry = shash_entry<uint64_convertible_time_point>;
using shash_heartbeat_table = shash_table<uint64_convertible_time_point>;

// Provides a container of byte spans wrapper around an iqueue, with additional type safetey.
struct iqueue
{
    using const_iterator = iqueue_const_iterator;
    using value_type = gsl::span<gsl::byte const>;
    using size_type = uint64_t;

    // Open the existing iqueue the specified 'filename', and the specified 'access', optionally checking that the
    // existing header matches the specified 'user_header'.
    iqueue(char const* filename, access_mode access, gsl::span<gsl::byte const> user_header = {})
        : _iqueue(iqueue_open(filename, access == access_mode::write)) {
        using namespace std::string_literals;

        if (_iqueue == nullptr) {
            throw std::runtime_error("failed to open iqueue: \""s + filename + "\"");
        }
        if (!user_header.empty()) {
            if (user_header != header()) {
                throw std::runtime_error("iqueue header doesn't match expectation!");
            }
        }
    }

    // Create a new iqueue or open an existing one with the specified 'filename', using the specified 'creation' time,
    // and the specified 'user_header'.
    template<typename Clock>
    iqueue(char const* filename, std::chrono::time_point<Clock> creation, gsl::span<gsl::byte const> user_header)
        : _iqueue(iqueue_create(filename, uint64_convertible_time_point::to_nanos(creation),
                                user_header.data(), user_header.size())) {
        using namespace std::string_literals;

        if (_iqueue == nullptr) {
            throw std::runtime_error("failed to create iqueue: \""s + filename + "\"");
        }
    }

    // accept already-open C iqueue
    explicit iqueue(iqueue_t* iqueue_)
        : _iqueue(iqueue_)
    {}

    // release the C iqueue inside to the user
    iqueue_t* release() {
        auto ret = _iqueue;
        _iqueue = nullptr;
        return ret;
    }

    iqueue(iqueue&& rhs) noexcept : _iqueue(rhs._iqueue) { rhs._iqueue = nullptr; }

    iqueue& operator=(iqueue&& rhs) noexcept {
        if (_iqueue != nullptr) {
            iqueue_close(_iqueue);
        }
        _iqueue = rhs._iqueue;
        rhs._iqueue = nullptr;
        return *this;
    }

    // non-copyable
    iqueue(iqueue const&) = delete;
    iqueue& operator=(iqueue const& rhs) = delete;

    const_iterator begin() const { return const_iterator(_iqueue, iqueue_begin(_iqueue)); }
    // Returns an iterator pointing to the current end of this iqueue. Note that the end of the iqueue may change
    // without invalidating any outstanding iterators.
    const_iterator end() const { return const_iterator(_iqueue, iqueue_end(_iqueue)); }
    const_iterator cbegin() const { return begin(); }
    const_iterator cend() const { return end(); }

    size_type size() const { return iqueue_entries(_iqueue); }

    ~iqueue() { if (_iqueue) iqueue_close(_iqueue); }

    gsl::span<gsl::byte const> header() const {
        std::size_t size = 0;
        auto data = iqueue_header(_iqueue, &size);
        return {reinterpret_cast<gsl::byte const *>(data), static_cast<size_t>(size)};
    }

    // Returns the file name of the iqueue.
    std::string name() const {
        return iqueue_name(_iqueue);
    }

    void append(value_type msg) {
        int const ret = iqueue_append(_iqueue, msg.data(), msg.size());
        if (ret != 0) {
            throw std::runtime_error(stringer::str(
                    "failed to append message (of ", msg.size(), " bytes) ",
                    "to iqueue \"", iqueue_name(_iqueue), "\":", ret));
        }
    }

    // Check if the specified 'position' is ready to be read.
    bool is_ready(const_iterator position) const {
        auto const status = iqueue_status(_iqueue, position._id);
        if (status != IQUEUE_STATUS_HAS_DATA && status != IQUEUE_STATUS_NO_DATA) {
            throw std::runtime_error(stringer::str(
                "iqueue status for index ", position._id, " was error ", status));
        }
        return status == IQUEUE_STATUS_HAS_DATA;
    }

    // Wait until the specified 'position' in the iqueue is available to be read, or until the specified 'timeout_ns'
    // has elapsed, returning 'true' if data is available to read or 'false' otherwise.
    bool wait(const_iterator position, std::chrono::nanoseconds timeout_ns) const {
        auto const status = iqueue_status_wait(_iqueue, position._id, static_cast<int64_t>(timeout_ns.count()));
        if (status != IQUEUE_STATUS_HAS_DATA && status != IQUEUE_STATUS_NO_DATA) {
            throw std::runtime_error(stringer::str(
                    "posdelta iqueue status for index ", position._id,
                    " was error ", status));
        }
        return status == IQUEUE_STATUS_HAS_DATA;
    }

    // Wait until the specified 'position' in the iqueue is available to be read.
    void wait(iqueue_const_iterator position) {
        int const status = iqueue_status_wait(_iqueue, position._id, -1);
        if (status != IQUEUE_STATUS_HAS_DATA && status != IQUEUE_STATUS_NO_DATA) {
            throw std::runtime_error(stringer::str(
                    "posdelta iqueue status for index ", position._id,
                    " was error ", status));
        }
    }

    // Get the heartbeat table, creating it if the specified 'mode' is 'access_mode::write', or else expect the
    // table to exist and return an 'nullopt' if it does not.
    std::optional<shash_heartbeat_table> heartbeat_table(access_mode mode) {
        auto table = iqueue_writer_table(_iqueue, 0, mode == access_mode::write ? 1 : 0);
        if (table == nullptr) {
            return std::nullopt;
        }
        return std::make_optional(shash_heartbeat_table{table});
    }

    // Create the heartbeat entry in the heartbeat table with the specified
    // 'heartbeat_key', throwing an exception if the key already exists. Creates
    // the heartbeat table if it doesn't already exist.
    shash_heartbeat_entry create_heartbeat_entry(shash_heartbeat_entry::key_type heartbeat_key) {
        shash_t* table = iqueue_writer_table(_iqueue, 0, 1);
        if (!table) {
            throw std::runtime_error("Failed to create heartbeat table");
        }
        return shash_heartbeat_table(table).create(heartbeat_key);
    }

    // Get the heartbeat entry in the heartbeat table with the specified
    // 'heartbeat_key', throwing an exception if the key or table do not exist.
    shash_heartbeat_entry get_heartbeat_entry(shash_heartbeat_entry::key_type heartbeat_key) {
        shash_t* table = iqueue_writer_table(_iqueue, 0, 0);
        if (table) {
            return shash_heartbeat_table(table).at(heartbeat_key);
        } else {
            throw std::runtime_error("Heartbeat table does not exist, has the writing process started?");
        }
    }

    explicit operator iqueue_t*() const { return _iqueue; }

private:
    iqueue_t* _iqueue;
};

}}}

