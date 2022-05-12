/**
 * @file heap_storage.cpp - Implementation of storage_engine.
 * SlottedPage: DbBlock
 * HeapFile: DbFile
 * HeapTable: DbRelation
 *
 * @author  Kevin Lundeen,
            Zhicong Zeng,
            Lolakumari Jayachandran, Vindhya Nair

 * @see "Seattle University, CPSC5300, Spring 2022"
**/
#include "heap_storage.h"
#include <cstring>
#include <iostream>

using namespace std;

typedef u_int16_t u16;

/**
 * @class SlottedPage - heap file implementation of DbBlock.
 *
 *      Manage a database block that contains several records.
        Modeled after slotted-page from Database Systems Concepts, 6ed, Figure 10-9.

        Record id are handed out sequentially starting with 1 as records are added with add().
        Each record has a header which is a fixed offset from the beginning of the block:
            Bytes 0x00 - Ox01: number of records
            Bytes 0x02 - 0x03: offset to end of free space
            Bytes 0x04 - 0x05: size of record 1
            Bytes 0x06 - 0x07: offset to record 1
            etc.
**/

// SlottedPage constructor:
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id)
{
    if (is_new)
    {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    }
    else
    {
        get_header(this->num_records, this->end_free);
    }
}

// Add a new record to the block. Return its id..
RecordID SlottedPage::add(const Dbt *data)
{
    if (!has_room(data->get_size() + 4))
        throw DbBlockNoRoomError("not enough room for new record");
    u16 id = ++this->num_records;
    u16 size = (u16)data->get_size();
    this->end_free -= size;
    u16 loc = this->end_free + 1U;
    put_header();
    put_header(id, size, loc);
    memcpy(this->address(loc), data->get_data(), size);
    return id;
}

// Get a record from the block. Return None if it has been deleted.
Dbt *SlottedPage::get(RecordID record_id)
{
    u16 size, loc;
    get_header(size, loc, record_id);
    if (loc == 0)
    {
        return nullptr; // this is just a tombstone, record has been deleted
    }
    return new Dbt(this->address(loc), size);
}

// Replace the record with the given data. Raises ValueError if it won't fit.
void SlottedPage::put(RecordID record_id, const Dbt &data)
{
    u16 header_size, loc;
    get_header(header_size, loc, record_id);
    u16 data_size = (u16)data.get_size();

    // That means this record is too large
    if (data_size > header_size)
    {
        u16 extra = data_size - header_size;
        if (!this->has_room(extra))
        {
            throw DbBlockNoRoomError("Not enough room in block");
        }
        // Put this record on index loc - extra and the header will be fulled.
        slide(loc, loc - extra);
        memcpy(this->address(loc - extra), data.get_data(), data_size);
    }
    else
    {
        // Put this record on index loc. Normally one.
        memcpy(this->address(loc), data.get_data(), data_size);
        slide(loc + data_size, loc + header_size);
    }
    get_header(header_size, loc, record_id);
    put_header(record_id, data_size, loc);
}

// Mark the given id as deleted by changing its size to zero and its location to 0.
// Compact the rest of the data in the block. But keep the record ids the same for everyone.
void SlottedPage::del(RecordID record_id)
{
    u16 size, loc;
    get_header(size, loc, record_id);
    put_header(record_id, 0, 0);
    slide(loc, loc + size);
}

// Sequence of all non-deleted record ids.
RecordIDs *SlottedPage::ids(void)
{
    RecordIDs *record_ids = new RecordIDs();
    u16 size, loc;
    for (RecordID record_id = 1; record_id <= this->num_records; record_id++)
    {
        get_header(size, loc, record_id);
        if (loc != 0)
        {
            record_ids->push_back(record_id);
        }
    }
    return record_ids;
}

// Get the size and offset for given id. For id of zero, it is the block header.
void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id)
{
    size = get_n(4 * id);
    loc = get_n(4 * id + 2);
}

// Store the size and offset for given id. For id of zero, store the block header.
void SlottedPage::put_header(RecordID id, u16 size, u16 loc)
{
    if (id == 0)
    { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n((u16)4 * id, size);
    put_n((u16)(4 * id + 2), loc);
}

// Calculate if we have room to store a record with given size.
// The size should include the 4 bytes
// for the header, too, if this is an add.
bool SlottedPage::has_room(u_int16_t size)
{
    u16 available = this->end_free - (this->num_records + 1) * 4;
    return (size <= available);
}

/**If start < end, then remove data from offset start up to but not including offset end by sliding data
    that is to the left of start to the right.
    If start > end, then make room for extra data from end to start
    by sliding data that is to the left of start to the left.
    Also fix up any record headers whose data has slid. Assumes there is enough room if it is a left
    shift (end < start).**/
void SlottedPage::slide(u_int16_t start, u_int16_t end)
{
    int shift = end - start;
    // u16 shift = end - start; We need int instead of u16
    if (shift == 0)
    {
        return;
    }

    // slide data
    // Copy data from [end_free + 1] to [end_free + 1 + shift]
    // Copy length is (end - start)
    int bytes = start - (this->end_free + 1U);
    memcpy((this->address((u16)(this->end_free + 1 + shift))), (this->address((u16)this->end_free + 1)), bytes);

    // fix up headers
    u16 size, loc;
    RecordIDs *record_ids = ids();
    for (auto const &record_id : *record_ids)
    {
        get_header(size, loc, record_id);
        if (loc <= start)
        {
            loc += shift;
            put_header(record_id, size, loc);
        }
    }
    delete record_ids;
    this->end_free += shift;
    put_header();
}

// Get 2-byte integer at given offset in block.
u16 SlottedPage::get_n(u16 offset)
{
    return *(u16 *)this->address(offset);
}

// Put a 2-byte integer at given offset in block.
void SlottedPage::put_n(u16 offset, u16 n)
{
    *(u16 *)this->address(offset) = n;
}

// Make a void* pointer for a given offset into the data block.
void *SlottedPage::address(u16 offset)
{
    return (void *)((char *)this->block.get_data() + offset);
}

// Already constructed
//  u16 SlottedPage::size() {
//  }

/**
 * @class HeapFile - heap file implementation of DbFile
 *
 * Heap file organization. Built on top of Berkeley DB RecNo file. There is one of our
        database blocks for each Berkeley DB record in the RecNo file.
        In this way we are using Berkeley DB
        for buffer management and file management.
        Uses SlottedPage for storing records within blocks.
**/

// HeapFile constructor has been declared in heap_storage.h

// Create physical file.
void HeapFile::create(void)
{
    db_open(DB_CREATE | DB_EXCL);
    SlottedPage *block = get_new(); // first block of the file
    delete block;
}

// Delete the physical file
void HeapFile::drop(void)
{
    close();
    Db db(_DB_ENV, 0);
    db.remove(this->dbfilename.c_str(), nullptr, 0);
}

// Open physical file.
void HeapFile::open(void)
{
    db_open();
}

// Close the physical file.
void HeapFile::close(void)
{
    db.close(0);
    closed = true;
}

// Allocate a new block for the database file.
// Returns the new empty DbBlock that is managing the records in this block and its block id.
SlottedPage *HeapFile::get_new(void)
{
    char block[DbBlock::BLOCK_SZ];
    memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));

    int block_id = ++this->last;
    Dbt key(&block_id, sizeof(block_id));

    // write out an empty block and read it back in so Berkeley DB is managing the memory
    SlottedPage *page = new SlottedPage(data, this->last, true);
    this->db.put(nullptr, &key, &data, 0); // write it out with initialization applied
    this->db.get(nullptr, &key, &data, 0);
    delete page;
    return new SlottedPage(data, this->last); // Return a new SlottedPage
}

// Get a block from the database file.
SlottedPage *HeapFile::get(BlockID block_id)
{
    Dbt key(&block_id, sizeof(block_id));
    Dbt data;
    this->db.get(nullptr, &key, &data, 0);
    return new SlottedPage(data, block_id, false); // Not a new one;
}

// Write a block back to the database file.
// put blockid into block.block...
void HeapFile::put(DbBlock *block)
{
    BlockID block_id(block->get_block_id());
    Dbt blockid(&block_id, sizeof(block_id));
    this->db.put(nullptr, &blockid, block->get_block(), 0);
}

// Sequence of all block ids
BlockIDs *HeapFile::block_ids()
{
    BlockIDs *id = new BlockIDs();
    for (BlockID i = 1; i <= this->last; i++)
    {
        id->push_back(i);
    }
    return id;
}

// Return the last block id
// Already constructed in heap_storage.h
// u_int32_t *HeapFile::get_last_block_id()

// Wrapper for Berkeley DB open, which does both open and creation.
void HeapFile::db_open(uint flags)
{
    if (!this->closed)
    {
        return;
    }
    this->db.set_re_len(DbBlock::BLOCK_SZ);
    const char *path = nullptr;
    _DB_ENV->get_home(&path);
    this->dbfilename = "./" + this->name + ".db"; // Get a db::open Is a directory otherwise
    this->db.open(nullptr, (this->dbfilename).c_str(), nullptr, DB_RECNO, flags, 0644);
    DB_BTREE_STAT *stat;
    this->db.stat(nullptr, &stat, DB_FAST_STAT);
    this->last = flags ? 0 : stat->bt_ndata;
    this->closed = false;
}

/**
 * @class HeapTable - Heap storage engine (implementation of DbRelation)
 */

// HeapTable constructor
// Just size. Heapfile doesn't contians DB_BLOCK_SIZE
HeapTable::HeapTable(Identifier table_name, ColumnNames column_names,
                     ColumnAttributes column_attributes) : DbRelation(table_name, column_names,
                                                                      column_attributes),
                                                           file(table_name) {}

// Execute: CREATE TABLE <table_name> ( <columns> )
// Is not responsible for metadata storage or validation.
void HeapTable::create()
{
    file.create();
}

// Execute: CREATE TABLE IF NOT EXISTS <table_name> ( <columns> )
// Is not responsible for metadata storage or validation.
void HeapTable::create_if_not_exists()
{
    try
    {
        open();
    }
    catch (DbException &e)
    {
        create();
    }
    // Almost any error in the DB library throws a DbException.
}

// Execute: DROP TABLE <table_name>
void HeapTable::drop()
{
    file.drop();
}

// Open existing table. Enables: insert, update, delete, select, project
void HeapTable::open()
{
    file.open();
}

// Closes the table. Disables: insert, update, delete, select, project
void HeapTable::close()
{
    file.close();
}

// Expect row to be a dictionary with column name keys.
// Execute: INSERT INTO <table_name> (<row_keys>) VALUES (<row_values>)
// Return the handle of the inserted row.
Handle HeapTable::insert(const ValueDict *row)
{
    open();
    return Handle(append(validate(row)));
}

// Expect new_values to be a dictionary with column name keys.
//  Conceptually, execute: UPDATE INTO <table_name> SET <new_values> WHERE <handle>
//  where handle is sufficient to identify one specific record (e.g., returned from an insert
//  or select
void HeapTable::update(const Handle handle, const ValueDict *new_values)
{
    throw DbRelationError("Not implemented");
}

// Conceptually, execute: DELETE FROM <table_name> WHERE <handle>
//  where handle is sufficient to identify one specific record (e.g., returned from an insert
//  or select).
void HeapTable::del(const Handle handle)
{
    open();
    BlockID block_id = handle.first;
    RecordID record_id = handle.second;
    SlottedPage *block = this->file.get(block_id);
    block->del(record_id);
    this->file.put(block);
    delete block;
}

// Conceptually, execute: SELECT <handle> FROM <table_name> WHERE <where>
// Returns a list of handles for qualifying rows.
Handles *HeapTable::select()
{
    Handles *handles = new Handles();
    BlockIDs *block_ids = file.block_ids();

    for (auto const &block_id : *block_ids)
    {
        SlottedPage *block = file.get(block_id);
        RecordIDs *record_ids = block->ids();
        for (auto const &record_id : *record_ids)
        {
            if (selected(handle, where))
                handles->push_back(Handle(block_id, record_id));
        }
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

// Select the specific handles from where
// Return a list of handles(rows)
Handles *HeapTable::select(const ValueDict *where)
{
    Handles *handles = new Handles();
    BlockIDs *block_ids = file.block_ids();

    for (auto const &block_id : *block_ids)
    {
        SlottedPage *block = file.get(block_id);
        RecordIDs *record_ids = block->ids();
        for (auto const &record_id : *record_ids)
        {
            handles->push_back(Handle(block_id, record_id));
        }
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

// Return all values for handle.
ValueDict *HeapTable::project(Handle handle)
{
    return project(handle, &this->column_names);
}

// Return a sequence of values for handle given by column_names.
ValueDict *HeapTable::project(Handle handle, const ColumnNames *column_names)
{
    // open(); Don't need to reopen
    BlockID block_id = handle.first;
    RecordID record_id = handle.second;
    SlottedPage *block = file.get(block_id);
    Dbt *data = block->get(record_id);
    ValueDict *row = unmarshal(data);
    if (column_names->empty())
    {
        return row;
    }
    ValueDict *result = new ValueDict();
    for (auto const &column_name : *column_names)
    {
        if (row->find(column_name) == row->end())
            throw DbRelationError("table does not have column named '" + column_name + "'");
        (*result)[column_name] = (*row)[column_name];
    }
    delete data;
    delete block;
    delete row;
    return result;
}

// Check if the given row is acceptable to insert. Raise ValueError if not.
// Otherwise return the full row dictionary.
ValueDict *HeapTable::validate(const ValueDict *row)
{
    ValueDict *full_row = new ValueDict();
    for (auto const &column_name : this->column_names)
    {
        Value value;
        ValueDict::const_iterator it = row->find(column_name);
        if (it == row->end())
        {
            throw DbRelationError("don't know how to handle NULLs, defaults, etc.");
        }
        else
        {
            value = it->second;
        }
        (*full_row)[column_name] = value;
    }
    return full_row;
}

// Assumes row is fully fleshed-out. Appends a record to the file.
Handle HeapTable::append(const ValueDict *row)
{
    Dbt *data = marshal(row);
    SlottedPage *block = this->file.get(this->file.get_last_block_id());
    RecordID recordID;
    try
    {
        recordID = block->add(data);
    }
    catch (DbBlockNoRoomError &e)
    {
        // need a new block
        delete block;
        block = this->file.get_new();
        recordID = block->add(data);
    }

    this->file.put(block);
    delete block;
    delete[](char *) data->get_data();
    delete data;
    return Handle(this->file.get_last_block_id(), recordID);
}

// return the bits to go into the file
// caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
Dbt *HeapTable::marshal(const ValueDict *row)
{
    char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const &column_name : this->column_names)
    {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT)
        {
            if (offset + 4 > DbBlock::BLOCK_SZ - 4)
                throw DbRelationError("row too big to marshal");
            *(int32_t *)(bytes + offset) = value.n;
            offset += sizeof(int32_t);
        }
        else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT)
        {
            u_long size = value.s.length();
            if (size > UINT16_MAX)
                throw DbRelationError("text field too long to marshal");
            if (offset + 2 + size > DbBlock::BLOCK_SZ)
                throw DbRelationError("row too big to marshal");
            *(u16 *)(bytes + offset) = size;
            offset += sizeof(u16);
            memcpy(bytes + offset, value.s.c_str(), size); // assume ascii for now
            offset += size;
        }
        else
        {
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
    }
    char *right_size_bytes = new char[offset];
    memcpy(right_size_bytes, bytes, offset);
    Dbt *data = new Dbt(right_size_bytes, offset);
    delete[] bytes;
    return data;
}

// Similar structure of marshal
ValueDict *HeapTable::unmarshal(Dbt *data)
{
    ValueDict *row = new ValueDict();
    uint offset = 0;
    uint col_num = 0;
    char *bytes = (char *)data->get_data();
    Value value; // New value

    for (auto const &column_name : this->column_names)
    {
        ColumnAttribute ca = this->column_attributes[col_num++];
        if (ca.get_data_type() == ColumnAttribute::DataType::INT)
        {
            value.n = *(int32_t *)(bytes + offset);
            offset += sizeof(int32_t);
        }
        else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT)
        {
            u16 size = *(u16 *)(bytes + offset);
            offset += sizeof(u16);
            char buffer[size];
            memcpy(buffer, bytes + offset, size); // Idea from marshal
            buffer[size] = (char)0;
            value.s = string(buffer); // assume ascii for now
            offset += size;
        }
        else
        {
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
        (*row)[column_name] = value;
    }
    return row;
}

// test function -- returns true if all tests pass
bool test_heap_storage()
{
    std::cout << "heap_storage" << std::endl;
    ColumnNames column_names;
    column_names.push_back("a");
    column_names.push_back("b");
    ColumnAttributes column_attributes;
    ColumnAttribute ca(ColumnAttribute::INT);
    column_attributes.push_back(ca);
    ca.set_data_type(ColumnAttribute::TEXT);
    column_attributes.push_back(ca);
    HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
    table1.create();
    // std::cout << "create ok" << std::endl;
    table1.drop(); // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
    // std::cout << "drop ok" << std::endl;

    HeapTable table("_test_data_cpp", column_names, column_attributes);
    table.create_if_not_exists();
    // std::cout << "create_if_not_exsts ok" << std::endl;

    ValueDict row;
    row["a"] = Value(12);
    row["b"] = Value("Hello!");
    // std::cout << "try insert" << std::endl;
    table.insert(&row);
    // std::cout << "insert ok" << std::endl;
    Handles *handles = table.select();
    // std::cout << "select ok " << handles->size() << std::endl;
    ValueDict *result = table.project((*handles)[0]);
    // std::cout << "project ok" << std::endl;
    Value value = (*result)["a"];
    if (value.n != 12)
    {
        table.drop();
        return false;
    }
    value = (*result)["b"];
    if (value.s != "Hello!")
    {
        table.drop();
        return false;
    }
    table.drop();
    delete result;
    delete handles;
    return true;
}

// Testing function for SlottedPage.
bool test_slotted_page()
{
    // construct one
    char block[DbBlock::BLOCK_SZ];
    Dbt data(block, sizeof(block));
    SlottedPage slot(data, 1, true);

    // Initilize objects
    RecordID id;
    Dbt *get_dbt;

    // Additions
    char record1[] = "Hello";
    char record2[] = "Wow";
    char record3[] = "George";
    std::vector<std::string> records;
    records.push_back(record1);
    records.push_back(record2);

    // Add record1 "Hello"
    Dbt rec1_dbt(record1, sizeof(record1));
    id = slot.add(&rec1_dbt);
    if (id != 1)
    {
        std::cout << "Wrong Add record1 id" << std::endl;
        return false;
    }

    // Add record2 "Wow"
    Dbt rec2_dbt(record2, sizeof(record2));
    id = slot.add(&rec2_dbt);
    if (id != 2)
    {
        std::cout << "Wrong Add record2 id" << std::endl;
        return false;
    }

    // Get record 2
    get_dbt = slot.get(id);
    string expected = record2;
    string actual = (char *)get_dbt->get_data();
    // if (record2 != (char *) get_dbt->get_data())
    // The char value is a little different
    if (actual != expected)
    {
        std::cout << "Wrong Get record2" << std::endl;
        return false;
    }

    // Put Record 1
    char record1_put[] = "Goodbye";
    rec1_dbt = Dbt(record1_put, sizeof(record1_put));
    slot.put(1, rec1_dbt);

    // Check id 1 and id 2
    // Get record 1
    get_dbt = slot.get(1);
    expected = record1_put;
    actual = (char *)get_dbt->get_data();
    if (actual != expected)
    {
        std::cout << "Wrong Get record1" << std::endl;
        return false;
    }
    delete get_dbt;

    // Get record 2
    get_dbt = slot.get(2);
    expected = record2;
    actual = (char *)get_dbt->get_data();
    if (actual != expected)
    {
        std::cout << "Wrong Get record2" << std::endl;
        return false;
    }
    delete get_dbt;

    // iteration
    RecordIDs *ids = slot.ids();
    int i = 1;
    for (RecordID id : *ids)
    {
        if (i != id)
        {
            std::cout << "Wrong id" << i << id << std::endl;
            return false;
        }
        i++;
    }

    // deletion
    slot.del(1);
    ids = slot.ids();
    if (ids->size() != 1 || ids->at(0) != 2)
    {
        std::cout << "Wrong id" << i << id << std::endl;
        return false;
    }

    // Get record 2 should be "Wow"
    get_dbt = slot.get(2);
    expected = record2;
    actual = (char *)get_dbt->get_data();
    if (actual != expected)
    {
        std::cout << "Wrong Get record2 After deleting" << std::endl;
        return false;
    }

    // Add record3 "George"
    Dbt rec3_dbt = Dbt(record3, sizeof(record3));
    id = slot.add(&rec3_dbt);
    if (id != 3)
    {
        std::cout << "Wrong Add record1 id" << std::endl;
        return false;
    }

    // Chech data consistency
    // char data_test = '\x00\x03\x00\x13\x00\x00\x00\x00\x00\x04\x00\x1a\x00\x06\x00\x14\x00\x00\x00WGeorgeWow!';
    // std::cout << slot.get_data() << std::endl;
    // std::cout << data_test << std::endl;
    // if (data != slot.get_data)
    // {
    //     std::cout << "Wrong data" << std::endl;
    //     return false;
    // }
    delete ids;
    return true;
}

// /**
//  * Testing function for SlottedPage.
//  * @return true if testing succeeded, false otherwise
//  */
// bool test_slotted_page() {
//     // construct one
//     char blank_space[DbBlock::BLOCK_SZ];
//     Dbt block_dbt(blank_space, sizeof(blank_space));
//     SlottedPage slot(block_dbt, 1, true);

//     // add a record
//     char rec1[] = "hello";
//     Dbt rec1_dbt(rec1, sizeof(rec1));
//     RecordID id = slot.add(&rec1_dbt);
//     if (id != 1)
//         return assertion_failure("add id 1");

//     // get it back
//     Dbt *get_dbt = slot.get(id);
//     string expected(rec1, sizeof(rec1));
//     string actual((char *) get_dbt->get_data(), get_dbt->get_size());
//     delete get_dbt;
//     if (expected != actual)
//         return assertion_failure("get 1 back " + actual);

//     // add another record and fetch it back
//     char rec2[] = "goodbye";
//     Dbt rec2_dbt(rec2, sizeof(rec2));
//     id = slot.add(&rec2_dbt);
//     if (id != 2)
//         return assertion_failure("add id 2");

//     // get it back
//     get_dbt = slot.get(id);
//     expected = string(rec2, sizeof(rec2));
//     actual = string((char *) get_dbt->get_data(), get_dbt->get_size());
//     delete get_dbt;
//     if (expected != actual)
//         return assertion_failure("get 2 back " + actual);

//     // test put with expansion (and slide and ids)
//     char rec1_rev[] = "something much bigger";
//     rec1_dbt = Dbt(rec1_rev, sizeof(rec1_rev));
//     slot.put(1, rec1_dbt);
//     // check both rec2 and rec1 after expanding put
//     get_dbt = slot.get(2);
//     expected = string(rec2, sizeof(rec2));
//     actual = string((char *) get_dbt->get_data(), get_dbt->get_size());
//     delete get_dbt;
//     if (expected != actual)
//         return assertion_failure("get 2 back after expanding put of 1 " + actual);
//     get_dbt = slot.get(1);
//     expected = string(rec1_rev, sizeof(rec1_rev));
//     actual = string((char *) get_dbt->get_data(), get_dbt->get_size());
//     delete get_dbt;
//     if (expected != actual)
//         return assertion_failure("get 1 back after expanding put of 1 " + actual);

//     // test put with contraction (and slide and ids)
//     rec1_dbt = Dbt(rec1, sizeof(rec1));
//     slot.put(1, rec1_dbt);
//     // check both rec2 and rec1 after contracting put
//     get_dbt = slot.get(2);
//     expected = string(rec2, sizeof(rec2));
//     actual = string((char *) get_dbt->get_data(), get_dbt->get_size());
//     delete get_dbt;
//     if (expected != actual)
//         return assertion_failure("get 2 back after contracting put of 1 " + actual);
//     get_dbt = slot.get(1);
//     expected = string(rec1, sizeof(rec1));
//     actual = string((char *) get_dbt->get_data(), get_dbt->get_size());
//     delete get_dbt;
//     if (expected != actual)
//         return assertion_failure("get 1 back after contracting put of 1 " + actual);

//     // test del (and ids)
//     RecordIDs *id_list = slot.ids();
//     if (id_list->size() != 2 || id_list->at(0) != 1 || id_list->at(1) != 2)
//         return assertion_failure("ids() with 2 records");
//     delete id_list;
//     slot.del(1);
//     id_list = slot.ids();
//     if (id_list->size() != 1 || id_list->at(0) != 2)
//         return assertion_failure("ids() with 1 record remaining");
//     delete id_list;
//     get_dbt = slot.get(1);
//     if (get_dbt != nullptr)
//         return assertion_failure("get of deleted record was not null");

//     // try adding something too big
//     rec2_dbt = Dbt(nullptr, DbBlock::BLOCK_SZ - 10); // too big, but only because we have a record in there
//     try {
//         slot.add(&rec2_dbt);
//         return assertion_failure("failed to throw when add too big");
//     } catch (const DbBlockNoRoomError &exc) {
//         // test succeeded - this is the expected path
//     } catch (...) {
//         // Note that this won't catch segfault signals -- but in that case we also know the test failed
//         return assertion_failure("wrong type thrown when add too big");
//     }

//     // more volume
//     string gettysburg = "Four score and seven years ago our fathers brought forth on this continent, a new nation, conceived in Liberty, and dedicated to the proposition that all men are created equal.";
//     int32_t n = -1;
//     uint16_t text_length = gettysburg.size();
//     uint total_size = sizeof(n) + sizeof(text_length) + text_length;
//     char *data = new char[total_size];
//     *(int32_t *) data = n;
//     *(uint16_t *) (data + sizeof(n)) = text_length;
//     memcpy(data + sizeof(n) + sizeof(text_length), gettysburg.c_str(), text_length);
//     Dbt dbt(data, total_size);
//     vector<SlottedPage> page_list;
//     BlockID block_id = 1;
//     Dbt slot_dbt(new char[DbBlock::BLOCK_SZ], DbBlock::BLOCK_SZ);
//     slot = SlottedPage(slot_dbt, block_id++, true);
//     for (int i = 0; i < 10000; i++) {
//         try {
//             slot.add(&dbt);
//         } catch (DbBlockNoRoomError &exc) {
//             page_list.push_back(slot);
//             slot_dbt = Dbt(new char[DbBlock::BLOCK_SZ], DbBlock::BLOCK_SZ);
//             slot = SlottedPage(slot_dbt, block_id++, true);
//             slot.add(&dbt);
//         }
//     }
//     page_list.push_back(slot);
//     for (const auto &slot : page_list) {
//         RecordIDs *ids = slot.ids();
//         for (RecordID id : *ids) {
//             Dbt *record = slot.get(id);
//             if (record->get_size() != total_size)
//                 return assertion_failure("more volume wrong size", block_id - 1, id);
//             void *stored = record->get_data();
//             if (memcmp(stored, data, total_size) != 0)
//                 return assertion_failure("more volume wrong data", block_id - 1, id);
//             delete record;
//         }
//         delete ids;
//         delete[] (char *) slot.block.get_data();  // this is why we need to be a friend--just convenient
//     }
//     delete[] data;
//     return true;
// }
