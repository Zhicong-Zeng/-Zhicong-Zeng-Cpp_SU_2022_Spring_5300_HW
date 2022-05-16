/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Spring 2022"
 */
#include "SQLExec.h"
#include <sstream>
#include "ParseTreeToString.h"

using namespace std;
using namespace hsql;

// define static data
Tables *SQLExec::tables = nullptr;
Indices *SQLExec::indices = nullptr;

// make query result be printable
ostream &operator<<(ostream &out, const QueryResult &qres)
{
    if (qres.column_names != nullptr)
    {
        for (auto const &column_name : *qres.column_names)
            out << column_name << " ";
        out << endl
            << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
        out << endl;
        for (auto const &row : *qres.rows)
        {
            for (auto const &column_name : *qres.column_names)
            {
                Value value = row->at(column_name);
                switch (value.data_type)
                {
                case ColumnAttribute::INT:
                    out << value.n;
                    break;
                case ColumnAttribute::TEXT:
                    out << "\"" << value.s << "\"";
                    break;
                case ColumnAttribute::BOOLEAN:
                    out << (value.n == 0 ? "false" : "true");
                    break;
                default:
                    out << "???";
                }
                out << " ";
            }
            out << endl;
        }
    }
    out << qres.message;
    return out;
}

// Delete all the result every time or it would remain
QueryResult::~QueryResult()
{
    if (column_names != nullptr)
    {
        delete column_names;
    }
    if (column_attributes != nullptr)
    {
        delete column_attributes;
    }
    if (rows != nullptr)
    {
        for (auto row : *rows)
        {
            delete row;
        }
        delete rows;
    }
}

// The Sqlstatement enter here and analysis to different methods.
QueryResult *SQLExec::execute(const SQLStatement *statement)
{
    // This object is a global variable to store the table
    // Should need to initiaize the indices
    if (SQLExec::tables == nullptr)
    {
        SQLExec::tables = new Tables();
        SQLExec::indices = new Indices();
    }

    try
    {
        // There are many types of statements but we just need these three
        // Check StatementType in SQLStatment.h
        switch (statement->type())
        {
        case kStmtCreate:
            return create((const CreateStatement *)statement);
        case kStmtDrop:
            return drop((const DropStatement *)statement);
        case kStmtShow:
            return show((const ShowStatement *)statement);
        default:
            return new QueryResult("not implemented");
        }
    }
    catch (DbRelationError &e)
    {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

// Check SQLExec.h and ParseTreeToString.h (class ColumnAttribute)
void SQLExec::column_definition(const ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute)
{
    column_name = col->name;
    switch (col->type)
    {
    case ColumnDefinition::INT:
        column_attribute.set_data_type(ColumnAttribute::INT);
        break;
    case ColumnDefinition::TEXT:
        column_attribute.set_data_type(ColumnAttribute::TEXT);
        break;
    default:
        throw SQLExecError("unrecognized ColumnAttribute type");
    }
}

// recursive decent into the AST
QueryResult *SQLExec::create(const CreateStatement *statement)
{
    switch (statement->type)
    {
    case CreateStatement::kTable:
        return create_table(statement);
    case CreateStatement::kIndex:
        return create_index(statement);
    default:
        return new QueryResult("unrecognized Create type");
    }
}

// Case create table
QueryResult *SQLExec::create_table(const CreateStatement *statement)
{
    // vector<Identifier> ColumnNames;
    // vector<ColumnAttribute> ColumnAttributes;

    Identifier table_name = statement->tableName;

    ColumnNames column_names;
    ColumnAttributes column_attributes;
    Identifier column_name;
    ColumnAttribute column_attribute;

    // Use last method to fill with the ColumnNames and ColumnAttributes
    // vector<ColumnDefinition*>* columns
    for (ColumnDefinition *col : *statement->columns)
    {
        column_definition(col, column_name, column_attribute);
        column_names.push_back(column_name);
        column_attributes.push_back(column_attribute);
    }

    // Execute the statement
    // update _tables schema
    ValueDict row;
    row["table_name"] = table_name;
    Handle table_handle = SQLExec::tables->insert(&row);

    try
    {
        // update _columns schema
        // pair<BlockID, RecordID> Handle;
        Handles columns_order;
        // Get Columns
        DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);

        try
        {
            for (uint i = 0; i < column_names.size(); i++)
            {
                row["column_name"] = column_names[i];
                // holds value for a field
                row["data_type"] = Value(column_attributes[i].get_data_type() == ColumnAttribute::INT ? "INT" : "TEXT");
                columns_order.push_back(columns.insert(&row));
            }

            // Get table
            DbRelation &table = SQLExec::tables->get_table(table_name);

            if (statement->ifNotExists)
            {
                table.create_if_not_exists();
            }
            else
            {
                table.create();
            }
        }
        catch (...)
        {
            // attempt to remove from _columns
            try
            {
                for (auto const &handle : columns_order)
                {
                    // Delete table
                    columns.del(handle);
                }
            }
            catch (...)
            {
            }
            throw;
        }
    }
    catch (exception &e)
    {
        try
        {
            // attempt to remove from _tables
            SQLExec::tables->del(table_handle);
        }
        catch (...)
        {
        }
        throw;
    }
    return new QueryResult("created " + table_name);
}

// Create index
// table_name index_name column_name seq_in_index index_type is_unique
QueryResult *SQLExec::create_index(const CreateStatement *statement)
{
    Identifier tableName = statement->tableName;
    Identifier indexName = statement->indexName;

    // insert a row for every column in index into _indices
    ValueDict row;
    row["table_name"] = Value(tableName);
    row["index_name"] = Value(indexName);
    row["index_type"] = Value(statement->indexType);
    row["is_unique"] = Value(string(statement->indexType) == "BTREE"); // Using BTREE is true, HASH is false
    int seq = 0;
    Handles handles;
    try
    {
        // Get indexColumns from indexColumns
        for (auto const &col_name : *statement->indexColumns)
        {
            row["seq_in_index"] = Value(++seq);
            row["column_name"] = Value(col_name);
            handles.push_back(SQLExec::indices->insert(&row));
        }
        // create index
        DbIndex &index = SQLExec::indices->get_index(tableName, indexName);
        index.create();
    }
    catch (...)
    {
        // delete all the handles if error occurs
        try
        {
            for (auto const &handle : handles)
                SQLExec::indices->del(handle);
        }
        catch (...)
        {
        }
        throw;
    }
    return new QueryResult();
}

// recursive decent into the AST
QueryResult *SQLExec::drop(const DropStatement *statement)
{
    switch (statement->type)
    {
    case DropStatement::kTable:
        return drop_table(statement);
    case DropStatement::kIndex:
        return drop_index(statement);
    default:
        return new QueryResult("unrecognized Drop type");
    }
}

QueryResult *SQLExec::drop_table(const DropStatement *statement)
{
    Identifier name = statement->name;

    // Check the table is not a schema table
    if (name == Tables::TABLE_NAME || name == Columns::TABLE_NAME)
        throw SQLExecError("Cannot drop a schema table!");

    // get the table
    DbRelation &table = SQLExec::tables->get_table(name);

    // remove table
    table.drop();

    // remove from _columns schema
    ValueDict where;
    where["table_name"] = Value(name);

    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
    Handles *handles = columns.select(&where);
    for (auto const &handle : *handles)
    {
        columns.del(handle);
    }

    // finally, remove from table schema
    // SQLExec::tables->del(->begin()); // expect only one row

    Handles *results = SQLExec::tables->select(&where);

    for (auto const &result : *results)
    {
        SQLExec::tables->del(result);
    }
    delete handles;
    delete results;
    return new QueryResult("dropped " + name);
}

QueryResult *SQLExec::drop_index(const DropStatement *statement)
{
    Identifier name = statement->name;
    Identifier indexName = statement->indexName;

    // drop index
    DbIndex &index = SQLExec::indices->get_index(name, indexName);
    index.drop();

    // remove rows from _indices for this index
    ValueDict where;
    where["table_name"] = Value(name);
    where["index_name"] = Value(indexName);
    Handles *handles = SQLExec::indices->select(&where);

    for (auto const &handle : *handles)
        SQLExec::indices->del(handle);
    delete handles;

    return new QueryResult("dropped index " + indexName + " From " + name);
}

// Check ShowStatement.h
QueryResult *SQLExec::show(const ShowStatement *statement)
{
    switch (statement->type)
    {
    case ShowStatement::kTables:
        return show_tables();
    case ShowStatement::kColumns:
        return show_columns(statement);
    case ShowStatement::kIndex:
        return show_index(statement);
    default:
        throw SQLExecError("unrecognized Show type");
    }
}

// Executes: SELECT * FROM _tables
// table_name column_name data_type
//  +----------+----------+----------+
//  "_tables" "table_name" "TEXT"
//  successfully returned 1 rows
// QueryResult() : column_names, column_attributes, rows, message("") {}
QueryResult *SQLExec::show_tables()
{
    // Construct the QueryResult
    ColumnNames *column_names = new ColumnNames;
    column_names->push_back("table_name");

    ColumnAttributes *column_attributes = new ColumnAttributes;
    // expected unqualified-id before ‘->’ token
    // The middle ColumnAttribute is Class type. The third one is DataType
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    //
    ValueDicts *rows = new ValueDicts();
    // Select all the rows from table into handles
    Handles *handles = SQLExec::tables->select();
    int count = handles->size() - 2;

    // Check not in schema_tables.SCHEMA_TABLES
    for (auto const &handle : *handles)
    {
        ValueDict *row = SQLExec::tables->project(handle, column_names);
        Identifier table_name = row->at("table_name").s;
        if (table_name != Tables::TABLE_NAME && table_name != Columns::TABLE_NAME)
        {
            rows->push_back(row);
        }
        delete row;
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows, " successfully returned " + to_string(count) + " rows");
}

// Returns a table of column names from the given table
// Executes SELECT table_name, column_name, data_type FROM _columns WHERE table_name = <table_name>;
QueryResult *SQLExec::show_columns(const ShowStatement *statement)
{
    // Construct the QueryResult
    ColumnNames *column_names = new ColumnNames;
    column_names->push_back("table_name");
    column_names->push_back("column_name");
    column_names->push_back("data_type");
    ColumnAttributes *column_attributes = new ColumnAttributes;

    // The middle ColumnAttribute is Class type. The third one is DataType
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    // First ValueDicts to store the data
    ValueDicts *rows = new ValueDicts();

    // Second ValueDicts to locate the table
    ValueDict where;
    where["table_name"] = Value(statement->tableName);

    // A different method to get the column name
    Handles *handles = SQLExec::tables->get_table(Columns::TABLE_NAME).select(&where);
    int count = handles->size();

    // Check not in schema_tables.SCHEMA_TABLES
    for (auto const &handle : *handles)
    {
        ValueDict *row = SQLExec::tables->get_table(Columns::TABLE_NAME).project(handle, column_names);
        rows->push_back(row);
    }

    delete handles;
    return new QueryResult(column_names, column_attributes, rows, " successfully returned " + to_string(count) + " rows");
}

// SHOW INDEX FROM goober
//  table_name index_name column_name seq_in_index index_type is_unique
//  +----------+----------+----------+----------+----------+----------+
QueryResult *SQLExec::show_index(const ShowStatement *statement)
{
    ColumnNames *column_names = new ColumnNames;
    ColumnAttributes *column_attributes = new ColumnAttributes;

    column_names->push_back("table_name");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("index_name");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("column_name");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("seq_in_index");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::INT));

    column_names->push_back("index_type");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("is_unique");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::BOOLEAN));

    ValueDict where;
    where["table_name"] = Value(string(statement->tableName));
    Handles *handles = SQLExec::indices->select(&where);
    u_long n = handles->size();

    ValueDicts *rows = new ValueDicts;
    for (auto const &handle : *handles)
    {
        ValueDict *row = SQLExec::indices->project(handle, column_names);
        rows->push_back(row);
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows,
                           "successfully returned " + to_string(n) + " rows");
}

// Test Function for SQLExec class
bool test_sqlexec_table()
{
    const int num_queries = 11;
    const string queries[num_queries] = {"show tables",
                                         "show columns from _tables",
                                         "show columns from _columns",
                                         "create table foo (id int, data text, x integer, y integer, z integer)",
                                         "create table foo (goober int)",
                                         "create table goo (x int, x text)",
                                         "show tables",
                                         "show columns from foo",
                                         "drop table foo",
                                         "show tables",
                                         "show columns from foo"};
    bool passed = true;
    const string results[num_queries] = {"SHOW TABLES table_name  successfully returned 0 rows",
                                         "SHOW COLUMNS FROM _tables table_name column_name data_type _tables table_name TEXT successfully returned 1 rows",
                                         "SHOW COLUMNS FROM _columns   table_name column_name data_type _columns   table_name TEXT_columns column_name  TEXT_columns data_type TEXT   successfully returned 3 rows",
                                         "CREATE TABLE foo (id INT, data TEXT, x INT, y INT, z INT)  created foo",
                                         "CREATE TABLE foo (goober INT)  Error: DbRelationError: foo already exists",
                                         "Error: DbRelationError: duplicate column goo.x",
                                         "SHOW TABLES  table_name foo successfully returned 1 rows",
                                         "SHOW COLUMNS FROM foo  table_name column_name data_type foo id INT foo data TEXT  foo x INT  foo y INT  foo z INT  successfully returned 5 rows",
                                         "DROP TABLE foo   dropped foo",
                                         "SHOW TABLES  table_name  successfully returned 0 rows",
                                         "SHOW COLUMNS FROM footable_name column_name data_type  successfully returned 0 rows"};

    for (int i = 0; i < num_queries; i++)
    {
        SQLParserResult *result = SQLParser::parseSQLString(queries[i]);
        if (result->isValid())
        {
            // if result is valid, pass result to our own execute function
            for (long unsigned int j = 0; j < result->size(); j++)
            {
                const SQLStatement *statement = result->getStatement(j);
                try
                {
                    string str1 = test_logic((const SQLStatement *)statement);
                    string str2 = str1;
                    str2.erase(remove_if(str2.begin(), str2.end(), [](char c)
                                         { return !(isalnum(c)); }),
                               str2.end());
                    string str3 = results[i];
                    str3.erase(remove_if(str3.begin(), str3.end(), [](char c)
                                         { return !(isalnum(c)); }),
                               str3.end());
                    if (str2 == str3)
                    {
                        cout << queries[i] << endl;
                        cout << str1 << endl;
                    }
                    else
                    {
                        cout << "Unexpected query  " << queries[i] << endl;
                        cout << "query_result  " << str2 << endl;
                        cout << "results[i]  " << str3 << endl;
                        passed = false;
                    }
                }
                catch (SQLExecError &e)
                {
                    cout << "Error: " << e.what() << endl;
                }
            }
        }
        else
        {
            passed = false;
            cout << "Invalid SQL" << endl;
        }
        delete result;
    }
    return passed;
}

// Test Function for SQLExec class
bool test_sqlexec_index()
{
    const int num_queries = 18;
    const string queries[num_queries] = {"create table goober (x integer, y integer, z integer)",
                                         "show tables",
                                         "show columns from goober",
                                         "create index fx on goober (x,y)",
                                         "show index from goober",
                                         "drop index fx from goober",
                                         "show index from goober",
                                         "create index fx on goober (x)",
                                         "show index from goober",
                                         "create index fx on goober (y,z)",
                                         "show index from goober",
                                         "create index fyz on goober (y,z)",
                                         "show index from goober",
                                         "drop index fx from goober",
                                         "show index from goober",
                                         "drop index fyz from goober",
                                         "show index from goober",
                                         "drop table goober"};
    bool passed = true;
    const string results[num_queries] = {"CREATE TABLE goober x INT yINT zINT created goober",
                                         "SHOW TABLES table_name goober successfully returned 1 rows",
                                         "SHOW COLUMNS FROM goober table_name column_name data_type goober x INT goober y INT goober z INT successfully returned 3 rows",
                                         "CREATE INDEX fx ON goober USING BTREE fx ON goober USING BTREE xy",
                                         "SHOW INDEX FROM goober table_name index_name column_name seq_in_index index_type is_unique goober fx x 1 BTREE true goober fx y 2 BTREE true successfully returned 2 rows",
                                         "DROP goober dropped index fx From goober",
                                         "SHOW INDEX FROM goober table_name index_name column_name seq_in_index index_type is_unique successfully returned 0 rows",
                                         "CREATE INDEX fx ON goober USING BTREE fx ON goober USING BTREE x",
                                         "SHOW INDEX FROM goober table_name index_name column_name seq_in_index index_type is_unique goober fx x 1 BTREE true successfully returned 1 rows",
                                         "CREATE INDEX fx ON goober USING BTREE (y, z) Error: DbRelationError: duplicate index goober fx",
                                         "SHOW INDEX FROM goober table_name index_name column_name seq_in_index index_type is_unique goober fx x 1 BTREE true successfully returned 1 rows",
                                         "CREATE INDEX fyz ON goober USING BTREE fyz ON goober USING BTREE yz",
                                         "SHOW INDEX FROM goober table_name index_name column_name seq_in_index index_type is_unique goober fx x 1 BTREE true goober fyz y 1 BTREE true goober fyz z 2 BTREE true successfully returned 3 rows",
                                         "DROP goober dropped index fx From goober",
                                         "SHOW INDEX FROM goober table_name index_name column_name seq_in_index index_type is_unique goober fyz y 1 BTREE true goober fyz z 2 BTREE true successfully returned 2 rows",
                                         "DROP goober dropped index fyz From goober",
                                         "SHOW INDEX FROM goober table_name index_name column_name seq_in_index index_type is_unique successfully returned 0 rows",
                                         "DROP TABLE goober dropped goober"};

    for (int i = 0; i < num_queries; i++)
    {
        SQLParserResult *result = SQLParser::parseSQLString(queries[i]);
        if (result->isValid())
        {
            // if result is valid, pass result to our own execute function
            for (long unsigned int j = 0; j < result->size(); j++)
            {
                const SQLStatement *statement = result->getStatement(j);
                try
                {
                    string str1 = test_logic((const SQLStatement *)statement);
                    string str2 = str1;
                    str2.erase(remove_if(str2.begin(), str2.end(), [](char c)
                                         { return !(isalnum(c)); }),
                               str2.end());
                    string str3 = results[i];
                    str3.erase(remove_if(str3.begin(), str3.end(), [](char c)
                                         { return !(isalnum(c)); }),
                               str3.end());
                    if (str2 == str3)
                    {
                        cout << queries[i] << endl;
                        cout << str1 << endl;
                    }
                    else
                    {
                        cout << "Unexpected query  " << queries[i] << endl;
                        cout << "query_result  " << str2 << endl;
                        cout << "results[i]  " << str3 << endl;
                        passed = false;
                    }
                }
                catch (SQLExecError &e)
                {
                    cout << "Error: " << e.what() << endl;
                }
            }
        }
        else
        {
            passed = false;
            cout << "Invalid SQL" << endl;
        }
        delete result;
    }
    return passed;
}

string test_logic(const SQLStatement *statement)
{
    QueryResult *query_result = SQLExec::execute(statement);
    std::stringstream buffer;
    buffer << ParseTreeToString::statement(statement) << std::endl;
    buffer << *query_result << std::endl;
    string str1 = buffer.str();

    delete query_result;
    query_result = nullptr;
    return buffer.str();
}