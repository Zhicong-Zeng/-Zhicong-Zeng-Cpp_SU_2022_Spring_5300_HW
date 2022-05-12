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

// make query result be printable // Nothing to do
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
    if (SQLExec::tables == nullptr)
    {
        SQLExec::tables = new Tables();
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
        throw SQLExecError("unrecogniaed ColumnAttribute type");
    }
}

// recursive decent into the AST
// There should have index and table according to milestone3 python
// We just implement create table now
QueryResult *SQLExec::create(const CreateStatement *statement)
{
    // Create a table with given table_name (string) and table_element_list (from parse tree).
    if (statement->type != hsql::CreateStatement::kTable)
    {
        throw SQLExecError("unrecogniaed Create type");
    }

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

// DROP ...
QueryResult *SQLExec::drop(const DropStatement *statement)
{
    if (statement->type != hsql::DropStatement::kTable)
    {
        return new QueryResult("Cannot drop a schema table!");
    }

    Identifier table_name = statement->name;

    // Check the table is not a schema table
    if (table_name == Tables::TABLE_NAME || table_name == Columns::TABLE_NAME)
        throw SQLExecError("Cannot drop a schema table!");

    // get the table
    DbRelation &table = SQLExec::tables->get_table(table_name);

    // remove table
    table.drop();

    // remove from _columns schema
    ValueDict where;
    where["table_name"] = Value(table_name);

    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
    Handles *handles = columns.select(&where);
    for (auto const &handle : *handles)
    {
        columns.del(handle);
    }
    delete handles;

    // finally, remove from table schema
    SQLExec::tables->del(*SQLExec::tables->select(&where)->begin()); // expect only one row

    return new QueryResult(std::string("dropped") + table_name);
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
    // Implement later
    // case ShowStatement::kIndex:
    //     return show_index(statement);
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

// Test Function for SQLExec class
bool test_sqlexec()
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