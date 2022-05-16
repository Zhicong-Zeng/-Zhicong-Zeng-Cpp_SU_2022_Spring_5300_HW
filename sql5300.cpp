#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>
#include "db_cxx.h"
#include "SQLParser.h"
#include "heap_storage.h"

// we allocate and initialize the _DB_ENV global
DbEnv *_DB_ENV;

using namespace std;
using namespace hsql;

// forward declare
string execute(const SQLStatement *stmt);
string printSelect(const SelectStatement *stmt);
string printCreate(const CreateStatement *stmt);
string columnDefinitionToString(const ColumnDefinition *col);
string printExpression(const Expr *expr);
string printTableRefInfo(const TableRef *table);
string printOperatorExpression(const Expr *expr);

/**
 * Main entry point of the sql5300 program
 * @args dbenvpath  the path to the BerkeleyDB database environment
 */
int main(int argc, char *argv[])
{
    // Open/create the db enviroment
    if (argc != 2)
    {
        cerr << "Usage: cpsc5300: dbenvpath" << endl;
        return 1;
    }
    char *envHome = argv[1];
    cout << "(sql5300: running with database environment at " << envHome << ")" << endl;
    DbEnv env(0U);
    env.set_message_stream(&cout);
    env.set_error_stream(&cerr);
    try
    {
        env.open(envHome, DB_CREATE | DB_INIT_MPOOL, 0);
    }
    catch (DbException &exc)
    {
        cerr << "(sql5300: " << exc.what() << ")";
        exit(1);
    }
    _DB_ENV = &env;

    // SQL entry
    while (true)
    {
        cout << "SQL> ";
        string sql;
        getline(cin, sql);

        if (sql == "quit")
            break;
        if (sql.length() < 1)
            continue;
        if (sql == "test")
        {
            cout << "test_slotted_page: " << (test_slotted_page() ? "Pass" : "Failed") << endl;
            cout << "test_heap_storage: " << (test_heap_storage() ? "Pass" : "Failed") << endl;
            continue;
        }
        if (query == "test2" || query == "test table") {
            cout << "Testing SQL executor:\n " << (test_sqlexec_table() ? "Tests passed" :  "Tests failed") << endl;
            continue;
        }
        if (query == "test3" || query == "test index") {
            cout << "Testing SQL executor:\n " << (test_sqlexec_index() ? "Tests passed" :  "Tests failed") << endl;
            continue;
        }

        // Use SQLParser
        SQLParserResult *parser = SQLParser::parseSQLString(sql);
        // hsql::SQLParserResult *parser = hsql::SQLParser::parseSQLString(sql);

        if (!parser->isValid())
        {
            cout << "inValid SQL:" << sql << endl;
            cout << parser->errorMsg() << endl;
            delete parser;
        }
        else
        {
            for (uint i = 0; i < parser->size(); i++)
            {
                cout << execute(parser->getStatement(i)) << endl;
            }
            delete parser;
        }
    }
    return EXIT_SUCCESS;
}

/**
 * Execute an SQL statement (but for now, just spit back the SQL)
 * @param stmt  Hyrise AST for the statement
 * @returns     a string (for now) of the SQL statment
 */
string execute(const SQLStatement *stmt)
{
    string str;
    switch (stmt->type())
    {
    case kStmtSelect:
        str += printSelect((const SelectStatement *)stmt);
        break;
    case kStmtCreate:
        str += printCreate((const CreateStatement *)stmt);
        break;
    /*We don't need these functions now
    case kStmtInsert:
        printInsertStatementInfo((const InsertStatement *)stmt, 0);
        break;
    case kStmtImport:
        printImportStatementInfo((const ImportStatement *)stmt, 0);
        break;
    case kStmtShow:
        inprint("SHOW", 0);
    */
    default:
        cout << "No implemented" << endl;
        break;
    }
    return str;
}

/**
 * Execute an SQL create statement (but for now, just spit back the SQL)
 * @param stmt  Hyrise AST for the create statement
 * @returns     a string (for now) of the SQL statment
 */
string printCreate(const CreateStatement *stmt)
{
    string sql_string, sql_string_final;

    if (stmt->columns != NULL)
    {
        sql_string += "(";
        for (auto col_name : *stmt->columns)
        {
            sql_string += " " + columnDefinitionToString(col_name) + ",";
        }
        sql_string.resize(sql_string.size() - 1);

        sql_string_final += "CREATE TABLE " + string(stmt->tableName) + " " + sql_string + ")";
    }
    return sql_string_final;
}

/**
 * Convert the hyrise ColumnDefinition AST back into the equivalent SQL
 * @param col  column definition to unparse
 * @return     SQL equivalent to *col
 */
string columnDefinitionToString(const ColumnDefinition *col)
{
    string str;
    str += col->name;

    switch (col->type)
    {
    case ColumnDefinition::DOUBLE:
        str += " DOUBLE";
        break;
    case ColumnDefinition::INT:
        str += " INT";
        break;
    case ColumnDefinition::TEXT:
        str += " TEXT";
        break;
    default:
        str += " ...";
        break;
    }
    return str;
}

/**
 * Execute an SQL select statement (but for now, just spit back the SQL)
 * @param stmt  Hyrise AST for the select statement
 * @returns     a string (for now) of the SQL statment
 */
string printSelect(const SelectStatement *stmt)
{
    string str;
    str += "SELECT ";

    bool first = false;
    for (Expr *expr : *stmt->selectList)
    {
        if (first)
        {
            str += ", ";
        }
        else
        {
            first = true;
        }
        str += printExpression(expr);
    }

    str += " FROM " + printTableRefInfo(stmt->fromTable);

    if (stmt->whereClause != NULL)
        str += " WHERE " + printExpression(stmt->whereClause);

    /* This function has bug
    if (stmt->unionSelect != NULL)
        str += "Union:";
        str += printSelect(stmt->unionSelect); */

    if (stmt->order != NULL)
    {
        std::cout << "ORDER BY ";
        str += printExpression(stmt->order->at(0)->expr);
        if (stmt->order->at(0)->type == kOrderAsc)
        {
            std::cout << "ASCENDING ";
        }
        else
        {
            std::cout << "DESCENDING ";
        }
    }
    return str;
}

/**
 * Convert the hyrise Expr AST back into the equivalent SQL
 * @param expr expression to unparse
 * @return     SQL equivalent to *expr
 */
string printExpression(const Expr *expr)
{
    string str;

    switch (expr->type)
    {
    case kExprStar:
        str += "*";
        break;
    case kExprColumnRef:
        if (expr->table != NULL)
            str += string(expr->table) + ".";
        str += expr->name;
        break;
    // case kExprTableColumnRef: inprint(expr->table, expr->name, numIndent); break;
    case kExprLiteralFloat:
        str += to_string(expr->fval);
        break;
    case kExprLiteralInt:
        str += to_string(expr->ival);
        break;
    case kExprLiteralString:
        str += string(expr->name);
        break;
    case kExprFunctionRef:
        str += string(expr->name);
        str += string(expr->expr->name);
        break;
    case kExprOperator:
        str += printOperatorExpression(expr);
        break;
    default:
        str += "Unrecognized expression type %d\n";
        return str;
    }

    if (expr->alias != NULL)
        str += string(" AS ") + expr->alias;
    return str;
}

/**
 * Convert the hyrise TableRef AST back into the equivalent SQL
 * @param table  table reference AST to unparse
 * @return       SQL equivalent to *table
 */
string printTableRefInfo(const TableRef *table)
{
    string str;
    switch (table->type)
    {
    case kTableName:
        str += table->name;
        break;
    case kTableSelect:
        str += printSelect(table->select);
        break;
    case kTableJoin:
        str += printTableRefInfo(table->join->left);

        switch (table->join->type)
        {
        case kJoinInner:
            str += " JOIN ";
            break;
        case kJoinOuter:
            str += " OUTER JOIN ";
            break;
        case kJoinLeft:
            str += " LEFT JOIN ";
            break;
        case kJoinRight:
            str += " RIGHT JOIN ";
            break;
        default:
            cout << "Not yet implemented" << endl;
            /*      kJoinLeftOuter,
                    kJoinRightOuter,
                    kJoinCross,
                    kJoinNatural*/
            break;
        }
        str += printTableRefInfo(table->join->right);
        if (table->join->condition != NULL)
        {
            str += " ON " + printExpression(table->join->condition);
        }
        break;
    case kTableCrossProduct:
        bool first = false;
        for (TableRef *tbl : *table->list)
        {
            if (first)
            {
                str += ", ";
            }
            else
            {
                first = true;
            }
            str += printTableRefInfo(tbl);
        }
        break;
    }
    if (table->alias != NULL)
    {
        str += " AS ";
        str += table->alias;
    }
    return str;
}

/**
 * Convert the hyrise Expr AST for an operator expression back into the equivalent SQL
 * @param expr operator expression to unparse
 * @return     SQL equivalent to *expr
 */
string printOperatorExpression(const Expr *expr)
{
    string str;
    if (expr == NULL)
        return "null";

    // Left-hand side of expression
    if (expr->expr != NULL)
        str += printExpression(expr->expr) + " ";

    switch (expr->opType)
    {
    case Expr::SIMPLE_OP:
        str += expr->opChar;
        break;
    case Expr::AND:
        str += "AND";
        break;
    case Expr::OR:
        str += "OR";
        break;
    case Expr::NOT:
        str += "NOT";
        break;
    default:
        str += expr->opType;
        break;
    }
    // Right-hand side of expression (only present for binary operators)
    if (expr->expr2 != NULL)
        str += " " + printExpression(expr->expr2);
    return str;
}
