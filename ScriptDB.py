# Details: Proc and data access method text generator for Dealspan referential data
# Author: Jon Fast
# Last Modified: 9/29/2014
import re
import pymssql

# common delimiters
commaDelim = ', '
commaNewLineDelim = ', \n'
slashDelim = '/'

# DealSpan proc names (imported from database)
tableNames = []
procNames = []

# ACAS specific table names
procPluralExceptions = [
        "TransactionsLastUpdated",
        "TransactionsRelated",
        "TransactionStatusHistory"
    ]

skipFields = ['username']
replacementFields = { 'username' : 'DealSpan.Security.GetUsername()'}

typeConverter = { 'nvarchar' : {'type' : 'string', 'default' : '\"\"' },
                   'varchar' : { 'type' : 'string', 'default' : '\"\"' },
                   'text' : { 'type' : 'string', 'default' : '\"\"' },
                   'int' : { 'type' : 'int', 'default' : '0' },
                   'decimal' : { 'type' : 'decimal', 'default' : '0.0' },
                   'bit' : { 'type' : 'bool', 'default' : 'false' },
                   'table type' : { 'type' : 'DataTable', 'default' : 'new DataTable()'},
                   'datetime' : {'type' : 'DateTime', 'default' : 'new DateTime()'},
                   'date' : {'type' : 'DateTime', 'default' : 'new DateTime()'},
                   'datetimeoffset' : {'type' : 'DateTimeOffset', 'default' : 'new DateTimeOffset()'}
                   }

def getXMLComment(title, does):
    title = re.sub(r"(\w)([A-Z])", r"\1 \2", title).lower()
    return '/// <summary>\n/// ' + does + 's ' + title + '\n/// </summary>' + ('\n/// <returns>' + title + '</returns>' if does.lower() == 'get' else '') + '\n'

def sqlParamToParamName(parameter):
    # remove @ symbol
    parameter = parameter[1:]
    # lower case the first character
    return parameter[0].lower() + parameter[1:]

def parseRouteName(route):
    # remove get or set
    if "get" in route.lower() or "set" in route.lower():
        return route[3:]
    return route

def sqlProcedureNameIsPlural(name, exceptions):
    if name in exceptions:
        return True
    return name.endswith('s') or name.lower().endswith('list') or name.lower().endswith('data') or name.lower().endswith('sdeleted')

def sqlSetToPutMethodName(procName):
    if procName[:3].lower() == "set":
        return "Put" + procName[3:]
    else:
        return procName

def setProcs(connection):
    cur = connection.cursor()
    queryString = "select SPECIFIC_NAME \n" \
                + "from INFORMATION_SCHEMA.Routines \n" \
                + "where ROUTINE_NAME LIKE 'Set%' and ROUTINE_TYPE = 'PROCEDURE'\n"
    cur.execute(queryString)
    for proc in cur:
        procNames.append(proc[0])
        
def getProcs(connection):
    cur = connection.cursor()
    queryString = "select SPECIFIC_NAME \n" \
                + "from INFORMATION_SCHEMA.Routines \n" \
                + "where ROUTINE_NAME LIKE 'Get%' and ROUTINE_TYPE = 'PROCEDURE'\n"
    cur.execute(queryString)
    for proc in cur:
        procNames.append(proc[0])

def getProc(connection, procName):
    cur = connection.cursor()
    queryString = "select SPECIFIC_NAME \n" \
                + "from INFORMATION_SCHEMA.Routines \n" \
                + "where ROUTINE_NAME = '" + procName + "' and ROUTINE_TYPE = 'PROCEDURE'\n"
    cur.execute(queryString)
    for proc in cur:
        procNames.append(proc[0])

def executeProc(connection, procName):
    cur = connection.cursor()
    queryString = "exec " + procName
    cur.execute(queryString)
    return cur.fetchall()

def getTables(connection):
    cur = connection.cursor()
    queryString = "select TABLE_NAME \n" \
                  + "from INFORMATION_SCHEMA.Tables \n" \
                  + "where TABLE_SCHEMA = 'dbo' and TABLE_TYPE = 'BASE TABLE'"
    cur.execute(queryString)
    for table in cur:
        tableNames.append(table[0])

def getTable(connection, tableName):
    cur = connection.cursor()
    queryString = "select TABLE_NAME \n" \
                  + "from INFORMATION_SCHEMA.Tables \n" \
                  + "where TABLE_SCHEMA = 'dbo' and TABLE_TYPE = 'BASE TABLE' and TABLE_NAME = '" + tableName + "'"
    cur.execute(queryString)
    for table in cur:
        tableNames.append(table[0])

def getTablesStartingWith(connection, sequence):
    cur = connection.cursor()
    queryString = "select TABLE_NAME \n" \
                  + "from INFORMATION_SCHEMA.Tables \n" \
                  + "where TABLE_SCHEMA = 'dbo' and TABLE_TYPE = 'BASE TABLE' and TABLE_NAME LIKE '" + sequence + "%'"
    cur.execute(queryString)
    for table in cur:
        tableNames.append(table[0])

def tableHasIdentity(tableName, connection):
    cur = connection.cursor()
    queryString = "select objectproperty(object_id('" + tableName + "'), 'TableHasIdentity')"
    cur.execute(queryString)
    for proc in cur:
        return proc[0] == 1
    return False

def getIdentityColumn(tableName, connection):
    cur = connection.cursor()
    queryString = "select COLUMN_NAME \n" \
                  + "from INFORMATION_SCHEMA.COLUMNS \n" \
                  + "where COLUMNPROPERTY(object_id(TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1 \n"\
                  + "and TABLE_SCHEMA = 'dbo'\n" \
                  + "and TABLE_NAME = '" + tableName + "'\n"
    cur.execute(queryString)
    for proc in cur:
        return proc[0]
    return ''

def compareTableWithGetProc(connection, tableName):
    proc = getProc(connection, 'Get' + tableName)
    table = getTable(connection, tableName)

def getTableColumns(connection, tableName):
    cur = connection.cursor()
    queryString = "select COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE \n" \
                  + "from INFORMATION_SCHEMA.COLUMNS \n" \
                  + "where TABLE_NAME = '" + tableName + "' \n" + "and TABLE_SCHEMA = 'dbo' \n"
    cur.execute(queryString)
    return cur.fetchall()

def getTableConstraints(connection, tableName, constraintType):
    cur = connection.cursor()
    queryString = "select kcu.TABLE_NAME, kcu.COLUMN_NAME, cts.CONSTRAINT_TYPE, c.DATA_TYPE, c.IS_NULLABLE \n" \
                  + "from INFORMATION_SCHEMA.TABLE_CONSTRAINTS cts, INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu \n" \
                  + "join INFORMATION_SCHEMA.COLUMNS c on c.COLUMN_NAME = kcu.COLUMN_NAME \n" \
                  + "and c.TABLE_NAME = kcu.TABLE_NAME \n" \
                  + "and c.TABLE_SCHEMA = kcu.TABLE_SCHEMA \n" \
                  + "where cts.TABLE_NAME = '" + tableName + "' \n" \
                  + "and cts.TABLE_NAME = kcu.TABLE_NAME \n" \
                  + "and cts.TABLE_SCHEMA = 'dbo' \n" \
                  + "and cts.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME \n" \
                  + "and c.COLUMN_NAME = kcu.COLUMN_NAME \n" \
                  + "and cts.CONSTRAINT_TYPE = '" + constraintType.upper() + "' \n"
    cur.execute(queryString)
    return cur.fetchall()

def createColumnDefinition(col, tableName, tableConstraints, connection):
    # get column name only, primary key can have null (allows insert)
    tableConstraints = map((lambda row : row[1]), tableConstraints)
    isNullColumn = ("NULL" if (col[2] == "YES" or col[0] in tableConstraints) else "NOT NULL")
    extendedTypeInformation = ''
    if col[1] == 'nvarchar' or col[1] == 'varchar':
        extendedTypeInformation += '(' + ('max' if str(col[3]) == '-1' else str(col[3])) + ')'
    elif col[1] == 'decimal' or col[1] == 'numeric':
        extendedTypeInformation += '(' + str(col[4]) + ', ' + str(col[5]) + ')'
    return col[0] + " " + col[1] + extendedTypeInformation.strip() + " " + isNullColumn

def createTableType(sqlStatementFile, tableName, tableCols, tableConstraints, connection):
    paramsString = ''
    paramDelimiter = ", \n"
    for col in tableCols:
        paramsString += "\t" + createColumnDefinition(col, tableName, tableConstraints, connection) + paramDelimiter
    paramsString = paramsString.rstrip(paramDelimiter)
    sqlStatementFile.write('\n-- Table type for: ' + tableName + '\n')
    sqlStatementFile.write("create type " + tableName + "Table" + " as table (\n")
    sqlStatementFile.write(paramsString)
    sqlStatementFile.write("\n)\ngo\n\n")

def createMergeStatementOnTableType(sqlStatementFile, tableName, tableCols, tableConstraints, connection):
    sourceTableAlias = 'source'
    sourceTableVariableName = '@' + sourceTableAlias[0].upper() + sourceTableAlias[1:] + "Table"
    tableTypeName = tableName + "Table"
    targetTableAlias = 'target'
    mergeString = ''
    mergeString += '\n-- Merge statement for: ' + tableName + '\n'
    mergeString += "create procedure Set" + tableName + "\n"
    mergeString += "\t" + sourceTableVariableName + " " + tableTypeName + " readonly\n" + "as\n" + "begin\n"
    """ if tableHasIdentity(tableName, connection):
        sqlStatementFile.write("\tset IDENTITY_INSERT " + tableName + " on;\n") """
    mergeString += "\tmerge " + tableName + " as " + targetTableAlias + '\n'
    mergeString += "\tusing " + sourceTableVariableName + " as " + sourceTableAlias + '\n'
    joinColumnString = ' '
    firstColumn = True
    identityColumnName = getIdentityColumn(tableName, connection)
    if len(tableConstraints) == 0:
        mergeString = '-- Merge statement could not be written for ' + tableName \
                      + ' as it\n-- contains no primary key columns for comparison in the \'on\' statement. \n'
        sqlStatementFile.write(mergeString)
        return
    else:
        for pkey in tableConstraints:
            if firstColumn == True:
                joinColumnString += "\t\ton "
                firstColumn = False
            else:
                joinColumnString += "\t\tand "
            joinColumnString += sourceTableAlias + "." + pkey[1] + " = " + targetTableAlias + "." + pkey[1] + '\n'
    mergeString += joinColumnString
    mergeString += "\twhen matched then update set\n"
    updateCols = ''
    for targetCol in tableCols:
        if targetCol[0] != identityColumnName:
            updateCols += "\t\t" + targetCol[0] + " = " + sourceTableAlias + "." + targetCol[0] + ", \n"
    updateCols = updateCols.rstrip(", \n")
    updateCols += '\n'
    mergeString += updateCols
    mergeString += "\twhen not matched by target then\n"
    insertCols = ''
    insertSourceCols = '';
    for targetCol in tableCols:
        if targetCol[0] != identityColumnName:
            insertCols += "\t\t\t" + targetCol[0] + ", \n"
            insertSourceCols += "\t\t\t" + sourceTableAlias + "." + targetCol[0] + ", \n"
    insertCols = insertCols.rstrip(", \n")
    insertSourceCols = insertSourceCols.rstrip(", \n")
    mergeString += "\t\tinsert \n\t\t(\n" + insertCols + "\n\t\t)\n\t\tvalues\n\t\t(\n" + insertSourceCols + "\n\t\t)\n"
    mergeString += "\twhen not matched by source then delete;\n"
    mergeString += 'end;\ngo\n'
    sqlStatementFile.write(mergeString)

# ACAS functionality only
def createDataSettersForTableType(dataAccessMethodsFile, controllerAccessMethodsFile, tableName, tableCols, tableConstraints, connection):
    camelCaseTableName = tableName[0].lower() + tableName[1:]
    dataAccessMethodsFile.write(getXMLComment(tableName, 'Set') +
              'internal static void Set' + tableName + '(DataTable ' + camelCaseTableName + 'Table)' + ' \n{\n'
              + '\tDataAccess.SetData(\"' + 'Set' + tableName + '\"'
              + ', new SqlParameter[] {\n'
              + '\t\t new SqlParameter(' + camelCaseTableName + 'Table)'
              + '\n\t}' + ')' + ';\n' + '}\n\n')
    controllerAccessMethodsFile.write(getXMLComment(tableName, 'Set')
              + '[Route(\"' + tableName + '\")]\n'
              + 'public PostResult Put' + tableName + '([FromBody] DataTable ' + camelCaseTableName + 'Table)\n'
              + '{\n'
              + '\tModels.Unknown.Set' + tableName + '(' + camelCaseTableName + 'Table)\n'
              + '\treturn new PostResult(PostResult.ResultType.Success);\n'
              + '}\n\n')

def createDataSetterDropStatements(sqlStatementFile, tableName):
    sqlStatementFile.write('\n\n-- Drops the table type and stored procedure for ' + tableName + '\n')
    sqlStatementFile.write('drop procedure Set' + tableName + ';\n')
    sqlStatementFile.write('drop type ' + tableName + ';\n\n')

def generateDataSettersFromTableDefinitions(controllerAccessMethodsFile, dataAccessMethodsFile, sqlStatementFile, connection):
    dataAccessMethodsFile.write('\n')
    controllerAccessMethodsFile.write('\n')
    sqlStatementFile.write('\n')
    for tableName in tableNames:
        tableCols = getTableColumns(connection, tableName)
        primaryKeys = getTableConstraints(connection, tableName, 'primary key')
        foreignKeys = getTableConstraints(connection, tableName, 'foreign key')
        createTableType(sqlStatementFile, tableName, tableCols, primaryKeys, connection)
        createMergeStatementOnTableType(sqlStatementFile, tableName, tableCols, primaryKeys, connection)
        createDataSetterDropStatements(sqlStatementFile, tableName)
        createDataSettersForTableType(dataAccessMethodsFile, controllerAccessMethodsFile, tableName, tableCols, primaryKeys, connection)

def insertParamNewLines(paramsString, count = 2, tabs = 4):
    newLinedParamString = ''
    if len(paramsString.split(commaDelim)) > 3:
        paramArray = paramsString.split(commaDelim)
        counter = 0
        for param in paramArray:
            newLinedParamString += ('\t' * tabs) + param + commaDelim
            counter += 1
            if counter % count == 0:
                newLinedParamString += '\n'
    return newLinedParamString.rstrip(commaDelim)

# ACAS functionality only
def generateDataAccessorsFromStoredProcedure(controllerAccessMethodsFile, dataAccessMethodsFile, action, connection):
    dataAccessMethodsFile.write('\n')
    controllerAccessMethodsFile.write('\n')
    for procName in procNames:
        cur = connection.cursor()
        queryString = "select PARAMETER_NAME, DATA_TYPE \n" \
                    + "from INFORMATION_SCHEMA.PARAMETERS \n" \
                    + "where SPECIFIC_SCHEMA = 'dbo' \n" \
                    + "and PARAMETER_MODE = 'IN' \n" \
                    + "and SPECIFIC_NAME = '" + procName + "'\n"
        cur.execute(queryString)
        paramsList = []
        paramsString = ''
        paramsStringSansTransaction = ''
        untypedParamsString = ''
        sqlParamsString = ''
        defaultValueParamsString = ''
        routeParamsString = '/'
        for sqlParameter in cur:
                paramsList.append((sqlParameter[0], typeConverter[sqlParameter[1]]["type"], sqlParamToParamName(sqlParameter[0])))
                if not sqlParamToParamName(sqlParameter[0]) in skipFields:
                    untypedParamsString += ('TransactionID' if sqlParamToParamName(sqlParameter[0]) == 'transactionID' else sqlParamToParamName(sqlParameter[0])) + ', '
                aliasedSqlParam = (replacementFields[sqlParamToParamName(sqlParameter[0])] if sqlParamToParamName(sqlParameter[0]) in replacementFields.keys() else sqlParamToParamName(sqlParameter[0]))
                sqlParamsString += '\t\tnew SqlParameter(\"' + sqlParameter[0] + '\", ' + aliasedSqlParam + '), \n'
                if not sqlParamToParamName(sqlParameter[0]) in skipFields:
                    paramsString += typeConverter[sqlParameter[1]]["type"] + ' ' + sqlParamToParamName(sqlParameter[0]) + ', '
                if not sqlParamToParamName(sqlParameter[0]) in skipFields:
                    paramsStringSansTransaction += typeConverter[sqlParameter[1]]["type"] + ' ' + sqlParamToParamName(sqlParameter[0]) + ', '
                    defaultValueParamsString += '\t' + typeConverter[sqlParameter[1]]["type"] + ' ' + sqlParamToParamName(sqlParameter[0]) + ' = ' + typeConverter[sqlParameter[1]]["default"] + ';\n'
                    routeParamsString += '{' + sqlParamToParamName(sqlParameter[0])
                    if sqlParamToParamName(sqlParameter[0])[-2:] == 'ID':
                        routeParamsString += ':id'
                    routeParamsString += '}/'

        paramsString = paramsString.rstrip(commaDelim)
        paramsStringSansTransaction = paramsStringSansTransaction.rstrip(commaDelim)
        untypedParamsString = untypedParamsString.rstrip(commaDelim)
        sqlParamsString = sqlParamsString.rstrip(commaNewLineDelim)
        routeParamsString = routeParamsString.rstrip(slashDelim)

        actionTemplate = ''
        if action.lower() == 'get':
            actionTemplate = 'DataTable' if sqlProcedureNameIsPlural(procName, procPluralExceptions) else 'JObject'
        elif action.lower() == 'set':
            actionTemplate = 'void'

        conversionTemplateBegin = ''
        conversionTemplateEnd = ''
        if action.lower() == 'get':
            conversionTemplateBegin = ('Utility.DataTableRowToObject(' if not sqlProcedureNameIsPlural(procName, procPluralExceptions) else '')
            conversionTemplateEnd = (')' if not sqlProcedureNameIsPlural(procName, procPluralExceptions) else '')

        actionMethodName = ''
        if action.lower() == 'get':
            actionMethodName = 'GetDataTable'
        elif action.lower() == 'set':
            actionMethodName = 'SetData'

        namespaceTemplate = ''
        if action.lower() == 'get':
            namespaceTemplate = 'Models.' + ('Transaction' if 'transaction' in procName.lower() else 'ReferentialData') + '.'
        else:
            namespaceTemplate = 'Models.Unknown.'

        paramsTemplate = (commaDelim + 'new SqlParameter[] {\n' if len(sqlParamsString) != 0 else '') + sqlParamsString + ('\n\t}' if len(sqlParamsString) != 0 else '')
        
        dataAccessMethodsFile.write(getXMLComment(procName, action) +
              'internal static ' + actionTemplate + ' ' + sqlSetToPutMethodName(procName) + '(' + paramsString + ')' + ' \n{\n'
              + '\t' + ('return ' if action.lower() == 'get' else '') + conversionTemplateBegin + 'DataAccess.' + actionMethodName + '(\"' + procName
              + '\"' + paramsTemplate + ')' + conversionTemplateEnd + ';\n' + '}\n\n')
        controllerAccessMethodsFile.write(getXMLComment(procName, 'Get')
              + '[Route(\"' + parseRouteName(procName) + routeParamsString + '\")]\n'
              + 'public ' + actionTemplate + ' ' + procName + '(' + paramsStringSansTransaction + ')\n'
              + '{\n'
              + '\treturn ' + namespaceTemplate + procName + '(' + untypedParamsString + ')' + ';\n'
              + '}\n\n')
def main():
    with open('../../controllerMethodsFile.cs', 'w') as controllerAccessMethodsFile:
        with open('../../dataAccessMethodsFile.cs', 'w') as dataAccessMethodsFile:
            with open('../../sqlStatementFile.sql', 'w') as sqlStatementFile:
                with pymssql.connect(host='tst25sqldbv04.test.lab.americancapital.com', database='DealSpanDEV') as connection:
                        setProcs(connection)
                        # getProcs(connection)
                        # getTablesStartingWith(connection, "Transaction")
                        generateDataAccessorsFromStoredProcedure(controllerAccessMethodsFile, dataAccessMethodsFile, 'set', connection)
                        # generateDataSettersFromTableDefinitions(controllerAccessMethodsFile, dataAccessMethodsFile, sqlStatementFile, connection)

main()
