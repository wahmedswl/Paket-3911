#If ManagedOdp Then
Imports Oracle.ManagedDataAccess.Client
#Else
Imports Oracle.DataAccess.Client
#End If

Imports System.Data
Imports SequelMed.Core
Imports SequelMed.Core.DB
Imports SequelMed.Core.Model
Imports System.Linq

Public Class CommonDataAccess

    Public Enum QueryType
        Insert = 0
        Update = 1
        Delete = 2
    End Enum

    Public UserIp As String

    Private mDbConnection As New OracleConnection
    Private mDbQueries As DatabaseQueries
    Private mId As String

    Public WriteOnly Property SequelSqlQueries() As DatabaseQueries
        Set(ByVal value As DatabaseQueries)
            mDbQueries = value
        End Set
    End Property

    Public Sub New(ByVal DbConnection As OracleConnection, ByVal Qureydb As DatabaseQueries, Optional ByVal Id As String = Nothing)
        mDbConnection = DbConnection
        mDbQueries = Qureydb
        [mId] = Id
    End Sub

    Public WriteOnly Property DbConnection() As OracleConnection
        Set(ByVal value As OracleConnection)
            mDbConnection = value
        End Set
    End Property

#Region "SimpleQueries-Insert/Update/Delete"
    Public Function doUpdate(ByVal nameArrayPairs() As NameArrayPair) As Object()
        Return doQuery(nameArrayPairs, QueryType.Update)
    End Function

    Public Function doDelete(ByVal nameArrayPairs() As NameArrayPair) As Object()
        Return doQuery(nameArrayPairs, QueryType.Delete)
    End Function

    Public Function doInsert(ByVal nameArrayPairs() As NameArrayPair) As Object()
        Return doQuery(nameArrayPairs, QueryType.Insert)
    End Function

    Public Function doUpdate(ByVal oracleTransaction As OracleTransaction, ByVal nameArrayPairs() As NameArrayPair) As Object()
        Return doQuery(oracleTransaction, nameArrayPairs, QueryType.Update)
    End Function

    Public Function doDelete(ByVal oracleTransaction As OracleTransaction, ByVal nameArrayPairs() As NameArrayPair) As Object()
        Return doQuery(oracleTransaction, nameArrayPairs, QueryType.Delete)
    End Function

    Public Function doInsert(ByVal oracleTransaction As OracleTransaction, ByVal nameArrayPairs() As NameArrayPair) As Object()
        Return doQuery(oracleTransaction, nameArrayPairs, QueryType.Insert)
    End Function

    Public Function ExecuteQuery(ByVal oracleTransaction As OracleTransaction, ByVal nameArrayPairs() As NameArrayPair, ByVal query_type As QueryType) As Object()
        Dim resultObjects(nameArrayPairs.Length) As Object
        Dim strQuery As String = Nothing
        Dim tagPair As NameArrayPair = Nothing
        Try
            If oracleTransaction Is Nothing Then
                Throw New ArgumentException([Error].REQUIRED_VALUE & "Transaction")
            End If

            Dim databaseQueries As DatabaseQueries = mDbQueries
            Dim i As Integer = 0
            Dim seqNums As New Generic.List(Of String)
            For Each naPair As NameArrayPair In nameArrayPairs
                Dim drView As DataRow = databaseQueries.Query(naPair.Key, transaction:=oracleTransaction)

                Dim paramCollection As OracleParameter() = Nothing
                Try
                    If query_type = QueryType.Insert Then
                        strQuery = CStr(drView("INSERT_QUERY"))
                        paramCollection = CreateParams(naPair.Params)
                    ElseIf query_type = QueryType.Delete Then
                        strQuery = CStr(drView("DELETE_QUERY"))

                        If Not SM.Cfg("DataAccess/Legacy").Bool() Then
                            paramCollection = CreateParamsforUpdate(naPair.Key, strQuery, naPair.Params, dbName:=DbEx.DataSource(oracleTransaction.Connection))
                        Else
                            paramCollection = CreateParams(strQuery, naPair.Params)
                        End If
                    ElseIf query_type = QueryType.Update Then
                        strQuery = CStr(drView("UPDATE_QUERY"))

                        If Not SM.Cfg("DataAccess/Legacy").Bool() Then
                            paramCollection = CreateParamsforUpdate(naPair.Key, strQuery, naPair.Params, dbName:=DbEx.DataSource(oracleTransaction.Connection))
                        Else
                            paramCollection = CreateParams(strQuery, naPair.Params)
                        End If
                    End If
                    tagPair = naPair
                    resultObjects(i) = OracleHelper.ExecuteNonQuery(oracleTransaction, CommandType.Text, strQuery, paramCollection)
                Finally
                    Disposer.Dispose(paramCollection)
                End Try
                If SequelSql.TrackUpdate() Then
                    SM.Sandbox(Sub()
                                   Dim filtered As DataRow() = drView.Table.Select("CACHE_FLAG='Y' and DML_TABLE='" & drView.Item("DML_TABLE").ToString & "'")
                                   If filtered.Length > 0 Then
                                       SyncLock SequelSql.Lock
                                           For Each row As DataRow In filtered
                                               row("LAST_UPDATE_DATE") = DateTime.Now
                                               seqNums.Add(row.Item("SEQ_NUM").ToString)
                                           Next
                                           databaseQueries.Data.AcceptChanges()
                                       End SyncLock
                                   End If
                               End Sub)
                End If
                i += 1
            Next
            ' Azam Khan
            OracleHelper.TagDmlUpdateDate(oracleTransaction, seqNums)

        Catch ex As Exception
            Logger.Instance(Constant.LG_DA).Ex(SM.GeneralError, ex, tag:=mId, data:=Function() SM.Join(tagPair.Dump(), SM.Fmt("Query", strQuery), SM.Fmt("Type", query_type.CStr()), nameArrayPairs.Dump(), SM.Fmt("Connection", oracleTransaction.Dump())))
            Throw
        End Try

        Return resultObjects
    End Function
    Private Function doQuery(ByVal oracleTransaction As OracleTransaction, ByVal nameArrayPairs() As NameArrayPair, ByVal query_type As QueryType) As Object()
        Return ExecuteQuery(oracleTransaction, nameArrayPairs, query_type)
    End Function
    Private Function doQuery(ByVal nameArrayPairs() As NameArrayPair, ByVal query_type As QueryType) As Object()
        Dim cn As OracleConnection = Nothing
        Dim oracleTransaction As OracleTransaction = Nothing
        Dim resultObjects As Object()

        Try
            If mDbConnection.NeedToConnect() Then
                Throw New ArgumentException([Error].REQUIRED_VALUE & "Connection")
            End If

            cn = mDbConnection
            oracleTransaction = cn.BeginTransaction()
            resultObjects = doQuery(oracleTransaction, nameArrayPairs, query_type)
            oracleTransaction.Commit()
        Catch ex As Exception
            Logger.Instance(Constant.LG_DA).Ex(SM.GeneralError, ex, tag:=mId, data:=Function() SM.Join(SM.Fmt("Type", query_type.CStr()), nameArrayPairs.Dump(), SM.Fmt("Connection", cn.Dump())))
            oracleTransaction.DoRollback()
            Throw
        End Try
        Return resultObjects

    End Function

#End Region

#Region "Async Large Data"
    Public Function doSelectLarge(ByVal alias1 As String, ByVal params() As Object, ByVal CHUNK_SIZE As Integer, ByRef dttemp As DataTable, ByRef Oreader As OracleDataReader, ByVal ExternalSqlWhere As Boolean, ByVal transaction As OracleTransaction) As DataTable
        Dim ds As New DataSet
        Dim i, j As Integer
        Dim dt As DataTable = dttemp.Clone
        Dim selectStatement As String = String.Empty
        Dim timer As System.Timers.Timer = Nothing
        Dim ocmd As OracleCommand = Nothing
        Dim paramCollection As OracleParameter() = Nothing

        Try
            If transaction Is Nothing Then
                Throw New ArgumentException([Error].REQUIRED_VALUE & "Transaction")
            End If

            If Oreader Is Nothing Then
                Dim databaseQueries As DatabaseQueries = mDbQueries
                Dim drView As DataRow = databaseQueries.Query(alias1, transaction:=transaction)
                Dim selectOnly As String = CStr(drView("SELECT_ONLY"))
                Dim whereOnly As String = ""

                If ExternalSqlWhere Then
                    If params IsNot Nothing Then
                        selectStatement = Nothing
                        For index As Integer = 0 To params.Length - 1
                            If selectStatement Is Nothing Then
                                selectStatement = selectOnly & " where " & CType(params(index), String)
                            Else
                                selectStatement = selectStatement & selectOnly & " where " & CType(params(index), String)
                            End If
                        Next
                    Else
                        selectStatement = selectOnly
                    End If
                    If selectStatement.Contains(" where ") Then
                        whereOnly = selectStatement.Substring(selectStatement.IndexOf(" where ") + 6)
                    End If
                    '==================================================
                Else
                    If Not TypeOf drView("SQL_WHERE") Is System.DBNull Then
                        whereOnly = drView("SQL_WHERE").ToString

                        If Not SM.Cfg("DataAccess/Legacy").Bool() Then
                            If whereOnly.ToUpper.IndexOf(" IN ") > -1 OrElse whereOnly.ToUpper.IndexOf(" IN(") > -1 Then
                                whereOnly = GetWhereWithIn(whereOnly, params)
                            Else
                                paramCollection = CreateParamsForSelect(alias1, whereOnly, params, dbName:=DbEx.DataSource(transaction.Connection))
                            End If
                        Else
                            paramCollection = CreateParams(whereOnly, params)
                        End If

                        If "".Equals(whereOnly.Trim()) OrElse params Is Nothing Then
                            selectStatement = selectOnly
                        Else
                            selectStatement = selectOnly & " where " & whereOnly
                        End If
                    Else
                        selectStatement = selectOnly
                    End If
                End If

                ocmd = New OracleCommand(selectStatement, transaction.Connection)

                If paramCollection IsNot Nothing Then
                    For Each op As OracleParameter In paramCollection
                        If op IsNot Nothing Then
                            ocmd.Parameters.Add(op)
                        End If
                    Next
                End If

                Try
                    If alias1.Trim.ToUpper = "RPT_PATIENTS_AR_BY_VISITS" Then
                        ocmd.BindByName = True
                    End If
                Catch ex As Exception
                End Try

                ocmd.CommandType = CommandType.Text
                Dim startDateTime As DateTime = DateTime.Now

                'Time Tracking
                Dim timeTaken As Double = 0
                Dim handler As Action(Of System.Timers.Timer) = Sub(x)
                                                                    timeTaken += x.Interval
                                                                    Logger.Instance(Constant.LG_DA_DL).Alert(DbEx.NotifyExhaustive(Database.DB_WAIT_INTERVAL, isDml:=False), tag:=mId, data:=Function() SM.Join(SM.Fmt("Query", selectStatement), Tag.Of(alias1, parameters:=params), SM.Fmt("Connection", transaction.Dump())), timeTaken:=timeTaken)
                                                                End Sub
                timer = TimerEx.Timer(Database.DB_WAIT_INTERVAL, handler)

                Oreader = ocmd.ExecuteReader()


                Dim endDateTime As DateTime = DateTime.Now
                OracleHelper.UpdateQueryTime(startDateTime, endDateTime, drView, transaction, mDbQueries, mId)
                'drView.Row("NO_OF_SELECTS") = ConvertToDecimal(drView.Row("NO_OF_SELECTS"))

                Oreader.FetchSize = ocmd.RowSize * CHUNK_SIZE
                Dim schematable As DataTable = Oreader.GetSchemaTable
                'Create Columns by taking Column name and type 
                Dim kount As Integer = 0
                For Each row As DataRow In schematable.Rows
                    Dim dc As New DataColumn
                    dc.ColumnName = CStr(row("Columnname"))
                    dc.DataType = CType(row("datatype"), Type)
                    Try
                        If Not (dc.DataType.FullName.StartsWith("System.Int") OrElse dc.DataType.FullName.StartsWith("System.Single") OrElse dc.DataType.FullName = "System.Decimal" OrElse dc.DataType.FullName = "System.DateTime" OrElse dc.DataType.FullName.StartsWith("System.Byte[]")) Then
                            dc.MaxLength = CInt(row("columnsize"))
                        End If
                    Catch ex As Exception
                        Trace.WriteLine(dc.DataType.FullName + "- Affected Datatypes", "Unspecified datatypes")
                    End Try

                    'Author: Armoghan. 
                    'Work around for Dotnet Bug for Changing time by Timezone during web service serialization
                    If dc.DataType.FullName = "System.DateTime" Then
                        dc.DateTimeMode = DataSetDateTime.Unspecified
                    End If
                    dt.Columns.Add(dc)
                Next

                Captioner.Caption(drView, dt)
                SetUniqueTableName(alias1, dt)
                AuditSelect(drView, transaction, whereOnly, paramCollection)
            End If

            i = 0
            While i < CHUNK_SIZE AndAlso (Oreader.Read())
                Dim dr As DataRow = dt.NewRow()
                For j = 0 To Oreader.FieldCount - 1
                    dr(Oreader.GetName(j)) = Oreader.GetValue(j)
                Next
                dt.Rows.Add(dr)
                i += 1
            End While
            Return dt
        Catch ex As Exception
            Logger.Instance(Constant.LG_DA).Ex(SM.GeneralError, ex, tag:=mId, data:=Function() SM.Join(Tag.Of(alias1, parameters:=params), ocmd.Dump(), SM.Fmt("Connection", transaction.Dump())))
            '@Inam 21/08/2013 As per discussion with KM , through Custom Exception here for Old ODP
            If ex.Message.ToLower.Contains("object reference not set") Then
                Throw New Exception("60010")
            End If

            Throw
        Finally
            TimerEx.Dispose(timer)
            Disposer.Dispose(ocmd, isolatedParams:=True)
            Disposer.Dispose(paramCollection)
        End Try
    End Function
#End Region

#Region "Update DataSet"

    Public Sub UpdateDataSet(ByVal transaction As OracleTransaction, ByVal aliases() As String, ByVal updateDS As DataSet, ByVal clientId As String)
        Dim databaseQueries As DatabaseQueries = mDbQueries 'CType(Application.Item(Constant.DB_QUERIES), DatabaseQueries)
        Try
            If transaction Is Nothing Then
                Throw New ArgumentException([Error].REQUIRED_VALUE & "Transaction")
            End If

            Dim tableId As Integer = 0
            For Each alias1 As String In aliases
                If alias1 IsNot Nothing AndAlso alias1 <> "" Then
                    Dim drView As DataRow = databaseQueries.Query(alias1, transaction:=transaction)
                    If clientId Is Nothing Then
                        OracleHelper.UpdateDataSet(transaction, updateDS.Tables(tableId), drView, "", UserIp)
                    Else
                        OracleHelper.UpdateDataSet(transaction, updateDS.Tables(tableId), drView, clientId, UserIp)
                    End If
                End If
                tableId += 1
            Next
        Catch ex As System.Data.DBConcurrencyException
            Logger.Instance(Constant.LG_DA).Ex(SM.GeneralError, ex, tag:=mId, client:=clientId, data:=Function() SM.Join(SM.Fmt("Tag(s)", SM.Join(aliases)), SM.Fmt("Connection", transaction.Dump())))
            Throw New Exception("60006")
        Catch ex As Exception
            Logger.Instance(Constant.LG_DA).Ex(SM.GeneralError, ex, tag:=mId, client:=clientId, data:=Function() SM.Join(SM.Fmt("Tag(s)", SM.Join(aliases)), SM.Fmt("Connection", transaction.Dump())))
            Throw
        End Try
    End Sub

    Public Function doUpdate(ByVal alias1 As String, ByVal ds As DataSet, ByVal clientId As String) As DataSet
        Return doUpdate(New String() {alias1}, ds, clientId)
    End Function

    Public Function doUpdate(ByVal aliases() As String, ByVal ds As DataSet, ByVal clientId As String) As DataSet
        ds.EnforceConstraints = False

        Dim cn As OracleConnection = Nothing
        Dim transaction As OracleTransaction = Nothing

        Try
            If mDbConnection.NeedToConnect() Then
                Throw New ArgumentException([Error].REQUIRED_VALUE & "Connection")
            End If

            cn = mDbConnection
            transaction = cn.BeginTransaction
            UpdateDataSet(transaction, aliases, ds, clientId)
            transaction.Commit()
        Catch ex As System.Data.DBConcurrencyException
            Logger.Instance(Constant.LG_DA).Ex(SM.GeneralError, ex, tag:=mId, client:=clientId, data:=Function() SM.Join(SM.Fmt("Tag(s)", SM.Join(aliases)), cn.Dump()))
            transaction.DoRollback()
            Throw New Exception("60006")
        Catch ex As Exception
            'Throw
            'Return Nothing
            Logger.Instance(Constant.LG_DA).Ex(SM.GeneralError, ex, tag:=mId, client:=clientId, data:=Function() SM.Join(SM.Fmt("Tag(s)", SM.Join(aliases)), cn.Dump()))
            transaction.DoRollback()

            Throw
            'ErrorMessage.Instance.ThrowError(ex, Me.Application)

        Finally
            Disposer.Dispose(transaction)
        End Try
        Return ds
    End Function
#End Region

#Region "Param Helpers"

    Private Sub removeNullParamFromQuery(ByRef query As String, ByVal i As Integer, ByVal totalCount As Integer)
        Dim parts As String() = query.Split("~"c)
        Dim j As Integer = -1
        Console.WriteLine("")
        Dim resultantQuery As String = ""
        Dim startInc As Boolean = False
        For q As Integer = 0 To parts.Length - 1
            If parts(q).IndexOf(":" & i & " ") = -1 Then
                If startInc Then
                    parts(q) = parts(q).Replace(":" & j & " ", ":" & (j - 1) & " ")
                    j += 1
                End If
                If Not parts(q).Trim() = "" Then
                    resultantQuery = resultantQuery & parts(q) & " ~"
                End If
            Else
                startInc = True
                j = i + 1
            End If
        Next
        query = resultantQuery
    End Sub

    Private Function CreateParams(ByVal whereParamSingle() As Object) As OracleParameter()
        Dim paramCollection(whereParamSingle.Length - 1) As OracleParameter
        Dim i As Integer = 0
        For Each p As Object In whereParamSingle
            Dim count As Integer = 0
            If p Is Nothing Then
                p = DBNull.Value
            End If
            paramCollection(i) = New OracleParameter("" & i, p)

            If TypeOf (p) Is String Then
                Dim strParam As String = CType(p, String)
                paramCollection(i).Size = strParam.Length
            End If
            i = i + 1
        Next
        Return paramCollection
    End Function

    Private Function CreateParams(ByRef whereClause As String, ByVal whereParamSingle() As Object) As OracleParameter()
        If whereParamSingle IsNot Nothing Then
            Dim paramCollection(whereParamSingle.Length - 1) As OracleParameter

            Dim i As Integer = 0
            Dim totalCount As Integer = whereParamSingle.Length

            For Each p As Object In whereParamSingle
                If p Is Nothing Then
                    removeNullParamFromQuery(whereClause, i, totalCount)
                    i = i - 1
                    totalCount = totalCount - 1
                Else
                    Dim count As Integer = 0
                    Dim strLength As Integer = -1
                    paramCollection(i) = New OracleParameter("" & i, p)

                    If p.GetType Is "".GetType Then
                        Dim strParam As String = CType(p, String)

                        ' NOTE: Commenting this line as No use of this line was identified
                        ' and it was causing problem for paramters like "Vital Signs" 
                        ' strParam = strParam.Replace(" ", "")

                        strLength = strParam.Length
                        Dim index As Integer = -1
                        Do
                            index += 1
                            index = strParam.IndexOf("%", index)
                            If index >= 0 Then count += 1
                        Loop Until index = -1
                        paramCollection(i).Size = strParam.Length
                    End If
                    paramCollection(i).Direction = ParameterDirection.InputOutput
                    If count = strLength Then
                        paramCollection(i) = Nothing
                        removeNullParamFromQuery(whereClause, i, totalCount)
                        i = i - 1
                        totalCount = totalCount - 1
                    End If
                End If
                i = i + 1
            Next

            ' START  
            ' Remove extra elements from query 
            ' Author Armoghan 
            ' NOTE: If whereclause has first param as Update or Delete it will not work 
            If Not (whereClause.ToUpper.StartsWith("Update".ToUpper) OrElse whereClause.ToUpper.StartsWith("Delete".ToUpper)) Then
                Dim notNullParams As Integer = 0
                Dim arr As New Generic.List(Of OracleParameter)
                For Each param1 As OracleParameter In paramCollection
                    If param1 IsNot Nothing Then
                        notNullParams += 1
                        arr.Add(param1)
                    End If
                Next
                Dim parts() As String = whereClause.Split("~"c)
                whereClause = ""
                For x As Integer = 0 To notNullParams - 1
                    whereClause += parts(x)
                Next


                paramCollection = arr.ToArray
            End If
            ' END 
            ' Remove extra elements from query 
            ' Author Armoghan 


            whereClause = whereClause.Replace("~", "")
            If whereClause.Trim().ToLower.StartsWith("and") Then
                whereClause = whereClause.Trim().Remove(0, 3)
            ElseIf whereClause.Trim().ToLower.StartsWith("or ") Then
                whereClause = whereClause.Trim().Remove(0, 2)
            ElseIf whereClause.Trim().ToLower.StartsWith(",") Then
                whereClause = whereClause.Trim().Remove(0, 1)
            End If

            ' Patch update
            'If first element is removed from update, remove the ","s
            If whereClause.ToUpper.StartsWith("Update".ToUpper) Then
                Dim firstPart As String = whereClause.Substring(0, whereClause.IndexOf(" SET ") + 5)
                Dim strSub As String = whereClause.Substring(whereClause.IndexOf(" SET ") + 5)
                If strSub.Trim.StartsWith(",") Then
                    strSub = strSub.Trim.Remove(0, 1)
                End If
                whereClause = firstPart + strSub

            End If
            Return paramCollection
        End If
        Return Nothing
    End Function

    Private Function CreateParamsForSelect(ByVal tagName As String, ByRef whereClause As String, ByVal whereParamSingle() As Object, Optional ByVal dbName As String = Nothing) As OracleParameter()
        If String.IsNullOrEmpty(whereClause) Then
            Return Nothing
        End If
        Dim allIdxs As New Generic.List(Of Integer)
        Dim parts1 As String() = whereClause.Split("~"c)
        Dim arylist As New Generic.List(Of OracleParameter)
        For intindex As Integer = 0 To parts1.Length - 1
            Dim part As String = parts1(intindex)
            Dim intTempIndex As Integer = -1
            If part.Contains(":") Then
                'ch :34 eck below is to identify the size of integer

                intTempIndex = getIndexfromPart(part)
                allIdxs.AddUnique(intTempIndex)
                If whereParamSingle Is Nothing OrElse whereParamSingle.Length <= intTempIndex OrElse whereParamSingle(intTempIndex) Is Nothing Then
                    parts1(intindex) = ""
                Else
                    Dim param As New OracleParameter(intTempIndex.ToString, whereParamSingle(intTempIndex))
                    param.Direction = ParameterDirection.InputOutput
                    If TypeOf (whereParamSingle(intTempIndex)) Is String Then
                        param.Size = whereParamSingle(intTempIndex).ToString.Length
                    End If
                    arylist.Add(param)
                End If
            End If
        Next

        SequelSql.CheckSqlWhere(tagName, whereParamSingle, allIdxs, dbName:=dbName)
        whereClause = String.Join("~", parts1)
        CleanQuery(whereClause)

        Return arylist.ToArray
    End Function

    Private Function getIndexfromPart(ByVal part As String) As Integer
        part = part.Substring(part.IndexOf(":") + 1).Trim.Trim(")"c)
        If part.Contains(" ") Then
            part = part.Remove(part.IndexOf(" "))
        End If
        Dim intTemp As Integer = -1
        Integer.TryParse(part, intTemp)
        Return intTemp
    End Function

    ' Remove extra elements from query 
    ' Author Armoghan 
    Private Sub CleanQuery(ByRef whereClause As String)
        whereClause = whereClause.Replace("~", "")
        If whereClause.Trim().ToLower.StartsWith("and") Then
            whereClause = whereClause.Trim().Remove(0, 3)
        ElseIf whereClause.Trim().ToLower.StartsWith("or ") Then
            whereClause = whereClause.Trim().Remove(0, 2)
        ElseIf whereClause.Trim().ToLower.StartsWith(",") Then
            whereClause = whereClause.Trim().Remove(0, 1)
        End If

    End Sub

    ''' <summary>
    ''' params for simple update command. (Action Query)
    ''' </summary>
    ''' <param name="whereClause"></param>
    ''' <param name="whereParamSingle"></param>
    ''' <returns></returns>
    ''' <remarks> Updated: Khalid   14/7/2007  </remarks>
    Private Function CreateParamsforUpdate(ByVal tagName As String, ByRef whereClause As String, ByVal whereParamSingle() As Object, Optional ByVal dbName As String = Nothing) As OracleParameter()
        If whereParamSingle IsNot Nothing Then
            For intcount As Integer = 0 To whereParamSingle.Length - 1
                If whereParamSingle(intcount) IsNot Nothing AndAlso whereParamSingle(intcount).ToString = "" Then
                    whereParamSingle(intcount) = DBNull.Value
                End If
            Next
        End If
        Dim paramArr As OracleParameter() = CreateParamsForSelect(tagName, whereClause, whereParamSingle, dbName:=dbName)

        Dim str As String = whereClause.Substring(whereClause.ToLower.IndexOf(" set ") + 4).TrimStart(" "c)
        If str.StartsWith(",") Then
            str = str.Substring(1)
            whereClause = whereClause.Remove(whereClause.ToLower.IndexOf(" set ") + 4) & str
        End If

        Return paramArr
    End Function
#End Region

#Region "Select"


    Public Function doSelect(ByVal alias1() As String) As DataSet

        Dim naPairs(alias1.Length - 1) As NameArrayPair
        Dim i As Integer = 0
        For Each key As String In alias1
            naPairs(i) = New NameArrayPair
            naPairs(i).Key = key
            i = i + 1
        Next
        Return doSelect(naPairs)
    End Function
    ''' <summary>
    ''' Sets the name of the table
    ''' </summary>
    ''' <param name="tagname"></param>
    ''' <param name="table"></param>
    ''' <remarks></remarks>
    Private Sub SetUniqueTableName(ByVal tagname As String, ByVal table As DataTable)
        Dim strtableName As String = tagname
        Dim intTableNameCounter As Integer = 1
        If table.DataSet Is Nothing Then
            table.TableName = tagname
            Exit Sub
        End If


        Do
            If table.DataSet.Tables(strtableName) Is Nothing Then
                table.TableName = strtableName
                Exit Do
            Else
                strtableName = tagname + "_" + intTableNameCounter.ToString
            End If
            intTableNameCounter += 1
        Loop
    End Sub

    Public Function doSelect(ByVal tagName As String, ByVal seqNumArray As String(), ByVal recursive As Boolean, ByVal transaction As OracleTransaction) As DataSet
        Dim cn As OracleConnection = Nothing
        Dim dsResult As DataSet = Nothing
        Dim databaseQueries As DatabaseQueries = mDbQueries 'CType(Application.Item(Constant.DB_QUERIES), DatabaseQueries)

        If seqNumArray.Empty() Then
            Return Nothing
        End If

        If tagName IsNot Nothing Then
            Try
                If transaction Is Nothing Then
                    Throw New ArgumentException([Error].REQUIRED_VALUE & "Transaction")
                End If
                cn = transaction.Connection

                Dim drView As DataRow = databaseQueries.Query(tagName, transaction:=transaction)
                Dim selectOnly As String = drView("SELECT_ONLY").ToString
                Dim captions As String() = Nothing
                If Not TypeOf drView("CAPTION") Is DBNull OrElse "".Equals(drView("CAPTION")) Then
                    captions = drView("CAPTION").ToString.Split(","c)
                End If

                Dim selectStatement As String
                Dim whereOnly As String = "SEQ_NUM IN (:SEQ_NUM)"
                selectStatement = selectOnly & " WHERE " & whereOnly

                Dim startDateTime As DateTime = DateTime.Now
                dsResult = transaction.Select(selectStatement, KVP:=SM.KV("SEQ_NUM", seqNumArray), table:=tagName, needSchema:=True)
                Dim endDateTime As DateTime = DateTime.Now

                If tagName.Contains("PQRI") = False Then
                    OracleHelper.UpdateQueryTime(startDateTime, endDateTime, drView, transaction, mDbQueries, mId)
                    'Azam
                    drView.Table.AcceptChanges()
                End If
                'drView.Row("NO_OF_SELECTS") = ConvertToDecimal(drView.Row("NO_OF_SELECTS"))

                Captioner.Caption(drView, dsResult.Tables(dsResult.Tables.Count - 1))
                AuditSelect(drView, transaction, whereOnly, Nothing)
            Catch ex As Exception
                Logger.Instance(Constant.LG_DA).Ex(SM.GeneralError, ex, tag:=mId, data:=Function() SM.Join(SM.Fmt("Tag", tagName), SM.Fmt("SeqNum", SM.Join(",", seqNumArray)), cn.Dump()))
                Throw
            End Try
        End If

        Return dsResult
    End Function

    Public Function doSelect(ByVal nameArrayPairs() As NameArrayPair, ByVal recursive As Boolean) As DataSet
        Dim cn As OracleConnection = Nothing
        Dim oracleTransation As OracleTransaction = Nothing

        Try
            If mDbConnection.NeedToConnect() Then
                Throw New ArgumentException([Error].REQUIRED_VALUE & "Connection")
            End If

            cn = mDbConnection
            oracleTransation = cn.BeginTransaction
            Dim ds As DataSet = doSelect(nameArrayPairs, recursive, oracleTransation)
            oracleTransation.Commit()
            Return ds
        Catch ex As Exception
            Logger.Instance(Constant.LG_DA).Ex(SM.GeneralError, ex, tag:=mId, data:=Function() SM.Join(nameArrayPairs.Dump(), SM.Fmt("Connection", cn.Dump())))
            oracleTransation.DoRollback()
            Throw
        End Try
        Return Nothing
    End Function

    Public Function doSelect(ByVal nameArrayPairs() As NameArrayPair, ByVal recursive As Boolean, ByVal transaction As OracleTransaction) As DataSet
        Dim dsResult As New DataSet
        Dim databaseQueries As DatabaseQueries = mDbQueries 'CType(Application.Item(Constant.DB_QUERIES), DatabaseQueries)
        Dim TagName As String = String.Empty
        Dim strQuery As String = String.Empty
        Dim tagPair As NameArrayPair = Nothing
        Try
            If nameArrayPairs IsNot Nothing Then
                If transaction Is Nothing Then
                    Throw New ArgumentException([Error].REQUIRED_VALUE & "Transaction")
                End If

                For Each naPair As NameArrayPair In nameArrayPairs
                    If naPair IsNot Nothing AndAlso naPair.Key IsNot Nothing Then
                        Dim drView As DataRow = databaseQueries.Query(naPair.Key, transaction:=transaction)
                        TagName = naPair.Key
                        Dim selectOnly As String = drView("SELECT_ONLY").ToString

                        Dim whereOnly As String = ""
                        Dim selectStatement As String
                        Dim paramCollection As OracleParameter() = Nothing
                        Try
                            If Not TypeOf drView("SQL_WHERE") Is System.DBNull Then
                                whereOnly = CStr(drView("SQL_WHERE"))

                                If Not SM.Cfg("DataAccess/Legacy").Bool() Then
                                    If whereOnly.ToUpper.IndexOf(" IN ") > -1 OrElse whereOnly.ToUpper.IndexOf(" IN(") > -1 Then
                                        whereOnly = GetWhereWithIn(whereOnly, naPair.Params)
                                    Else
                                        paramCollection = CreateParamsForSelect(TagName, whereOnly, naPair.Params, dbName:=DbEx.DataSource(transaction.Connection))
                                    End If
                                Else
                                    paramCollection = CreateParams(whereOnly, naPair.Params)
                                End If

                                If "".Equals(whereOnly.Trim()) OrElse naPair.Params Is Nothing Then
                                    selectStatement = selectOnly
                                Else
                                    selectStatement = selectOnly & " where " & whereOnly
                                End If
                            Else
                                selectStatement = selectOnly
                            End If
                            If naPair.StartIndex <> 0 OrElse naPair.EndIndex <> 0 Then
                                Dim dlength As Integer
                                If paramCollection IsNot Nothing Then
                                    dlength = paramCollection.Length
                                End If
                                ReDim Preserve paramCollection(dlength + 1)
                                paramCollection(dlength) = New OracleParameter(":ends", naPair.EndIndex)
                                paramCollection(dlength + 1) = New OracleParameter(":starts", naPair.StartIndex)
                            End If
                            Dim startDateTime As DateTime = DateTime.Now

                            tagPair = naPair
                            '@Inam for Logging Purpose
                            strQuery = GetQueryTextForPaging(selectStatement, naPair.StartIndex, naPair.EndIndex, naPair.OrderBy)

                            dsResult = OracleHelper.ExecuteDataset(dsResult, transaction, transaction.Connection, CommandType.Text, strQuery, recursive, TagName, paramCollection)

                            Dim endDateTime As DateTime = DateTime.Now
                            If naPair.Key.Contains("PQRI") = False Then
                                OracleHelper.UpdateQueryTime(startDateTime, endDateTime, drView, transaction, mDbQueries, mId)
                                drView.Table.GetChanges()
                            End If
                            'drView.Row("NO_OF_SELECTS") = ConvertToDecimal(drView.Row("NO_OF_SELECTS"))

                            If naPair.NeedRowCount Then
                                Dim dc As New DataColumn("TOTAL_ROWS_COUNT", GetType(Decimal))
                                dc.ColumnMapping = MappingType.Hidden
                                dc.AllowDBNull = False
                                If naPair.StartIndex <> 0 OrElse naPair.EndIndex <> 0 Then
                                    paramCollection(paramCollection.Length - 1) = Nothing
                                    paramCollection(paramCollection.Length - 2) = Nothing
                                End If

                                dc.DefaultValue = OracleHelper.ExecuteScalar(transaction, CommandType.Text, RowCountQuery(selectStatement), GetParamsCopy(paramCollection))
                                dsResult.Tables(dsResult.Tables.Count - 1).Columns.Add(dc)
                                dsResult.AcceptChanges()
                            End If

                            Captioner.Caption(drView, dsResult.Tables(dsResult.Tables.Count - 1))

                            If drView("CACHE_FLAG").ToString = "Y" Then
                                dsResult.Tables(dsResult.Tables.Count - 1).Prefix = "YCACHE"
                            ElseIf drView("CACHE_FLAG").ToString = "L" Then
                                dsResult.Tables(dsResult.Tables.Count - 1).Prefix = "LCACHE"
                            End If

                            SetUniqueTableName(naPair.Key, dsResult.Tables(dsResult.Tables.Count - 1))
                            AuditSelect(drView, transaction, whereOnly, paramCollection, naPair.StartIndex, naPair.EndIndex, naPair.OrderBy, dsResult.Tables(dsResult.Tables.Count - 1).Rows.Count)
                        Finally
                            Disposer.Dispose(paramCollection)
                        End Try
                    Else
                        If dsResult Is Nothing Then
                            dsResult = New DataSet
                        End If
                        dsResult.Tables.Add()
                    End If
                Next
            End If
            Return dsResult
        Catch exp As Exception
            Logger.Instance(Constant.LG_DA).Ex(SM.GeneralError, exp, tag:=mId, data:=Function() SM.Join(SM.Fmt("Tag", TagName), tagPair.Dump(), SM.Fmt("Query", strQuery), nameArrayPairs.Dump(), SM.Fmt("Connection", transaction.Dump())))
            Throw
        End Try
    End Function

    Private Sub AuditSelect(ByVal drView As DataRow, ByVal transaction As OracleTransaction, ByVal whereOnly As String, ByVal paramCollection() As OracleParameter, Optional ByVal startIndex As Object = Nothing, Optional ByVal EndIndex As Object = Nothing, Optional ByVal orderby As Object = Nothing, Optional ByVal RowsCount As Object = Nothing)
        If drView("AUDIT_OPTIONS").ToString().IndexOf("S") <> -1 Then
            Dim params(7) As OracleParameter

            Try
                Dim rowSeqNum As Long = CLng(OracleHelper.ExecuteScalar(transaction, CommandType.Text, "select CLINICAL_AUDIT_SEQ.NEXTVAL from DUAL"))
                params(0) = New OracleParameter("0", rowSeqNum)


                Dim resultantWhere As String = whereOnly
                If paramCollection IsNot Nothing Then
                    For i As Integer = paramCollection.Length - 1 To 0 Step -1
                        If paramCollection(i) IsNot Nothing Then
                            If TypeOf (paramCollection(i).Value) Is String Then
                                resultantWhere = resultantWhere.Replace(":" & paramCollection(i).ParameterName, "'" + paramCollection(i).Value.ToString + "'")
                            Else
                                resultantWhere = resultantWhere.Replace(":" & paramCollection(i).ParameterName, paramCollection(i).Value.ToString)
                            End If
                        End If
                    Next
                End If
                params(1) = New OracleParameter("1", resultantWhere)
                params(2) = New OracleParameter("2", SM.DbVal(startIndex))
                params(3) = New OracleParameter("3", SM.DbVal(EndIndex))
                params(4) = New OracleParameter("4", SM.DbVal(orderby))
                params(5) = New OracleParameter("5", SM.DbVal(RowsCount))
                params(6) = New OracleParameter("6", UserIp.DbVal())
                params(7) = New OracleParameter("7", drView("TAG"))
                For Each parm As OracleParameter In params
                    If TypeOf (parm.Value) Is String Then
                        Dim strParam As String = CType(parm.Value, String)
                        parm.Size = strParam.Length
                    End If
                Next
                Dim strQuery As String = "INSERT INTO SEQUEL1.AUDIT_TABLE_SELECT ( SEQ_NUM, WHERE_TEXT, START_INDEX, END_INDEX, ORDER_BY,ROWS_SELECTED,MACHINE,TAG) VALUES ( " +
                ":0 , :1 , :2 , :3 , :4,:5,:6,:7)"

                Dim onb As Object = OracleHelper.ExecuteScalar(transaction, CommandType.Text, strQuery, params)
            Finally
                Disposer.Dispose(params)
            End Try
        End If
    End Sub

    Public Function GetWhereWithIn(ByRef StrWhere As String, ByVal whereParam() As Object) As String

        Dim parts As String() = StrWhere.Split("~"c)
        Dim finalWhere As String = ""
        If whereParam IsNot Nothing Then
            For i As Integer = 0 To whereParam.Length - 1
                If whereParam(i) IsNot Nothing Then
                    finalWhere += CStr(parts(i)).Replace(":" + i.ToString, whereParam(i).ToString)
                End If
            Next
        End If

        If finalWhere IsNot Nothing Then
            finalWhere = finalWhere.Replace("~", "")
            If finalWhere.Trim().ToLower.StartsWith("and") Then
                finalWhere = finalWhere.Trim().Remove(0, 3)
            ElseIf finalWhere.Trim().ToLower.StartsWith("or") Then
                finalWhere = finalWhere.Trim().Remove(0, 2)
            ElseIf finalWhere.Trim().ToLower.StartsWith(",") Then
                finalWhere = finalWhere.Trim().Remove(0, 1)
            End If
        End If
        Return finalWhere
    End Function

    Public Function doSelect(ByVal nameArrayPairs() As NameArrayPair) As DataSet
        Return doSelect(nameArrayPairs, False)
    End Function

    Public Function doSelectQuery(ByVal nameArrayPair As NameArrayPair) As DataSet
        Dim cn As OracleConnection = Nothing
        Dim oracleTransation As OracleTransaction = Nothing
        Dim parameters As OracleParameter() = Nothing
        Try
            If mDbConnection.NeedToConnect() Then
                Throw New ArgumentException([Error].REQUIRED_VALUE & "Connection")
            End If

            cn = mDbConnection
            oracleTransation = cn.BeginTransaction()
            parameters = CreateParams(nameArrayPair.Params)
            Dim ds As DataSet = OracleHelper.ExecuteDataset(oracleTransation, CommandType.Text, nameArrayPair.Key, parameters)
            oracleTransation.Commit()
            Return ds
        Catch ex As Exception
            Logger.Instance(Constant.LG_DA).Ex(SM.GeneralError, ex, tag:=mId, data:=Function() SM.Join(nameArrayPair.Dump(), SM.Fmt("Connection", cn.Dump())))
            oracleTransation.DoRollback()
            Throw
        Finally
            Disposer.Dispose(parameters)
            Disposer.Dispose(oracleTransation)
        End Try
        Return Nothing
    End Function

    Public Function ExecuteScalar(ByVal Query As String) As Object
        Dim cn As OracleConnection = Nothing

        Try
            If mDbConnection.NeedToConnect() Then
                Throw New ArgumentException([Error].REQUIRED_VALUE & "Connection")
            End If
            cn = mDbConnection

            Return OracleHelper.ExecuteScalar(cn, CommandType.Text, Query)
        Catch ex As Exception
            Logger.Instance(Constant.LG_DA).Ex(SM.GeneralError, ex, tag:=mId, data:=Function() SM.Join(SM.Fmt("Query", Query), SM.Fmt("Connection", cn.Dump())))
            Throw
        End Try
        Return Nothing
    End Function

    Public Function ExecuteNonQuery(ByVal Query As String) As Integer
        Dim cn As OracleConnection = Nothing

        Try
            If mDbConnection.NeedToConnect() Then
                Throw New ArgumentException([Error].REQUIRED_VALUE & "Connection")
            End If

            cn = mDbConnection
            Return OracleHelper.ExecuteNonQuery(cn, CommandType.Text, Query)
        Catch ex As Exception
            Logger.Instance(Constant.LG_DA).Ex(SM.GeneralError, ex, tag:=mId, data:=Function() SM.Join(SM.Fmt("Query", Query), SM.Fmt("Connection", cn.Dump())))
            Throw
        End Try
    End Function

    Public Function doSelect(ByVal alias1 As String) As DataSet
        Return doSelect(alias1, New Object() {Nothing})
    End Function

    Public Function doSelect(ByVal alias1 As String, ByVal whereParam() As Object) As DataSet
        Dim naPairs(0) As NameArrayPair
        naPairs(0) = New NameArrayPair
        naPairs(0).Key = alias1
        naPairs(0).Params = whereParam

        Return doSelect(naPairs, False)
    End Function

    Public Function doSelect(ByVal alias1 As String, ByVal whereParam() As Object, ByVal start As Integer, ByVal ends As Integer, ByVal GetRowCount As Boolean) As DataSet
        Dim naPairs(0) As NameArrayPair
        naPairs(0) = New NameArrayPair
        naPairs(0).Key = alias1
        naPairs(0).StartIndex = start
        naPairs(0).EndIndex = ends
        naPairs(0).NeedRowCount = GetRowCount
        naPairs(0).Params = whereParam
        Return doSelect(naPairs, False)
    End Function

    Public Function doSelect(ByVal alias1 As String, ByVal whereParam() As Object, ByVal transaction As OracleTransaction) As DataSet
        Dim naPairs(0) As NameArrayPair
        naPairs(0) = New NameArrayPair
        naPairs(0).Key = alias1
        naPairs(0).Params = whereParam
        Return doSelect(naPairs, False, transaction)
    End Function
#End Region

#Region "Procedure Call"
    Public Function ExecuteProcedure(ByVal nameArrayPairs() As NameArrayPair) As Object()
        Dim conn As OracleConnection = Nothing
        Dim oracleTS As OracleTransaction = Nothing

        Try
            If mDbConnection.NeedToConnect() Then
                Throw New ArgumentException([Error].REQUIRED_VALUE & "Connection")
            End If

            conn = mDbConnection
            oracleTS = conn.BeginTransaction()

            Dim resultArray As Object() = ExecuteProcedure(oracleTS, nameArrayPairs)
            oracleTS.Commit()
            Return resultArray
        Catch ex As Exception
            Logger.Instance(Constant.LG_DA).Ex(SM.GeneralError, ex, tag:=mId, data:=Function() SM.Join(nameArrayPairs.Dump(), SM.Fmt("Connection", conn.Dump())))
            oracleTS.DoRollback()
            Throw
        Finally
            Disposer.Dispose(oracleTS)
        End Try
        Return Nothing
    End Function

    Public Function ExecuteProcedure(ByVal oracleTS As OracleTransaction, ByVal nameArrayPairs() As NameArrayPair) As Object()
        Dim databaseQueries As DatabaseQueries = mDbQueries
        Dim resultArray(nameArrayPairs.Length - 1) As Object
        Dim tagPair As NameArrayPair = Nothing
        Try
            If oracleTS Is Nothing Then
                Throw New ArgumentException([Error].REQUIRED_VALUE & "Transaction")
            End If

            Dim resultCounter As Integer = 0
            For Each nameAP As NameArrayPair In nameArrayPairs
                If nameAP IsNot Nothing AndAlso nameAP.Key IsNot Nothing Then
                    Dim drView As DataRow = databaseQueries.Query(nameAP.Key, transaction:=oracleTS)
                    Dim strProcName As String = drView("PROCEDURE_NAME").ToString
                    Dim strParam As String = drView("PROCEDURE_PARAMS_NAMES").ToString
                    Dim params As OracleParameter() = Nothing
                    Try
                        If nameAP.Params IsNot Nothing Then
                            params = CreateParams(nameAP.Params)
                            Dim parts As String() = strParam.Split("~"c)
                            Dim i As Integer = 0
                            For Each param As OracleParameter In params
                                param.ParameterName = parts(i)
                                i += 1
                            Next
                        End If

                        tagPair = nameAP
                        resultArray(resultCounter) = OracleHelper.ExecuteScalar(oracleTS, CommandType.StoredProcedure, strProcName, params)
                    Finally
                        Disposer.Dispose(params)
                    End Try
                End If
                resultCounter += 1
            Next
        Catch ex As Exception
            Logger.Instance(Constant.LG_DA).Ex(SM.GeneralError, ex, tag:=mId, data:=Function() SM.Join(tagPair.Dump(), nameArrayPairs.Dump(), SM.Fmt("Connection", oracleTS.Dump())))
            Throw
        End Try

        Return resultArray
    End Function
#End Region

#Region "For paging"
    Private Function GetQueryTextForPaging(ByVal strq As String, ByVal start As Integer, ByVal ends As Integer, ByVal orderby As String) As String
        Dim strOrderby As String = " order by " & orderby
        If String.IsNullOrEmpty(orderby) Then
            strOrderby = ""
        End If
        If start = 0 AndAlso ends = 0 Then
            Return strq + strOrderby
        End If

        If SM.ProductName.Eq(Product.PatientPortal.CStr()) OrElse SM.ProductName.Eq(Product.SupportPortal.CStr()) OrElse SM.Cfg("DataAccess/Paging/Selective").Bool() Then
            strq &= strOrderby

            'strq = strq.ToUpper
            'Dim newquery As String = "select *  FROM ( " + strq.Replace(" FROM ", ",ROWNUM ROWNO FROM ")
            'If strq.Contains(" WHERE ") Then
            '    newquery += " AND ROWNUM<=:ends " & strOrderby & ") WHERE ROWNO >=:starts"
            'Else
            '    newquery += " WHERE ROWNUM<=" + ends.ToString + strOrderby & ") WHERE ROWNO >=" + start.ToString
            'End If
            'CHANGE FOR ORDER BY DESC 
            If strq.ToUpper.Contains(" WHERE ") Then
                strq = String.Format("SELECT * FROM (SELECT TAB_CHUNK.*, ROWNUM RN FROM ({0}) TAB_CHUNK) WHERE RN <= {1} AND RN >= {2}", strq, ":ends", ":starts")
            Else
                strq = String.Format("SELECT * FROM (SELECT TAB_CHUNK.*, ROWNUM RN FROM ({0}) TAB_CHUNK) WHERE RN <= {1} AND RN >= {2}", strq, ends, start)
            End If

            Return strq
        End If

        strq = strq.ToUpper
        Dim newquery As String = "select *  FROM ( " + strq.Replace(" FROM ", ",ROWNUM ROWNO FROM ")
        If strq.Contains(" WHERE ") Then
            newquery += " AND ROWNUM<=:ends " & strOrderby & ") WHERE ROWNO >=:starts"
        Else
            newquery += " WHERE ROWNUM<=" & ends.ToString & strOrderby & ") WHERE ROWNO >=" & start.ToString
        End If

        Return newquery
    End Function

    Private Function RowCountQuery(ByVal strQ As String) As String
        strQ = strQ.ToUpper
        Return "select nvl(Count(*),0)TOTAL_ROWS  " + strQ.Substring(strQ.IndexOf(" FROM "))
    End Function

    Private Function GetParamsCopy(ByVal params As OracleParameter()) As OracleParameter()
        Dim gl As New Generic.List(Of OracleParameter)
        If params IsNot Nothing Then
            For i As Integer = 0 To params.Length - 1
                Dim orp As OracleParameter = params(i)
                If orp IsNot Nothing Then
                    gl.Add(New OracleParameter(orp.ParameterName, orp.Value))
                End If
            Next
        End If
        Return gl.ToArray
    End Function
#End Region

End Class
