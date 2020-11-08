#If ManagedOdp Then
Imports Oracle.ManagedDataAccess.Client
#Else
Imports Oracle.DataAccess.Client
#End If

Imports System.Collections
Imports System.Configuration
Imports System.Collections.Generic
Imports System.Data
Imports SequelMed.Core.DB
Imports SequelMed.Core

Public NotInheritable Class OracleHelper
    Private Sub New()
    End Sub

    Private Shared Sub AttachParameters(ByVal command As OracleCommand, ByVal commandParameters As OracleParameter())
        For Each p As OracleParameter In commandParameters
            If p IsNot Nothing Then
                If p.Direction = ParameterDirection.InputOutput Then
                    Sanitize.Param(p)
                End If
                command.Parameters.Add(p)
            End If
        Next
    End Sub

    Private Shared Sub AssignParameterValues(ByVal commandParameters As OracleParameter(), ByVal parameterValues As Object())
        If (commandParameters Is Nothing) OrElse (parameterValues Is Nothing) Then
            Return
        End If
        If Not (commandParameters.Length = parameterValues.Length) Then
            Throw New ArgumentException("Parameter count does not match Parameter Value count.")
        End If
        Dim i As Integer = 0
        Dim j As Integer = commandParameters.Length
        While i < j
            commandParameters(i).Value = parameterValues(i)
            System.Math.Min(System.Threading.Interlocked.Increment(i), i - 1)
        End While
    End Sub

    Private Shared Sub PrepareCommand(ByVal command As OracleCommand, ByVal connection As OracleConnection, ByVal commandType As CommandType, ByVal commandText As String, ByVal commandParameters As OracleParameter())
        If Not (connection.State = ConnectionState.Open) Then
            connection.Open()
        End If
        command.Connection = connection
        command.CommandText = commandText
        command.FetchSize *= Database.FETCH_SIZE
        command.BindByName = SM.ProductName.Eq(Model.Product.SupportPortal.CStr()) OrElse SM.Cfg("DataAccess/BindByName").Bool()
        command.CommandType = commandType
        If commandParameters IsNot Nothing Then
            AttachParameters(command, commandParameters)
        End If
        Return
    End Sub

    Public Shared Function ExecuteNonQuery(ByVal connectionString As String, ByVal commandType As CommandType, ByVal commandText As String) As Integer
        Return ExecuteNonQuery(connectionString, commandType, commandText, CType(Nothing, OracleParameter()))
    End Function

    Public Shared Function ExecuteNonQuery(ByVal connectionString As String, ByVal commandType As CommandType, ByVal commandText As String, ByVal ParamArray commandParameters As OracleParameter()) As Integer
        ' Using 
        Dim cn As OracleConnection = New OracleConnection(connectionString)
        Try
            cn.Open()
            Return ExecuteNonQuery(cn, commandType, commandText, commandParameters)
        Finally
            Disposer.Dispose(cn)
        End Try
    End Function

    Public Shared Function ExecuteNonQuery(ByVal connectionString As String, ByVal spName As String, ByVal ParamArray parameterValues As Object()) As Integer
        If parameterValues IsNot Nothing AndAlso parameterValues.Length > 0 Then
            Dim commandParameters As OracleParameter() = OracleHelperParameterCache.GetSpParameterSet(connectionString, spName)
            AssignParameterValues(commandParameters, parameterValues)
            Return ExecuteNonQuery(connectionString, CommandType.StoredProcedure, spName, commandParameters)
        Else
            Return ExecuteNonQuery(connectionString, CommandType.StoredProcedure, spName)
        End If
    End Function

    Public Shared Function ExecuteNonQuery(ByVal connection As OracleConnection, ByVal commandType As CommandType, ByVal commandText As String) As Integer
        Return ExecuteNonQuery(connection, commandType, commandText, CType(Nothing, OracleParameter()))
    End Function

    Public Shared Function ExecuteNonQuery(ByVal connection As OracleConnection, ByVal commandType As CommandType, ByVal commandText As String, ByVal ParamArray commandParameters As OracleParameter()) As Integer
        Dim cmd As OracleCommand = New OracleCommand
        Dim timer As System.Timers.Timer = Nothing
        Dim tmp As Model.DbServer = connection.RetrieveDbSvr()
        Try
            If connection.NeedToConnect() Then
                Throw New ArgumentException([Error].REQUIRED_VALUE & "Connection")
            End If

            PrepareCommand(cmd, connection, commandType, commandText, commandParameters)

            'Time Tracking
            Dim timeTaken As Double = 0
            Dim handler As Action(Of System.Timers.Timer) = Sub(x)
                                                                timeTaken += x.Interval
                                                                Logger.Instance(Constant.LG_DA_DL).Alert(DbEx.NotifyExhaustive(Database.DB_WAIT_INTERVAL, isDml:=True), tag:=tmp.Id, data:=Function() SM.Join(cmd.Dump(), SM.Fmt("Connection", connection.Dump())), timeTaken:=timeTaken)
                                                            End Sub
            timer = TimerEx.Timer(Database.DB_WAIT_INTERVAL, handler)

            Return cmd.ExecuteNonQuery
        Catch ex As Exception
            Logger.Instance(Constant.LG_DA).Ex(SM.GeneralError, ex, tag:=tmp.Id, data:=Function() SM.Join(cmd.Dump(), SM.Fmt("Connection", connection.Dump())))
            If ex.Message.IndexOf("ORA-12500") > -1 Then
                Database.ClearPools()
            End If
            Throw
        Finally
            TimerEx.Dispose(timer)
            Disposer.Dispose(cmd)
        End Try

    End Function

    Public Shared Function ExecuteNonQuery(ByVal connection As OracleConnection, ByVal spName As String, ByVal ParamArray parameterValues As Object()) As Integer
        If connection Is Nothing Then
            Throw New ArgumentException([Error].REQUIRED_VALUE & "Connection")
        End If

        If parameterValues IsNot Nothing AndAlso parameterValues.Length > 0 Then
            Dim commandParameters As OracleParameter() = OracleHelperParameterCache.GetSpParameterSet(connection.ConnectionString, spName)
            AssignParameterValues(commandParameters, parameterValues)
            Return ExecuteNonQuery(connection, CommandType.StoredProcedure, spName, commandParameters)
        Else
            Return ExecuteNonQuery(connection, CommandType.StoredProcedure, spName)
        End If
    End Function

    Public Shared Function ExecuteNonQuery(ByVal transaction As OracleTransaction, ByVal commandType As CommandType, ByVal commandText As String) As Integer
        Return ExecuteNonQuery(transaction, commandType, commandText, CType(Nothing, OracleParameter()))
    End Function

    Public Shared Function ExecuteNonQuery(ByVal transaction As OracleTransaction, ByVal commandType As CommandType, ByVal commandText As String, ByVal ParamArray commandParameters As OracleParameter()) As Integer
        Dim cmd As OracleCommand = New OracleCommand
        Dim timer As System.Timers.Timer = Nothing
        Dim tmp As Model.DbServer = transaction.RetrieveDbSvr()
        Try
            If transaction Is Nothing Then
                Throw New ArgumentException([Error].REQUIRED_VALUE & "Transaction")
            End If

            PrepareCommand(cmd, transaction.Connection, commandType, commandText, commandParameters)

            'Time Tracking
            Dim timeTaken As Double = 0
            Dim handler As Action(Of System.Timers.Timer) = Sub(x)
                                                                timeTaken += x.Interval
                                                                Logger.Instance(Constant.LG_DA_DL).Alert(DbEx.NotifyExhaustive(Database.DB_WAIT_INTERVAL, isDml:=True), tag:=tmp.Id, data:=Function() SM.Join(cmd.Dump(), SM.Fmt("Connection", transaction.Dump())), timeTaken:=timeTaken)
                                                            End Sub
            timer = TimerEx.Timer(Database.DB_WAIT_INTERVAL, handler)

            Return cmd.ExecuteNonQuery
        Catch ex As Exception
            Logger.Instance(Constant.LG_DA).Ex(SM.GeneralError, ex, tag:=tmp.Id, data:=Function() SM.Join(cmd.Dump(), SM.Fmt("Connection", transaction.Dump())))
            If ex.Message.IndexOf("ORA-12500") > -1 Then
                Database.ClearPools()
            End If
            Throw
        Finally
            TimerEx.Dispose(timer)
            Disposer.Dispose(cmd)
        End Try
    End Function

    Public Shared Function ExecuteNonQuery(ByVal transaction As OracleTransaction, ByVal spName As String, ByVal ParamArray parameterValues As Object()) As Integer
        If transaction Is Nothing Then
            Throw New ArgumentException([Error].REQUIRED_VALUE & "Transaction")
        End If

        If parameterValues IsNot Nothing AndAlso parameterValues.Length > 0 Then
            Dim commandParameters As OracleParameter() = OracleHelperParameterCache.GetSpParameterSet(transaction.Connection.ConnectionString, spName)
            AssignParameterValues(commandParameters, parameterValues)
            Return ExecuteNonQuery(transaction, CommandType.StoredProcedure, spName, commandParameters)
        Else
            Return ExecuteNonQuery(transaction, CommandType.StoredProcedure, spName)
        End If
    End Function

    Private Enum OracleConnectionOwnership
        Internal = 0
        External = 1
    End Enum

    Private Shared Function ExecuteReader(ByVal connection As OracleConnection, ByVal commandType As CommandType, ByVal commandText As String, ByVal commandParameters As OracleParameter(), ByVal ownership As OracleConnectionOwnership) As OracleDataReader
        Dim cmd As OracleCommand = New OracleCommand
        Dim timer As System.Timers.Timer = Nothing
        Dim tmp As Model.DbServer = connection.RetrieveDbSvr()
        Try
            If connection.NeedToConnect() Then
                Throw New ArgumentException([Error].REQUIRED_VALUE & "Connection")
            End If

            PrepareCommand(cmd, connection, commandType, commandText, commandParameters)

            'Time Tracking
            Dim timeTaken As Double = 0
            Dim handler As Action(Of System.Timers.Timer) = Sub(x)
                                                                timeTaken += x.Interval
                                                                Logger.Instance(Constant.LG_DA_DL).Alert(DbEx.NotifyExhaustive(Database.DB_WAIT_INTERVAL, isDml:=False), tag:=tmp.Id, data:=Function() SM.Join(cmd.Dump(), SM.Fmt("Connection", connection.Dump())), timeTaken:=timeTaken)
                                                            End Sub
            timer = TimerEx.Timer(Database.DB_WAIT_INTERVAL, handler)

            Dim dr As OracleDataReader
            If ownership = OracleConnectionOwnership.External Then
                dr = cmd.ExecuteReader
            Else
                dr = cmd.ExecuteReader(CType(CType(CommandBehavior.CloseConnection, Integer), CommandBehavior))
            End If
            Return CType(dr, OracleDataReader)
        Catch ex As Exception
            Logger.Instance(Constant.LG_DA).Ex(SM.GeneralError, ex, tag:=tmp.Id, data:=Function() SM.Join(cmd.Dump(), SM.Fmt("Connection", connection.Dump())))
            If ex.Message.IndexOf("ORA-12500") > -1 Then
                Database.ClearPools()
            End If
            '@Inam 21/08/2013 As per discussion with KM , through Custom Exception here for Old ODP
            If ex.Message.ToLower.Contains("object reference not set") Then
                Throw New Exception("Please try again")
            End If
            Disposer.Dispose(cmd)
            Throw
        Finally
            TimerEx.Dispose(timer)
        End Try
    End Function

    Public Shared Function ExecuteReader(ByVal connectionString As String, ByVal commandType As CommandType, ByVal commandText As String) As OracleDataReader
        Return CType(ExecuteReader(connectionString, commandType, commandText, CType(Nothing, OracleParameter())), OracleDataReader)
    End Function

    Public Shared Function ExecuteReader(ByVal connectionString As String, ByVal spName As String, ByVal ParamArray parameterValues As Object()) As OracleDataReader
        If parameterValues IsNot Nothing AndAlso parameterValues.Length > 0 Then
            Dim commandParameters As OracleParameter() = OracleHelperParameterCache.GetSpParameterSet(connectionString, spName)
            AssignParameterValues(commandParameters, parameterValues)
            Return CType(ExecuteReader(connectionString, CommandType.StoredProcedure, spName, commandParameters), OracleDataReader)
        Else
            Return ExecuteReader(connectionString, CommandType.StoredProcedure, spName)
        End If
    End Function

    Public Shared Function ExecuteReader(ByVal connection As OracleConnection, ByVal commandType As CommandType, ByVal commandText As String) As OracleDataReader
        Return ExecuteReader(connection, commandType, commandText, CType(Nothing, OracleParameter()))
    End Function

    Public Shared Function ExecuteReader(ByVal connection As OracleConnection, ByVal commandType As CommandType, ByVal commandText As String, ByVal ParamArray commandParameters As OracleParameter()) As OracleDataReader
        Return ExecuteReader(connection, commandType, commandText, commandParameters, OracleConnectionOwnership.External)
    End Function

    Public Shared Function ExecuteReader(ByVal transaction As OracleTransaction, ByVal spName As String, ByVal ParamArray parameterValues As Object()) As OracleDataReader
        If transaction Is Nothing Then
            Throw New ArgumentException([Error].REQUIRED_VALUE & "Transaction")
        End If

        If parameterValues IsNot Nothing AndAlso parameterValues.Length > 0 Then
            Dim commandParameters As OracleParameter() = OracleHelperParameterCache.GetSpParameterSet(transaction.Connection.ConnectionString, spName)
            AssignParameterValues(commandParameters, parameterValues)
            Return ExecuteReader(transaction, CommandType.StoredProcedure, spName, commandParameters)
        Else
            Return ExecuteReader(transaction, CommandType.StoredProcedure, spName)
        End If
    End Function

    Public Shared Function ExecuteReader(ByVal connectionString As String, ByVal commandType As CommandType, ByVal commandText As String, ByVal ParamArray commandParameters As OracleParameter()) As OracleDataReader
        'create & open an OraclebConnection
        Dim cn As OracleConnection = New OracleConnection(connectionString)
        Try
            cn.Open()
            'call the private overload that takes an internally owned connection in place of the connection string
            '(ByVal connection As OracleConnection, ByVal transaction As OracleTransaction, ByVal commandType As CommandType, ByVal commandText As String, ByVal commandParameters As OracleParameter(), ByVal enum1 As OracleConnectionOwnership)
            Return ExecuteReader(cn, commandType, commandText, commandParameters, OracleConnectionOwnership.Internal)
        Catch
            'if we fail to return the OracleDataReader, we need to close the connection ourselves
            Disposer.Dispose(cn)
            Throw
        End Try
    End Function

    Public Shared Function ExecuteReader(ByVal connection As OracleConnection, ByVal spName As String, ByVal ParamArray parameterValues As Object()) As OracleDataReader
        If connection Is Nothing Then
            Throw New ArgumentException([Error].REQUIRED_VALUE & "Connection")
        End If

        If parameterValues IsNot Nothing AndAlso parameterValues.Length > 0 Then
            Dim commandParameters As OracleParameter() = OracleHelperParameterCache.GetSpParameterSet(connection.ConnectionString, spName)
            AssignParameterValues(commandParameters, parameterValues)
            Return ExecuteReader(connection, CommandType.StoredProcedure, spName, commandParameters)
        Else
            Return ExecuteReader(connection, CommandType.StoredProcedure, spName)
        End If
    End Function

    Public Shared Function ExecuteReader(ByVal transaction As OracleTransaction, ByVal commandType As CommandType, ByVal commandText As String, ByVal ParamArray commandParameters As OracleParameter()) As OracleDataReader
        Return ExecuteReader(transaction.Connection, commandType, commandText, commandParameters, OracleConnectionOwnership.External)
    End Function

    Public Shared Function ExecuteReader(ByVal transaction As OracleTransaction, ByVal commandType As CommandType, ByVal commandText As String) As OracleDataReader
        Return ExecuteReader(transaction, commandType, commandText, CType(Nothing, OracleParameter()))
    End Function


#Region "Execute DataSet"

    Public Shared Function ExecuteDataset(ByVal connectionString As String, ByVal commandType As CommandType, ByVal commandText As String) As DataSet
        Return ExecuteDataset(connectionString, commandType, commandText, False, CType(Nothing, OracleParameter()))
    End Function

    Public Shared Function ExecuteDataset(ByVal connectionString As String, ByVal commandType As CommandType, ByVal commandText As String, ByVal recursive As Boolean, ByVal ParamArray commandParameters As OracleParameter()) As DataSet
        ' Using for Stored Proc call
        Dim cn As OracleConnection = New OracleConnection(connectionString)
        Try
            cn.Open()
            Return ExecuteDataset(cn, commandType, commandText, recursive, commandParameters)
        Finally
            Disposer.Dispose(cn)
        End Try
    End Function

    Public Shared Function ExecuteDataset(ByVal connectionString As String, ByVal spName As String, ByVal ParamArray parameterValues As Object()) As DataSet
        If parameterValues IsNot Nothing AndAlso parameterValues.Length > 0 Then
            Dim commandParameters As OracleParameter() = OracleHelperParameterCache.GetSpParameterSet(connectionString, spName)
            AssignParameterValues(commandParameters, parameterValues)
            Return ExecuteDataset(connectionString, CommandType.StoredProcedure, spName, False, commandParameters)
        Else
            Return ExecuteDataset(connectionString, CommandType.StoredProcedure, spName)
        End If
    End Function

    Public Shared Function ExecuteDataset(ByVal ds As DataSet, ByVal connection As OracleConnection, ByVal commandType As CommandType, ByVal commandText As String, ByVal recursive As Boolean) As DataSet
        Return ExecuteDataset(ds, connection, commandType, commandText, recursive, CType(Nothing, OracleParameter()))
    End Function

    Public Shared Function ExecuteDataset(ByVal connection As OracleConnection, ByVal commandType As CommandType, ByVal commandText As String, ByVal recursive As Boolean) As DataSet
        Return ExecuteDataset(CType(Nothing, DataSet), connection, commandType, commandText, recursive, CType(Nothing, OracleParameter()))
    End Function

    Public Shared Function ExecuteDataset(ByVal connection As OracleConnection, ByVal commandType As CommandType, ByVal commandText As String, ByVal recursive As Boolean, ByVal ParamArray commandParameters As OracleParameter()) As DataSet
        Return ExecuteDataset(CType(Nothing, DataSet), connection, commandType, commandText, recursive, commandParameters)
    End Function

    Public Shared Function ExecuteDataset(ByVal ds As DataSet, ByVal transaction As OracleTransaction, ByVal connection As OracleConnection, ByVal commandType As CommandType, ByVal commandText As String, ByVal recursive As Boolean, ByVal ParamArray commandParameters As OracleParameter()) As DataSet
        Return ExecuteDataset(ds, transaction, connection, commandType, commandText, recursive, "", commandParameters)
    End Function

    Public Shared Function ExecuteDataset(ByVal ds As DataSet, ByVal transaction As OracleTransaction, ByVal connection As OracleConnection, ByVal commandType As CommandType, ByVal commandText As String, ByVal recursive As Boolean, ByVal TagName As String, ByVal ParamArray commandParameters As OracleParameter()) As DataSet
        Dim cmd As OracleCommand = New OracleCommand
        Dim timer As System.Timers.Timer = Nothing
        Dim da As OracleDataAdapter = Nothing
        Dim tmp As Model.DbServer = connection.RetrieveDbSvr()
        Try
            If connection Is Nothing AndAlso transaction IsNot Nothing Then
                connection = transaction.Connection
            End If

            If connection.NeedToConnect() Then
                Throw New ArgumentException([Error].REQUIRED_VALUE & "Connection")
            End If

            If Not String.IsNullOrEmpty(TagName) Then
                If TagName.ToUpper() = "EOB_PAYMENT" OrElse TagName.ToUpper = "CHARGE_LISTING" OrElse TagName.ToUpper = "BATCH_EOB_SERVICE_LINE" Then
                    cmd.BindByName = True
                End If
            End If
            PrepareCommand(cmd, connection, commandType, commandText, commandParameters)

            da = New OracleDataAdapter(cmd)

            If Not recursive Then
                da.MissingSchemaAction = MissingSchemaAction.AddWithKey
            End If
            If ds Is Nothing Then
                ds = New DataSet
            End If
            ds.EnforceConstraints = False
            Dim dataTable1 As DataTable = ds.Tables.Add()

            'Time Tracking
            Dim timeTaken As Double = 0
            Dim handler As Action(Of System.Timers.Timer) = Sub(x)
                                                                timeTaken += x.Interval
                                                                Logger.Instance(Constant.LG_DA_DL).Alert(DbEx.NotifyExhaustive(Database.DB_WAIT_INTERVAL, isDml:=False), tag:=tmp.Id, data:=Function() SM.Join(cmd.Dump(), SM.Fmt("Connection", connection.Dump())), timeTaken:=timeTaken)
                                                            End Sub
            timer = TimerEx.Timer(Database.DB_WAIT_INTERVAL, handler)

            da.Fill(dataTable1)

            Return ds.Normalize()
        Catch ex As Exception
            Logger.Instance(Constant.LG_DA).Ex(SM.GeneralError, ex, tag:=tmp.Id, data:=Function() SM.Join(cmd.Dump(), SM.Fmt("Connection", connection.Dump())))
            If ex.Message.IndexOf("ORA-12500") > -1 Then
                Database.ClearPools()
            End If
            '@Inam 21/08/2013 As per discussion with KM , through Custom Exception here for Old ODP
            If ex.Message.ToLower.Contains("object reference not set") Then
                Throw New Exception("Please try again")
            End If

            Throw
        Finally
            TimerEx.Dispose(timer)
            Disposer.Dispose(da)
            Disposer.Dispose(cmd)
        End Try
    End Function

    Public Shared Function ExecuteDataset(ByVal ds As DataSet, ByVal connection As OracleConnection, ByVal commandType As CommandType, ByVal commandText As String, ByVal recursive As Boolean, ByVal ParamArray commandParameters As OracleParameter()) As DataSet
        Return ExecuteDataset(ds, CType(Nothing, OracleTransaction), connection, commandType, commandText, recursive, "", commandParameters)
    End Function

    Public Shared Function ExecuteDataset(ByVal connection As OracleConnection, ByVal spName As String, ByVal ParamArray parameterValues As Object()) As DataSet
        If connection Is Nothing Then
            Throw New ArgumentException([Error].REQUIRED_VALUE & "Connection")
        End If

        If parameterValues IsNot Nothing AndAlso parameterValues.Length > 0 Then
            Dim commandParameters As OracleParameter() = OracleHelperParameterCache.GetSpParameterSet(connection.ConnectionString, spName)
            AssignParameterValues(commandParameters, parameterValues)
            Return ExecuteDataset(connection, CommandType.StoredProcedure, spName, False, commandParameters)
        Else
            Return ExecuteDataset(connection, CommandType.StoredProcedure, spName, False)
        End If
    End Function

    Public Shared Function ExecuteDataset(ByVal transaction As OracleTransaction, ByVal commandType As CommandType, ByVal commandText As String, ByVal recursive As Boolean) As DataSet
        Return ExecuteDataset(transaction, commandType, commandText, CType(Nothing, OracleParameter()))
    End Function

    Public Shared Function ExecuteDataset(ByVal transaction As OracleTransaction, ByVal commandType As CommandType, ByVal commandText As String, ByVal ParamArray commandParameters As OracleParameter()) As DataSet
        Dim cmd As OracleCommand = New OracleCommand
        Dim timer As System.Timers.Timer = Nothing
        Dim da As OracleDataAdapter = Nothing
        Dim tmp As Model.DbServer = transaction.RetrieveDbSvr()
        Try
            If transaction Is Nothing Then
                Throw New ArgumentException([Error].REQUIRED_VALUE & "Transaction")
            End If

            PrepareCommand(cmd, transaction.Connection, commandType, commandText, commandParameters)
            da = New OracleDataAdapter(cmd)
            Dim ds As DataSet = New DataSet

            'Time Tracking
            Dim timeTaken As Double = 0
            Dim handler As Action(Of System.Timers.Timer) = Sub(x)
                                                                timeTaken += x.Interval
                                                                Logger.Instance(Constant.LG_DA_DL).Alert(DbEx.NotifyExhaustive(Database.DB_WAIT_INTERVAL, isDml:=False), tag:=tmp.Id, data:=Function() SM.Join(cmd.Dump(), SM.Fmt("Connection", transaction.Dump())), timeTaken:=timeTaken)
                                                            End Sub
            timer = TimerEx.Timer(Database.DB_WAIT_INTERVAL, handler)

            da.Fill(ds)

            Return ds

        Catch ex As Exception
            Logger.Instance(Constant.LG_DA).Ex(SM.GeneralError, ex, tmp.Id, data:=Function() SM.Join(cmd.Dump(), SM.Fmt("Connection", transaction.Dump())))
            If ex.Message.IndexOf("ORA-12500") > -1 Then
                Database.ClearPools()
            End If
            '@Inam 21/08/2013 As per discussion with KM , through Custom Exception here for Old ODP
            If ex.Message.ToLower.Contains("object reference not set") Then
                Throw New Exception("Please try again")
            End If

            Throw
        Finally
            TimerEx.Dispose(timer)
            Disposer.Dispose(da)
            Disposer.Dispose(cmd)
        End Try
    End Function

    Public Shared Function ExecuteDataset(ByVal transaction As OracleTransaction, ByVal spName As String, ByVal ParamArray parameterValues As Object()) As DataSet
        If transaction Is Nothing Then
            Throw New ArgumentException([Error].REQUIRED_VALUE & "Transaction")
        End If

        If parameterValues IsNot Nothing AndAlso parameterValues.Length > 0 Then
            Dim commandParameters As OracleParameter() = OracleHelperParameterCache.GetSpParameterSet(transaction.Connection.ConnectionString, spName)
            AssignParameterValues(commandParameters, parameterValues)
            Return ExecuteDataset(transaction, CommandType.StoredProcedure, spName, commandParameters)
        Else
            Return ExecuteDataset(transaction, CommandType.StoredProcedure, spName)
        End If
    End Function
#End Region

    Public Shared Function ExecuteScalar(ByVal connectionString As String, ByVal commandType As CommandType, ByVal commandText As String) As Object
        Return ExecuteScalar(connectionString, commandType, commandText, CType(Nothing, OracleParameter()))
    End Function

    Public Shared Function ExecuteScalar(ByVal connectionString As String, ByVal commandType As CommandType, ByVal commandText As String, ByVal ParamArray commandParameters As OracleParameter()) As Object
        ' Using 
        Dim cn As OracleConnection = New OracleConnection(connectionString)
        Try
            cn.Open()
            Return ExecuteScalar(cn, commandType, commandText, commandParameters)
        Finally
            Disposer.Dispose(cn)
        End Try
    End Function

    Public Shared Function ExecuteScalar(ByVal connectionString As String, ByVal spName As String, ByVal ParamArray parameterValues As Object()) As Object
        If parameterValues IsNot Nothing AndAlso parameterValues.Length > 0 Then
            Dim commandParameters As OracleParameter() = OracleHelperParameterCache.GetSpParameterSet(connectionString, spName)
            AssignParameterValues(commandParameters, parameterValues)
            Return ExecuteScalar(connectionString, CommandType.StoredProcedure, spName, commandParameters)
        Else
            Return ExecuteScalar(connectionString, CommandType.StoredProcedure, spName)
        End If
    End Function

    Public Shared Function ExecuteScalar(ByVal connection As OracleConnection, ByVal commandType As CommandType, ByVal commandText As String) As Object
        Return ExecuteScalar(connection, commandType, commandText, CType(Nothing, OracleParameter()))
    End Function

    Public Shared Function ExecuteScalar(ByVal connection As OracleConnection, ByVal commandType As CommandType, ByVal commandText As String, ByVal ParamArray commandParameters As OracleParameter()) As Object
        Dim cmd As OracleCommand = New OracleCommand
        Dim timer As System.Timers.Timer = Nothing
        Dim tmp As Model.DbServer = connection.RetrieveDbSvr()
        Try
            If connection.NeedToConnect() Then
                Throw New ArgumentException([Error].REQUIRED_VALUE & "Connection")
            End If

            PrepareCommand(cmd, connection, commandType, commandText, commandParameters)

            'Time Tracking
            Dim timeTaken As Double = 0
            Dim handler As Action(Of System.Timers.Timer) = Sub(x)
                                                                timeTaken += x.Interval
                                                                Logger.Instance(Constant.LG_DA_DL).Alert(DbEx.NotifyExhaustive(Database.DB_WAIT_INTERVAL, isDml:=False), tag:=tmp.Id, data:=Function() SM.Join(cmd.Dump(), SM.Fmt("Connection", connection.Dump())), timeTaken:=timeTaken)
                                                            End Sub
            timer = TimerEx.Timer(Database.DB_WAIT_INTERVAL, handler)

            Return cmd.ExecuteScalar
        Catch ex As Exception
            Logger.Instance(Constant.LG_DA).Ex(SM.GeneralError, ex, tag:=tmp.Id, data:=Function() SM.Join(cmd.Dump(), SM.Fmt("Connection", connection.Dump())))
            If ex.Message.IndexOf("ORA-12500") > -1 Then
                Database.ClearPools()
            End If
            Throw
        Finally
            TimerEx.Dispose(timer)
            Disposer.Dispose(cmd)
        End Try

    End Function

    Public Shared Function ExecuteScalar(ByVal connection As OracleConnection, ByVal spName As String, ByVal ParamArray parameterValues As Object()) As Object
        If connection Is Nothing Then
            Throw New ArgumentException([Error].REQUIRED_VALUE & "Connection")
        End If

        If parameterValues IsNot Nothing AndAlso parameterValues.Length > 0 Then
            Dim commandParameters As OracleParameter() = OracleHelperParameterCache.GetSpParameterSet(connection.ConnectionString, spName)
            AssignParameterValues(commandParameters, parameterValues)
            Return ExecuteScalar(connection, CommandType.StoredProcedure, spName, commandParameters)
        Else
            Return ExecuteScalar(connection, CommandType.StoredProcedure, spName)
        End If
    End Function

    Public Shared Function ExecuteScalar(ByVal transaction As OracleTransaction, ByVal commandType As CommandType, ByVal commandText As String) As Object
        Return ExecuteScalar(transaction, commandType, commandText, CType(Nothing, OracleParameter()))
    End Function

    Public Shared Function ExecuteScalar(ByVal transaction As OracleTransaction, ByVal commandType As CommandType, ByVal commandText As String, ByVal ParamArray commandParameters As OracleParameter()) As Object
        Dim cmd As OracleCommand = New OracleCommand
        Dim timer As System.Timers.Timer = Nothing
        Dim tmp As Model.DbServer = transaction.RetrieveDbSvr()
        Try
            If transaction Is Nothing Then
                Throw New ArgumentException([Error].REQUIRED_VALUE & "Transaction")
            End If

            PrepareCommand(cmd, transaction.Connection, commandType, commandText, commandParameters)

            'Time Tracking
            Dim timeTaken As Double = 0
            Dim handler As Action(Of System.Timers.Timer) = Sub(x)
                                                                timeTaken += x.Interval
                                                                Logger.Instance(Constant.LG_DA_DL).Alert(DbEx.NotifyExhaustive(Database.DB_WAIT_INTERVAL, isDml:=False), tag:=tmp.Id, data:=Function() SM.Join(cmd.Dump(), SM.Fmt("Connection", transaction.Dump())), timeTaken:=timeTaken)
                                                            End Sub
            timer = TimerEx.Timer(Database.DB_WAIT_INTERVAL, handler)

            Return cmd.ExecuteScalar
        Catch ex As Exception
            Logger.Instance(Constant.LG_DA).Ex(SM.GeneralError, ex, tag:=tmp.Id, data:=Function() SM.Join(cmd.Dump(), SM.Fmt("Connection", transaction.Dump())))
            If ex.Message.IndexOf("ORA-12500") > -1 Then
                Database.ClearPools()
            End If
            Throw
        Finally
            TimerEx.Dispose(timer)
            Disposer.Dispose(cmd)
        End Try

    End Function

    Public Shared Function ExecuteScalar(ByVal transaction As OracleTransaction, ByVal spName As String, ByVal ParamArray parameterValues As Object()) As Object
        If transaction Is Nothing Then
            Throw New ArgumentException([Error].REQUIRED_VALUE & "Transaction")
        End If

        If parameterValues IsNot Nothing AndAlso parameterValues.Length > 0 Then
            Dim commandParameters As OracleParameter() = OracleHelperParameterCache.GetSpParameterSet(transaction.Connection.ConnectionString, spName)
            AssignParameterValues(commandParameters, parameterValues)
            Return ExecuteScalar(transaction, CommandType.StoredProcedure, spName, commandParameters)
        Else
            Return ExecuteScalar(transaction, CommandType.StoredProcedure, spName)
        End If
    End Function

    'Functions made by Armoghan 
    Public Enum QueryType
        Update = 1
        Insert = 2
        Delete = 3
    End Enum

    Private Shared Function CreateCaptionsHashTable(ByVal strCaption As String) As Generic.Dictionary(Of String, String)
        Dim captions As String() = Nothing
        Dim ht As New Generic.Dictionary(Of String, String)

        If "".Equals(strCaption) Then
            Return ht
        End If
        captions = strCaption.Split(","c)
        For Each str As String In captions
            Dim nameCaption As String() = str.Split("|"c)
            If nameCaption.Length = 2 AndAlso nameCaption(1).Trim <> "" Then
                ht.Add(nameCaption(0).Trim, nameCaption(1).Trim)
            End If
        Next
        Return ht
    End Function

    Private Shared Sub AuditRow(ByVal transaction As OracleTransaction, ByVal drView As DataRow, ByVal row As DataRow, ByVal type As String, ByVal userip As String)
        Dim htCaptions As Generic.Dictionary(Of String, String) = CreateCaptionsHashTable(drView("AUDIT_COLUMNS").ToString)
        If htCaptions.Count = 0 Then
            Exit Sub
        End If

        Dim htInfo As Generic.Dictionary(Of String, String) = CreateCaptionsHashTable(drView("UNIQUE_AUDITING_COLUMNS").ToString)
        Dim rowParentSeqNum As Long = 0
        For Each col As DataColumn In row.Table.Columns
            If htCaptions.ContainsKey(col.ColumnName) AndAlso (type.ToString = "D" OrElse type.ToString = "I" OrElse Not row(col.ColumnName).ToString.Equals(row(col.ColumnName, DataRowVersion.Original).ToString)) Then
                Dim uniqueValueString As String = "", uniqueCaptionString As String = ""
                Dim rowSeqNum As Long = 0
                If rowParentSeqNum = 0 Then
                    rowParentSeqNum = CLng(OracleHelper.ExecuteScalar(transaction, CommandType.Text, "SELECT CLINICAL_AUDIT_SEQ.NEXTVAL from DUAL"))
                    rowSeqNum = rowParentSeqNum
                Else
                    rowSeqNum = CLng(OracleHelper.ExecuteScalar(transaction, CommandType.Text, "select CLINICAL_AUDIT_SEQ.NEXTVAL from DUAL"))
                End If
                Dim paramCollection(13) As OracleParameter
                Try
                    paramCollection(0) = New OracleParameter("0", rowSeqNum)
                    paramCollection(1) = New OracleParameter("1", row.Table.TableName)
                    paramCollection(2) = New OracleParameter("2", drView("AUDIT_TABLE_DISPLAY").ToString)
                    paramCollection(3) = New OracleParameter("3", col.ColumnName.ToString)

                    paramCollection(4) = New OracleParameter("4", htCaptions.Item(col.ColumnName).ToString)
                    If type = "I" Then
                        paramCollection(5) = New OracleParameter("5", "SEQ_NUM;" + row("SEQ_NUM").ToString)
                    Else
                        paramCollection(5) = New OracleParameter("5", "SEQ_NUM;" + row("SEQ_NUM", DataRowVersion.Original).ToString)
                    End If



                    paramCollection(6) = New OracleParameter("6", rowParentSeqNum)

                    Dim tempParam As OracleParameter
                    If type = "I" Then
                        tempParam = New OracleParameter("temp", row(col.ColumnName))
                    Else
                        tempParam = New OracleParameter("temp", row(col.ColumnName, DataRowVersion.Original))
                    End If


                    paramCollection(7) = New OracleParameter("7", tempParam.OracleDbType.ToString)
                    If type = "I" Then
                        paramCollection(8) = New OracleParameter("8", "")
                    Else
                        paramCollection(8) = New OracleParameter("8", row(col.ColumnName, DataRowVersion.Original).ToString)
                    End If

                    If type = "D" Then
                        paramCollection(9) = New OracleParameter("9", row(col.ColumnName, DataRowVersion.Original).ToString)
                    Else
                        paramCollection(9) = New OracleParameter("9", row(col.ColumnName).ToString)
                    End If

                    paramCollection(10) = New OracleParameter("10", type.ToString)

                    For Each key As String In htInfo.Keys
                        If row.Table.Columns.Contains(key.ToString.Trim) Then
                            If type = "D" Then
                                'If row(key.ToString.Trim, DataRowVersion.Original).ToString <> "" Then
                                uniqueValueString += row(key.ToString.Trim, DataRowVersion.Original).ToString & ", "
                                'End If
                            Else
                                'If row(key.ToString.Trim).ToString <> "" Then
                                uniqueValueString += row(key.ToString.Trim).ToString & ", "
                                'End If
                            End If
                            uniqueCaptionString += htInfo(key).ToString & ", "
                        End If
                    Next
                    If uniqueCaptionString.Trim(","c).Trim <> "" Then
                        paramCollection(11) = New OracleParameter("11", uniqueCaptionString.Trim(","c).Trim)
                    Else
                        paramCollection(11) = New OracleParameter("11", DBNull.Value)
                    End If
                    If uniqueValueString.Trim(","c).Trim <> "" Then
                        paramCollection(12) = New OracleParameter("12", uniqueValueString.Trim(","c).Trim)
                    Else
                        paramCollection(12) = New OracleParameter("12", DBNull.Value)
                    End If
                    paramCollection(13) = New OracleParameter("13", userip.DbVal())

                    For Each parm As OracleParameter In paramCollection
                        If TypeOf (parm.Value) Is String Then
                            Dim strParam As String = CType(parm.Value, String)
                            parm.Size = strParam.Length
                        End If
                    Next


                    Dim strQuery As String = ""
                    strQuery = "INSERT INTO " + drView("AUDIT_TABLE").ToString + " ( SEQ_NUM, DATA_TABLE_NAME, AUDIT_TABLE_NAME, DATA_COL_NAME,DISPLAY_COL_NAME, PRIMARY_KEY, ROW_SEQ_NUM, COL_TYPE, ORG_VALUE, CURRENT_VALUE, AUDIT_ACTION,UNIQUE_NAME,UNIQUE_VALUE,MACHINE) VALUES ( " +
                                                        ":0 , :1 , :2 , :3 , :4, :5, :6, :7, :8 , :9 , :10, :11 , :12,:13)"

                    Dim onb As Object = OracleHelper.ExecuteScalar(transaction, CommandType.Text, strQuery, paramCollection)
                Finally
                    Disposer.Dispose(paramCollection)
                End Try
            End If
        Next
    End Sub

    Public Shared Function UpdateDataSet(ByVal transaction As OracleTransaction, ByVal dataTable As DataTable, ByVal drView As DataRow, ByVal clientId As String, ByVal UserIp As String) As DataTable
        Dim da As New OracleDataAdapter
        Dim timer As System.Timers.Timer = Nothing
        Dim tmp As Model.DbServer = transaction.RetrieveDbSvr()
        Try
            If transaction Is Nothing Then
                Throw New ArgumentException([Error].REQUIRED_VALUE & "Transaction")
            End If

            Dim strSelectInsertQuery As String
            Dim strSelectUpdateQuery As String
            Dim strSelectDeleteQuery As String
            Dim sequencer As String

            If TypeOf (drView("SELECT_INSERT")) Is DBNull Then
                strSelectInsertQuery = Nothing
            Else
                strSelectInsertQuery = CStr(drView("SELECT_INSERT"))
            End If

            If TypeOf (drView("SELECT_UPDATE")) Is DBNull Then
                strSelectUpdateQuery = Nothing
            Else
                strSelectUpdateQuery = CStr(drView("SELECT_UPDATE"))
            End If

            If TypeOf (drView("SELECT_DELETE")) Is DBNull Then
                strSelectDeleteQuery = Nothing
            Else
                strSelectDeleteQuery = CStr(drView("SELECT_DELETE"))
            End If

            If TypeOf (drView("SEQUENCE_TAG")) Is DBNull Then
                sequencer = Nothing
            Else
                sequencer = CStr(drView("SEQUENCE_TAG"))
            End If

            Dim added, updated, deleted As Boolean

            If sequencer Is Nothing Then
                sequencer = "PRIMARY_SEQ"
            End If
            For Each row As DataRow In dataTable.Rows
                If row.RowState = DataRowState.Added Then
                    added = True
                    If row.Table.Columns.Contains("SEQ_NUM") AndAlso ((row.Item("SEQ_NUM") Is Nothing OrElse TypeOf (row.Item("SEQ_NUM")) Is System.DBNull) OrElse "".Equals(CType(row.Item("SEQ_NUM"), String))) Then
                        Dim sequenceNum As Long = CType(OracleHelper.ExecuteScalar(transaction, CommandType.Text, "select " & sequencer & ".NEXTVAL from DUAL"), Long)
                        If Not TypeOf (drView("PROFILE_TYPE_ID")) Is DBNull Then
                            Dim newSeq As String = CStr(sequenceNum)
                            newSeq += CStr(drView("PROFILE_TYPE_ID")) + clientId
                            sequenceNum = CLng(newSeq)
                        End If
                        row.Item("SEQ_NUM") = sequenceNum
                    End If

                    If drView("AUDIT_OPTIONS").ToString().IndexOf("I") <> -1 Then
                        AuditRow(transaction, drView, row, "I", UserIp)
                    End If
                    'drView.Row("NO_OF_INSERTS") = ConvertToDecimal(drView.Row("NO_OF_INSERTS")) + 1
                ElseIf row.RowState = DataRowState.Modified Then
                    updated = True

                    If drView("AUDIT_OPTIONS").ToString().IndexOf("U") <> -1 Then
                        AuditRow(transaction, drView, row, "U", UserIp)
                    End If
                    'drView.Row("NO_OF_UPDATES") = ConvertToDecimal(drView.Row("NO_OF_UPDATES")) + 1
                ElseIf row.RowState = DataRowState.Deleted Then
                    deleted = True
                    If drView("AUDIT_OPTIONS").ToString().IndexOf("D") <> -1 Then
                        AuditRow(transaction, drView, row, "D", UserIp)
                    End If
                    'drView.Row("NO_OF_DELETES") = ConvertToDecimal(drView.Row("NO_OF_DELETES")) + 1
                End If
            Next
            If Not (strSelectInsertQuery Is Nothing OrElse strSelectInsertQuery = "") AndAlso added Then
                Dim command As New OracleCommand(strSelectInsertQuery, transaction.Connection)
                da.InsertCommand = New SequelOracleCommandBuilder(New OracleDataAdapter(command)).GetInsertCommand
            End If
            If Not (strSelectUpdateQuery Is Nothing OrElse strSelectUpdateQuery = "") AndAlso updated Then
                'The below condition works in case of only one row in table to check that only change column will work
                If dataTable.Rows.Count = 1 AndAlso Not String.IsNullOrEmpty(drView("UNIQUE_COLUMN").ToString) Then
                    strSelectUpdateQuery = ModifyUpdateQueryString(dataTable, strSelectUpdateQuery)
                    If String.IsNullOrEmpty(strSelectUpdateQuery) Then
                        Logger.Instance(Constant.LG_DA).Notice("DataAdapter.Update will not be called as changes not detected in DataTable", tag:=tmp.Id, data:=Function() SM.Join(dataTable.Dump(), drView.Dump(), SM.Fmt("Connection", transaction.Dump())))
                        Return dataTable
                    End If
                End If
                '
                Dim command As New OracleCommand(strSelectUpdateQuery, transaction.Connection)
                da.UpdateCommand = New SequelOracleCommandBuilder(New OracleDataAdapter(command), drView("Unique_Column").ToString).GetUpdateCommand
            End If
            If Not (strSelectDeleteQuery Is Nothing OrElse strSelectDeleteQuery = "") AndAlso deleted Then
                Dim command As New OracleCommand(strSelectDeleteQuery, transaction.Connection)
                da.DeleteCommand = New SequelOracleCommandBuilder(New OracleDataAdapter(command), drView("Unique_Column").ToString).GetDeleteCommand
            End If

            Dim seqNums As New List(Of String)
            If (deleted OrElse updated OrElse added) Then
                If SequelSql.TrackUpdate() Then
                    SM.Sandbox(Sub()
                                   Dim filtered As DataRow() = drView.Table.Select("CACHE_FLAG='Y' and DML_TABLE='" & drView.Item("DML_TABLE").ToString & "'")
                                   If filtered.Length > 0 Then
                                       SyncLock SequelSql.Lock
                                           For Each row As DataRow In filtered
                                               row("LAST_UPDATE_DATE") = DateTime.Now
                                               seqNums.Add(row.Item("SEQ_NUM").ToString)
                                           Next
                                           drView.Table.AcceptChanges()
                                       End SyncLock
                                   End If
                               End Sub)
                End If
            End If

            'Time Tracking
            Dim timeTaken As Double = 0
            Dim handler As Action(Of System.Timers.Timer) = Sub(x)
                                                                timeTaken += x.Interval
                                                                Logger.Instance(Constant.LG_DA_DL).Alert(DbEx.NotifyExhaustive(Database.DB_WAIT_INTERVAL, isDml:=True), tag:=tmp.Id, data:=Function() SM.Join(da.Dump(), drView.Dump(), SM.Fmt("Connection", transaction.Dump())), timeTaken:=timeTaken)
                                                            End Sub
            timer = TimerEx.Timer(Database.DB_WAIT_INTERVAL, handler)

            da.Update(dataTable)
            TagDmlUpdateDate(transaction, seqNums)

            Return dataTable
        Catch ex As Exception
            Logger.Instance(Constant.LG_DA).Ex(SM.GeneralError, ex, tag:=tmp.Id, client:=clientId, data:=Function() SM.Join(da.Dump(), drView.Dump(), SM.Fmt("Connection", transaction.Dump())))
            If ex.Message.IndexOf("ORA-12500") > -1 Then
                Database.ClearPools()
            End If
            If Not dataTable.TableName.Equals("CLINICAL_VISIT_CHIEFCOMPLAINT") Then
                Throw
            Else
                Return Nothing
            End If
        Finally
            TimerEx.Dispose(timer)
            Disposer.Dispose(da)
        End Try
    End Function

    Public Shared Function UpdateDataSet(ByVal transaction As OracleTransaction, ByVal dataTable As DataTable, ByVal drView As DataRow) As DataTable
        Return UpdateDataSet(transaction, dataTable, drView, "", "")
    End Function

    Private Shared Function ModifyUpdateQueryString(ByVal dt As DataTable, ByVal strSelectUpdateQuery As String) As String
        If dt.Rows(0).HasVersion(DataRowVersion.Original) AndAlso dt.Rows(0).HasVersion(DataRowVersion.Current) Then
            strSelectUpdateQuery = strSelectUpdateQuery.Trim.ToUpper.Substring(6)
            Dim lastIndex As Integer = strSelectUpdateQuery.LastIndexOf(" FROM ")
            Dim strSelect As String = strSelectUpdateQuery.Substring(lastIndex)
            strSelectUpdateQuery = strSelectUpdateQuery.Substring(0, lastIndex)

            Dim strs As String() = strSelectUpdateQuery.Split(","c)
            For i As Integer = 0 To strs.Length - 1
                strs(i) = strs(i).Trim
            Next

            Dim strLst As New Generic.List(Of String)
            strLst.AddRange(strs)
            For Each col As DataColumn In dt.Columns
                If dt.Rows(0).Item(col.ColumnName, DataRowVersion.Current).Equals(dt.Rows(0).Item(col.ColumnName, DataRowVersion.Original)) Then
                    strLst.Remove(col.ColumnName.ToString)
                End If
            Next
            If strLst.Count > 0 Then
                strSelectUpdateQuery = String.Join(", ", strLst.ToArray)
            Else
                Return Nothing
            End If
            strSelectUpdateQuery = "SELECT " + strSelectUpdateQuery + strSelect
        Else
            Return Nothing
        End If
        Return strSelectUpdateQuery
    End Function


    Public Shared Function UpdateDataSet(ByVal transaction As OracleTransaction, ByVal dataTable As DataTable, ByVal strQuery As String, ByVal query_type As QueryType, ByVal sequencer As String, ByVal UniqueColumn As String) As DataTable
        Dim da As OracleDataAdapter = Nothing
        Dim timer As System.Timers.Timer = Nothing
        Dim tmp As Model.DbServer = transaction.RetrieveDbSvr()
        Try
            If transaction Is Nothing Then
                Throw New ArgumentException([Error].REQUIRED_VALUE & "Transaction")
            End If

            da = New OracleDataAdapter(strQuery, transaction.Connection)

            If query_type = QueryType.Update Then
                Dim command As New OracleCommand(strQuery, transaction.Connection)
                'command.Transaction = transaction
                da.UpdateCommand = New SequelOracleCommandBuilder(New OracleDataAdapter(command), UniqueColumn).GetUpdateCommand
            ElseIf query_type = QueryType.Delete Then
                Dim command As New OracleCommand(strQuery, transaction.Connection)
                'command.Transaction = transaction
                da.DeleteCommand = New SequelOracleCommandBuilder(New OracleDataAdapter(command), UniqueColumn).GetDeleteCommand
            ElseIf query_type = QueryType.Insert Then
                Dim command As New OracleCommand(strQuery, transaction.Connection)
                ' command.Transaction = transaction
                da.InsertCommand = New SequelOracleCommandBuilder(New OracleDataAdapter(command)).GetInsertCommand
                If sequencer Is Nothing Then
                    sequencer = "PRIMARY_SEQ"
                End If
                For Each row As DataRow In dataTable.Rows
                    Dim sequenceNum As Integer = CInt(OracleHelper.ExecuteScalar(transaction, CommandType.Text, "select " & sequencer & ".NEXTVAL from DUAL"))
                    row.Item("SEQ_NUM") = sequenceNum
                Next
            End If

            'Time Tracking
            Dim timeTaken As Double = 0
            Dim handler As Action(Of System.Timers.Timer) = Sub(x)
                                                                timeTaken += x.Interval
                                                                Logger.Instance(Constant.LG_DA_DL).Alert(DbEx.NotifyExhaustive(Database.DB_WAIT_INTERVAL, isDml:=True), tag:=tmp.Id, data:=Function() SM.Join(SM.Fmt("Query", strQuery), SM.Fmt("Type", query_type.CStr()), SM.Fmt("Sequence", sequencer), SM.Fmt("Unique", UniqueColumn), da.Dump(), SM.Fmt("Connection", transaction.Dump())), timeTaken:=timeTaken)
                                                            End Sub
            timer = TimerEx.Timer(Database.DB_WAIT_INTERVAL, handler)

            da.Update(dataTable)

        Catch ex As Exception
            Logger.Instance(Constant.LG_DA).Ex(SM.GeneralError, ex, tag:=tmp.Id, data:=Function() SM.Join(SM.Fmt("Query", strQuery), SM.Fmt("Type", query_type.CStr()), SM.Fmt("Sequence", sequencer), SM.Fmt("Unique", UniqueColumn), da.Dump(), SM.Fmt("Connection", transaction.Dump())))
            If ex.Message.IndexOf("ORA-12500") > -1 Then
                Database.ClearPools()
            End If
            Throw
        Finally
            TimerEx.Dispose(timer)
            Disposer.Dispose(da)
        End Try
        Return dataTable
    End Function

    Public Shared Function GetPKParameterNumber(ByVal strQuery As String, ByVal pcount As Integer) As String
        Dim count As Integer
        'Dim ret As String
        count = 0
        Dim i As Integer
        i = strQuery.IndexOf("SEQ_NUM")
        While Not strQuery.Chars(i) = ")"
            If strQuery.Chars(i) = "," Then
                count += 1
            End If
            i += 1
        End While

        Return ":p" + (pcount - count).ToString
    End Function

    Enum ParamterType
        Input
        Output
        InputOutput
    End Enum

    Public Class NameValuePair
        Implements ICloneable

        Dim _name As String = String.Empty
        Dim _value As Object = Nothing
        Dim _type As ParamterType

        Public Property ColumnName() As String
            Get
                Return _name
            End Get
            Set(ByVal Value As String)
                _name = Value
            End Set
        End Property

        Public Property ColumnValue() As Object
            Get
                Return _value
            End Get
            Set(ByVal Value As Object)
                _value = Value
            End Set
        End Property

        Public Property Type() As ParamterType
            Get
                Return _type
            End Get
            Set(ByVal Value As ParamterType)
                _type = Value
            End Set
        End Property

        Public Function Clone() As Object Implements System.ICloneable.Clone
            'Return Me.Clone()
            Return Me
        End Function
    End Class

    Public Shared Function ExecuteProcedureOut(ByVal connection As OracleConnection, ByVal ProcedureName As String, ByVal CommandParams As List(Of NameValuePair)
                                                ) As Dictionary(Of String, Object)

        If connection.NeedToConnect() Then
            Throw New ArgumentException([Error].REQUIRED_VALUE & "Connection")
        End If

        Dim cmd As New OracleCommand
        Dim timer As System.Timers.Timer = Nothing
        Dim tmp As Model.DbServer = connection.RetrieveDbSvr()
        Try
            cmd.Connection = connection
            cmd.CommandText = ProcedureName
            cmd.CommandType = CommandType.StoredProcedure
            If CommandParams IsNot Nothing AndAlso CommandParams.Count > 0 Then
                For Each param As NameValuePair In CommandParams
                    If Not String.IsNullOrEmpty(param.ColumnName) Then
                        Dim p As New OracleParameter
                        p.ParameterName = param.ColumnName
                        p.Value = param.ColumnValue
                        If param.Type = ParamterType.Input Then
                            p.Direction = ParameterDirection.Input
                        ElseIf param.Type = ParamterType.InputOutput Then
                            p.Direction = ParameterDirection.InputOutput
                        ElseIf param.Type = ParamterType.Output Then
                            p.Direction = ParameterDirection.Output
                        End If
                        cmd.Parameters.Add(p)
                    End If
                Next
            End If

            'Time Tracking
            Dim timeTaken As Double = 0
            Dim handler As Action(Of System.Timers.Timer) = Sub(x)
                                                                timeTaken += x.Interval
                                                                Logger.Instance(Constant.LG_DA_DL).Alert(DbEx.NotifyExhaustive(Database.DB_WAIT_INTERVAL, isDml:=True), tag:=tmp.Id, data:=Function() SM.Join(cmd.Dump(), SM.Fmt("Connection", connection.Dump())), timeTaken:=timeTaken)
                                                            End Sub
            timer = TimerEx.Timer(Database.DB_WAIT_INTERVAL, handler)

            Dim i As Integer = cmd.ExecuteNonQuery()
            Dim result As New Dictionary(Of String, Object)

            For Each par As OracleParameter In cmd.Parameters
                If par.Direction <> ParameterDirection.Input Then
                    'result(par.ParameterName.ToUpper) = par.Value
                    result.Add(par.ParameterName.ToUpper, par.Value)
                End If
            Next

            Return result

        Catch ex As Exception
            Logger.Instance(Constant.LG_DA).Ex(SM.GeneralError, ex, tag:=tmp.Id, data:=Function() SM.Join(cmd.Dump(), SM.Fmt("Connection", connection.Dump())))
            Throw
        Finally
            TimerEx.Dispose(timer)
            Disposer.Dispose(cmd)
        End Try
    End Function

    Public Shared Sub TagDmlUpdateDate(ByVal transaction As OracleTransaction, ByVal seqNums As List(Of String))
        If Not seqNums.Empty Then
            SM.Sandbox(Sub()
                           transaction.Update("UPDATE " & SequelSql.TblOfSql() & " SET LAST_UPDATE_DATE = SYSDATE WHERE SEQ_NUM IN (:SEQ_NUM)", KVP:=SM.KV("SEQ_NUM", seqNums.ToArray()), timeout:=SequelSql.TrackUpdateTimeout())
                       End Sub)
        End If
    End Sub

    Public Shared Sub UpdateQueryTime(ByVal startDateTime As DateTime, ByVal endDateTime As DateTime, ByVal row As DataRow, ByVal objOracleTransaction As OracleTransaction, ByVal mDbQueries As DatabaseQueries, ByVal mId As String)
        If SequelSql.TrackSelect() Then
            SM.Sandbox(Sub()
                           Dim timeSpane As TimeSpan = endDateTime.Subtract(startDateTime)
                           Dim currentTime As Double = Math.Round(timeSpane.TotalMilliseconds, 2)
                           Dim seqNum As String = Nothing
                           SyncLock SequelSql.Lock
                               If String.IsNullOrEmpty(row.Item("SELECT_ONLY_TIME").ToString) OrElse CDbl(row.Item("SELECT_ONLY_TIME")) < currentTime Then
                                   row("SELECT_ONLY_TIME") = currentTime
                                   mDbQueries.Data.AcceptChanges()
                                   seqNum = row.Item("SEQ_NUM").ToString
                               End If
                           End SyncLock
                           If Not String.IsNullOrEmpty(seqNum) Then
                               objOracleTransaction.Update("UPDATE " & SequelSql.TblOfSql() & " SET SELECT_ONLY_TIME = :SELECT_ONLY_TIME WHERE SEQ_NUM = :SEQ_NUM", KVP:=SM.KV("SEQ_NUM", seqNum, "SELECT_ONLY_TIME", currentTime), timeout:=SequelSql.TrackSelectTimeout(), Id:=mId)
                           End If
                       End Sub)
        End If
    End Sub

End Class

Public NotInheritable Class OracleHelperParameterCache

    Private Sub New()
    End Sub
    Private Shared paramCache As Hashtable = Hashtable.Synchronized(New Hashtable)

    Private Shared Function DiscoverSpParameterSet(ByVal connectionString As String, ByVal spName As String, ByVal includeReturnValueParameter As Boolean) As OracleParameter()
        ' Using 
        Dim cn As OracleConnection = New OracleConnection(connectionString)
        Dim cmd As OracleCommand = Nothing

        Try
            cn.Open()
            cmd = New OracleCommand(spName, cn)
            cmd.CommandType = CommandType.StoredProcedure
            cmd.Prepare()
            'OracleCommandBuilder.DeriveParameters(cmd)
            If Not includeReturnValueParameter Then
                If ParameterDirection.ReturnValue = cmd.Parameters(0).Direction Then
                    cmd.Parameters.RemoveAt(0)
                End If
            End If
            Dim discoveredParameters(cmd.Parameters.Count) As OracleParameter
            cmd.Parameters.CopyTo(discoveredParameters, 0)

            Return discoveredParameters

        Finally
            Disposer.Dispose(cmd)
            Disposer.Dispose(cn)
        End Try
    End Function

    Private Shared Function CloneParameters(ByVal originalParameters As OracleParameter()) As OracleParameter()
        Dim clonedParameters(originalParameters.Length) As OracleParameter
        Dim i As Integer = 0
        Dim j As Integer = originalParameters.Length
        While i < j
            clonedParameters(i) = CType(CType(originalParameters(i), ICloneable).Clone, OracleParameter)
            System.Math.Min(System.Threading.Interlocked.Increment(i), i - 1)
        End While
        Return clonedParameters
    End Function

    Public Shared Sub CacheParameterSet(ByVal connectionString As String, ByVal commandText As String, ByVal ParamArray commandParameters As OracleParameter())
        Dim hashKey As String = connectionString + ":" + commandText
        paramCache(hashKey) = commandParameters
    End Sub

    Public Shared Function GetCachedParameterSet(ByVal connectionString As String, ByVal commandText As String) As OracleParameter()
        Dim hashKey As String = connectionString + ":" + commandText
        Dim cachedParameters As OracleParameter() = CType(paramCache(hashKey), OracleParameter())
        If cachedParameters Is Nothing Then
            Return Nothing
        Else
            Return CloneParameters(cachedParameters)
        End If
    End Function

    Public Shared Function GetSpParameterSet(ByVal connectionString As String, ByVal spName As String) As OracleParameter()
        Return GetSpParameterSet(connectionString, spName, False)
    End Function

    Public Shared Function GetSpParameterSet(ByVal connectionString As String, ByVal spName As String, ByVal includeReturnValueParameter As Boolean) As OracleParameter()
        Dim str As String
        If includeReturnValueParameter Then
            str = ":include ReturnValue Parameter"
        Else
            str = ""
        End If


        Dim hashKey As String = connectionString + ":" + spName + str
        Dim cachedParameters As OracleParameter()
        cachedParameters = CType(paramCache(hashKey), OracleParameter())
        If cachedParameters Is Nothing Then
            ' Check this line
            paramCache(hashKey) = DiscoverSpParameterSet(connectionString, spName, includeReturnValueParameter)
            cachedParameters = CType(paramCache(hashKey), OracleParameter())
        End If
        Return CloneParameters(cachedParameters)
    End Function

End Class



