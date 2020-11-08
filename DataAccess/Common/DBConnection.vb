#If ManagedOdp Then
Imports Oracle.ManagedDataAccess.Client
#Else
Imports Oracle.DataAccess.Client
#End If

Imports System.Runtime.CompilerServices
Imports SequelMed.Core
Imports SequelMed.Core.DB
Imports SequelMed.Core.Pattern

Public Class DBConnection
    Inherits AbstractDisposable

#Region "Member Declaration"

    Private mstrDbUserName As String
    Private mstrDbPassword As String
    Private mstrDbServer As String

    Private oConnection As OracleConnection

    Public ContextUser As String
    Public TimeZone As String
    Public Id As String

#End Region

#Region "Class Properties let Gets"

    Public Property DbUsername() As String
        Get
            Return mstrDbUserName
        End Get
        Set(ByVal Value As String)
            mstrDbUserName = Value
        End Set
    End Property

    Public Property DbPassword() As String
        Get
            Return mstrDbPassword
        End Get
        Set(ByVal Value As String)
            mstrDbPassword = Value
        End Set
    End Property

    Public Property DbServer() As String
        Get
            Return mstrDbServer
        End Get
        Set(ByVal Value As String)
            mstrDbServer = Value
        End Set
    End Property

    Public ReadOnly Property DBConnection() As OracleConnection
        Get
            If oConnection.NeedToConnect() Then
                Return Nothing
            End If

            Return oConnection
        End Get
    End Property

#End Region

#Region "Functions"

    Public Function BuildConnection(Optional ByVal validateWith As String = Nothing, <CallerFilePath> ByVal Optional callerFilePath As String = "", <CallerMemberName> ByVal Optional callerMemberName As String = "", <CallerLineNumber> ByVal Optional callerLineNumber As Integer = 0) As Boolean
        Try
            oConnection = Database.Connection(mstrDbServer, mstrDbUserName, mstrDbPassword, decodePwd:=False, contextUser:=ContextUser, timeZone:=TimeZone, Id:=Id, validateWith:=validateWith, callerFilePath:=callerFilePath, callerMemberName:=callerMemberName, callerLineNumber:=callerLineNumber)

            Return Not oConnection.NeedToConnect
        Catch exp As Exception
            'Muhammad Tayyab Sheikh - June 15, 2011 - Added the check for nothing before disposing the connection object
            Trace.WriteLine(exp, exp.Message)
            '@Inam :  28/01/2013 - Data Access Logging
            Logger.Instance(Constant.LG_DA).Ex(SM.GeneralError, exp, tag:=Id, data:=Function() SM.DB(mstrDbServer, mstrDbUserName, mstrDbPassword), caller:=SM.Caller(callerFilePath:=callerFilePath, callerMemberName:=callerMemberName, callerLineNumber:=callerLineNumber))
            Disposer.Dispose(oConnection)
            Throw
        End Try

        Return False
    End Function

#End Region

    Protected Overrides Sub OnDispose(disposing As Boolean)
        Disposer.Dispose(oConnection)
    End Sub

End Class
