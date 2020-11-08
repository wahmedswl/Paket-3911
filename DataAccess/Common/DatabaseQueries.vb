#If ManagedOdp Then
Imports Oracle.ManagedDataAccess.Client
#Else
Imports Oracle.DataAccess.Client
#End If

Imports System.Data
Imports System.Runtime.CompilerServices
Imports SequelMed.Core
Imports SequelMed.Core.DB
Imports SequelMed.Core.Model

Public Class DatabaseQueries

    Private ds As DataSet
    Public Table As DataTable

    Public Sub New()
        MyBase.New()
    End Sub

    Public Property Data() As DataSet
        Set(ByVal Value As DataSet)
            ds = Value
            If ds.Tbl(needSchema:=True) IsNot Nothing Then
                Table = ds.Tables(0)
            End If
        End Set
        Get
            Return ds
        End Get
    End Property

    Public Shared Function Instance(ByVal data As DataTable) As DatabaseQueries
        Dim result As New DatabaseQueries With {
            .Data = SM.DataSet(data)
        }

        Return result
    End Function

End Class

Public Module DbQueriesEx

    <Extension()>
    Public Function Query(ByVal this As DatabaseQueries, ByVal tagName As String, Optional ByVal dbName As String = Nothing, Optional ByVal dbSvr As Model.DbServer = Nothing, Optional ByVal fnConnection As Func(Of OracleConnection) = Nothing, Optional ByVal connection As OracleConnection = Nothing, Optional ByVal transaction As OracleTransaction = Nothing) As DataRow
        If this Is Nothing OrElse this.Table Is Nothing Then
            Throw New ArgumentException([Error].REQUIRED_VALUE & "DatabaseQueries")
        End If

        Dim dbSource As New Context.DbSource(dbSvr:=dbSvr, fnConnection:=fnConnection, transaction:=transaction, connection:=connection)
        Return SequelSql.Filter(this.Table, tagName, dbSource:=dbSource, dbName:=dbName)
    End Function

End Module


