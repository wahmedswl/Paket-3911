#If ManagedOdp Then
Imports Oracle.ManagedDataAccess.Client
#Else
Imports Oracle.DataAccess.Client
#End If

Imports System.Data

Public Class SequelOracleCommandBuilder
    Private _cb As OracleCommandBuilder
    Private _uniqueColumns As String
    Public Sub New(ByVal da As OracleDataAdapter)
        _cb = New OracleCommandBuilder(da)
    End Sub
    Public Sub New(ByVal da As OracleDataAdapter, ByVal uniqueColumns As String)
        _cb = New OracleCommandBuilder(da)
        Me._uniqueColumns = uniqueColumns
    End Sub
    Public Function GetDeleteCommand() As OracleCommand
        If String.IsNullOrEmpty(Me._uniqueColumns) Then
            Return _cb.GetDeleteCommand
        Else
            Return GetDeleteCommand(_cb.DataAdapter.SelectCommand.CommandText, Me._uniqueColumns, _cb.DataAdapter.SelectCommand.Connection)
        End If
    End Function
    Public Function GetInsertCommand() As OracleCommand
        Return _cb.GetInsertCommand
    End Function
    Public Function GetUpdateCommand() As OracleCommand
        If String.IsNullOrEmpty(Me._uniqueColumns) Then
            Return _cb.GetUpdateCommand
        Else
            Return GetUpdateCommand(_cb.DataAdapter.SelectCommand.CommandText, Me._uniqueColumns, _cb.DataAdapter.SelectCommand.Connection)
        End If
    End Function
    Private Function GetUpdateCommand(ByVal strSelectCommand As String, ByVal strUniqueColumns As String, ByVal con As OracleConnection) As OracleCommand
        Dim Table As String = strSelectCommand.ToUpper.Substring(strSelectCommand.ToUpper.LastIndexOf(" FROM ")).Replace(" FROM ", "").Replace(" ", "")
        strSelectCommand = strSelectCommand.ToUpper.Remove(strSelectCommand.ToUpper.LastIndexOf(" FROM "))
        Dim columnString As String = strSelectCommand.ToUpper.Replace("SELECT ", "")
        Dim columns As String() = columnString.Split(","c)
        Dim intCOunt As Integer = 0
        Dim cmd As New OracleCommand
        For Each strColumn As String In columns
            Dim param As OracleParameter = GetParam(columns, intCOunt, strColumn, intCOunt)
            cmd.Parameters.Add(param)
            intCOunt += 1
        Next
        Dim uniqueColumns As String() = strUniqueColumns.Split(","c)
        Dim whereIndex As Integer = 0
        For Each whereColumn As String In uniqueColumns
            Dim params As OracleParameter() = GetWhereParam(uniqueColumns, intCOunt, whereColumn, whereIndex, DataRowVersion.Original)
            For Each param As OracleParameter In params
                cmd.Parameters.Add(param)
            Next
            intCOunt += 1
            whereIndex += 1
        Next
        cmd.CommandText = "Update " & Table & " SET " & String.Join(" , ", columns) & " Where " & String.Join(" AND ", uniqueColumns)
        cmd.Connection = con
        Return cmd
    End Function
    Private Function GetDeleteCommand(ByVal strSelectCommand As String, ByVal strUniqueColumns As String, ByVal con As OracleConnection) As OracleCommand
        Dim tableName As String = strSelectCommand.ToUpper.Substring(strSelectCommand.ToUpper.LastIndexOf(" FROM ")).Replace(" FROM ", "").Replace(" ", "")
        Dim intCount As Integer = 0
        Dim cmd As New OracleCommand
        Dim uniqueColumns As String() = strUniqueColumns.Split(","c)
        For Each whereColumn As String In uniqueColumns
            Dim params As OracleParameter() = GetWhereParam(uniqueColumns, intCount, whereColumn, intCount, DataRowVersion.Original)
            For Each param As OracleParameter In params
                cmd.Parameters.Add(param)
            Next
            intCount += 1
        Next
        cmd.CommandText = "DELETE FROM " & tableName & " Where " & String.Join(" AND ", uniqueColumns)
        cmd.Connection = con
        Return cmd
    End Function
    Private Function GetParam(ByVal columns As String(), ByVal paramIndex As Integer, ByRef strColumn As String, ByVal intCount As Integer, Optional ByVal vrsn As DataRowVersion = DataRowVersion.Default) As OracleParameter
        strColumn = strColumn.Trim.TrimStart(" "c)
        columns(intCount) = strColumn & " = :" & paramIndex.ToString
        Dim param As New OracleParameter
        param.ParameterName = ":" & paramIndex.ToString
        param.SourceColumn = strColumn
        param.SourceVersion = vrsn
        Return param
    End Function
    Private Function GetWhereParam(ByVal columns As String(), ByVal paramIndex As Integer, ByRef strColumn As String, ByVal intCount As Integer, Optional ByVal vrsn As DataRowVersion = DataRowVersion.Default) As OracleParameter()
        strColumn = strColumn.Trim.TrimStart(" "c)
        columns(intCount) = "( " & strColumn & " = :" & paramIndex.ToString & " or ( " & strColumn & " is null and :" & (1000 + paramIndex).ToString & " is null ) ) "
        Dim param As New OracleParameter
        param.ParameterName = ":" & paramIndex.ToString
        param.SourceColumn = strColumn
        param.SourceVersion = vrsn
        Dim param2 As New OracleParameter
        param2.ParameterName = ":" & (1000 + paramIndex).ToString
        param2.SourceColumn = strColumn
        param2.SourceVersion = vrsn
        Return New OracleParameter() {param, param2}
    End Function
End Class
